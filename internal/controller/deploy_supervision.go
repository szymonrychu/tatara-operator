package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

const (
	// deployPollRequeue paces the pod-less Deploying-phase cascade poll.
	deployPollRequeue = 60 * time.Second
	// helmfileRepoName is the terminal CD repo every component cascade ends at: a
	// successful apply.yaml run there is the authoritative cluster-applied signal.
	helmfileRepoName = "tatara-helmfile"
	// applyWorkflowFile is the tatara-helmfile push-to-main apply workflow.
	applyWorkflowFile = "apply.yaml"
	// deployStalledFactor multiplies the per-artifact budget to set the cdScan
	// backstop threshold (1.5x budget): fire only after the deadline + a recovery
	// attempt have lapsed.
	deployStalledFactor = 1.5
)

// deployPinFiles are the tatara-helmfile files where component version pins land
// (the cd-release `bump` targets, parentMap). deploy-supervision reads them at a
// successful apply commit to confirm a Task's published version was applied. This
// is the one place the operator (the terminal watcher) couples to helmfile
// layout; keep it in lockstep with tatara-helmfile's pin locations.
var deployPinFiles = []string{
	"helmfile.yaml.gotmpl",
	"values/tatara-operator/common.yaml",
	"values/tatara-operator/default.yaml",
	"values/project-tatara/common.yaml",
	"values/project-infrastructure/common.yaml",
}

// pushCDEligible reports whether a merged change rides the push-CD cascade: the
// agent declared a change significance (the lever that cut the semver tag). A
// change with no declared significance keeps the legacy close+Done path.
func pushCDEligible(task *tatarav1alpha1.Task) bool {
	cs := task.Status.ChangeSummary
	return cs != nil && cs.Significance != ""
}

// isMultiHopRepo reports whether a component repo reaches tatara-helmfile through
// an intermediate parent rebuild (cli + skills cascade through the wrapper, two
// tag-cut hops). Everything else is one hop from tatara-helmfile.
func isMultiHopRepo(repoName string) bool {
	switch repoName {
	case "tatara-cli", "tatara-agent-skills":
		return true
	}
	return false
}

// deployBudget returns the Deploying deadline budget for a component repo: the
// 1.2x-worst-case multi-hop budget for cli/skills, the tighter single-hop budget
// otherwise. Falls back to the spec defaults (3300 / 2100) when unset.
func deployBudget(proj *tatarav1alpha1.Project, repoName string) time.Duration {
	if isMultiHopRepo(repoName) {
		s := proj.Spec.DeployBudgetSeconds
		if s <= 0 {
			s = 3300
		}
		return time.Duration(s) * time.Second
	}
	s := proj.Spec.DeploySingleHopBudgetSeconds
	if s <= 0 {
		s = 2100
	}
	return time.Duration(s) * time.Second
}

// deployLedger constructs the per-namespace deploy ledger handle.
func (r *TaskReconciler) deployLedger(namespace string) *DeployLedger {
	return &DeployLedger{Client: r.Client, Namespace: namespace}
}

// enterDeploying transitions a just-merged push-CD lifecycle Task into the
// pod-less Deploying phase: it tears down the agent pod, stamps the deploy budget
// + cascade status, records the Task in the per-Project deploy ledger, and hands
// off to reconcileDeploying. The originating issue is NOT closed here - the
// deploy-supervision sweep closes it on a confirmed apply, with the deployed
// version (D9).
func (r *TaskReconciler) enterDeploying(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, repo *tatarav1alpha1.Repository, provider string) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	repoName := repo.Name
	if _, name, err := scm.OwnerRepo(repo.Spec.URL); err == nil {
		repoName = name
	}
	budget := deployBudget(project, repoName)
	deadline := metav1.NewTime(time.Now().Add(budget))

	// Tear down the agent pod: Deploying is pod-less and must release its lane.
	if err := r.deleteWrapper(ctx, task); err != nil {
		l.Error(err, "deploy: teardown wrapper on Deploying entry (non-fatal)", "resource_id", task.Name)
	}

	issueRef := ""
	if task.Spec.Source != nil {
		issueRef = task.Spec.Source.IssueRef
	}
	if err := r.deployLedger(task.Namespace).Add(ctx, project.Name, DeployLedgerEntry{
		Artifact:      repoName,
		SourceTaskRef: task.Name,
		IssueRef:      issueRef,
		HeadSHA:       task.Status.MergeCommitSHA,
		State:         DeployStateDeploying,
	}); err != nil {
		// Non-fatal: the ledger is a dedup optimisation; the Task's own status
		// fields still drive its supervision. Log and continue.
		l.Error(err, "deploy: add ledger entry on Deploying entry (non-fatal)", "resource_id", task.Name)
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.Phase = tatarav1alpha1.PhaseDeploying
		fresh.Status.LifecycleState = tatarav1alpha1.LifecycleStateDeploying
		fresh.Status.DeployDeadline = &deadline
		fresh.Status.CascadeStage = "tagged"
		fresh.Status.DeployArtifact = repoName
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("deploy: enter Deploying: %w", err)
	}
	task.Status.Phase = tatarav1alpha1.PhaseDeploying
	task.Status.LifecycleState = tatarav1alpha1.LifecycleStateDeploying
	task.Status.DeployDeadline = &deadline
	task.Status.DeployArtifact = repoName

	if r.LifecycleMetrics != nil {
		r.LifecycleMetrics.RecordTransition("MainCI", tatarav1alpha1.LifecycleStateDeploying)
	}
	l.Info("deploy: entering Deploying phase",
		"action", "deploy_enter", "resource_id", task.Name, "artifact", repoName,
		"budget_seconds", int(budget.Seconds()), "deadline", deadline.Format(time.RFC3339), "provider", provider)
	return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
}

// reconcileDeploying drives the pod-less push-CD cascade for one Deploying Task:
// it learns the cut version, polls the tatara-helmfile apply.yaml outcome, and on
// a confirmed apply resolves every converging Task in one sweep (D10). On the
// budget deadline or an apply failure it rerolls the change to fix the cascade
// (reusing the bounded-reroll machinery). No agent pod runs during this state.
func (r *TaskReconciler) reconcileDeploying(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	// Deadline guard: a cascade that has not applied within its budget rerolls.
	if dl := task.Status.DeployDeadline; dl != nil && time.Now().After(dl.Time) {
		l.Info("deploy: budget deadline exceeded; rerolling",
			"action", "deploy_timeout", "resource_id", task.Name, "deadline", dl.Format(time.RFC3339))
		return r.rerollDeploy(ctx, project, task, "deploy_timeout",
			"Deploy cascade did not reach a tatara-helmfile apply within its budget. The change is merged but undeployed: investigate the stalled cascade (component tag, parent bump PR, helmfile apply) and push a fix.")
	}

	if task.Spec.RepositoryRef == "" || r.ReaderFor == nil || r.SCMFor == nil {
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return ctrl.Result{}, fmt.Errorf("deploy: get repository: %w", err)
	}
	provider := "github"
	if task.Spec.Source != nil && task.Spec.Source.Provider != "" {
		provider = task.Spec.Source.Provider
	}
	token, err := r.scmToken(ctx, task.Namespace, project.Spec.ScmSecretRef)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("deploy: scm token: %w", err)
	}
	reader, err := r.ReaderFor(provider, token)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("deploy: reader: %w", err)
	}
	dw, ok := reader.(scm.DeployWatcher)
	if !ok {
		// The cd-release cascade is GitHub-only; a non-GitHub reader cannot supervise
		// the helmfile apply. Requeue and let the deadline backstop park if needed.
		l.Info("deploy: reader is not a DeployWatcher; cascade unsupervisable here (cascade is GitHub-only)",
			"action", "deploy_no_watcher", "resource_id", task.Name, "provider", provider)
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}

	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("deploy: parse repo url: %w", err)
	}

	// Learn the version the merged component repo cut (cd-release tag), if not yet
	// recorded. Until the tag exists the cascade has not started publishing.
	version := task.Status.DeployedVersion
	if version == "" {
		tag, found, terr := dw.LatestSemverTag(ctx, owner, name)
		if terr != nil {
			l.Error(terr, "deploy: read latest semver tag (requeue)", "resource_id", task.Name, "repo", name)
			return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
		}
		if !found {
			l.Info("deploy: component tag not cut yet; waiting",
				"action", "deploy_await_tag", "resource_id", task.Name, "repo", name)
			return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
		}
		version = tag
		artifact := name + "@" + tag
		if err := r.setDeployVersion(ctx, task, tag, artifact); err != nil {
			return ctrl.Result{}, err
		}
		_ = r.deployLedger(task.Namespace).Add(ctx, project.Name, DeployLedgerEntry{
			Artifact: name, Version: tag, SourceTaskRef: task.Name,
			IssueRef: issueRefOf(task), HeadSHA: task.Status.MergeCommitSHA, State: DeployStateDeploying,
		})
		l.Info("deploy: learned cut version", "action", "deploy_version", "resource_id", task.Name, "version", tag, "artifact", artifact)
	}

	// Resolve the terminal tatara-helmfile repo within the Project.
	hfOwner, hfRepo, hfFound := r.helmfileRepoSlug(ctx, project)
	if !hfFound {
		l.Info("deploy: tatara-helmfile repo not enrolled in project; cannot poll apply (requeue)",
			"action", "deploy_no_helmfile", "resource_id", task.Name)
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}

	run, runFound, rerr := dw.LatestWorkflowRun(ctx, hfOwner, hfRepo, applyWorkflowFile, "main")
	if rerr != nil {
		l.Error(rerr, "deploy: read helmfile apply run (requeue)", "resource_id", task.Name)
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}
	if !runFound || run.Status != "completed" {
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}

	// Read the applied helmfile pin state at the run's head SHA once and reuse it
	// for both the success match and the failure attribution.
	pinState, perr := r.helmfilePinState(ctx, dw, hfOwner, hfRepo, run.HeadSHA)
	if perr != nil {
		l.Error(perr, "deploy: read helmfile pin state (requeue)", "resource_id", task.Name, "sha", run.HeadSHA)
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}
	carriesVersion := pinCarriesVersion(pinState, version)

	switch run.Conclusion {
	case "success":
		if !carriesVersion {
			// This successful apply predates this Task's pin; wait for the next apply.
			return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
		}
		return r.resolveDeployedSweep(ctx, project, run.HeadSHA, pinState)
	case "failure", "cancelled", "timed_out", "startup_failure":
		if !carriesVersion {
			// The failed apply did not carry this pin; not this cascade's failure.
			return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
		}
		l.Info("deploy: helmfile apply failed for this change; rerolling",
			"action", "deploy_apply_failed", "resource_id", task.Name, "run_url", run.HTMLURL)
		return r.rerollDeploy(ctx, project, task, "apply_failed",
			fmt.Sprintf("The tatara-helmfile apply that carried %s failed (%s). Investigate the apply run and push a fix MR; the change is merged but not applied to the cluster.", version, run.HTMLURL))
	default:
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}
}

// resolveDeployedSweep resolves EVERY Deploying Task whose published version is
// present in the applied helmfile pin state at applySHA: it closes the
// originating issue with the deployed-version comment, marks the Task Done, and
// flips its ledger entry to applied. N converging Tasks resolve in one pass (D10).
func (r *TaskReconciler) resolveDeployedSweep(ctx context.Context, project *tatarav1alpha1.Project, applySHA, pinState string) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	entries, err := r.deployLedger(project.Namespace).List(ctx, project.Name)
	if err != nil {
		l.Error(err, "deploy: list ledger for resolution sweep (requeue)", "project", project.Name)
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}
	resolved := 0
	for _, e := range entries {
		if e.State == DeployStateApplied {
			continue
		}
		if e.Version == "" || !pinCarriesVersion(pinState, e.Version) {
			continue
		}
		var t tatarav1alpha1.Task
		if gerr := r.Get(ctx, types.NamespacedName{Namespace: project.Namespace, Name: e.SourceTaskRef}, &t); gerr != nil {
			// Task gone: drop the ledger entry to applied so it is not re-swept.
			_ = r.deployLedger(project.Namespace).SetState(ctx, project.Name, e.SourceTaskRef, DeployStateApplied)
			continue
		}
		if !tatarav1alpha1.TaskDeploying(&t) {
			continue
		}
		r.resolveDeployedTask(ctx, project, &t, e, applySHA)
		_ = r.deployLedger(project.Namespace).SetState(ctx, project.Name, e.SourceTaskRef, DeployStateApplied)
		resolved++
	}
	l.Info("deploy: resolution sweep complete", "action", "deploy_resolved", "project", project.Name,
		"apply_sha", applySHA, "resolved", resolved)
	return ctrl.Result{}, nil
}

// resolveDeployedTask closes one resolved Task's issue with the deployed-version
// comment and marks it Done. Best-effort egress; the Done transition always lands.
func (r *TaskReconciler) resolveDeployedTask(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, entry DeployLedgerEntry, applySHA string) {
	l := log.FromContext(ctx)
	comment := fmt.Sprintf("Deployed %s, applied via %s@%s.", entry.Artifact+"@"+entry.Version, helmfileRepoName, shortSHA(applySHA))

	if r.SCMFor != nil && task.Spec.Source != nil && task.Spec.Source.IssueRef != "" && !task.Spec.Source.IsPR {
		provider := task.Spec.Source.Provider
		if provider == "" {
			provider = "github"
		}
		if writer, werr := r.SCMFor(provider); werr == nil {
			if token, terr := r.scmToken(ctx, task.Namespace, project.Spec.ScmSecretRef); terr == nil {
				if repoSlug, number := parseIssueRef(task.Spec.Source.IssueRef); number > 0 {
					cerr := writer.CloseIssue(ctx, token, repoSlug, number, comment)
					r.recordSCM(provider, "close_issue", cerr)
					if cerr != nil {
						l.Error(cerr, "deploy: close issue on apply (non-fatal)", "resource_id", task.Name, "issue_ref", task.Spec.Source.IssueRef)
					} else {
						l.Info("deploy: issue closed on apply",
							"action", "scm_issue_closed_on_deploy", "resource_id", task.Name, "issue_ref", task.Spec.Source.IssueRef)
					}
				}
			}
		}
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); gerr != nil {
			return gerr
		}
		fresh.Status.Phase = ""
		fresh.Status.LifecycleState = "Done"
		fresh.Status.CascadeStage = "helmfile-applied"
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		l.Error(err, "deploy: mark Task Done on apply", "resource_id", task.Name)
		return
	}
	if r.LifecycleMetrics != nil {
		r.LifecycleMetrics.RecordTransition(tatarav1alpha1.LifecycleStateDeploying, "Done")
		r.LifecycleMetrics.ObserveLifecycle(time.Since(task.CreationTimestamp.Time).Seconds())
	}
	r.Metrics.CDResolved()
	_ = r.deleteWrapper(ctx, task)
	l.Info("deploy: cascade resolved Done",
		"action", "deploy_done", "resource_id", task.Name, "artifact", entry.Artifact, "version", entry.Version)
}

// rerollDeploy handles a failed/timed-out cascade: it records the failure, marks
// the ledger entry failed, and either rerolls the change back to Implement with a
// fix prompt (reusing the bounded-reroll machinery) or, once the auto-reroll
// budget is spent, parks recoverable for a human. The bound is the shared
// ImplementGiveUps cap.
func (r *TaskReconciler) rerollDeploy(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, metricReason, contextMsg string) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	r.Metrics.CDCascadeFailed(metricReason)
	_ = r.deployLedger(task.Namespace).SetState(ctx, project.Name, task.Name, DeployStateFailed)

	// Exhausted auto-recovery: park recoverable for a human, comment the cause.
	if task.Status.ImplementGiveUps >= maxImplGiveUps {
		if err := r.clearDeployState(ctx, task, false); err != nil {
			return ctrl.Result{}, err
		}
		writer, token, provider := r.deployWriter(ctx, project, task)
		if writer != nil {
			msg := "Deploy cascade recovery is exhausted after repeated attempts; leaving this for a human. " + contextMsg
			if perr := r.parkWithComment(ctx, task, writer, token, "deploy-timeout", msg); perr != nil {
				return ctrl.Result{}, perr
			}
		} else {
			if perr := r.setLifecycleState(ctx, task, "Parked", "deploy-timeout"); perr != nil {
				return ctrl.Result{}, perr
			}
		}
		l.Info("deploy: cascade recovery exhausted; parked",
			"action", "deploy_park_exhausted", "resource_id", task.Name, "reason", metricReason, "provider", provider)
		return ctrl.Result{}, nil
	}

	// Reroll: re-enter Implement to fix the failing stage, consuming one attempt
	// from the shared auto-reroll budget.
	if err := r.setImplementContext(ctx, task, contextMsg); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.clearMergedChangeState(ctx, task); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.clearDeployState(ctx, task, true); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.setLifecycleState(ctx, task, "Implement", "deploy-failure"); err != nil {
		return ctrl.Result{}, err
	}
	l.Info("deploy: cascade failed; rerolled to Implement",
		"action", "deploy_reroll", "resource_id", task.Name, "reason", metricReason)
	return ctrl.Result{}, nil
}

// clearDeployState clears the Deploying phase + deploy-supervision status fields.
// When bumpGiveup is set it also increments the auto-reroll attempt counter so a
// Deploying->Implement reroll consumes the shared ImplementGiveUps budget.
func (r *TaskReconciler) clearDeployState(ctx context.Context, task *tatarav1alpha1.Task, bumpGiveup bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.Phase = ""
		fresh.Status.DeployDeadline = nil
		fresh.Status.CascadeStage = ""
		fresh.Status.DeployedVersion = ""
		fresh.Status.DeployArtifact = ""
		if bumpGiveup {
			fresh.Status.ImplementGiveUps++
		}
		return r.Status().Update(ctx, fresh)
	})
}

// setDeployVersion records the learned cut version + artifact identity.
func (r *TaskReconciler) setDeployVersion(ctx context.Context, task *tatarav1alpha1.Task, version, artifact string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.DeployedVersion = version
		fresh.Status.DeployArtifact = artifact
		fresh.Status.CascadeStage = "parent-pr-open"
		return r.Status().Update(ctx, fresh)
	})
}

// deployWriter resolves the SCM writer + token + provider for a Deploying Task.
// Returns a nil writer when the SCM is not wired (tests).
func (r *TaskReconciler) deployWriter(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (scm.SCMWriter, string, string) {
	provider := "github"
	if task.Spec.Source != nil && task.Spec.Source.Provider != "" {
		provider = task.Spec.Source.Provider
	}
	if r.SCMFor == nil {
		return nil, "", provider
	}
	writer, err := r.SCMFor(provider)
	if err != nil {
		return nil, "", provider
	}
	token, terr := r.scmToken(ctx, task.Namespace, project.Spec.ScmSecretRef)
	if terr != nil {
		return nil, "", provider
	}
	return writer, token, provider
}

// helmfileRepoSlug returns the owner/name of the project's tatara-helmfile repo.
func (r *TaskReconciler) helmfileRepoSlug(ctx context.Context, project *tatarav1alpha1.Project) (string, string, bool) {
	repos, err := r.projectRepos(ctx, project)
	if err != nil {
		return "", "", false
	}
	for i := range repos {
		owner, name, oerr := scm.OwnerRepo(repos[i].Spec.URL)
		if oerr != nil {
			continue
		}
		if name == helmfileRepoName {
			return owner, name, true
		}
	}
	return "", "", false
}

// pinCarriesVersion reports whether the applied helmfile pin state references a
// component version. Image pins carry the v-prefixed tag (`tag: "vX.Y.Z"`) while
// chart pins carry the bare `version: X.Y.Z`, so both forms are checked: the
// v-prefixed tag and its bare form. The bare check is a substring heuristic
// (v1, trusted-claim model); a future significance-verification pass can tighten
// it to an exact pin-key match.
func pinCarriesVersion(pinState, version string) bool {
	if version == "" {
		return false
	}
	if strings.Contains(pinState, version) {
		return true
	}
	if bare := strings.TrimPrefix(version, "v"); bare != version {
		return strings.Contains(pinState, bare)
	}
	return false
}

// helmfilePinState concatenates the deploy pin files at ref into one string so a
// version substring match confirms a pin was applied. Missing files (404) are
// skipped; GetFileContent returns "" for them.
func (r *TaskReconciler) helmfilePinState(ctx context.Context, dw scm.DeployWatcher, owner, repo, ref string) (string, error) {
	var b strings.Builder
	for _, f := range deployPinFiles {
		content, err := dw.GetFileContent(ctx, owner, repo, f, ref)
		if err != nil {
			return "", err
		}
		b.WriteString(content)
		b.WriteByte('\n')
	}
	return b.String(), nil
}

// cdScan is the push-CD deploy-supervision backstop (peer of mrScan/issueScan):
// it finds Deploying Tasks whose cascade has stalled past 1.5x their deploy
// budget with no progress and rerolls them to a fix run, bounded by the shared
// auto-reroll cap. It catches cascades the per-Task reconcile missed (operator
// restart / dropped requeue).
func (r *ProjectReconciler) cdScan(ctx context.Context, proj *tatarav1alpha1.Project, existing []tatarav1alpha1.Task) {
	l := log.FromContext(ctx)
	now := time.Now()
	for i := range existing {
		t := &existing[i]
		if t.Spec.ProjectRef != proj.Name || !tatarav1alpha1.TaskDeploying(t) {
			continue
		}
		dl := t.Status.DeployDeadline
		if dl == nil {
			continue
		}
		budget := deployBudget(proj, t.Spec.RepositoryRef)
		stallThreshold := dl.Add(time.Duration(float64(budget) * (deployStalledFactor - 1.0)))
		if now.Before(stallThreshold) {
			continue
		}
		r.Metrics.CDCascadeStalled()
		if t.Status.ImplementGiveUps >= maxImplGiveUps {
			l.Info("cdScan: deploy cascade stalled but auto-reroll budget spent; leaving for a human",
				"action", "cd_scan_exhausted", "resource_id", t.Name, "artifact", t.Status.DeployArtifact)
			continue
		}
		// Reroll: clear the Deploying phase and re-enter Implement to fix the stalled
		// cascade. adoptLifecycleTaskAt clears Phase and re-arms the lifecycle clocks;
		// the injected context tells the agent what stalled.
		if err := r.setTaskImplementContext(ctx, t,
			"The push-CD deploy cascade for this change stalled (no tatara-helmfile apply within the backstop window). Investigate the cascade (component tag, parent bump PR, helmfile apply) and push a fix; the change is merged but not deployed."); err != nil {
			l.Error(err, "cdScan: set implement context (skipping reroll)", "resource_id", t.Name)
			continue
		}
		if err := r.adoptLifecycleTaskAt(ctx, proj, t, "Implement"); err != nil {
			l.Error(err, "cdScan: reroll stalled deploy task", "resource_id", t.Name)
			continue
		}
		l.Info("cdScan: rerolled stalled deploy cascade",
			"action", "cd_scan_reroll", "resource_id", t.Name, "artifact", t.Status.DeployArtifact)
	}
}

// setTaskImplementContext writes the re-entry prompt onto a Task (ProjectReconciler
// path; mirrors TaskReconciler.setImplementContext). Bumps ImplementGiveUps so the
// reroll consumes the shared auto-reroll budget.
func (r *ProjectReconciler) setTaskImplementContext(ctx context.Context, task *tatarav1alpha1.Task, msg string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.ImplementContext = msg
		fresh.Status.Phase = ""
		fresh.Status.DeployDeadline = nil
		fresh.Status.CascadeStage = ""
		fresh.Status.DeployedVersion = ""
		fresh.Status.DeployArtifact = ""
		fresh.Status.ImplementGiveUps++
		return r.Status().Update(ctx, fresh)
	})
}

// issueRefOf returns a Task's originating issue ref, or "".
func issueRefOf(task *tatarav1alpha1.Task) string {
	if task.Spec.Source != nil {
		return task.Spec.Source.IssueRef
	}
	return ""
}

// shortSHA trims a commit SHA to 7 chars for human-facing comments.
func shortSHA(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}
