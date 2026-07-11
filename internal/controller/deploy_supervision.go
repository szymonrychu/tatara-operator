package controller

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
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
	// deployParkReason is the ParkReason set when a deploy cascade exhausts its
	// bounded auto-reroll budget and is parked recoverable for a human. cdScan
	// counts Parked tasks carrying it as currently-failed cascades.
	deployParkReason = "deploy-timeout"
	// mergeParkReason is the ParkReason set when a discrete-implement umbrella's
	// member PR(s) stay unmerged past the merge-wait budget (item 3). Like
	// deployParkReason it is a recoverable park a human resolves; cdScan counts it
	// in the CD-health failed gauge so a stuck-at-merge stream is visible.
	mergeParkReason = "merge-timeout"
	// defaultMergeWaitBudget is the fallback pre-merge (review + merge) window when
	// Project.Spec.MergeWaitBudgetMinutes is unset.
	defaultMergeWaitBudget = 720 * time.Minute
	// Per-member deploy states (WorkItemRef.DeployState) for the umbrella cascade.
	memberDeployStateDeploying = "deploying"
	memberDeployStateApplied   = "applied"
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

// mergePRSquash is the SINGLE writer.Merge call site in the controller. Every
// operator-driven merge funnels through here: the issueLifecycle drain
// (handleMerge) and the review-approved deploy supervisor (superviseApprovedPRs).
// This keeps "agents never merge" structural (there is exactly one merge egress)
// and satisfies the acceptance grep. Squash is the fixed method: push-CD cuts one
// commit per merged change.
func mergePRSquash(ctx context.Context, writer scm.SCMWriter, repoURL, token string, number int) (string, error) {
	return writer.Merge(ctx, repoURL, token, number, "squash")
}

// superviseApprovedPRs is the review-approved merge trigger and the deploy
// supervisor's gated fallback to the forge's native auto-merge. For each open bot
// PR carrying tatara-approved on a project repo it merges only when the PR is
// green (CIStatus==success), mergeable (not dirty/blocked), and NOT already merged
// (native auto-merge, enabled at implement-PR-open, remains the primary path; the
// !Merged gate prevents a double-merge). A semver:* label is stamped first so
// push-CD's tag step never fails closed (issue #229). Non-bot PRs and PRs without
// the approval label are never merged: agents never merge, and merges are gated on
// green + tatara-approved (CROSS-REPO-CONTRACT). Best-effort per PR; a merge error
// is logged non-fatally and the sweep continues.
func (r *ProjectReconciler) superviseApprovedPRs(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository) {
	l := log.FromContext(ctx)
	if proj.Spec.Scm == nil || reader == nil {
		return
	}
	bot := proj.Spec.Scm.BotLogin
	provider := proj.Spec.Scm.Provider
	_, approvedLabel, _, _ := lifecycleLabels(proj.Spec.Scm)
	writer, token, werr := r.scanWriter(ctx, proj)
	if werr != nil {
		l.Error(werr, "superviseApprovedPRs: scanWriter (skipping sweep)", "action", "scm_merge_approved", "resource_id", proj.Name)
		return
	}
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		prs, lerr := reader.ListOpenPRs(ctx, owner, name)
		if lerr != nil {
			l.Error(lerr, "superviseApprovedPRs: ListOpenPRs (skipping repo)",
				"action", "scm_merge_approved", "resource_id", proj.Name, "repo", name)
			continue
		}
		for _, pr := range prs {
			// Gate 1: only bot-authored PRs, and only those carrying tatara-approved.
			if bot != "" && pr.Author != bot {
				continue
			}
			if !hasLabel(pr.Labels, approvedLabel) {
				continue
			}
			// Gate 2: green + not-already-merged. GetPRState is authoritative for the
			// live CI + merged state (the list snapshot lacks both).
			st, serr := writer.GetPRState(ctx, repos[i].Spec.URL, token, pr.Number)
			r.Metrics.SCMWrite(provider, "get_pr_state", scmResult(serr))
			if serr != nil {
				continue
			}
			if st.Merged || st.Closed {
				continue // native auto-merge already landed it, or it was closed
			}
			if st.CIStatus != "success" {
				continue
			}
			// Gate 3: mergeability. A dirty (conflict) or blocked PR is not merged;
			// review re-adds tatara-implementation for those. Fail-open on read error.
			if ms, mserr := writer.GetMergeState(ctx, repos[i].Spec.URL, token, pr.Number); mserr == nil &&
				(ms == scm.MergeStateDirty || ms == scm.MergeStateBlocked) {
				continue
			}
			// Ensure a semver:* label before the merge (issue #229): a directly-merged
			// PR with no semver label makes push-CD's release tag step fail closed.
			r.ensureSemverBeforeApprovedMerge(ctx, proj, repos[i], writer, token, provider, pr)
			sha, merr := mergePRSquash(ctx, writer, repos[i].Spec.URL, token, pr.Number)
			r.Metrics.SCMWrite(provider, "merge", scmResult(merr))
			if merr != nil {
				l.Error(merr, "superviseApprovedPRs: merge approved PR (non-fatal)",
					"action", "scm_merge_approved", "resource_id", proj.Name, "repo", name, "pr", pr.Number)
				continue
			}
			l.Info("superviseApprovedPRs: merged approved green PR",
				"action", "scm_merge_approved", "resource_id", proj.Name, "repo", name, "pr", pr.Number, "sha", sha)
		}
	}
}

// stampDeploying performs the Task status transition into the pod-less Deploying
// phase and records the deploy-ledger entry. It is the SINGLE implementation of the
// deploy transition, shared by TaskReconciler.enterDeploying (the issueLifecycle
// bridge) and ProjectReconciler.enterDeployingFromMerge (the discrete-kind flow), so
// the two producers can never drift (the "orphaned enterDeploying" landmine). Pod
// teardown, budget computation, metrics and logging are the caller's responsibility.
// The ledger Add is non-fatal (a dedup optimisation; the Task's own status drives
// supervision), matching the original enterDeploying behaviour byte-for-byte.
func stampDeploying(ctx context.Context, c client.Client, ledger *DeployLedger, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, repoName string, deadline metav1.Time) error {
	issueRef := ""
	if task.Spec.Source != nil {
		issueRef = task.Spec.Source.IssueRef
	}
	if err := ledger.Add(ctx, project.Name, DeployLedgerEntry{
		Artifact:      repoName,
		SourceTaskRef: task.Name,
		IssueRef:      issueRef,
		HeadSHA:       task.Status.MergeCommitSHA,
		State:         DeployStateDeploying,
	}); err != nil {
		log.FromContext(ctx).Error(err, "deploy: add ledger entry on Deploying entry (non-fatal)", "resource_id", task.Name)
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.Phase = tatarav1alpha1.PhaseDeploying
		fresh.Status.DeployState = tatarav1alpha1.DeployStateDeploying
		fresh.Status.DeployDeadline = &deadline
		fresh.Status.CascadeStage = "tagged"
		fresh.Status.DeployArtifact = repoName
		return c.Status().Update(ctx, fresh)
	})
}

// superviseMergedPRs is the discrete-kind (umbrella implement) deploy entry +
// merge-wait supervisor (peer of superviseApprovedPRs, run on the same mrScan
// cadence). superviseApprovedPRs (and the forge's native auto-merge) MERGE an
// approved+green bot PR; this sweep watches each umbrella implement Task's
// role:openedPR ledger MEMBERS (the N cross-repo PRs the stream opened - NOT the
// single Status.PRNumber scalar, which the umbrella writeback never sets) and:
//
//   - projects each member's live merge state onto the ledger (GetPRState.Merged,
//     read-only: this NEVER calls Merge, so the single merge egress mergePRSquash
//     is preserved). A merged member flips to WIMerged; a closed-not-merged member
//     flips to WIClosed so it stops blocking entry.
//   - once EVERY non-closed member is merged, enters the pod-less Deploying phase
//     (enterDeployingUmbrella) so reconcileDeployingUmbrella supervises each
//     member's cascade and closes the issue on a confirm-all apply.
//   - while a member is still unmerged, bounds the wait with a wall-clock
//     merge-wait deadline (item 3): a stream whose member(s) stay unmerged past
//     the budget is parked recoverable WITH an issue comment naming the stuck
//     member(s) (parkUmbrellaMergeTimeout), instead of sitting open+approved
//     forever with no visibility.
//
// Guards (umbrellaDeployEligible): Kind=="implement" (the issueLifecycle drain
// owns its own Deploying entry via handleMainCI, so skipping other kinds avoids a
// double-deploy); an issue source (not a PR source); pushCDEligible (only a change
// that declared a significance rides the cascade); and not already Deploying /
// terminal-resolved / parked (the durable double-deploy fence: Deploying is owned
// by reconcileDeploying, and a Done/Parked stream is finished).
func (r *ProjectReconciler) superviseMergedPRs(ctx context.Context, proj *tatarav1alpha1.Project, repos []tatarav1alpha1.Repository) {
	l := log.FromContext(ctx)
	if proj.Spec.Scm == nil {
		return
	}
	provider := proj.Spec.Scm.Provider
	writer, token, werr := r.scanWriter(ctx, proj)
	if werr != nil {
		l.Error(werr, "superviseMergedPRs: scanWriter (skipping sweep)", "action", "deploy_enter_merged", "resource_id", proj.Name)
		return
	}

	// member slug -> repo URL, for per-member PR-state polls.
	repoURLBySlug := map[string]string{}
	for i := range repos {
		if slug, _, e := repoSlugFromURL(repos[i].Spec.URL, provider); e == nil && slug != "" {
			repoURLBySlug[slug] = repos[i].Spec.URL
		}
	}

	var list tatarav1alpha1.TaskList
	if err := r.List(ctx, &list, client.InNamespace(proj.Namespace)); err != nil {
		l.Error(err, "superviseMergedPRs: list tasks (skipping sweep)", "action", "deploy_enter_merged", "resource_id", proj.Name)
		return
	}
	for i := range list.Items {
		task := &list.Items[i]
		if !umbrellaDeployEligible(task, proj.Name) {
			continue
		}
		members := openedPRDeployMembers(task)
		if len(members) == 0 {
			continue
		}
		// Project each member's live merge/closed state onto the ledger (read-only).
		for _, m := range members {
			repoURL := repoURLBySlug[m.Repo]
			if repoURL == "" {
				continue
			}
			st, serr := writer.GetPRState(ctx, repoURL, token, m.Number)
			r.Metrics.SCMWrite(provider, "get_pr_state", scmResult(serr))
			if serr != nil {
				continue
			}
			newState := ""
			switch {
			case st.Merged:
				newState = tatarav1alpha1.WIMerged
			case st.Closed:
				newState = tatarav1alpha1.WIClosed
			}
			if newState != "" && newState != m.State {
				if uerr := r.setMemberState(ctx, task, m, newState); uerr != nil {
					l.Error(uerr, "superviseMergedPRs: project member merge state (retry next sweep)",
						"resource_id", task.Name, "repo", m.Repo, "pr", m.Number)
				}
			}
		}
		// Re-read the freshly-projected member states.
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			continue
		}
		members = openedPRDeployMembers(fresh)
		if len(members) == 0 {
			continue
		}
		if allMembersMerged(members) {
			if err := r.enterDeployingUmbrella(ctx, proj, fresh); err != nil {
				l.Error(err, "deploy: enter Deploying (umbrella; retry next sweep)",
					"action", "deploy_enter_merged", "resource_id", fresh.Name)
			}
			continue
		}
		// Some member(s) still unmerged: bound the wait, park + surface if overrun.
		r.superviseMergeWait(ctx, proj, fresh, writer, token, provider, unmergedMemberRepos(members))
	}
}

// enterDeployingUmbrella drives a discrete-implement umbrella Task whose members
// have all merged into the pod-less Deploying phase, seeding each merged member's
// per-member cascade state. Peer of TaskReconciler.enterDeploying (the
// issueLifecycle bridge), but multi-member: there is no single scalar
// DeployArtifact/DeployedVersion - reconcileDeployingUmbrella tracks each member
// independently. Idempotent (a re-entry on an already-Deploying Task no-ops) and
// conflict-safe (mutates the fresh read in place under RetryOnConflict).
func (r *ProjectReconciler) enterDeployingUmbrella(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) error {
	l := log.FromContext(ctx)
	budget := umbrellaDeployBudget(proj, task)
	deadline := metav1.NewTime(time.Now().Add(budget))

	// Deploying is pod-less: tear down any lingering agent pod.
	if err := agent.DeleteWrapper(ctx, r.Client, task.Namespace, task); err != nil {
		l.Error(err, "deploy: teardown wrapper on umbrella Deploying entry (non-fatal)", "resource_id", task.Name)
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if tatarav1alpha1.TaskDeploying(fresh) {
			return nil // already entered concurrently
		}
		fresh.Status.Phase = tatarav1alpha1.PhaseDeploying
		fresh.Status.DeployState = tatarav1alpha1.DeployStateDeploying
		fresh.Status.DeployDeadline = &deadline
		fresh.Status.CascadeStage = "tagged"
		// Seed each merged member's per-member cascade state in place.
		for j := range fresh.Status.WorkItems {
			wi := &fresh.Status.WorkItems[j]
			if wi.Kind == tatarav1alpha1.WorkItemPR && wi.Role == tatarav1alpha1.RoleOpenedPR &&
				wi.State == tatarav1alpha1.WIMerged && wi.DeployState == "" {
				wi.DeployState = memberDeployStateDeploying
			}
		}
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		return err
	}
	if r.LifecycleMetrics != nil {
		r.LifecycleMetrics.RecordTransition("MainCI", tatarav1alpha1.DeployStateDeploying)
	}
	l.Info("deploy: all umbrella member PRs merged; entering Deploying (discrete-kind flow)",
		"action", "deploy_enter_merged", "resource_id", task.Name,
		"members", len(openedPRDeployMembers(task)), "budget_seconds", int(budget.Seconds()),
		"deadline", deadline.Format(time.RFC3339))
	return nil
}

// superviseMergeWait bounds a not-yet-fully-merged umbrella's pre-Deploying wait
// with the wall-clock merge-wait deadline (item 3). It stamps the deadline on
// first sight, then parks the stream recoverable with an issue comment naming the
// stuck member(s) once the budget is overrun.
func (r *ProjectReconciler) superviseMergeWait(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, writer scm.SCMWriter, token, provider string, stuckRepos []string) {
	now := time.Now()
	if task.Status.MergeWaitDeadline == nil {
		budget := mergeWaitBudget(proj)
		dl := metav1.NewTime(now.Add(budget))
		if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
			if fresh.Status.MergeWaitDeadline != nil {
				return false
			}
			fresh.Status.MergeWaitDeadline = &dl
			return true
		}); err != nil {
			log.FromContext(ctx).Error(err, "deploy: stamp umbrella merge-wait deadline (retry next sweep)", "resource_id", task.Name)
		}
		return
	}
	if now.Before(task.Status.MergeWaitDeadline.Time) {
		return // still within the merge-wait budget
	}
	r.parkUmbrellaMergeTimeout(ctx, proj, task, writer, token, provider, stuckRepos)
}

// parkUmbrellaMergeTimeout parks a discrete-implement umbrella recoverable with an
// issue comment naming the member PR(s) that stayed unmerged past the merge-wait
// budget, so a stuck-at-merge stream is surfaced instead of sitting open+approved
// forever. The originating issue is left OPEN (a human resolves/closes the stuck
// PR). cdScan counts a mergeParkReason park in its CD-health failed gauge.
func (r *ProjectReconciler) parkUmbrellaMergeTimeout(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, writer scm.SCMWriter, token, provider string, stuckRepos []string) {
	l := log.FromContext(ctx)
	if writer != nil && task.Spec.Source != nil && task.Spec.Source.IssueRef != "" {
		msg := "Deploy blocked: member PR(s) in " + strings.Join(stuckRepos, ", ") +
			" stayed unmerged past the merge-wait budget. Review/merge or close them - the originating issue stays open until every member deploys."
		_, cerr := r.gatedComment(ctx, proj, nil, writer, token, provider, task.Spec.Source.Number, task.Spec.Source.IsPR, task.Spec.Source.AuthorLogin, task.Spec.Source.IssueRef, msg)
		if cerr != nil {
			l.Error(cerr, "deploy: umbrella merge-timeout comment (non-fatal)", "resource_id", task.Name, "issue_ref", task.Spec.Source.IssueRef)
		}
	}
	if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if fresh.Status.DeployState == "Parked" && fresh.Status.ParkReason == mergeParkReason {
			return false
		}
		fresh.Status.DeployState = "Parked"
		fresh.Status.ParkReason = mergeParkReason
		return true
	}); err != nil {
		l.Error(err, "deploy: park umbrella merge-timeout (retry next sweep)", "resource_id", task.Name)
		return
	}
	l.Info("deploy: umbrella member PR(s) stuck unmerged past merge-wait budget; parked recoverable",
		"action", "deploy_merge_timeout", "resource_id", task.Name, "stuck", strings.Join(stuckRepos, ","))
}

// parkStalledDeploy parks a Deploying cascade that stalled past its 1.5x backstop
// threshold AND spent its bounded auto-reroll budget (liveness finding #1). It
// transitions the Task to Parked with the recoverable deployParkReason (terminal,
// reaper-eligible, counted in the cdScan CD-health failed gauge) and posts one
// issue comment naming the stuck artifact so the stall carries a human signal
// instead of sitting Deploying forever. Idempotent: the status patch is a no-op
// when the Task is already parked (or resolved concurrently), so a subsequent
// cdScan tick never re-comments. It does NOT double-comment a merge-timeout park:
// that path parks BEFORE the Deploying phase, so a Deploying Task here was never
// merge-timeout-parked.
func (r *ProjectReconciler) parkStalledDeploy(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) {
	l := log.FromContext(ctx)
	commented := false
	if r.SCMFor != nil && task.Spec.Source != nil && task.Spec.Source.IssueRef != "" {
		provider := "github"
		if task.Spec.Source.Provider != "" {
			provider = task.Spec.Source.Provider
		} else if proj.Spec.Scm != nil && proj.Spec.Scm.Provider != "" {
			provider = proj.Spec.Scm.Provider
		}
		if writer, token, err := r.scanWriter(ctx, proj); err == nil && writer != nil {
			artifact := task.Status.DeployArtifact
			if artifact == "" {
				artifact = task.Spec.RepositoryRef
			}
			msg := "Deploy blocked: the push-CD cascade for " + artifact +
				" stalled past the backstop window and the automatic recovery budget is exhausted; leaving this for a human. " +
				"The change is merged but not deployed - investigate the cascade (component tag, parent bump PR, tatara-helmfile apply) and push a fix."
			posted, cerr := r.gatedComment(ctx, proj, nil, writer, token, provider, task.Spec.Source.Number, task.Spec.Source.IsPR, task.Spec.Source.AuthorLogin, task.Spec.Source.IssueRef, msg)
			if cerr != nil {
				l.Error(cerr, "cdScan: exhausted-stall park comment (non-fatal)",
					"resource_id", task.Name, "issue_ref", task.Spec.Source.IssueRef)
			} else {
				commented = posted
			}
		}
	}
	if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		// Only park a still-Deploying Task; a concurrent resolveDeployedSweep may have
		// resolved it to Done since the cdScan snapshot.
		if !tatarav1alpha1.TaskDeploying(fresh) {
			return false
		}
		if fresh.Status.DeployState == "Parked" {
			return false
		}
		clearCascadeStatusFields(&fresh.Status)
		fresh.Status.DeployState = "Parked"
		fresh.Status.ParkReason = deployParkReason
		return true
	}); err != nil {
		l.Error(err, "cdScan: park exhausted stalled deploy (retry next sweep)", "resource_id", task.Name)
		return
	}
	l.Info("cdScan: deploy cascade stalled with auto-reroll budget spent; parked recoverable for a human",
		"action", "cd_scan_exhausted_parked", "resource_id", task.Name,
		"artifact", task.Status.DeployArtifact, "commented", commented)
}

// patchTaskStatus applies mutate to a freshly-read Task under RetryOnConflict and
// persists the status when mutate reports a change. The ProjectReconciler peer of
// TaskReconciler.patchTaskStatus, used by the umbrella deploy sweeps so their
// per-Task writes are conflict-safe (mutate the fresh object in place; never
// blind-overwrite Status).
func (r *ProjectReconciler) patchTaskStatus(ctx context.Context, task *tatarav1alpha1.Task, mutate func(*tatarav1alpha1.Task) bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if !mutate(fresh) {
			*task = *fresh
			return nil
		}
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*task = *fresh
		return nil
	})
}

// setMemberState projects a member PR's merge/closed state onto the ledger under
// RetryOnConflict, via the shared UpsertWorkItem merge primitive so a concurrently
// appended member (e.g. a late stream PR) is never dropped.
func (r *ProjectReconciler) setMemberState(ctx context.Context, task *tatarav1alpha1.Task, m tatarav1alpha1.WorkItemRef, state string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		UpsertWorkItem(fresh, tatarav1alpha1.WorkItemRef{
			Provider: m.Provider, Repo: m.Repo, Number: m.Number,
			Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: state,
		})
		return r.Status().Update(ctx, fresh)
	})
}

// ensureSemverBeforeApprovedMerge stamps semver:patch on an approved bot PR that
// carries no semver:* label yet, so the operator merge never leaves push-CD's tag
// step failing closed. The change significance normally rides in from implement's
// PR-open stamping (applySemverAutoMerge); this is the belt-and-braces default for
// a PR that reached approval without one. GitHub-only (the cd-release cascade is
// GitHub-only) and best-effort.
func (r *ProjectReconciler) ensureSemverBeforeApprovedMerge(ctx context.Context, proj *tatarav1alpha1.Project, repo tatarav1alpha1.Repository, writer scm.SCMWriter, token, provider string, pr scm.PRRef) {
	if provider != "github" {
		return
	}
	for _, lb := range pr.Labels {
		if strings.HasPrefix(lb, "semver:") {
			return
		}
	}
	l := log.FromContext(ctx)
	label := semverLabelPatch
	color := managedLabelColors(proj.Spec.Scm)[label]
	if eerr := writer.EnsureLabel(ctx, repo.Spec.URL, token, label, color); eerr != nil {
		r.Metrics.SCMWrite(provider, "ensure_label", scmResult(eerr))
		l.Error(eerr, "superviseApprovedPRs: ensure semver label (non-fatal)",
			"action", "scm_merge_approved", "resource_id", proj.Name, "repo", repo.Name, "label", label)
		return
	}
	slug, _, serr := repoSlugFromURL(repo.Spec.URL, provider)
	if serr != nil {
		return
	}
	prRef := fmt.Sprintf("%s#%d", slug, pr.Number)
	if aerr := writer.AddLabel(ctx, token, prRef, label); aerr != nil {
		r.Metrics.SCMWrite(provider, "add_label", scmResult(aerr))
		l.Error(aerr, "superviseApprovedPRs: add semver label (non-fatal)",
			"action", "scm_merge_approved", "resource_id", proj.Name, "repo", repo.Name, "label", label)
	}
}

// scmResult maps an error to the SCMWrite result label ("ok"|"error").
func scmResult(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
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

	if err := stampDeploying(ctx, r.Client, r.deployLedger(task.Namespace), project, task, repoName, deadline); err != nil {
		return ctrl.Result{}, fmt.Errorf("deploy: enter Deploying: %w", err)
	}
	task.Status.Phase = tatarav1alpha1.PhaseDeploying
	task.Status.DeployState = tatarav1alpha1.DeployStateDeploying
	task.Status.DeployDeadline = &deadline
	task.Status.DeployArtifact = repoName

	if r.LifecycleMetrics != nil {
		r.LifecycleMetrics.RecordTransition("MainCI", tatarav1alpha1.DeployStateDeploying)
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
	// Discrete-implement umbrella tasks carry N cross-repo openedPR members and no
	// single RepositoryRef/scalar version, so they are supervised per-member. The
	// scalar path below stays for the issueLifecycle bridge (single repo).
	if task.Spec.Kind == "implement" {
		return r.reconcileDeployingUmbrella(ctx, project, task)
	}

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

	// tatara-helmfile self-target (e.g. a GoalTierRevert incident's revert MR): the
	// terminal CD repo cuts NO semver tag and carries no component pin of its own, so
	// the tag+pin resolution below would wait forever (deadlock -> deadline -> reroll
	// -> park, silently). Resolve instead on the tatara-helmfile apply.yaml success
	// whose head is this change's merge commit: a completed successful apply of the
	// merge means the change is live on the cluster.
	if name == helmfileRepoName {
		return r.reconcileDeployingHelmfileSelf(ctx, project, task, dw, owner, name)
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
	// Scope the trigger-task gate to THIS component's own pin (name = the merged
	// repo). A different component's apply carrying a coincidentally-equal version
	// string must not resolve / fail this Task's cascade.
	carriesVersion := pinCarriesArtifactVersion(pinState, name, version)

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

// reconcileDeployingHelmfileSelf resolves a Deploying Task whose target repo IS
// tatara-helmfile (the tier-revert self-heal MR). There is no component tag or pin
// to match, so it polls the tatara-helmfile apply.yaml outcome and resolves on the
// completed successful apply whose head is this change's merge commit. On that
// apply failing it rerolls (fix the revert). An apply that predates our merge, or
// is still in-flight, requeues; the top-of-reconcileDeploying deadline guard backs
// this off to a reroll/park if it never lands.
func (r *TaskReconciler) reconcileDeployingHelmfileSelf(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, dw scm.DeployWatcher, owner, name string) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	run, found, err := dw.LatestWorkflowRun(ctx, owner, name, applyWorkflowFile, "main")
	if err != nil {
		l.Error(err, "deploy: read helmfile self apply run (requeue)", "resource_id", task.Name)
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}
	if !found || run.Status != "completed" {
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}
	// The apply.yaml run triggered by our merge push carries head_sha == the merge
	// commit. A run whose head is a different commit either predates our merge (wait)
	// or superseded it; the deadline guard backstops the rare superseded case.
	carriesMerge := task.Status.MergeCommitSHA != "" && run.HeadSHA == task.Status.MergeCommitSHA
	switch run.Conclusion {
	case "success":
		if !carriesMerge {
			return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
		}
		comment := fmt.Sprintf("Applied via %s@%s.", helmfileRepoName, shortSHA(run.HeadSHA))
		r.closeIssueOnDeploy(ctx, project, task, comment)
		return r.markTaskDeployDone(ctx, task, "helmfile-applied",
			[]any{"action", "deploy_done", "resource_id", task.Name, "repo", name, "apply_sha", run.HeadSHA})
	case "failure", "cancelled", "timed_out", "startup_failure":
		if !carriesMerge {
			return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
		}
		l.Info("deploy: helmfile self apply failed for this change; rerolling",
			"action", "deploy_apply_failed", "resource_id", task.Name, "run_url", run.HTMLURL)
		return r.rerollDeploy(ctx, project, task, "apply_failed",
			fmt.Sprintf("The tatara-helmfile apply that carried this change failed (%s). Investigate the apply run and push a fix; the change is merged but not applied to the cluster.", run.HTMLURL))
	default:
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}
}

// markTaskDeployDone marks a resolved Task Done (Phase cleared, DeployState Done,
// CascadeStage set), records the resolve metrics, and tears down any wrapper. It is
// the terminal-transition core shared by the helmfile self-target resolve path.
func (r *TaskReconciler) markTaskDeployDone(ctx context.Context, task *tatarav1alpha1.Task, cascadeStage string, logKV []any) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); gerr != nil {
			return gerr
		}
		fresh.Status.Phase = ""
		fresh.Status.DeployState = "Done"
		fresh.Status.CascadeStage = cascadeStage
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		l.Error(err, "deploy: mark Task Done on apply", "resource_id", task.Name)
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}
	if r.LifecycleMetrics != nil {
		r.LifecycleMetrics.RecordTransition(tatarav1alpha1.DeployStateDeploying, "Done")
		r.LifecycleMetrics.ObserveLifecycle(time.Since(task.CreationTimestamp.Time).Seconds())
	}
	r.Metrics.CDResolved()
	_ = r.deleteWrapper(ctx, task)
	l.Info("deploy: cascade resolved Done", logKV...)
	return ctrl.Result{}, nil
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
		// Scope to the entry's OWN artifact pin: a sibling component sharing the
		// same version string (or a substring like v1.4.1 inside v1.4.10) must not
		// prematurely resolve this Task and close its issue with a bogus version.
		if e.Version == "" || !pinCarriesArtifactVersion(pinState, e.Artifact, e.Version) {
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
	r.closeIssueOnDeploy(ctx, project, task, comment)

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); gerr != nil {
			return gerr
		}
		fresh.Status.Phase = ""
		fresh.Status.DeployState = "Done"
		fresh.Status.CascadeStage = "helmfile-applied"
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		l.Error(err, "deploy: mark Task Done on apply", "resource_id", task.Name)
		return
	}
	if r.LifecycleMetrics != nil {
		r.LifecycleMetrics.RecordTransition(tatarav1alpha1.DeployStateDeploying, "Done")
		r.LifecycleMetrics.ObserveLifecycle(time.Since(task.CreationTimestamp.Time).Seconds())
	}
	r.Metrics.CDResolved()
	_ = r.deleteWrapper(ctx, task)
	l.Info("deploy: cascade resolved Done",
		"action", "deploy_done", "resource_id", task.Name, "artifact", entry.Artifact, "version", entry.Version)
}

// closeIssueOnDeploy closes a resolved Task's originating issue with the
// deployed-version comment. Best-effort egress shared by the scalar
// (resolveDeployedTask) and umbrella (resolveDeployedUmbrella) resolve paths so
// the close is written in exactly one place. A permanently-gone target (404/410)
// is recorded result="gone", not "error" (issue #268).
func (r *TaskReconciler) closeIssueOnDeploy(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, comment string) {
	if r.SCMFor == nil || task.Spec.Source == nil || task.Spec.Source.IssueRef == "" || task.Spec.Source.IsPR {
		return
	}
	l := log.FromContext(ctx)
	provider := task.Spec.Source.Provider
	if provider == "" {
		provider = "github"
	}
	writer, werr := r.SCMFor(provider)
	if werr != nil {
		return
	}
	token, terr := r.scmToken(ctx, task.Namespace, project.Spec.ScmSecretRef)
	if terr != nil {
		return
	}
	repoSlug, number := parseIssueRef(task.Spec.Source.IssueRef)
	if number <= 0 {
		return
	}
	cerr := writer.CloseIssue(ctx, token, repoSlug, number, comment)
	if cerr != nil {
		if isPermanentTargetGone(cerr) {
			r.recordSCMGone(provider, "close_issue", cerr)
		} else {
			r.recordSCM(provider, "close_issue", cerr)
		}
		l.Error(cerr, "deploy: close issue on apply (non-fatal)", "resource_id", task.Name, "issue_ref", task.Spec.Source.IssueRef)
		return
	}
	r.recordSCM(provider, "close_issue", nil)
	l.Info("deploy: issue closed on apply",
		"action", "scm_issue_closed_on_deploy", "resource_id", task.Name, "issue_ref", task.Spec.Source.IssueRef)
}

// reconcileDeployingUmbrella supervises a discrete-implement umbrella Task's
// push-CD cascade PER MEMBER: it learns each merged member's cut version, polls
// the terminal tatara-helmfile apply, confirms each member's pin against the
// applied state, and closes the originating issue ONLY once EVERY merged member
// is confirmed applied (confirm-all, item 2). Per-member deploy state persists on
// the ledger (WorkItemRef.DeployState) so members applied across different apply
// runs accumulate. On the budget deadline with a member still unconfirmed it
// rerolls (reusing the bounded-reroll machinery), surfacing the stuck member(s).
func (r *TaskReconciler) reconcileDeployingUmbrella(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	members := openedPRDeployMembers(task)

	// Deadline guard: not every member confirmed within budget -> reroll (parks
	// recoverable once the auto-reroll budget is spent), surfacing the stuck ones.
	if dl := task.Status.DeployDeadline; dl != nil && time.Now().After(dl.Time) && !allMembersApplied(members) {
		stuck := unappliedMemberRepos(members)
		l.Info("deploy: umbrella budget deadline exceeded with members unconfirmed; rerolling",
			"action", "deploy_timeout", "resource_id", task.Name, "stuck", strings.Join(stuck, ","))
		return r.rerollDeploy(ctx, project, task, "deploy_timeout",
			"Umbrella deploy cascade did not confirm all members applied within budget. Stuck member(s): "+strings.Join(stuck, ", ")+
				". Investigate the cascade (component tag, parent bump PR, tatara-helmfile apply) and push a fix; the change is merged but not fully deployed.")
	}

	if len(members) == 0 || r.ReaderFor == nil || r.SCMFor == nil {
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
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
		l.Info("deploy: reader is not a DeployWatcher; umbrella cascade unsupervisable here (cascade is GitHub-only)",
			"action", "deploy_no_watcher", "resource_id", task.Name, "provider", provider)
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}

	repos, rerr := r.projectRepos(ctx, project)
	if rerr != nil {
		return ctrl.Result{}, fmt.Errorf("deploy: list project repos: %w", rerr)
	}
	repoBySlug := map[string]tatarav1alpha1.Repository{}
	for i := range repos {
		if slug, _, e := repoSlugFromURL(repos[i].Spec.URL, provider); e == nil && slug != "" {
			repoBySlug[slug] = repos[i]
		}
	}

	// Learn each merged member's cut version (cd-release tag) if not yet recorded.
	for _, m := range members {
		if m.DeployState == memberDeployStateApplied || m.DeployedVersion != "" {
			continue
		}
		// A tatara-helmfile member cuts no semver tag; it is confirmed directly off
		// the apply outcome in the confirm loop below (no tag to learn).
		if repoNameFromSlug(m.Repo) == helmfileRepoName {
			continue
		}
		repo, found := repoBySlug[m.Repo]
		if !found {
			continue
		}
		owner, name, oerr := scm.OwnerRepo(repo.Spec.URL)
		if oerr != nil {
			continue
		}
		tag, tagFound, terr := dw.LatestSemverTag(ctx, owner, name)
		if terr != nil {
			l.Error(terr, "deploy: read latest semver tag (requeue)", "resource_id", task.Name, "repo", name)
			continue
		}
		if !tagFound {
			continue // member's component tag not cut yet
		}
		if uerr := r.setMemberDeploy(ctx, task, m, tag, memberDeployStateDeploying); uerr != nil {
			l.Error(uerr, "deploy: record member cut version (retry next sweep)", "resource_id", task.Name, "repo", name)
		}
	}

	// Resolve the terminal tatara-helmfile repo and poll its apply outcome once.
	hfOwner, hfRepo, hfFound := r.helmfileRepoSlug(ctx, project)
	if !hfFound {
		l.Info("deploy: tatara-helmfile repo not enrolled in project; cannot poll apply (requeue)",
			"action", "deploy_no_helmfile", "resource_id", task.Name)
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}
	run, runFound, wrErr := dw.LatestWorkflowRun(ctx, hfOwner, hfRepo, applyWorkflowFile, "main")
	if wrErr != nil {
		l.Error(wrErr, "deploy: read helmfile apply run (requeue)", "resource_id", task.Name)
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}
	if !runFound || run.Status != "completed" || run.Conclusion != "success" {
		// A failed/in-flight apply confirms nothing this sweep; the deadline guard
		// above rerolls a persistently-stuck cascade.
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}
	pinState, perr := r.helmfilePinState(ctx, dw, hfOwner, hfRepo, run.HeadSHA)
	if perr != nil {
		l.Error(perr, "deploy: read helmfile pin state (requeue)", "resource_id", task.Name, "sha", run.HeadSHA)
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}

	// Confirm each still-unconfirmed member whose OWN pin is in the applied state.
	fresh := &tatarav1alpha1.Task{}
	if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); gerr != nil {
		return ctrl.Result{}, gerr
	}
	for _, m := range openedPRDeployMembers(fresh) {
		if m.DeployState == memberDeployStateApplied {
			continue
		}
		// A tatara-helmfile member carries no component tag/pin. This sweep already
		// established a completed successful apply of main, and Deploying is entered
		// only after every member merged, so a merged helmfile member is live on
		// main: confirm it applied, keyed on the apply head sha as its "version".
		if repoNameFromSlug(m.Repo) == helmfileRepoName {
			if uerr := r.setMemberDeploy(ctx, task, m, shortSHA(run.HeadSHA), memberDeployStateApplied); uerr != nil {
				l.Error(uerr, "deploy: confirm helmfile member applied (retry next sweep)", "resource_id", task.Name, "repo", m.Repo)
			}
			continue
		}
		if m.DeployedVersion == "" {
			continue
		}
		if pinCarriesArtifactVersion(pinState, repoNameFromSlug(m.Repo), m.DeployedVersion) {
			if uerr := r.setMemberDeploy(ctx, task, m, m.DeployedVersion, memberDeployStateApplied); uerr != nil {
				l.Error(uerr, "deploy: confirm member applied (retry next sweep)", "resource_id", task.Name, "repo", m.Repo)
			}
		}
	}

	// Confirm-all: close the originating issue only when EVERY member is applied.
	final := &tatarav1alpha1.Task{}
	if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), final); gerr != nil {
		return ctrl.Result{}, gerr
	}
	finalMembers := openedPRDeployMembers(final)
	if len(finalMembers) > 0 && allMembersApplied(finalMembers) {
		return r.resolveDeployedUmbrella(ctx, project, final, run.HeadSHA)
	}
	return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
}

// resolveDeployedUmbrella closes a fully-deployed umbrella Task's originating
// issue with a comment spanning every member artifact@version and marks the Task
// Done. Called only once confirm-all holds (every merged member applied).
func (r *TaskReconciler) resolveDeployedUmbrella(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, applySHA string) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	members := openedPRDeployMembers(task)
	parts := make([]string, 0, len(members))
	for _, m := range members {
		parts = append(parts, repoNameFromSlug(m.Repo)+"@"+m.DeployedVersion)
	}
	comment := fmt.Sprintf("Deployed %s, applied via %s@%s.", strings.Join(parts, ", "), helmfileRepoName, shortSHA(applySHA))
	r.closeIssueOnDeploy(ctx, project, task, comment)

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); gerr != nil {
			return gerr
		}
		fresh.Status.Phase = ""
		fresh.Status.DeployState = "Done"
		fresh.Status.CascadeStage = "helmfile-applied"
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		l.Error(err, "deploy: mark umbrella Task Done on apply", "resource_id", task.Name)
		return ctrl.Result{RequeueAfter: deployPollRequeue}, nil
	}
	if r.LifecycleMetrics != nil {
		r.LifecycleMetrics.RecordTransition(tatarav1alpha1.DeployStateDeploying, "Done")
		r.LifecycleMetrics.ObserveLifecycle(time.Since(task.CreationTimestamp.Time).Seconds())
	}
	r.Metrics.CDResolved()
	_ = r.deleteWrapper(ctx, task)
	l.Info("deploy: umbrella cascade resolved Done (all members applied)",
		"action", "deploy_done", "resource_id", task.Name, "members", strings.Join(parts, ","))
	return ctrl.Result{}, nil
}

// setMemberDeploy records a member's cut version + cascade state on the ledger
// under RetryOnConflict, via the shared UpsertWorkItem merge primitive (the
// concurrency-safe path: mutate the fresh read in place, never blind-overwrite
// Status.WorkItems).
func (r *TaskReconciler) setMemberDeploy(ctx context.Context, task *tatarav1alpha1.Task, m tatarav1alpha1.WorkItemRef, version, state string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		UpsertWorkItem(fresh, tatarav1alpha1.WorkItemRef{
			Provider: m.Provider, Repo: m.Repo, Number: m.Number,
			Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR,
			DeployedVersion: version, DeployState: state,
		})
		return r.Status().Update(ctx, fresh)
	})
}

// rerollDeploy handles a failed/timed-out cascade: it records the failure, marks
// the ledger entry failed, and either rerolls the change back to Implement with a
// fix prompt (reusing the bounded-reroll machinery) or, once the auto-reroll
// budget is spent, parks recoverable for a human. The bound is the shared
// ImplementGiveUps cap.
func (r *TaskReconciler) rerollDeploy(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, metricReason, contextMsg string) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	_ = r.deployLedger(task.Namespace).SetState(ctx, project.Name, task.Name, DeployStateFailed)

	// Exhausted auto-recovery: park recoverable for a human, comment the cause.
	if task.Status.ImplementGiveUps >= maxImplGiveUps {
		if err := r.clearDeployState(ctx, task, false); err != nil {
			return ctrl.Result{}, err
		}
		writer, token, provider := r.deployWriter(ctx, project, task)
		if writer != nil {
			msg := "Deploy cascade recovery is exhausted after repeated attempts; leaving this for a human. " + contextMsg
			if perr := r.parkWithComment(ctx, task, writer, token, deployParkReason, msg); perr != nil {
				return ctrl.Result{}, perr
			}
		} else {
			if perr := r.setDeployState(ctx, task, "Parked", deployParkReason); perr != nil {
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
	if err := r.setDeployState(ctx, task, "Implement", "deploy-failure"); err != nil {
		return ctrl.Result{}, err
	}
	l.Info("deploy: cascade failed; rerolled to Implement",
		"action", "deploy_reroll", "resource_id", task.Name, "reason", metricReason)
	return ctrl.Result{}, nil
}

// clearCascadeStatusFields resets the Deploying-phase cascade fields shared by
// clearDeployState and setTaskImplementContext.
func clearCascadeStatusFields(s *tatarav1alpha1.TaskStatus) {
	s.Phase = ""
	s.DeployDeadline = nil
	s.CascadeStage = ""
	s.DeployedVersion = ""
	s.DeployArtifact = ""
}

// clearDeployState clears the Deploying phase + deploy-supervision status fields.
// When bumpGiveup is set it also increments the auto-reroll attempt counter so a
// Deploying->Implement reroll consumes the shared ImplementGiveUps budget, and
// resets every openedPR member's per-member deploy state (WorkItemRef.
// DeployState/DeployedVersion) back to empty. bumpGiveup is set ONLY on the
// reroll-to-Implement branch (never the exhausted-park branch), so this is
// exactly the re-entry that needs it: without the reset, a member that already
// learned a cut version or confirmed applied before the reroll keeps that STALE
// state - the learn loop in reconcileDeployingUmbrella skips a member with a
// non-empty DeployedVersion, and the confirm loop checks that stale version
// against the NEW post-reroll pin state, which never matches, so the member can
// never re-confirm and a successful redeploy parks instead of resolving.
// Mutates the fresh read's own WorkItems slice in place (never assigns a
// snapshot wholesale), consistent with the UpsertWorkItem merge-safety pattern.
func (r *TaskReconciler) clearDeployState(ctx context.Context, task *tatarav1alpha1.Task, bumpGiveup bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		clearCascadeStatusFields(&fresh.Status)
		if bumpGiveup {
			fresh.Status.ImplementGiveUps++
			for i := range fresh.Status.WorkItems {
				wi := &fresh.Status.WorkItems[i]
				if wi.Kind == tatarav1alpha1.WorkItemPR && wi.Role == tatarav1alpha1.RoleOpenedPR {
					wi.DeployState = ""
					wi.DeployedVersion = ""
				}
			}
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

// releaseArtifact maps a tatara-helmfile release name to the component artifact
// (repo) whose version its chart `version:` line pins. Chart-version pin lines
// carry no artifact token themselves (just `version: X.Y.Z`), so they are
// attributed to the artifact of the enclosing `- name: <release>` block during
// the apply sweep. Keep in lockstep with parentMap's helmfile chart pins.
var releaseArtifact = map[string]string{
	"tatara-operator":        "tatara-operator",
	"project-tatara":         "tatara-operator",
	"project-infrastructure": "tatara-operator",
	"tatara-chat":            "tatara-chat",
}

// helmfileReleaseRe matches a `- name: <release>` line in helmfile.yaml.gotmpl so
// the chart `version:` pin that follows can be attributed to the right component.
var helmfileReleaseRe = regexp.MustCompile(`^\s*-\s*name:\s*(\S+)\s*$`)

// isVersionByte reports whether b can be part of a semver token (digit or dot),
// used as the token boundary so v1.4.1 does not match inside v1.4.10.
func isVersionByte(b byte) bool {
	return (b >= '0' && b <= '9') || b == '.'
}

// tokenMatch reports whether tok occurs in s as a whole version token: the
// characters immediately before and after the match must not be semver bytes.
// This is the substring fix - v1.4.1 no longer matches v1.4.10 (trailing '0' is a
// version byte) while v1.4.0 still matches `tag: "v1.4.0"` (trailing '"' is not).
func tokenMatch(s, tok string) bool {
	if tok == "" {
		return false
	}
	for idx := 0; ; {
		i := strings.Index(s[idx:], tok)
		if i < 0 {
			return false
		}
		i += idx
		var before, after byte = ' ', ' '
		if i > 0 {
			before = s[i-1]
		}
		if i+len(tok) < len(s) {
			after = s[i+len(tok)]
		}
		if !isVersionByte(before) && !isVersionByte(after) {
			return true
		}
		idx = i + 1
	}
}

// lineCarriesVersion reports whether line carries version as a whole token, in
// either the v-prefixed (image tag) or bare (chart version) form.
func lineCarriesVersion(line, version, bare string) bool {
	if tokenMatch(line, version) {
		return true
	}
	return bare != version && tokenMatch(line, bare)
}

// pinCarriesVersion reports whether the applied helmfile pin state references a
// component version anywhere (artifact-agnostic). Image pins carry the
// v-prefixed tag (`tag: "vX.Y.Z"`) while chart pins carry the bare
// `version: X.Y.Z`, so both forms are token-matched. Prefer
// pinCarriesArtifactVersion where the artifact is known: the artifact-agnostic
// form can be tripped by a sibling component sharing the same version string.
func pinCarriesVersion(pinState, version string) bool {
	if version == "" {
		return false
	}
	bare := strings.TrimPrefix(version, "v")
	for _, line := range strings.Split(pinState, "\n") {
		if lineCarriesVersion(line, version, bare) {
			return true
		}
	}
	return false
}

// pinCarriesArtifactVersion reports whether the applied helmfile pin state
// carries version on a pin line that belongs to artifact (the component repo
// name). This scopes the apply-outcome match to the entry's OWN pin so a sibling
// component sharing the same version string (plausible while every repo is seeded
// near low semvers) cannot prematurely resolve the wrong Task. Two attribution
// rules cover every parentMap pin shape:
//
//   - image pins embed the artifact in the image path: a line containing
//     "/<artifact>:" with the version as a whole token (e.g.
//     ".../tatara-memory:v1.4.0"). The trailing ':' keeps tatara-memory from
//     matching tatara-memory-repo-ingester.
//   - chart-version pins carry no artifact token, so they are attributed to the
//     artifact of the enclosing helmfile `- name: <release>` block (the operator
//     chart's bare version equals its image version, so the chart line alone
//     confirms the operator cascade without needing the artifact-token-less
//     `tag:` line in values/tatara-operator/common.yaml).
func pinCarriesArtifactVersion(pinState, artifact, version string) bool {
	if version == "" || artifact == "" {
		return false
	}
	bare := strings.TrimPrefix(version, "v")
	imageToken := "/" + artifact + ":"
	currentRelease := ""
	for _, line := range strings.Split(pinState, "\n") {
		if m := helmfileReleaseRe.FindStringSubmatch(line); m != nil {
			currentRelease = m[1]
			continue
		}
		if strings.Contains(line, imageToken) && lineCarriesVersion(line, version, bare) {
			return true
		}
		if releaseArtifact[currentRelease] == artifact && lineCarriesVersion(line, version, bare) {
			return true
		}
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
	// CD-health gauges (G5): count cascades currently in a durable failed/stalled
	// state and publish them at the end of the scan. Derived from authoritative
	// Task state (not per-event counters) so max()>0 means "broken right now" and
	// the gauge self-clears once a reroll or a human resolves the cascade.
	var failed, stalled int
	for i := range existing {
		t := &existing[i]
		if t.Spec.ProjectRef != proj.Name {
			continue
		}
		// Durable failed: parked recoverable after the bounded auto-reroll budget was
		// spent (rerollDeploy exhausted branch parks with deployParkReason), or a
		// discrete umbrella whose member PR(s) stayed unmerged past the merge-wait
		// budget (parkUmbrellaMergeTimeout, mergeParkReason). Both surface a stuck
		// stream a human must resolve.
		if t.Status.DeployState == "Parked" &&
			(t.Status.ParkReason == deployParkReason || t.Status.ParkReason == mergeParkReason) {
			failed++
			continue
		}
		if !tatarav1alpha1.TaskDeploying(t) {
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
		if t.Status.ImplementGiveUps >= maxImplGiveUps {
			// Liveness finding #1: a stalled Deploying cascade with no auto-recovery
			// left USED to sit DEPLOYING forever (non-terminal, no comment, never
			// GC-eligible) - a silent permanent wedge. It must instead PARK terminal
			// (recoverable, reaper-eligible) with an issue comment naming the stuck
			// artifact so the stall is surfaced with a human signal. Once parked it is
			// counted failed (top-of-loop Parked+deployParkReason branch) and never
			// re-commented (parkStalledDeploy is a no-op when already parked).
			r.parkStalledDeploy(ctx, proj, t)
			failed++
			continue
		}
		// Reroll: clear the Deploying phase and re-enter Implement to fix the stalled
		// cascade, in ONE guarded status write that re-asserts the still-Deploying +
		// deadline-exceeded precondition against the freshly-read object. `existing`
		// is a snapshot; a concurrent resolveDeployedSweep may have resolved this
		// Task to Done (Phase cleared) since it was listed, and a blind reroll would
		// drag an already-resolved change back to Implement -> a DUPLICATE production
		// deploy. The guard aborts the reroll in that case.
		rerolled, err := r.cdRerollStalled(ctx, proj, t, now,
			"The push-CD deploy cascade for this change stalled (no tatara-helmfile apply within the backstop window). Investigate the cascade (component tag, parent bump PR, helmfile apply) and push a fix; the change is merged but not deployed.")
		if err != nil {
			l.Error(err, "cdScan: reroll stalled deploy task", "resource_id", t.Name)
			continue
		}
		if !rerolled {
			l.Info("cdScan: stalled deploy task resolved concurrently; skipping reroll",
				"action", "cd_scan_reroll_skipped", "resource_id", t.Name)
			continue
		}
		l.Info("cdScan: rerolled stalled deploy cascade",
			"action", "cd_scan_reroll", "resource_id", t.Name, "artifact", t.Status.DeployArtifact)
	}
	r.Metrics.SetCDCascadeFailed(proj.Name, float64(failed))
	r.Metrics.SetCDCascadeStalled(proj.Name, float64(stalled))
}

// cdRerollStalled re-enters a stalled Deploying Task to Implement to fix the
// cascade, in ONE guarded status write (ProjectReconciler path; combines the old
// setTaskImplementContext + adoptLifecycleTaskAt("Implement") so the transition is
// atomic). It re-asserts the precondition against the FRESHLY-READ object inside
// the retry: the Task must still be in the pod-less Deploying phase AND its deploy
// deadline must still be exceeded. A concurrent resolveDeployedSweep may have
// flipped the Task to Done (Phase cleared, DeployState="Done") after the cdScan
// snapshot; rerolling then would resurrect a resolved change into a duplicate
// production deploy. Returns (false, nil) when the precondition no longer holds
// (reroll skipped, no mutation). It bumps ImplementGiveUps (the reroll consumes the
// shared auto-reroll budget) and re-arms the lifecycle clocks.
func (r *ProjectReconciler) cdRerollStalled(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, now time.Time, msg string) (bool, error) {
	idleMinutes := 60
	if proj.Spec.Scm != nil && proj.Spec.Scm.ConversationIdleMinutes > 0 {
		idleMinutes = proj.Spec.Scm.ConversationIdleMinutes
	}
	rerolled := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rerolled = false
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		// Precondition, re-asserted on the fresh read: still Deploying AND still past
		// the deploy deadline. If the cascade resolved to Done concurrently, Phase is
		// cleared (TaskDeploying==false) -> abort without mutating.
		if !tatarav1alpha1.TaskDeploying(fresh) {
			return nil
		}
		dl := fresh.Status.DeployDeadline
		if dl == nil || !now.After(dl.Time) {
			return nil
		}
		fresh.Status.ImplementContext = msg
		clearCascadeStatusFields(&fresh.Status)
		fresh.Status.ImplementGiveUps++
		fresh.Status.DeployState = "Implement"
		fresh.Status.ImplementEmptyRetries = 0
		fresh.Status.ParkReason = ""
		reNow := metav1.Now()
		deadline := metav1.NewTime(reNow.Add(time.Duration(idleMinutes) * time.Minute))
		fresh.Status.LastActivityAt = &reNow
		fresh.Status.DeadlineAt = &deadline
		rerolled = true
		return r.Status().Update(ctx, fresh)
	})
	return rerolled, err
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

// umbrellaDeployEligible reports whether a Task is a discrete-implement umbrella
// stream this sweep should supervise into Deploying: a pushCDEligible implement
// Task sourced from an issue that is not yet Deploying / terminal-resolved /
// parked. It is the durable double-deploy fence: Deploying is owned by
// reconcileDeploying, and a Done/Parked stream is finished.
func umbrellaDeployEligible(task *tatarav1alpha1.Task, projName string) bool {
	if task.Spec.ProjectRef != projName || task.Spec.Kind != "implement" {
		return false
	}
	if task.Spec.Source == nil || task.Spec.Source.IssueRef == "" || task.Spec.Source.IsPR {
		return false
	}
	if !pushCDEligible(task) {
		return false
	}
	if tatarav1alpha1.TaskDeploying(task) {
		return false
	}
	if task.Status.DeployState == "Done" || task.Status.DeployState == "Parked" {
		return false
	}
	return true
}

// openedPRDeployMembers returns the role:openedPR PR members of an umbrella Task
// that are deploy candidates: a real PR number that is not closed/declined. A
// MERGED member IS included (it is exactly what deploys) - this differs from
// umbrellaPRMembers, which excludes WIMerged for the review path.
func openedPRDeployMembers(task *tatarav1alpha1.Task) []tatarav1alpha1.WorkItemRef {
	var out []tatarav1alpha1.WorkItemRef
	for _, wi := range task.Status.WorkItems {
		if wi.Kind == tatarav1alpha1.WorkItemPR && wi.Role == tatarav1alpha1.RoleOpenedPR &&
			wi.Number > 0 && wi.State != tatarav1alpha1.WIClosed && wi.State != tatarav1alpha1.WIDeclined {
			out = append(out, wi)
		}
	}
	return out
}

// allMembersMerged reports whether every deploy-candidate member has merged.
func allMembersMerged(members []tatarav1alpha1.WorkItemRef) bool {
	for _, m := range members {
		if m.State != tatarav1alpha1.WIMerged {
			return false
		}
	}
	return len(members) > 0
}

// allMembersApplied reports whether every deploy-candidate member is confirmed
// applied (confirm-all: the gate to close the originating issue).
func allMembersApplied(members []tatarav1alpha1.WorkItemRef) bool {
	for _, m := range members {
		if m.DeployState != memberDeployStateApplied {
			return false
		}
	}
	return len(members) > 0
}

// unmergedMemberRepos returns the repo names of members not yet merged.
func unmergedMemberRepos(members []tatarav1alpha1.WorkItemRef) []string {
	var out []string
	for _, m := range members {
		if m.State != tatarav1alpha1.WIMerged {
			out = append(out, repoNameFromSlug(m.Repo))
		}
	}
	return out
}

// unappliedMemberRepos returns the repo names of members not yet confirmed applied.
func unappliedMemberRepos(members []tatarav1alpha1.WorkItemRef) []string {
	var out []string
	for _, m := range members {
		if m.DeployState != memberDeployStateApplied {
			out = append(out, repoNameFromSlug(m.Repo))
		}
	}
	return out
}

// repoNameFromSlug returns the repo name (last path segment) of an "owner/name"
// slug; it is the artifact identity for LatestSemverTag / pinCarriesArtifactVersion.
func repoNameFromSlug(slug string) string {
	if i := strings.LastIndexByte(slug, '/'); i >= 0 {
		return slug[i+1:]
	}
	return slug
}

// umbrellaDeployBudget is the Deploying deadline budget for a multi-member
// umbrella: the MAX per-member deploy budget (the slowest member's path bounds the
// whole stream). Falls back to the multi-hop budget when no member repo resolves.
func umbrellaDeployBudget(proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) time.Duration {
	var max time.Duration
	for _, m := range openedPRDeployMembers(task) {
		if b := deployBudget(proj, repoNameFromSlug(m.Repo)); b > max {
			max = b
		}
	}
	if max == 0 {
		max = deployBudget(proj, "") // multi-hop fallback
	}
	return max
}

// mergeWaitBudget is the pre-merge (review + merge) wait window before an umbrella
// with unmerged members is parked recoverable (item 3).
func mergeWaitBudget(proj *tatarav1alpha1.Project) time.Duration {
	if proj.Spec.MergeWaitBudgetMinutes > 0 {
		return time.Duration(proj.Spec.MergeWaitBudgetMinutes) * time.Minute
	}
	return defaultMergeWaitBudget
}
