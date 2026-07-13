package controller

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// THE DEPLOYING STAGE HAS AN ACTOR, AND THE MERGEREQUEST CR IS THE LEDGER.
//
// F.3 gates deploying -> delivered on "every owned MR merged AND deployedAt !=
// nil" (merge.go, CloseIssuesOnDelivery). The contract names the READER of
// deployedAt and never names its WRITER: v7 has exactly one reference to the
// field in the whole operator, and it is that read. Nothing stamped it, so
// deploying was a black hole - every Task sat there until the 2h budget parked
// it.
//
// ReconcileDeploying is the writer. It re-points the SURVIVING push-CD
// machinery (the DeployWatcher poll, the helmfile pin state, deployPinFiles,
// pinCarriesArtifactVersion's attribution rules) off the deleted phase machine
// model and onto the MergeRequest CR:
//
//	merged MR -> component cuts its semver tag -> the pin propagates ->
//	tatara-helmfile's apply.yaml goes green carrying it -> stamp
//	mr.status.deployedAt + mr.status.deployedVersion.
//
// The per-Project deploy-ledger ConfigMap (deploy_ledger.go) is NOT used: the MR
// CR is a strictly better ledger - it is per-artifact, durable in etcd, owned by
// the Task, and it is already what the delivery gate reads.
//
// TWO FAIL-OPEN RULES, both there because a WEDGE IS WORSE THAN AN EARLY
// DELIVERY on a change that is already merged:
//
//  1. NO OBSERVABLE CASCADE (the reader is not a scm.DeployWatcher, or the
//     Project enrols no tatara-helmfile): merge IS delivery.
//  2. THE REPO CARRIES NO HELMFILE PIN AT ALL (tatara-documentation,
//     tatara-observability, any repo the cluster does not run): there is no pin
//     that will ever move, so waiting for one is an infinite wait. Merge is
//     delivery.
//
// Everything else waits for the apply, and the F.4 deploying budget (2h ->
// parked(deploy-timeout), bounded by deployReentries) is what ends the wait.

// deployStageRequeue paces the deploying poll.
const deployStageRequeue = 60 * time.Second

// ReconcileDeploying drives ONE Task through the deploying stage: it stamps
// deployedAt on every owned MR whose merge the apply sweep has observed applied,
// and hands off to the C.4 delivery postcondition once they all carry it.
func (d *StageDriver) ReconcileDeploying(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task) (ctrl.Result, error) {
	mrs, err := ownedMergeRequests(ctx, d.Client, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	// THE EMPTY SET IS NOT A LICENCE (C.4): a Task in deploying that owns no
	// MergeRequest has delivered nothing. Its budget parks it.
	if len(mrs) == 0 {
		return ctrl.Result{RequeueAfter: deployStageRequeue}, nil
	}

	pending := 0
	for i := range mrs {
		mr := &mrs[i]
		if mr.Status.State != "merged" {
			pending++
			continue
		}
		if mr.Status.DeployedAt != nil {
			continue
		}
		deployed, err := d.resolveDeployed(ctx, proj, task, mr)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !deployed {
			pending++
		}
	}
	if pending > 0 {
		return ctrl.Result{RequeueAfter: deployStageRequeue}, nil
	}

	// Every owned MR is merged AND deployed: close the Issues, THEN stamp
	// deliveredAt. In that order (Section I).
	if err := d.CloseIssuesOnDelivery(ctx, proj, task); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// resolveDeployed reports whether one merged MR's change is live on the cluster,
// stamping deployedAt + deployedVersion when it is. A false with a nil error is
// "not yet": the caller polls.
func (d *StageDriver) resolveDeployed(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, mr *tatarav1alpha1.MergeRequest) (bool, error) {
	l := log.FromContext(ctx)

	var repo tatarav1alpha1.Repository
	if err := d.Get(ctx, types.NamespacedName{Namespace: mr.Namespace, Name: mr.Spec.RepositoryRef}, &repo); err != nil {
		return false, fmt.Errorf("deploy: get repository %s: %w", mr.Spec.RepositoryRef, err)
	}
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return false, fmt.Errorf("deploy: owner/repo for %s: %w", repo.Name, err)
	}
	_, token, provider, err := d.forge(ctx, proj)
	if err != nil {
		return false, err
	}
	reader, err := d.reader(provider, token)
	if err != nil {
		return false, err
	}

	dw, isWatcher := reader.(scm.DeployWatcher)
	hfOwner, hfRepo, hfFound := helmfileRepoFor(ctx, d.Client, proj)
	if !isWatcher || !hfFound {
		// FAIL-OPEN 1: no observable cascade. The cd-release cascade is GitHub-only
		// and terminates at tatara-helmfile; without both, nothing will ever confirm
		// this merge, and the Task would sit here until its budget parked it.
		l.Info("deploy: no observable cascade for this Project; the merge IS the delivery",
			"action", "deploy_unsupervised", "resource_id", task.Name, "repo", name,
			"pr", mr.Spec.Number, "provider", provider, "deploy_watcher", isWatcher, "helmfile_enrolled", hfFound)
		return true, d.stampDeployed(ctx, proj, task, mr, "")
	}

	// The terminal CD repo itself (a helmfile revert MR): it cuts no tag and
	// carries no pin of its own. A completed, successful apply.yaml run that
	// STARTED AFTER the merge landed on main necessarily built from a tree that
	// contains it.
	if name == helmfileRepoName {
		run, ok, err := d.applyRun(ctx, dw, task, owner, name)
		if err != nil || !ok || !applyPostdatesMerge(run, mr) {
			return false, err
		}
		return true, d.stampDeployed(ctx, proj, task, mr, helmfileRepoName+"@"+shortSHA(run.HeadSHA))
	}

	// The version the merged component cut. ReconcileMerging already waited for
	// the release job at the merge commit to go green before it left merging, so
	// by here the tag for THIS merge exists (a later, higher tag is fine: it
	// contains this merge).
	tag, tagFound, err := dw.LatestSemverTag(ctx, owner, name)
	if err != nil {
		l.Error(err, "deploy: read latest semver tag (requeue)",
			"action", "deploy_poll", "resource_id", task.Name, "repo", name)
		return false, nil
	}

	// FAIL-OPEN 2: is this repo deployed by the helmfile AT ALL? Probed at the
	// helmfile's main, not at an apply run, so a repo with no pin does not first
	// have to wait for somebody else's apply to find that out.
	mainPin, err := helmfilePinState(ctx, dw, hfOwner, hfRepo, "main")
	if err != nil {
		l.Error(err, "deploy: read helmfile pin state at main (requeue)",
			"action", "deploy_poll", "resource_id", task.Name, "repo", name)
		return false, nil
	}
	if !pinArtifactPresent(mainPin, name) {
		l.Info("deploy: the helmfile carries no pin for this repo; the merge IS the delivery",
			"action", "deploy_not_pinned", "resource_id", task.Name, "repo", name, "pr", mr.Spec.Number)
		return true, d.stampDeployed(ctx, proj, task, mr, tag)
	}
	if !tagFound {
		l.Info("deploy: component tag not cut yet; waiting",
			"action", "deploy_await_tag", "resource_id", task.Name, "repo", name)
		return false, nil
	}

	run, ok, err := d.applyRun(ctx, dw, task, hfOwner, hfRepo)
	if err != nil || !ok || !applyPostdatesMerge(run, mr) {
		return false, err
	}
	appliedPin, err := helmfilePinState(ctx, dw, hfOwner, hfRepo, run.HeadSHA)
	if err != nil {
		l.Error(err, "deploy: read applied helmfile pin state (requeue)",
			"action", "deploy_poll", "resource_id", task.Name, "sha", run.HeadSHA)
		return false, nil
	}
	if !pinAtOrPastArtifactVersion(appliedPin, name, tag) {
		return false, nil
	}
	l.Info("deploy: the apply carried this change",
		"action", "deploy_applied", "resource_id", task.Name, "repo", name,
		"pr", mr.Spec.Number, "version", tag, "apply_sha", run.HeadSHA, "run_url", run.HTMLURL)
	return true, d.stampDeployed(ctx, proj, task, mr, tag)
}

// applyRun returns the latest COMPLETED, SUCCESSFUL tatara-helmfile apply run on
// main. A failed apply is not an error here: it is "not applied", and the
// deploying budget is what ends the wait (F.4 -> parked(deploy-timeout)).
func (d *StageDriver) applyRun(ctx context.Context, dw scm.DeployWatcher, task *tatarav1alpha1.Task,
	owner, repo string) (scm.WorkflowRun, bool, error) {
	run, found, err := dw.LatestWorkflowRun(ctx, owner, repo, applyWorkflowFile, "main")
	if err != nil {
		log.FromContext(ctx).Error(err, "deploy: read helmfile apply run (requeue)",
			"action", "deploy_poll", "resource_id", task.Name, "repo", repo)
		return scm.WorkflowRun{}, false, nil
	}
	if !found || run.Status != "completed" || run.Conclusion != "success" {
		return scm.WorkflowRun{}, false, nil
	}
	return run, true, nil
}

// applyPostdatesMerge reports whether an apply run STARTED after the merge it is
// being credited with. An apply that ran before the merge commit existed cannot
// have carried it - and a pin state that happens to satisfy the version match is
// then somebody else's release, not ours.
func applyPostdatesMerge(run scm.WorkflowRun, mr *tatarav1alpha1.MergeRequest) bool {
	if mr.Status.MergedAt == nil {
		return true
	}
	return !run.CreatedAt.Before(mr.Status.MergedAt.Time)
}

// stampDeployed writes the delivery evidence onto the MergeRequest. It is the
// SOLE writer of status.deployedAt.
func (d *StageDriver) stampDeployed(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, mr *tatarav1alpha1.MergeRequest, version string) error {
	now := metav1.NewTime(d.now())
	key := client.ObjectKeyFromObject(mr)
	if err := objbudget.FitMergeRequest(ctx, d.Client, d.spiller(proj), key, func(m *tatarav1alpha1.MergeRequest) {
		if m.Status.DeployedAt == nil {
			m.Status.DeployedAt = &now
		}
		if version != "" {
			m.Status.DeployedVersion = version
		}
	}); err != nil {
		return fmt.Errorf("deploy: stamp deployed on %s: %w", key.Name, err)
	}
	mr.Status.DeployedAt = &now
	if version != "" {
		mr.Status.DeployedVersion = version
	}
	log.FromContext(ctx).Info("deploy: merge request deployed",
		"action", "mr_deployed", "resource_id", task.Name, "repo", mr.Spec.RepositoryRef,
		"pr", mr.Spec.Number, "version", version)
	return nil
}

// helmfileRepoFor resolves the Project's terminal CD repo (tatara-helmfile). A
// Project that does not enrol it has no cascade to watch.
func helmfileRepoFor(ctx context.Context, c client.Client, proj *tatarav1alpha1.Project) (string, string, bool) {
	var list tatarav1alpha1.RepositoryList
	if err := c.List(ctx, &list, client.InNamespace(proj.Namespace)); err != nil {
		return "", "", false
	}
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef != proj.Name {
			continue
		}
		owner, name, err := scm.OwnerRepo(list.Items[i].Spec.URL)
		if err == nil && name == helmfileRepoName {
			return owner, name, true
		}
	}
	return "", "", false
}

// semverRe matches the first vX.Y.Z / X.Y.Z token on a pin line.
var semverRe = regexp.MustCompile(`v?(\d+)\.(\d+)\.(\d+)`)

// parseSemver reads the first semver token out of s.
func parseSemver(s string) ([3]int, bool) {
	m := semverRe.FindStringSubmatch(s)
	if m == nil {
		return [3]int{}, false
	}
	var out [3]int
	for i := 0; i < 3; i++ {
		n, err := strconv.Atoi(m[i+1])
		if err != nil {
			return [3]int{}, false
		}
		out[i] = n
	}
	return out, true
}

func semverAtLeast(have, want [3]int) bool {
	for i := 0; i < 3; i++ {
		if have[i] != want[i] {
			return have[i] > want[i]
		}
	}
	return true
}

// artifactPinLines yields the lines of an applied helmfile pin state that belong
// to artifact, under the SAME two attribution rules pinCarriesArtifactVersion
// uses: an image pin embeds "/<artifact>:" in the image path, and a bare chart
// `version:` pin belongs to the artifact of its enclosing `- name: <release>`
// block.
func artifactPinLines(pinState, artifact string) []string {
	if artifact == "" {
		return nil
	}
	imageToken := "/" + artifact + ":"
	currentRelease := ""
	var out []string
	for _, line := range strings.Split(pinState, "\n") {
		if m := helmfileReleaseRe.FindStringSubmatch(line); m != nil {
			currentRelease = m[1]
			continue
		}
		if strings.Contains(line, imageToken) || releaseArtifact[currentRelease] == artifact {
			out = append(out, line)
		}
	}
	return out
}

// pinArtifactPresent reports whether the helmfile deploys artifact AT ALL: a pin
// line for it, carrying a version, exists. A repo with none of those is not part
// of the cluster's deploy state and cannot be waited for.
func pinArtifactPresent(pinState, artifact string) bool {
	for _, line := range artifactPinLines(pinState, artifact) {
		if _, ok := parseSemver(line); ok {
			return true
		}
	}
	return false
}

// pinAtOrPastArtifactVersion reports whether the applied pin state carries
// artifact at version OR PAST IT. The ">=" is deliberate and it is the fix for a
// real wedge: the pins only ever move FORWARD, so an apply that carries a HIGHER
// version of this artifact necessarily contains this one. An exact-equality match
// (pinCarriesArtifactVersion, the pre-cutover form) leaves a Task whose own apply
// was superseded by the next release waiting for a pin that has already gone past
// it, until the deploy budget parks it.
//
// An unparseable version falls back to the exact token match.
func pinAtOrPastArtifactVersion(pinState, artifact, version string) bool {
	want, ok := parseSemver(version)
	if !ok {
		return pinCarriesArtifactVersion(pinState, artifact, version)
	}
	for _, line := range artifactPinLines(pinState, artifact) {
		if have, ok := parseSemver(line); ok && semverAtLeast(have, want) {
			return true
		}
	}
	return false
}
