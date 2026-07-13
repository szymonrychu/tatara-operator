package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ONE WRITER PER MEDIUM (contract C.4, C.5).
//
// The agent writes the CONVERSATION. The OPERATOR writes the reviews, the
// merges, the labels and the status. StageDriver is that operator egress: it is
// the SOLE merge caller (through mergePRSquash, the single writer.Merge call
// site) and the SOLE review poster (DrainPendingReview).
//
// Nothing here trusts the mirror for a decision. status.headSHA can be an hour
// stale, and a merge pinned to a stale SHA is a TOCTOU hole on the repo that
// deploys the cluster: every merge re-reads the head LIVE and pins the merge to
// it.
type StageDriver struct {
	client.Client
	// SCMFor returns the provider's writer (the merge + review egress).
	SCMFor func(provider string) (scm.SCMWriter, error)
	// ReaderFor returns a token-bound reader: the release-job CI status at the
	// merge commit, and the thread listings the pending-comment drain dedups
	// against.
	ReaderFor func(provider, token string) (scm.SCMReader, error)
	// SpillerFor returns the tatara-memory spiller for the A.7 byte-budget guard.
	SpillerFor func(proj *tatarav1alpha1.Project) objbudget.Spiller
	// Metrics carries the D1 terminal counter. The driver enters BOTH terminal
	// stages the platform actually reaches in anger - failed(merge-*) and
	// delivered - so a driver without it makes 29 tatara-observability rules report
	// OK forever. It routes through the EnterStage choke point, which is what makes
	// the emit total rather than a thing each call site remembers.
	Metrics *obs.OperatorMetrics
	// Now is the clock, injectable in tests.
	Now func() time.Time
}

// mergeRequeue paces the merging stage: a PR waiting on CI, on mergeability, or
// on its release job. The 4h merging budget (F.4) is what ends the wait.
const mergeRequeue = 60 * time.Second

func (d *StageDriver) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d *StageDriver) spiller(proj *tatarav1alpha1.Project) objbudget.Spiller {
	if d.SpillerFor == nil {
		return nil
	}
	return d.SpillerFor(proj)
}

// forge resolves (writer, token, provider) for a Project. It is the one place
// the driver reaches for credentials.
func (d *StageDriver) forge(ctx context.Context, proj *tatarav1alpha1.Project) (scm.SCMWriter, string, string, error) {
	provider := "github"
	if proj.Spec.Scm != nil && proj.Spec.Scm.Provider != "" {
		provider = proj.Spec.Scm.Provider
	}
	if d.SCMFor == nil {
		return nil, "", provider, fmt.Errorf("merge: no SCM writer wired")
	}
	writer, err := d.SCMFor(provider)
	if err != nil {
		return nil, "", provider, fmt.Errorf("merge: scm writer: %w", err)
	}
	token, err := mirrorSCMToken(ctx, d.Client, proj)
	if err != nil {
		return nil, "", provider, err
	}
	return writer, token, provider, nil
}

// ownedMergeRequests returns every MergeRequest the Task CONTROLLER-owns.
func ownedMergeRequests(ctx context.Context, c client.Client, task *tatarav1alpha1.Task) ([]tatarav1alpha1.MergeRequest, error) {
	var list tatarav1alpha1.MergeRequestList
	if err := c.List(ctx, &list, client.InNamespace(task.Namespace)); err != nil {
		return nil, fmt.Errorf("merge: list mergerequests: %w", err)
	}
	out := make([]tatarav1alpha1.MergeRequest, 0, len(list.Items))
	for i := range list.Items {
		if ctrl, ok := own.ControllerOwner(&list.Items[i]); ok && ctrl == task.Name {
			out = append(out, list.Items[i])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ownedIssueCRs returns every Issue the Task CONTROLLER-owns.
func ownedIssueCRs(ctx context.Context, c client.Client, task *tatarav1alpha1.Task) ([]tatarav1alpha1.Issue, error) {
	var list tatarav1alpha1.IssueList
	if err := c.List(ctx, &list, client.InNamespace(task.Namespace)); err != nil {
		return nil, fmt.Errorf("merge: list issues: %w", err)
	}
	out := make([]tatarav1alpha1.Issue, 0, len(list.Items))
	for i := range list.Items {
		if ctrl, ok := own.ControllerOwner(&list.Items[i]); ok && ctrl == task.Name {
			out = append(out, list.Items[i])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func mrForRepo(mrs []tatarav1alpha1.MergeRequest, repo string) *tatarav1alpha1.MergeRequest {
	for i := range mrs {
		if mrs[i].Spec.RepositoryRef == repo && mrs[i].Status.State != "closed" {
			return &mrs[i]
		}
	}
	return nil
}

// UnexpectedMerge reports whether mr is MERGED on the forge while task's
// mergeCursor never advanced past its repo (contract C.9). The operator is the
// SOLE merge caller, so this can only be a human, or a native auto-merge armed
// before the cutover: the sequential mergeOrder was bypassed.
func UnexpectedMerge(task *tatarav1alpha1.Task, mr *tatarav1alpha1.MergeRequest) bool {
	if mr.Status.State != "merged" {
		return false
	}
	for i, repo := range task.Spec.MergeOrder {
		if repo == mr.Spec.RepositoryRef {
			return i >= task.Status.MergeCursor
		}
	}
	return true
}

// RecordUnexpectedMerge fires the C.9 accepted-risk DETECTOR
// (operator_unexpected_merge_total{repo}) when UnexpectedMerge holds. It returns
// whether it fired.
func RecordUnexpectedMerge(task *tatarav1alpha1.Task, mr *tatarav1alpha1.MergeRequest) bool {
	if !UnexpectedMerge(task, mr) {
		return false
	}
	obs.UnexpectedMergeTotal.WithLabelValues(mr.Spec.RepositoryRef).Inc()
	return true
}

// mergeAllowed enforces MergePolicy. autoMergeOnGreenCI waits for green CI;
// absent CI falls back to afterApproval (trust pr_outcome=merge as the agent's
// relay of an approving signal). st is the PR state fetched by the caller.
//
// afterApproval is an intentional trust-the-agent policy: the bot's pr_outcome=merge
// signal is treated as the agent relaying an approving signal (human review happened
// outside this gate). It does NOT consult live PR review state. If real approval
// gating is required, use autoMergeOnGreenCI combined with a branch protection rule
// requiring an approved review before CI can pass.
func (r *TaskReconciler) mergeAllowed(proj *tatarav1alpha1.Project, st scm.PRState) bool {
	policy := "afterApproval"
	if proj.Spec.Scm != nil && proj.Spec.Scm.MergePolicy != "" {
		policy = proj.Spec.Scm.MergePolicy
	}
	if policy == "autoMergeOnGreenCI" {
		if st.CIStatus == "success" {
			return true
		}
		if st.CIStatus != "" {
			return false // CI present but not green
		}
		// CI absent -> fall back to afterApproval below.
	}
	// afterApproval: trust pr_outcome=merge as the agent's relay of an approving signal.
	return true
}

// mergePolicyAllows is the MergePolicy gate applied to the operator's own merge.
// The receiver is unused by mergeAllowed.
func mergePolicyAllows(proj *tatarav1alpha1.Project, st scm.PRState) bool {
	var r TaskReconciler
	return r.mergeAllowed(proj, st)
}

// ReconcileMerging is contract C.5.2: the SEQUENTIAL, dependency-ordered merge.
//
// It is idempotent at every step. The cursor only advances past a repo whose MR
// is merged AND whose release job is green, and every merge is pinned to the
// head that was actually REVIEWED - a head that moved is re-reviewed, never
// merged.
func (d *StageDriver) ReconcileMerging(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	// fix C4 / V7-1: a kind=review Task NEVER merges. Not on approve, not on
	// request_changes, not from any un-park rule. stage.LegalFor refuses the
	// transition INTO merging; this is the last line of defence behind it, because
	// what is on the other side of this call is an irreversible write to a human's
	// pull request.
	if task.Spec.Kind == "review" {
		return ctrl.Result{}, fmt.Errorf("merge: refusing to merge on a kind=review Task %s: merging a human's PR is a HUMAN action", task.Name)
	}

	// fix C2: mergeOrder is resolved at /outcome (a single-repo change needs no
	// order at all). An EMPTY one HERE is a bug, and it is treated as one.
	if len(task.Spec.MergeOrder) == 0 {
		obs.ClearMergeCursorStalled(task.Name)
		l.Error(nil, "merge: merging entered with an empty mergeOrder",
			"action", "merge_order_missing", "resource_id", task.Name)
		return ctrl.Result{}, d.enterStage(ctx, proj, task, tatarav1alpha1.StageFailed, stage.ReasonMergeOrderMissing, nil)
	}

	mrs, err := ownedMergeRequests(ctx, d.Client, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	writer, token, provider, err := d.forge(ctx, proj)
	if err != nil {
		return ctrl.Result{}, err
	}

	cursor := task.Status.MergeCursor
	for i := cursor; i < len(task.Spec.MergeOrder); i++ {
		repoRef := task.Spec.MergeOrder[i]
		mr := mrForRepo(mrs, repoRef)
		if mr == nil {
			obs.ClearMergeCursorStalled(task.Name)
			l.Error(nil, "merge: mergeOrder names a repo this Task owns no open MR for",
				"action", "merge_operator_error", "resource_id", task.Name, "repo", repoRef)
			return ctrl.Result{}, d.enterStage(ctx, proj, task, tatarav1alpha1.StageFailed, stage.ReasonOperatorError, mrs)
		}

		// IDEMPOTENT RESUME: an MR already merged (by us on an earlier pass, or by
		// the mirror sync's own read) advances the cursor and nothing else.
		if mr.Status.State == "merged" {
			cursor = i + 1
			continue
		}

		var repo tatarav1alpha1.Repository
		if err := d.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: repoRef}, &repo); err != nil {
			return ctrl.Result{}, fmt.Errorf("merge: get repository %s: %w", repoRef, err)
		}

		// THE SEMVER LABEL LANDS BEFORE THE MERGE (H.4). CI cuts the release tag
		// from the label AT THE MERGE COMMIT: a merge that lands first and a label
		// that lands second is a release that is never tagged. The MergeRequest
		// reconciler has almost certainly projected it already; this is the ORDERED
		// guarantee, and it is idempotent.
		if err := d.ProjectSemverLabel(ctx, proj, &repo, mr); err != nil {
			return ctrl.Result{}, err
		}
		if mr.Status.Significance == "" {
			// THE WEDGE, MADE LOUD. No significance -> no label -> CI cuts no tag ->
			// nothing publishes -> no pin propagates -> deployedAt is never stamped ->
			// this Task sits in deploying until its budget parks it.
			//
			// /outcome REQUIRES changeSignificance on action=submitted, so reaching
			// here means an operator bug or a hand-mutated MergeRequest. The merge
			// still proceeds: a human can label and tag an already-merged commit,
			// whereas stalling here would strand a reviewed, approved change behind
			// a bug in the operator. The counter is what an alert reads.
			obs.SemverLabelMissingTotal.WithLabelValues(repoRef).Inc()
			l.Error(nil, "merge: MR carries no declared change significance; CI will cut NO release tag",
				"action", "semver_label_missing", "resource_id", task.Name,
				"repo", repoRef, "pr", mr.Spec.Number)
		}

		// THE LIVE HEAD. Never the mirror.
		liveHead, err := writer.GetPRHead(ctx, repo.Spec.URL, token, mr.Spec.Number)
		RecordSCM(d.Metrics, provider, "get_pr_head", err)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("merge: get pr head %s!%d: %w", repoRef, mr.Spec.Number, err)
		}
		if liveHead != mr.Status.ReviewedSHA {
			return ctrl.Result{}, d.headMoved(ctx, proj, task, mrs, mr, cursor,
				"the live head moved off the reviewed SHA")
		}

		st, err := writer.GetPRState(ctx, repo.Spec.URL, token, mr.Spec.Number)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("merge: get pr state %s!%d: %w", repoRef, mr.Spec.Number, err)
		}
		if st.Merged {
			// Merged out from under the operator - the C.9 accepted risk, or our own
			// merge from a pass that died before it could stamp. Record it and resume.
			RecordUnexpectedMerge(task, mr)
			if err := d.stampMerged(ctx, proj, mr); err != nil {
				return ctrl.Result{}, err
			}
			cursor = i + 1
			continue
		}
		if st.CIStatus != "success" || !mergePolicyAllows(proj, st) {
			return d.stallMerge(ctx, proj, task, repoRef, cursor, "ci-not-green")
		}
		ms, err := writer.GetMergeState(ctx, repo.Spec.URL, token, mr.Spec.Number)
		RecordSCM(d.Metrics, provider, "get_merge_state", err)
		if err == nil && (ms == scm.MergeStateDirty || ms == scm.MergeStateBlocked) {
			return d.stallMerge(ctx, proj, task, repoRef, cursor, "not-mergeable")
		}

		// THE SINGLE writer.Merge EGRESS, now pinned to the reviewed head.
		sha, mergeErr := mergePRSquash(ctx, writer, repo.Spec.URL, token, mr.Spec.Number, liveHead)
		RecordSCM(d.Metrics, provider, "merge", mergeErr)
		switch {
		case mergeErr == nil:
		case errors.Is(mergeErr, scm.ErrHeadMoved):
			// The TOCTOU close: the head moved between the read and the merge.
			return ctrl.Result{}, d.headMoved(ctx, proj, task, mrs, mr, cursor,
				"Merge 409'd: the head moved since the review")
		case errors.Is(mergeErr, scm.ErrMergeConflict):
			l.Info("merge: PR not mergeable; waiting",
				"action", "merge_blocked", "resource_id", task.Name, "repo", repoRef, "pr", mr.Spec.Number)
			return d.stallMerge(ctx, proj, task, repoRef, cursor, "merge-conflict")
		default:
			return ctrl.Result{}, fmt.Errorf("merge: %s!%d: %w", repoRef, mr.Spec.Number, mergeErr)
		}
		l.Info("merge: merged",
			"action", "scm_merged", "resource_id", task.Name, "repo", repoRef,
			"pr", mr.Spec.Number, "head_sha", liveHead, "merge_sha", sha, "provider", provider)

		if err := d.stampMerged(ctx, proj, mr); err != nil {
			return ctrl.Result{}, err
		}
		// Wait for the release job at sha to go green before the NEXT repo merges:
		// the sequential order exists precisely so a dependent repo never ships
		// against a parent that did not publish.
		green, err := d.releaseGreen(ctx, proj, &repo, provider, token, sha)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !green {
			return d.stallMerge(ctx, proj, task, repoRef, cursor, "release-job-not-green")
		}
		cursor = i + 1
	}

	obs.ClearMergeCursorStalled(task.Name)
	l.Info("merge: every repo in mergeOrder merged",
		"action", "merge_complete", "resource_id", task.Name, "repos", len(task.Spec.MergeOrder))
	return ctrl.Result{}, d.enterStageWithCursor(ctx, proj, task, tatarav1alpha1.StageDeploying, "", mrs, cursor)
}

// headMoved is CYCLE 4 (fix M3-9): merging -> reviewing, bounded by
// maxHeadMoveReentries. It is the only merging cycle that SPAWNS A POD every
// lap, and v4 had no counter on it at all.
func (d *StageDriver) headMoved(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task,
	mrs []tatarav1alpha1.MergeRequest, mr *tatarav1alpha1.MergeRequest, cursor int, why string) error {
	l := log.FromContext(ctx)
	obs.ClearMergeCursorStalled(task.Name)

	edge, _ := stage.HeadMoved(task, tatarav1alpha1.MaxHeadMoveReentries)
	reentries := task.Status.HeadMoveReentries
	if edge.To == tatarav1alpha1.StageFailed {
		l.Error(nil, "merge: head keeps moving; failing the Task",
			"action", "merge_head_moving", "resource_id", task.Name,
			"repo", mr.Spec.RepositoryRef, "pr", mr.Spec.Number, "reason", why)
		return d.enterStage(ctx, proj, task, tatarav1alpha1.StageFailed, stage.ReasonHeadMoving, mrs)
	}

	// The MR goes back to unreviewed. A head nobody reviewed is never merged.
	key := client.ObjectKeyFromObject(mr)
	if err := objbudget.FitMergeRequest(ctx, d.Client, d.spiller(proj), key, func(m *tatarav1alpha1.MergeRequest) {
		m.Status.Status = "new"
		m.Status.ReviewedSHA = ""
	}); err != nil {
		return fmt.Errorf("merge: reset mr %s: %w", key.Name, err)
	}
	l.Info("merge: head moved; re-reviewing",
		"action", "merge_head_moved", "resource_id", task.Name, "repo", mr.Spec.RepositoryRef,
		"pr", mr.Spec.Number, "head_move_reentries", reentries, "reason", why)

	// Re-read the MRs so the pendingReview gate sees the reset copy.
	fresh, err := ownedMergeRequests(ctx, d.Client, task)
	if err != nil {
		return err
	}
	return d.enterStageWithCursor(ctx, proj, task, tatarav1alpha1.StageReviewing, "", fresh, cursor)
}

// stallMerge parks the pass on a poll: the cursor stays put and
// operator_merge_cursor_stalled_seconds reports how long it has been stuck. The
// 4h merging budget (F.4) is what ends the wait, at parked(merge-timeout).
func (d *StageDriver) stallMerge(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task,
	repo string, cursor int, why string) (ctrl.Result, error) {
	stalledFor := 0.0
	if task.Status.StageEnteredAt != nil {
		stalledFor = d.now().Sub(task.Status.StageEnteredAt.Time).Seconds()
	}
	obs.MergeCursorStalledSeconds.WithLabelValues(task.Name, repo).Set(stalledFor)
	log.FromContext(ctx).Info("merge: waiting",
		"action", "merge_waiting", "resource_id", task.Name, "repo", repo,
		"reason", why, "stalled_seconds", stalledFor)
	if cursor != task.Status.MergeCursor {
		if err := d.enterStageWithCursor(ctx, proj, task, "", "", nil, cursor); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: mergeRequeue}, nil
}

// releaseGreen reports whether the release job at the merge commit is green. An
// empty or unknown status is "not yet": the caller polls.
func (d *StageDriver) releaseGreen(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, provider, token, sha string) (bool, error) {
	if d.ReaderFor == nil || sha == "" {
		return true, nil
	}
	reader, err := d.ReaderFor(provider, token)
	if err != nil {
		return false, fmt.Errorf("merge: scm reader: %w", err)
	}
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return false, fmt.Errorf("merge: owner/repo for %s: %w", repo.Name, err)
	}
	status, err := reader.GetCommitCIStatus(ctx, owner, name, sha)
	if err != nil {
		return false, fmt.Errorf("merge: release job status at %s: %w", sha, err)
	}
	_ = proj
	// An absent status ("") means the repo runs no push CI at all: nothing to
	// wait for. "failure" is left to the merging budget, which parks it.
	return status == "success" || status == "", nil
}

// stampMerged writes the merge the operator just performed straight onto the
// mirror. The sweep is HOURLY; the merge loop cannot wait an hour to learn what
// it did one line ago.
func (d *StageDriver) stampMerged(ctx context.Context, proj *tatarav1alpha1.Project, mr *tatarav1alpha1.MergeRequest) error {
	now := metav1.NewTime(d.now())
	key := client.ObjectKeyFromObject(mr)
	if err := objbudget.FitMergeRequest(ctx, d.Client, d.spiller(proj), key, func(m *tatarav1alpha1.MergeRequest) {
		m.Status.State = "merged"
		if m.Status.MergedAt == nil {
			m.Status.MergedAt = &now
		}
	}); err != nil {
		return fmt.Errorf("merge: stamp merged on %s: %w", key.Name, err)
	}
	mr.Status.State = "merged"
	return nil
}

// enterStage is the ONE way this driver writes a stage: stage.Enter stamps the
// clocks, and objbudget.FitTask sizes the write (A.7).
func (d *StageDriver) enterStage(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task,
	to, reason string, mrs []tatarav1alpha1.MergeRequest) error {
	return d.enterStageWithCursor(ctx, proj, task, to, reason, mrs, -1)
}

// enterStageWithCursor persists the merge cursor (and the head-move counter) in
// the SAME write as the stage. A cursor of -1 leaves it alone; an empty `to`
// writes the counters without a transition.
//
// It routes every transition through EnterStage - the ONE choke point - so the
// driver's terminal entries (failed(merge-blocked|head-moving|merge-order-missing|
// operator-error)) fire operator_task_terminal_total like everybody else's. They
// did not before, and the merge-failure alert family rides on exactly them.
func (d *StageDriver) enterStageWithCursor(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task,
	to, reason string, mrs []tatarav1alpha1.MergeRequest, cursor int) error {
	now := d.now()
	reentries := task.Status.HeadMoveReentries
	// Absolute assignments only: the closure is re-run to size the write and again
	// on every conflict retry.
	mutate := func(t *tatarav1alpha1.Task) {
		t.Status.HeadMoveReentries = reentries
		if cursor >= 0 {
			t.Status.MergeCursor = cursor
		}
	}
	if to == "" {
		// Counters only, no transition.
		key := client.ObjectKeyFromObject(task)
		if err := objbudget.FitTask(ctx, d.Client, d.spiller(proj), key, mutate); err != nil {
			return fmt.Errorf("merge: write task %s: %w", key.Name, err)
		}
		mutate(task)
		return nil
	}
	return EnterStage(ctx, d.Client, d.spiller(proj), d.Metrics, task, mrs, to, reason, now, mutate)
}

// CloseIssuesOnDelivery is contract C.4: DELIVERY HAS AN ACTOR.
//
// v1 required "every owned Issue closed" for deploying -> delivered while
// issue_write(close) was gated to clarify + refine, neither of which runs at
// merging or deploying. NOBODY could satisfy the precondition. The operator
// closes them: this is an operator egress with no endpoint and no MCP tool, by
// design.
func (d *StageDriver) CloseIssuesOnDelivery(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task) error {
	l := log.FromContext(ctx)

	mrs, err := ownedMergeRequests(ctx, d.Client, task)
	if err != nil {
		return err
	}
	// THE EMPTY SET IS NOT A LICENCE: all([]) == true must never gate delivery.
	if len(mrs) == 0 {
		return nil
	}
	for i := range mrs {
		if mrs[i].Status.State != "merged" || mrs[i].Status.DeployedAt == nil {
			return nil
		}
	}

	issues, err := ownedIssueCRs(ctx, d.Client, task)
	if err != nil {
		return err
	}
	writer, token, provider, err := d.forge(ctx, proj)
	if err != nil {
		return err
	}
	cite := deliveryCitation(mrs)
	for i := range issues {
		iss := &issues[i]
		if iss.Status.State != "open" {
			continue
		}
		var repo tatarav1alpha1.Repository
		if err := d.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: iss.Spec.RepositoryRef}, &repo); err != nil {
			return fmt.Errorf("delivery: get repository %s: %w", iss.Spec.RepositoryRef, err)
		}
		slug, _, serr := repoSlugFromURL(repo.Spec.URL, provider)
		if serr != nil {
			return fmt.Errorf("delivery: repo slug for %s: %w", repo.Name, serr)
		}
		comment := fmt.Sprintf("Delivered in %s. Closed by tatara.", cite)
		closeErr := writer.CloseIssue(ctx, token, slug, iss.Spec.Number, comment)
		RecordSCM(d.Metrics, provider, "close_issue", closeErr)
		if closeErr != nil {
			return fmt.Errorf("delivery: close issue %s#%d: %w", slug, iss.Spec.Number, closeErr)
		}
		key := client.ObjectKeyFromObject(iss)
		if err := objbudget.FitIssue(ctx, d.Client, d.spiller(proj), key, func(is *tatarav1alpha1.Issue) {
			is.Status.State = "closed"
			is.Status.Status = "done"
		}); err != nil {
			return fmt.Errorf("delivery: stamp issue %s: %w", key.Name, err)
		}
		l.Info("delivery: issue closed",
			"action", "scm_issue_closed_on_delivery", "resource_id", task.Name,
			"repo", repo.Name, "issue", iss.Spec.Number)
	}

	// AND ONLY THEN deliveredAt. Through the choke point: `delivered` is the ONLY
	// success outcome the platform has, and every failure-ratio alert divides by it.
	now := metav1.NewTime(d.now())
	if err := EnterStage(ctx, d.Client, d.spiller(proj), d.Metrics, task, mrs,
		tatarav1alpha1.StageDelivered, "", d.now(), func(t *tatarav1alpha1.Task) {
			t.Status.DeliveredAt = &now
		}); err != nil {
		return fmt.Errorf("delivery: %w", err)
	}
	obs.ClearMergeCursorStalled(task.Name)
	l.Info("delivery: task delivered",
		"action", "task_delivered", "resource_id", task.Name,
		"issues_closed", len(issues), "mrs", len(mrs),
		"turns", task.Status.Stats.Turns, "pod_runs", task.Status.Stats.PodRuns,
		"wall_seconds", task.Status.Stats.WallSeconds,
		"tokens_input", task.Status.Stats.TokensInput,
		"tokens_output", task.Status.Stats.TokensOutput,
		"tokens_cache_read", task.Status.Stats.TokensCacheRead,
		"tokens_cache_creation", task.Status.Stats.TokensCacheCreation)
	return nil
}

// deliveryCitation renders "<repo>!<number> (<version>)" for every delivered MR.
func deliveryCitation(mrs []tatarav1alpha1.MergeRequest) string {
	parts := make([]string, 0, len(mrs))
	for i := range mrs {
		v := mrs[i].Status.DeployedVersion
		if v == "" {
			v = "unversioned"
		}
		parts = append(parts, fmt.Sprintf("%s!%d (%s)", mrs[i].Spec.RepositoryRef, mrs[i].Spec.Number, v))
	}
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ", "
		}
		out += p
	}
	return out
}
