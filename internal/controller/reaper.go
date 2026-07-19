package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ReapOrphans deletes wrapper Pods (and their fronting Services) whose owning
// Task no longer warrants a running agent: the Task is gone, in a terminal phase
// (Succeeded/Failed), or in a terminal lifecycle state (Done/Parked/Stopped).
//
// It is the backstop for the one-shot teardown at terminate/resetAgentRun/
// terminal-lifecycle transitions. Those can be missed (an older pod-naming
// scheme, a transient delete error, or the Task already terminal when the
// operator restarted), leaving zombie pods that consume cluster resources and
// can block a new Task from spawning its pod. The reaper re-reaps periodically.
//
// Pods are correlated to their Task via the tatara.dev/task[-uid] labels, so the
// reaper never reconstructs PodName. A pod whose task-uid label disagrees with
// the live Task's UID is a leftover from a prior incarnation that reused the
// name and is reaped too.
func (s *CallbackServer) ReapOrphans(ctx context.Context) {
	l := log.FromContext(ctx)

	var pods corev1.PodList
	if err := s.Client.List(ctx, &pods,
		client.InNamespace(s.Namespace),
		client.MatchingLabels(agent.WrapperPodSelector()),
	); err != nil {
		l.Error(err, "reaper: list wrapper pods")
		return
	}

	// Build a name->Task map with one List instead of one Get per pod.
	var taskList tatarav1alpha1.TaskList
	if err := s.Client.List(ctx, &taskList, client.InNamespace(s.Namespace)); err != nil {
		l.Error(err, "reaper: list tasks")
		return
	}
	tasks := make(map[string]*tatarav1alpha1.Task, len(taskList.Items))
	for i := range taskList.Items {
		tasks[taskList.Items[i].Name] = &taskList.Items[i]
	}

	// Track which pod names are still alive (present and non-orphan) so the
	// Service pass below can identify Services whose Pod is gone.
	alivePods := make(map[string]struct{}, len(pods.Items))
	for i := range pods.Items {
		// Check context cancellation before each pod so we stop cleanly on shutdown.
		if ctx.Err() != nil {
			return
		}
		pod := &pods.Items[i]
		reason, orphan := s.orphanReason(pod, tasks)
		if !orphan {
			alivePods[pod.Name] = struct{}{}
			continue
		}
		if err := s.reapWrapper(ctx, pod.Name); err != nil {
			l.Error(err, "reaper: failed to reap orphan wrapper pod", "action", "reap_orphan",
				"resource_id", pod.Name, "task", pod.Labels[agent.LabelTask], "reason", reason)
		} else {
			l.Info("reaped orphan wrapper pod", "action", "reap_orphan",
				"resource_id", pod.Name, "task", pod.Labels[agent.LabelTask], "reason", reason)
			s.Metrics.OrphanReaped(reason)
		}
	}

	// Second pass: reap Services whose backing Pod is absent or was just reaped.
	// This gives an independent retry path for Services whose delete failed
	// transiently on a previous reaper cycle (the Pod may already be gone by
	// then, so the pod-list-only pass would never see them again).
	var svcs corev1.ServiceList
	if err := s.Client.List(ctx, &svcs,
		client.InNamespace(s.Namespace),
		client.MatchingLabels(agent.WrapperPodSelector()),
	); err != nil {
		l.Error(err, "reaper: list wrapper services")
		return
	}
	grace := s.ReaperGrace
	if grace == 0 {
		grace = pollRequeue
	}
	for i := range svcs.Items {
		if ctx.Err() != nil {
			return
		}
		svc := &svcs.Items[i]
		if _, alive := alivePods[svc.Name]; alive {
			// Pod is present and non-orphan; Service is fine.
			continue
		}
		// Creation grace: a Service is created right after its Pod (see
		// ensurePodAndService: Pod first, then Service). The Pod LIST above and
		// this Service LIST hit the cache at different instants, so a freshly
		// spawned Pod can be missing from alivePods while its Service is already
		// visible. Never reap a Service younger than the grace window or this pass
		// would delete a live Service out from under a still-propagating agent Pod
		// and sever the operator -> wrapper connection.
		if time.Since(svc.CreationTimestamp.Time) < grace {
			continue
		}
		// No live Pod backs this Service; delete it.
		del := svc.DeepCopy()
		if err := s.Client.Delete(ctx, del); err != nil && !apierrors.IsNotFound(err) {
			l.Error(err, "reaper: delete orphan service", "action", "reap_orphan_service", "resource_id", svc.Name)
			s.Metrics.ReapDeleteError("service")
		} else {
			l.Info("reaped orphan wrapper service", "action", "reap_orphan_service", "resource_id", svc.Name)
			s.Metrics.OrphanReaped("orphan service")
		}
	}
}

// orphanReason decides whether a wrapper pod should be reaped and why. It returns
// (reason, true) when the pod is an orphan, or ("", false) when it backs a live,
// non-terminal Task and must be left running.
//
// Creation grace: never reap a pod younger than the grace window to avoid the
// spawn-vs-reap race where a freshly created pod (or its Task) has not yet
// propagated through the cache (fixing findings 1, 2, and 7).
func (s *CallbackServer) orphanReason(pod *corev1.Pod, tasks map[string]*tatarav1alpha1.Task) (string, bool) {
	grace := s.ReaperGrace
	if grace == 0 {
		grace = pollRequeue
	}
	// Grace window: a pod just spawned may have a transiently absent Task in
	// cache, or its Task may briefly appear terminal before the spawn settles.
	if time.Since(pod.CreationTimestamp.Time) < grace {
		return "", false
	}

	taskName := pod.Labels[agent.LabelTask]
	if taskName == "" {
		// Unlabelled wrapper pod: cannot correlate, leave it for a human.
		return "", false
	}

	task, ok := tasks[taskName]
	if !ok {
		return "task absent", true
	}

	if uid := pod.Labels[agent.LabelTaskUID]; uid != "" && uid != string(task.UID) {
		return "stale task incarnation", true
	}
	// A Task whose work is over (a closed-set terminal, or delivered) runs no
	// pod: reap it promptly. A POD-LESS but live stage (triaging/approved/
	// merging/deploying) is NOT reaped here - the Task is alive and its pod, if
	// any, belongs to the stage it is about to enter; the idle backstop below is
	// what bounds a wrapper the operator forgot to tear down.
	if tatarav1alpha1.TaskDone(task) {
		return fmt.Sprintf("task stage %s", task.Status.Stage), true
	}
	// Superseded-stage pod: the pod was built for an earlier stage (its stamped
	// LabelAgentKind) but the Task has since advanced to a stage that wants a
	// DIFFERENT agent kind - e.g. an incident's investigating pod still Running
	// after the Task moved on to clarifying. None of the terminal/gone/idle
	// rules above catch this: the Task is alive and non-terminal, so TaskDone is
	// false, and the pod may still be doing work right up until its stale turn
	// times out. Only fire on a DEFINITE mismatch (both kinds non-empty and
	// different) - a pod-less current stage (AgentKindFor == "") is between
	// stages and is left to the idle backstop instead, to stay conservative.
	if podKind := pod.Labels[agent.LabelAgentKind]; podKind != "" {
		if wantKind := stage.AgentKindFor(task.Status.Stage); wantKind != "" && wantKind != podKind {
			return fmt.Sprintf("superseded: pod kind %s, stage wants %s", podKind, wantKind), true
		}
	}
	// Idle backstop (issue #237): a non-terminal Task whose pod holds no live turn
	// and whose last turn activity is older than IdlePodReapAfter is a leaked
	// wrapper. Its turn timed out or completed and the wrapper parked idle in
	// epoll, but the operator never submitted a next turn or tore it down - e.g. a
	// memory outage trapped the lifecycle reconcile in the spawn gate before it
	// could process the completed turn. The in-flight case is owned by the
	// turn-timeout path (driveTurns/PollOnce), so reap only when NO turn is in
	// flight. Conversation persistence lets a still-live Task re-spawn a fresh pod
	// and resume, so reaping is safe.
	if s.IdlePodReapAfter > 0 && !taskHasInflightTurn(task) {
		if time.Since(podLastActivity(pod, task)) > s.IdlePodReapAfter {
			return "idle no live turn", true
		}
	}
	return "", false
}

// podLastActivity returns the most recent moment this pod's Task showed agent
// activity: the freshest of the turn-started, turn-last-activity, and
// turn-complete annotations, floored at the pod's creation time. Absent or
// unparseable annotations are ignored. The idle backstop measures how long a pod
// has sat with no live turn from this instant.
func podLastActivity(pod *corev1.Pod, task *tatarav1alpha1.Task) time.Time {
	latest := pod.CreationTimestamp.Time
	for _, ann := range []string{annTurnStartedAt, annTurnLastActivity, annTurnComplete} {
		v := task.Annotations[ann]
		if v == "" {
			continue
		}
		ts, err := time.Parse(time.RFC3339, v)
		if err != nil {
			continue
		}
		if ts.After(latest) {
			latest = ts
		}
	}
	return latest
}

// reapWrapper best-effort deletes a wrapper Pod and its same-named Service by
// name. A missing object is not an error. Returns a non-nil error only when an
// unexpected delete failure occurs on the Pod (the primary resource).
func (s *CallbackServer) reapWrapper(ctx context.Context, name string) error {
	l := log.FromContext(ctx)
	pod := &corev1.Pod{}
	pod.Name = name
	pod.Namespace = s.Namespace
	if err := s.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		l.Error(err, "reaper: delete pod", "resource_id", name)
		s.Metrics.ReapDeleteError("pod")
		return err
	}
	svc := &corev1.Service{}
	svc.Name = name
	svc.Namespace = s.Namespace
	if err := s.Client.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		l.Error(err, "reaper: delete service", "resource_id", name)
		s.Metrics.ReapDeleteError("service")
	}
	return nil
}

// ============================================================================
// THE TERMINAL-STAGE REAPER (contract B.5, B.6).
//
// It is the ONLY Task GC: the phase-machine's gcTerminalTasks was reaped with
// the machine. It is gated on status.stage != "", so a Task the stage machine
// has not touched yet is left alone.
//
// THE ONE RULE, and everything below is a consequence of it:
//
//	NEVER CLOSE OR DELETE ANYTHING WE DID NOT CREATE.
//
// v5's rule was "on terminal entry, close every owned open MR and delete its
// head branch". Fix C3-2 makes every review-kind Task non-bot-authored BY
// CONSTRUCTION - a review Task controller-owns a CONTRIBUTOR'S MergeRequest, its
// only terminal is parked(awaiting-human), and B.6 reaps a non-backlog park at
// 7d. So v5 closed the contributor's PR and deleted their branch; on a fork the
// branch delete 403s, the step is BLOCKING, and the reap requeues FOREVER,
// hammering the forge. closeExhaustedPR (projectscan.go) already said so in its
// own comment: "The branch is preserved (ClosePR does not delete it), so a human
// can reopen to retry."
// ============================================================================

const (
	// AnnTerminalCommented is stamped on an ISSUE CR once the B.6 step-1 bot
	// comment for a given Task has LANDED on the forge (value: the Task name).
	// The step is BLOCKING and therefore RETRIED, and a forge comment is not
	// idempotent: without a durable per-issue marker a 403 on issue 3 of 4 makes
	// the requeue re-post on issues 1 and 2.
	AnnTerminalCommented = "tatara.dev/terminal-commented"
	// AnnTerminalClosed is stamped on a MERGEREQUEST CR once B.6 step 4 has
	// closed it (value: the Task name). ClosePR posts a comment with the reason,
	// so re-closing an already-closed PR is not free.
	AnnTerminalClosed = "tatara.dev/terminal-closed"
	// AnnTerminalReleased is stamped on the TASK once its whole B.6 terminal
	// sequence has completed. The Task then survives its retention window as a
	// DEBUGGING ARTIFACT - owning nothing, blocking nothing.
	AnnTerminalReleased = "tatara.dev/terminal-released"
	// AnnDocBatchResolved is stamped on a documentation batch once its members
	// have been resolved (stamped, or deliberately not stamped under the M21
	// never-ran carve-out), so the abandonment counter cannot be double-counted
	// on every subsequent reaper pass.
	AnnDocBatchResolved = "tatara.dev/doc-batch-resolved"
	// AnnHumanReviewRounds carries a dying review Task's status.humanReviewRounds
	// (the V7-9 cap, 5) onto the MergeRequest CR that OUTLIVES it, so the sweep's
	// re-mint can seed the new Task with it.
	//
	// It exists because the Task is the only thing that holds the counter and the
	// Task is exactly what the 7d reap deletes. Without the carry, a review Task
	// sitting AT the cap is reaped, re-minted with humanReviewRounds = 0, and the
	// human's PR is worth five MORE review pods - every seven days, forever. That
	// is the V7-9 cost amplifier with a week-long period.
	AnnHumanReviewRounds = "tatara.dev/human-review-rounds"
)

// BranchDeleter is an OPTIONAL SCMWriter capability, in the same spirit as
// scm.PRCommentLister and scm.DeployWatcher: a writer that can delete a head
// branch implements it, one that cannot does not.
//
// IT IS OPTIONAL BY NECESSITY, NOT BY CHOICE: scm.SCMWriter has NO DeleteBranch
// method and neither *scm.GitHub nor *scm.GitLab implements one - the platform
// has never deleted a branch (see closeExhaustedPR's comment). B.6 step 4 is the
// first rule that needs it. Until the adapters grow the method, the branch half
// of step 4 is a logged no-op; the CLOSE half is fully live. A type assertion
// keeps the reaper honest instead of silently pretending the branch is gone.
type BranchDeleter interface {
	DeleteBranch(ctx context.Context, repoURL, token, branch string) error
}

// ReapTerminal is the B.6 reaper. ONE pass over proj's new-model Tasks.
//
// A blocking step that fails REQUEUES (a non-nil error): the reap does not
// proceed without it. That is the whole point of fix M25 - the sweep's mint-stage
// predicate READS the tatara-parked label, so a label that silently fails to land
// makes the next sweep mint the issue ACTIVE, the pod re-triages, it fails again.
func (r *ProjectReconciler) ReapTerminal(ctx context.Context, proj *tatarav1alpha1.Project) error {
	l := log.FromContext(ctx)

	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return fmt.Errorf("reap: list tasks: %w", err)
	}
	now := time.Now()

	// live is EVERY Task that currently exists, and fold is the SKIP list: any
	// Task named in a live Task's status.foldInFlight.
	live := make(map[string]bool, len(tl.Items))
	fold := map[string]bool{}
	for i := range tl.Items {
		live[tl.Items[i].Name] = true
		for _, member := range tl.Items[i].Status.FoldInFlight {
			fold[member] = true
		}
	}

	var firstErr error
	for i := range tl.Items {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		t := &tl.Items[i]
		if t.Spec.ProjectRef != proj.Name || t.Status.Stage == "" {
			continue // the stage machine has not touched it yet
		}
		if fold[t.Name] {
			obs.GCBlockedTotal.WithLabelValues(obs.GCBlockedFoldInFlight).Inc()
			l.V(1).Info("reap: skipping a Task a live refine umbrella is mid-fold of",
				"action", "reap_blocked", "resource_id", t.Name, "reason", obs.GCBlockedFoldInFlight)
			continue
		}
		if err := r.reapOne(ctx, proj, t, live, now); err != nil {
			l.Error(err, "reap: terminal task", "action", "reap_error",
				"resource_id", t.Name, "stage", t.Status.Stage, "stage_reason", t.Status.StageReason)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// reapOne is the B.6 table, row by row.
func (r *ProjectReconciler) reapOne(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, live map[string]bool, now time.Time) error {

	// A documentation batch reaching delivered or parked THROUGH THE NORMAL
	// review/merge/deploy path (reason "") never goes through forceDocTimeout, so
	// this is the only other place "on that Task reaching delivered [or parked]"
	// (B.6) can fire. Without it, every SUCCESSFUL nightly batch - the common case
	// - would leave its parents documentedBy="" forever and get re-covered by
	// every subsequent night's MintDocBatch. ResolveDocBatch is idempotent
	// (guarded by AnnDocBatchResolved), so re-calling it from forceDocTimeout's
	// delivered(doc-timeout) case too is a harmless no-op.
	if t.Spec.Kind == DocBatchKind && len(t.Spec.DocumentsTasks) > 0 &&
		(t.Status.Stage == tatarav1alpha1.StageDelivered || t.Status.Stage == tatarav1alpha1.StageParked) {
		if err := r.ResolveDocBatch(ctx, t); err != nil {
			return err
		}
	}

	switch t.Status.Stage {

	case tatarav1alpha1.StageFailed:
		// FIX H13: a failed Task RELEASES ITS ISSUES IMMEDIATELY, not in 7 days.
		// v3 let it hold them hostage for a week, SILENTLY: still controller-owner
		// (so no re-mint), no bot comment (unlike parked), no re-entry. The cutover
		// amplifier makes that fatal - an image-pin skew fails every Task INSTANTLY,
		// which is correct, and would freeze every Issue for 7 days with no comment.
		if err := r.releaseTerminal(ctx, proj, t, live); err != nil {
			return err
		}
		if now.After(stageEnteredAt(t).Add(tatarav1alpha1.FailedRetention)) {
			return r.deleteReapedTask(ctx, proj, t, live)
		}
		return nil

	case tatarav1alpha1.StageRejected:
		if err := r.releaseTerminal(ctx, proj, t, live); err != nil {
			return err
		}
		if now.After(stageEnteredAt(t).Add(tatarav1alpha1.RejectedRetention)) {
			return r.deleteReapedTask(ctx, proj, t, live)
		}
		return nil

	case tatarav1alpha1.StageParked:
		return r.reapParked(ctx, proj, t, live, now)

	case tatarav1alpha1.StageDocumenting:
		if now.After(stageEnteredAt(t).Add(tatarav1alpha1.DocStageBudget)) {
			return r.forceDocTimeout(ctx, t, now)
		}
		return nil

	case tatarav1alpha1.StageDelivered:
		return r.reapDelivered(ctx, proj, t, live, now)
	}
	return nil
}

// reapParked splits the ONE park stage into its TWO populations.
//
//	parked(backlog-sweep)  NEVER on age. It is the durable mirror ANCHOR - it
//	                       never ran, it owns its Issues at zero agent cost, and
//	                       ageing it out would churn mint/reap forever. It is
//	                       reaped only when EVERY owned Issue is closed.
//	parked(anything else)  ages out at parkRetention, IF no F.6 re-entry rule
//	                       fires AND the bot park comment has LANDED.
func (r *ProjectReconciler) reapParked(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, live map[string]bool, now time.Time) error {

	if t.Status.StageReason == stage.ReasonBacklogSweep {
		issues, err := r.ownedIssues(ctx, t)
		if err != nil {
			return err
		}
		for i := range issues {
			if issues[i].Status.State != "closed" {
				return nil // still anchoring open work
			}
		}
		return r.deleteReapedTask(ctx, proj, t, live)
	}

	if !now.After(stageEnteredAt(t).Add(tatarav1alpha1.ParkRetention)) {
		return nil
	}
	fires, err := r.unparkFires(ctx, proj, t, now)
	if err != nil {
		return err
	}
	if fires {
		// An F.6 re-entry rule matches. The un-park path owns this Task, not us.
		return nil
	}
	if err := r.releaseTerminal(ctx, proj, t, live); err != nil {
		return err
	}
	return r.deleteReapedTask(ctx, proj, t, live)
}

// reapDelivered holds a delivered Task until the nightly batch has covered it.
//
// THE GATE AND THE COVERED-SET PREDICATE ARE THE SAME FUNCTION (needsDocumenting)
// ON PURPOSE. B.6's table gates the reap on "documentedBy != "" OR the Task had
// ZERO merged MRs" while the batch covers only Tasks whose MRs are ALL merged: a
// Task with one merged MR and one closed one satisfies neither, and would be
// pinned in the cluster forever. One predicate, both places, no gap.
func (r *ProjectReconciler) reapDelivered(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, live map[string]bool, now time.Time) error {

	needs, err := r.needsDocumenting(ctx, proj, t)
	if err != nil {
		return err
	}
	if needs {
		obs.GCBlockedTotal.WithLabelValues(obs.GCBlockedDocReference).Inc()
		mrs, err := r.ownedMRs(ctx, t)
		if err != nil {
			return err
		}
		mrStates := make([]string, len(mrs))
		for i := range mrs {
			mrStates[i] = mrs[i].Status.State
		}
		log.FromContext(ctx).Info("reap: delivered task pinned by the doc_reference GC block",
			"action", "reap_blocked", "resource_id", t.Name, "reason", obs.GCBlockedDocReference,
			"mr_states", mrStates)
		return nil
	}
	delivered := stageEnteredAt(t)
	if t.Status.DeliveredAt != nil {
		delivered = t.Status.DeliveredAt.Time
	}
	if !now.After(delivered.Add(tatarav1alpha1.DeliveredRetention)) {
		return nil
	}
	_ = proj
	return r.deleteReapedTask(ctx, proj, t, live)
}

// releaseTerminal is THE TERMINAL SEQUENCE (B.6), and the ORDER IS THE FIX:
//
//  1. post a bot comment on every owned OPEN Issue naming stageReason  (BLOCKING)
//  2. stamp the tatara-parked label                                    (BLOCKING TOO)
//  3. RELEASE controller-ownership FIRST
//  4. ONLY NOW, and ONLY for MRs WE CREATED, close them
//
// Step 2 is blocking because the sweep's mint-stage predicate READS that label
// (fix M3-11): making step 1 blocking and leaving step 2 best-effort means a
// label that silently fails to land makes the next sweep mint the issue ACTIVE,
// the pod re-triages, it fails again - the exact loop this kills.
//
// Step 3 comes BEFORE step 4 because v5 closed MRs first and then handed the
// corpse to the live survivor.
func (r *ProjectReconciler) releaseTerminal(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, live map[string]bool) error {

	if t.Annotations[AnnTerminalReleased] == "true" {
		return nil
	}
	l := log.FromContext(ctx)

	issues, err := r.ownedIssues(ctx, t)
	if err != nil {
		return err
	}
	mrs, err := r.ownedMRs(ctx, t)
	if err != nil {
		return err
	}

	// Clause (c) of step 4 reads "this Task IS (WAS) its CONTROLLER owner": the
	// snapshot is taken HERE, before step 3 releases the flag.
	wasController := make(map[string]bool, len(mrs))
	for i := range mrs {
		if owner, ok := own.ControllerOwner(&mrs[i]); ok && owner == t.Name {
			wasController[mrs[i].Name] = true
		}
	}

	// Steps 1 and 2, on every owned OPEN Issue. Both BLOCKING.
	for i := range issues {
		if issues[i].Status.State == "closed" {
			continue
		}
		if err := r.notifyTerminalIssue(ctx, proj, t, &issues[i]); err != nil {
			return err
		}
	}

	// Step 3: the B.5 handover, or an outright DROP.
	survivors, err := r.releaseOwnership(ctx, proj, t, issues, mrs, live)
	if err != nil {
		return err
	}

	// Step 4: ONLY for MRs WE CREATED.
	if err := r.closeOwnMRs(ctx, proj, t, mrs, wasController, survivors); err != nil {
		return err
	}

	// Step 5: the Task CR itself survives as a DEBUGGING ARTIFACT - owning
	// nothing, blocking nothing.
	if err := r.annotateTask(ctx, t, AnnTerminalReleased, "true"); err != nil {
		return err
	}
	l.Info("released a terminal task's artifacts",
		"action", "reap_release", "resource_id", t.Name, "stage", t.Status.Stage,
		"stage_reason", t.Status.StageReason, "issues", len(issues), "mrs", len(mrs))
	return nil
}

// notifyTerminalIssue is steps 1 and 2 for ONE Issue. BOTH are blocking: a 403
// on either returns an error and the reap requeues.
func (r *ProjectReconciler) notifyTerminalIssue(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, iss *tatarav1alpha1.Issue) error {

	writer, token, err := r.scanWriter(ctx, proj)
	if err != nil {
		return fmt.Errorf("reap: scm writer: %w", err)
	}
	repo, err := r.repositoryFor(ctx, proj.Namespace, iss.Spec.RepositoryRef)
	if err != nil {
		return err
	}
	slug, err := scm.RepoSlugFromURL(repo.Spec.URL)
	if err != nil {
		return fmt.Errorf("reap: repo slug for %s: %w", repo.Name, err)
	}
	issueRef := fmt.Sprintf("%s#%d", slug, iss.Spec.Number)
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	l := log.FromContext(ctx)

	// Step 1. Not idempotent on the forge, so the marker is per (Issue, Task).
	if iss.Annotations[AnnTerminalCommented] != t.Name {
		body := terminalIssueComment(t)
		commentErr := writer.Comment(ctx, token, issueRef, body)
		RecordSCM(r.Metrics, provider, "comment", commentErr)
		if commentErr != nil {
			if !isPermanentTargetGone(commentErr) {
				return fmt.Errorf("reap: comment on %s: %w", issueRef, commentErr)
			}
			l.Info("reap: terminal comment target is gone; skipping",
				"action", "reap_comment", "resource_id", t.Name, "issue_ref", issueRef)
		}
		if err := r.annotateIssue(ctx, iss, AnnTerminalCommented, t.Name); err != nil {
			return err
		}
		l.Info("posted the terminal notice on an owned issue",
			"action", "reap_comment", "resource_id", t.Name, "issue_ref", issueRef,
			"stage", t.Status.Stage, "stage_reason", t.Status.StageReason)
	}

	// Step 2. AddLabel IS idempotent on both forges, so it needs no marker - but
	// it is just as BLOCKING as step 1.
	labelErr := writer.AddLabel(ctx, token, issueRef, TataraParkedLabel)
	RecordSCM(r.Metrics, provider, "add_label", labelErr)
	if labelErr != nil {
		if !isPermanentTargetGone(labelErr) {
			return fmt.Errorf("reap: stamp %s on %s: %w", TataraParkedLabel, issueRef, labelErr)
		}
		l.Info("reap: label target is gone; skipping",
			"action", "reap_label", "resource_id", t.Name, "issue_ref", issueRef)
		return nil
	}
	l.Info("stamped the tatara-parked label on an owned issue",
		"action", "reap_label", "resource_id", t.Name, "issue_ref", issueRef, "label", TataraParkedLabel)
	return nil
}

// terminalIssueComment names the stageReason. It is the ONLY signal a human gets
// that the platform stopped working their issue.
func terminalIssueComment(t *tatarav1alpha1.Task) string {
	return fmt.Sprintf(
		"tatara has stopped working this issue: task `%s` ended in `%s` (`%s`).\n\n"+
			"The issue stays open and is labelled `%s`, so the platform will not spend "+
			"another agent on it until a human replies here. Comment to pick it back up.",
		t.Name, t.Status.Stage, t.Status.StageReason, TataraParkedLabel)
}

// ourMR is clauses (a) and (b) of B.6 step 4 - "this MR is ONE WE CREATED" - and
// it is the SINGLE definition of that predicate, on purpose.
//
// Step 3 (releaseOwnership) and step 4 (closeOwnMRs) MUST agree on it. Step 3
// decides whether the MIRROR cascades with the Task; step 4 decides whether the
// forge PR is CLOSED. When the two disagree, you get precisely the production
// bug this exists to kill: step 4 correctly refuses to close a human's PR, step 3
// keeps the ownerRef anyway, and the human's mirror cascades away while their PR
// is still open on the forge.
func ourMR(proj *tatarav1alpha1.Project, t *tatarav1alpha1.Task, mr *tatarav1alpha1.MergeRequest) bool {
	bot := botLoginOf(proj)
	return bot != "" && mr.Status.Author == bot && mr.Status.HeadBranch == agent.TaskBranch(t)
}

// releaseOwnership is B.6 step 3 / B.5. For every artifact this Task
// CONTROLLER-owns:
//
//	>= 1 SURVIVING plain owner  ->  hand controller=true to the OLDEST of them
//	                                (own.HandOverController: the atomic single-PUT
//	                                swap - the API server 422s two controller refs,
//	                                so a demote-then-promote is not available).
//	no survivor, still OPEN and  ->  DROP the ownerRef entirely. The artifact then
//	NOT OURS TO CLOSE               has ZERO owners, which the GC never collects and
//	                                the sweep's orphan predicate ADOPTS (B.4, fix
//	                                M3-10).
//	otherwise                    ->  keep the ref: the artifact CASCADES with the
//	                                Task when it is deleted (B.5).
//
// THE MERGEREQUEST RULE IS THE ISSUE RULE. v6 passed dropWhenOrphaned=false for
// EVERY MR, justified by "a bot MR we are about to close must never be re-adopted
// by anything". That is right for a BOT MR and WRONG for a HUMAN's. Fix C3-2
// makes every review-kind Task non-bot-authored BY CONSTRUCTION, so a review Task
// controller-owns a CONTRIBUTOR'S MergeRequest; step 4 (correctly) leaves it
// completely alone; and keeping the ref then CASCADED the mirror of a still-OPEN
// human PR the moment B.6 reaped the 7d park. The contributor's PR stayed open on
// the forge with no mirror, nothing re-minted it, and their next comment landed on
// nothing.
//
// So dropWhenOrphaned is TRUE for an MR exactly when step 4 will NOT close it
// (!ourMR) AND it is still OPEN - the same "an OPEN artifact must be re-mintable
// RIGHT NOW" rule fix H13 gave the Issue path one loop below. An MR that IS ours
// keeps its ref and cascades, exactly as before: we are about to close it, and a
// closed bot MR must never be re-adopted. A NOT-ours MR that is already CLOSED
// keeps its ref too - there is nothing left to mirror, and orphaning it would leak
// a zero-owner CR nothing ever collects and nothing ever re-adopts.
//
// It returns the set of artifacts that HAVE a surviving owner - clause (d) of
// step 4.
func (r *ProjectReconciler) releaseOwnership(ctx context.Context, proj *tatarav1alpha1.Project, t *tatarav1alpha1.Task,
	issues []tatarav1alpha1.Issue, mrs []tatarav1alpha1.MergeRequest, live map[string]bool) (map[string]bool, error) {

	// The dying Task is NOT its own survivor.
	others := make(map[string]bool, len(live))
	for name, ok := range live {
		if ok && name != t.Name {
			others[name] = true
		}
	}
	survivors := map[string]bool{}
	l := log.FromContext(ctx)

	release := func(obj client.Object, dropWhenOrphaned bool) error {
		owner, ok := own.ControllerOwner(obj)
		if !ok || owner != t.Name {
			return nil
		}
		heir, hasHeir := own.OldestSurvivingOwner(obj, others)
		if hasHeir {
			survivors[obj.GetName()] = true
			if err := own.HandOverController(obj, t, &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: heir, Namespace: obj.GetNamespace()},
			}); err != nil {
				obs.GCBlockedTotal.WithLabelValues(obs.GCBlockedNoControllerOwner).Inc()
				return fmt.Errorf("reap: hand the controller flag to %q on %s: %w", heir, obj.GetName(), err)
			}
			if err := r.Update(ctx, obj); err != nil {
				obs.GCBlockedTotal.WithLabelValues(obs.GCBlockedNoControllerOwner).Inc()
				return fmt.Errorf("reap: update %s after handover to %q: %w", obj.GetName(), heir, err)
			}
			l.Info("handed the controller flag to the oldest surviving owner",
				"action", "reap_handover", "resource_id", obj.GetName(), "from", t.Name, "to", heir)
			return nil
		}
		if !dropWhenOrphaned {
			return nil // it cascades with the Task
		}
		dropOwnerRef(obj, t.Name)
		if err := r.Update(ctx, obj); err != nil {
			obs.GCBlockedTotal.WithLabelValues(obs.GCBlockedNoControllerOwner).Inc()
			return fmt.Errorf("reap: drop the owner ref of %q from %s: %w", t.Name, obj.GetName(), err)
		}
		l.Info("dropped the owner ref of a terminal task; the next sweep re-mints and adopts the artifact",
			"action", "reap_drop_owner", "resource_id", obj.GetName(), "task", t.Name)
		return nil
	}

	// orphaning reports whether release is ABOUT TO leave obj with zero owners. The
	// humanReviewRounds carry below has to know that BEFORE the drop happens, and
	// only an artifact that actually ends up ownerless is ever re-minted.
	orphaning := func(obj client.Object, drop bool) bool {
		if !drop {
			return false
		}
		if owner, ok := own.ControllerOwner(obj); !ok || owner != t.Name {
			return false
		}
		_, hasHeir := own.OldestSurvivingOwner(obj, others)
		return !hasHeir
	}

	for i := range issues {
		// An OPEN issue must be re-mintable RIGHT NOW (fix H13).
		if err := release(&issues[i], issues[i].Status.State != "closed"); err != nil {
			return nil, err
		}
	}
	for i := range mrs {
		mr := &mrs[i]
		// An OPEN MR THAT IS NOT OURS TO CLOSE must be re-mintable RIGHT NOW, for
		// the same reason and by the same rule. See the doc comment above.
		drop := !ourMR(proj, t, mr) && mr.Status.State == "open"
		if orphaning(mr, drop) {
			if err := r.carryHumanReviewRounds(ctx, t, mr); err != nil {
				return nil, err
			}
		}
		if err := release(mr, drop); err != nil {
			return nil, err
		}
	}
	return survivors, nil
}

// carryHumanReviewRounds persists a dying review Task's V7-9 counter onto the
// mirror that is about to outlive it. It runs BEFORE the ownerRef is dropped, so
// a failure here requeues the reap with the Task still owning the MR.
//
// The counter lives on the TASK and the reap DELETES the Task, so without this
// the re-mint starts from zero and a PR parked AT the cap becomes worth five more
// review pods every seven days. The MergeRequest CR is the only thing that
// survives the reap, so it is the only place the counter can live.
func (r *ProjectReconciler) carryHumanReviewRounds(ctx context.Context, t *tatarav1alpha1.Task,
	mr *tatarav1alpha1.MergeRequest) error {

	if t.Status.HumanReviewRounds <= 0 {
		return nil
	}
	rounds := strconv.Itoa(t.Status.HumanReviewRounds)
	if err := r.annotateMR(ctx, mr, AnnHumanReviewRounds, rounds); err != nil {
		return err
	}
	log.FromContext(ctx).Info("carried the human-review-round count onto a surviving mirror",
		"action", "reap_carry_review_rounds", "resource_id", mr.Name, "task", t.Name, "rounds", rounds)
	return nil
}

// closeOwnMRs is B.6 step 4. It closes an owned open MR and deletes its head
// branch IF AND ONLY IF ALL FOUR clauses hold:
//
//	a. mr.status.author     == Project.spec.scm.botLogin
//	b. mr.status.headBranch == agent.TaskBranch(this Task)
//	c. this Task IS (WAS) its CONTROLLER owner
//	d. NO surviving plain owner exists
//
// Otherwise: LEAVE IT COMPLETELY ALONE. No close. No branch delete. No comment.
//
// All four are load-bearing. (a) is what stops a human's PR being closed. (a) and
// (b) together stop the branch-delete CALL being made at all on a fork, where it
// 403s and - the step being blocking - requeues the reap forever. (d), plus the
// step-3-first ordering, stops an MR being closed and then handed to a live
// survivor.
func (r *ProjectReconciler) closeOwnMRs(ctx context.Context, proj *tatarav1alpha1.Project, t *tatarav1alpha1.Task,
	mrs []tatarav1alpha1.MergeRequest, wasController, survivors map[string]bool) error {

	l := log.FromContext(ctx)

	for i := range mrs {
		mr := &mrs[i]
		if mr.Status.State != "open" || mr.Annotations[AnnTerminalClosed] != "" {
			continue
		}
		if !wasController[mr.Name] || survivors[mr.Name] {
			continue // clauses (c) and (d)
		}
		if !ourMR(proj, t, mr) {
			// Clauses (a) and (b): NOT OURS, or NOT OUR BRANCH. Step 3 has already
			// orphaned it (if it is open), so its mirror survives for the sweep to
			// re-adopt. We touch the forge not at all.
			continue
		}

		writer, token, err := r.scanWriter(ctx, proj)
		if err != nil {
			return fmt.Errorf("reap: scm writer: %w", err)
		}
		repo, err := r.repositoryFor(ctx, proj.Namespace, mr.Spec.RepositoryRef)
		if err != nil {
			return err
		}
		provider := ""
		if proj.Spec.Scm != nil {
			provider = proj.Spec.Scm.Provider
		}
		body := fmt.Sprintf(
			"Closing: the tatara task that opened this PR ended in `%s` (`%s`). "+
				"Its head branch is deleted with it.", t.Status.Stage, t.Status.StageReason)
		closeErr := writer.ClosePR(ctx, repo.Spec.URL, token, mr.Spec.Number, body)
		RecordSCM(r.Metrics, provider, "close_pr", closeErr)
		if closeErr != nil {
			if !isPermanentTargetGone(closeErr) {
				return fmt.Errorf("reap: close our own MR %s#%d: %w", repo.Name, mr.Spec.Number, closeErr)
			}
			l.Info("reap: close target is gone; skipping",
				"action", "reap_close_mr", "resource_id", t.Name, "repo", repo.Name, "number", mr.Spec.Number)
		}
		if err := r.annotateMR(ctx, mr, AnnTerminalClosed, t.Name); err != nil {
			return err
		}
		l.Info("closed an agent MR whose task went terminal",
			"action", "reap_close_mr", "resource_id", t.Name, "repo", repo.Name,
			"number", mr.Spec.Number, "head_branch", mr.Status.HeadBranch)

		deleter, ok := writer.(BranchDeleter)
		if !ok {
			// scm.SCMWriter has no DeleteBranch and neither adapter implements one.
			// The branch is preserved, exactly as closeExhaustedPR has always
			// preserved it. Logged, never silently assumed done.
			l.Info("reap: this SCM writer cannot delete branches; the head branch is preserved",
				"action", "reap_delete_branch", "resource_id", t.Name,
				"repo", repo.Name, "head_branch", mr.Status.HeadBranch, "result", "unsupported")
			continue
		}
		deleteErr := deleter.DeleteBranch(ctx, repo.Spec.URL, token, mr.Status.HeadBranch)
		RecordSCM(r.Metrics, provider, "delete_branch", deleteErr)
		if deleteErr != nil {
			if !isPermanentTargetGone(deleteErr) {
				return fmt.Errorf("reap: delete our own head branch %s: %w", mr.Status.HeadBranch, deleteErr)
			}
			l.Info("reap: head branch already gone",
				"action", "reap_delete_branch", "resource_id", t.Name, "head_branch", mr.Status.HeadBranch)
			continue
		}
		l.Info("deleted the head branch of an agent MR we closed",
			"action", "reap_delete_branch", "resource_id", t.Name,
			"repo", repo.Name, "head_branch", mr.Status.HeadBranch)
	}
	return nil
}

// deleteReapedTask is the B.5 half of the delete: hand the controller flag over
// FIRST, THEN delete. Skipping the handover leaves artifacts with ZERO controller
// owners - worked by nobody, re-minted by nobody, because the orphan predicate
// sees an OWNED Issue.
func (r *ProjectReconciler) deleteReapedTask(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, live map[string]bool) error {

	issues, err := r.ownedIssues(ctx, t)
	if err != nil {
		return err
	}
	mrs, err := r.ownedMRs(ctx, t)
	if err != nil {
		return err
	}
	if _, err := r.releaseOwnership(ctx, proj, t, issues, mrs, live); err != nil {
		return err
	}
	policy := metav1.DeletePropagationBackground
	if err := r.Delete(ctx, t.DeepCopy(), &client.DeleteOptions{PropagationPolicy: &policy}); err != nil &&
		!apierrors.IsNotFound(err) {
		return fmt.Errorf("reap: delete task %s: %w", t.Name, err)
	}
	// operator_task_tokens_total/operator_task_turns_total are per-issue
	// labeled counters (bounded cardinality assumes live + recently-live
	// issues only); a Task's GC is the one point that knows it is safe to
	// drop them. DeleteTaskSeries existed but had zero production callers
	// before this (metric-wiring audit, issue #370) - an unbounded leak on
	// every Task GC. DeleteTaskSeries itself no-ops when issue=="" (a
	// project-scoped task shares that label with every other project-scoped
	// task and must not have its series cleared by one task's GC).
	if r.Metrics != nil {
		project, repo, kind, issue, _ := taskTokenLabels(t)
		r.Metrics.DeleteTaskSeries(project, repo, kind, issue)
	}
	log.FromContext(ctx).Info("reaped a terminal task",
		"action", "reap_task", "resource_id", t.Name, "kind", t.Spec.Kind,
		"stage", t.Status.Stage, "stage_reason", t.Status.StageReason)
	return nil
}

// unparkFires reports whether an F.6 re-entry rule matches t RIGHT NOW. A parked
// Task that can still come back is never reaped. stage.Unpark MUTATES its Task, so
// it is asked on a DeepCopy: this is a probe, not a transition.
func (r *ProjectReconciler) unparkFires(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, now time.Time) (bool, error) {

	issues, err := r.ownedIssues(ctx, t)
	if err != nil {
		return false, err
	}
	mrs, err := r.ownedMRs(ctx, t)
	if err != nil {
		return false, err
	}
	active, err := r.activeTaskCount(ctx, proj)
	if err != nil {
		return false, err
	}
	maxOpen := proj.Spec.MaxOpenTasks
	if maxOpen <= 0 {
		maxOpen = 6
	}
	_, ok := stage.Unpark(stage.UnparkInput{
		Task:         t.DeepCopy(),
		Issues:       issues,
		MRs:          mrs,
		ActiveTasks:  active,
		MaxOpenTasks: maxOpen,
		BotLogin:     botLoginOf(proj),
		Now:          now,
	})
	return ok, nil
}

// ownedIssues / ownedMRs resolve status.issueRefs / status.mrRefs. A ref whose CR
// is gone is skipped: the mirror is not authoritative over the reap.
func (r *ProjectReconciler) ownedIssues(ctx context.Context, t *tatarav1alpha1.Task) ([]tatarav1alpha1.Issue, error) {
	out := make([]tatarav1alpha1.Issue, 0, len(t.Status.IssueRefs))
	for _, name := range t.Status.IssueRefs {
		var iss tatarav1alpha1.Issue
		err := r.Get(ctx, types.NamespacedName{Namespace: t.Namespace, Name: name}, &iss)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("reap: get issue %s: %w", name, err)
		}
		out = append(out, iss)
	}
	return out, nil
}

func (r *ProjectReconciler) ownedMRs(ctx context.Context, t *tatarav1alpha1.Task) ([]tatarav1alpha1.MergeRequest, error) {
	out := make([]tatarav1alpha1.MergeRequest, 0, len(t.Status.MRRefs))
	for _, name := range t.Status.MRRefs {
		var mr tatarav1alpha1.MergeRequest
		err := r.Get(ctx, types.NamespacedName{Namespace: t.Namespace, Name: name}, &mr)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("reap: get mergerequest %s: %w", name, err)
		}
		out = append(out, mr)
	}
	return out, nil
}

// repositoryFor resolves a Repository CR by name.
func (r *ProjectReconciler) repositoryFor(ctx context.Context, ns, name string) (*tatarav1alpha1.Repository, error) {
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &repo); err != nil {
		return nil, fmt.Errorf("reap: get repository %s: %w", name, err)
	}
	return &repo, nil
}

// dropOwnerRef removes taskName's ownerRef from obj in memory.
func dropOwnerRef(obj client.Object, taskName string) {
	refs := obj.GetOwnerReferences()
	kept := refs[:0]
	for _, ref := range refs {
		if ref.Kind == "Task" && ref.APIVersion == tatarav1alpha1.GroupVersion.String() && ref.Name == taskName {
			continue
		}
		kept = append(kept, ref)
	}
	obj.SetOwnerReferences(kept)
}

// stageEnteredAt is the clock every B.6 age is measured against. A Task with no
// stamp is treated as having entered its stage at creation - it can then only age
// OUT, never be kept alive by a missing timestamp.
func stageEnteredAt(t *tatarav1alpha1.Task) time.Time {
	if t.Status.StageEnteredAt != nil {
		return t.Status.StageEnteredAt.Time
	}
	return t.CreationTimestamp.Time
}

// annotateTask / annotateIssue / annotateMR persist ONE metadata marker. They are
// metadata writes, not status writes: nothing here competes with the status
// subresource, so a reap marker can never lose a race with a reconciler.
func (r *ProjectReconciler) annotateTask(ctx context.Context, t *tatarav1alpha1.Task, key, value string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cur tatarav1alpha1.Task
		if err := r.Get(ctx, client.ObjectKeyFromObject(t), &cur); err != nil {
			return err
		}
		if cur.Annotations == nil {
			cur.Annotations = map[string]string{}
		}
		cur.Annotations[key] = value
		if err := r.Update(ctx, &cur); err != nil {
			return err
		}
		t.SetAnnotations(cur.Annotations)
		return nil
	})
}

func (r *ProjectReconciler) annotateIssue(ctx context.Context, iss *tatarav1alpha1.Issue, key, value string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cur tatarav1alpha1.Issue
		if err := r.Get(ctx, client.ObjectKeyFromObject(iss), &cur); err != nil {
			return err
		}
		if cur.Annotations == nil {
			cur.Annotations = map[string]string{}
		}
		cur.Annotations[key] = value
		if err := r.Update(ctx, &cur); err != nil {
			return err
		}
		iss.SetAnnotations(cur.Annotations)
		iss.SetResourceVersion(cur.GetResourceVersion())
		return nil
	})
}

func (r *ProjectReconciler) annotateMR(ctx context.Context, mr *tatarav1alpha1.MergeRequest, key, value string) error {
	return annotateMergeRequest(ctx, r.Client, mr, key, value)
}
