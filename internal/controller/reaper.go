package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objstore"
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

	// Garbage-collect S3 conversation objects for fully-closed batches BEFORE the
	// task GC below, so the conversations are deleted while their Tasks (which
	// carry the keys) still exist (issue #114 decision 5).
	s.gcConversations(ctx, taskList.Items)

	// Garbage-collect terminal Tasks past the retention window, reusing the list
	// already fetched above (no extra API call). Subtasks cascade via owner ref.
	s.gcTerminalTasks(ctx, taskList.Items)

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
	if isTerminal(task.Status.Phase) {
		return fmt.Sprintf("task phase %s", task.Status.Phase), true
	}
	if isLifecycleTerminal(task.Status.DeployState) {
		return fmt.Sprintf("task lifecycle %s", task.Status.DeployState), true
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

// gcConversations deletes S3 conversation objects whose whole batch has gone
// terminal past the conversation-retention grace (issue #114 decision 5: delete
// a conversation when all sibling issues are closed).
//
// Batching: conversation-bearing Tasks (issueLifecycle, review, brainstorm) are
// grouped by batch id = the fork-from-conversation key when set, else the Task's
// own conversation key. A brainstorm Task's own key equals the fork key of the
// issues forked from it, so the brainstorm and all its sibling issues land in
// one batch; a solo issue/PR is its own singleton batch. A batch is deleted only
// when EVERY member is terminal and the newest went terminal more than the grace
// ago (so a quick reopen-after-close keeps the thread). Deletes are idempotent
// (Exists-then-Delete), so re-running until the Tasks are reaped is a no-op.
//
// The grace is kept under TaskRetention so this runs before gcTerminalTasks
// removes the Tasks that carry the keys; a brainstorm batch whose sibling close
// times span more than (TaskRetention - grace) can still orphan a key if an
// early sibling's Task is reaped first - a small, documented leak.
func (s *CallbackServer) gcConversations(ctx context.Context, tasks []tatarav1alpha1.Task) {
	if s.ConvStore == nil || s.ConversationRetention <= 0 {
		return
	}
	l := log.FromContext(ctx)

	type batch struct {
		keys             map[string]struct{}
		allTermPastGrace bool
	}
	batches := map[string]*batch{}
	for i := range tasks {
		tk := &tasks[i]
		switch tk.Spec.Kind {
		case "issueLifecycle", "review", "brainstorm", "documentation":
		default:
			continue
		}
		ownKey := conversationKeyForGC(tk)
		if ownKey == "" {
			continue
		}
		forkKey := tk.Annotations[annForkFromConversationKey]
		batchID := forkKey
		if batchID == "" {
			batchID = ownKey
		}
		b := batches[batchID]
		if b == nil {
			b = &batch{keys: map[string]struct{}{}, allTermPastGrace: true}
			batches[batchID] = b
		}
		b.keys[ownKey] = struct{}{}
		if forkKey != "" {
			b.keys[forkKey] = struct{}{}
		}
		if !terminalConversationPastGrace(tk, s.ConversationRetention) {
			b.allTermPastGrace = false
		}
	}

	// skipUnreachable short-circuits the whole pass on a store-wide / connection-
	// level failure (issue #149): without this, one object-store outage produces
	// one ERROR per backlog key (dozens) every reaper cycle and trips the
	// ">20 ERROR/5m" log-burst alert even though the operator is healthy and the
	// cause is a transient external dependency. We log ONE non-ERROR line and
	// record result="unavailable" so a dedicated, quieter alert can key off it;
	// the next reaper cycle retries once the store recovers.
	skipUnreachable := func(err error) {
		l.Info("reaper: conversation GC skipped: object store unreachable",
			"action", "gc_conversation", "error", err.Error())
		s.Metrics.ConversationGC("unavailable")
	}

batchLoop:
	for _, b := range batches {
		if !b.allTermPastGrace {
			continue
		}
		for key := range b.keys {
			if ctx.Err() != nil {
				return
			}
			exists, err := s.ConvStore.Exists(ctx, key)
			if err != nil {
				if objstore.IsUnavailable(err) {
					skipUnreachable(err)
					break batchLoop
				}
				l.Error(err, "reaper: probe conversation object", "action", "gc_conversation", "key", key)
				s.Metrics.ConversationGC("error")
				continue
			}
			if !exists {
				continue
			}
			if err := s.ConvStore.Delete(ctx, key); err != nil {
				if objstore.IsUnavailable(err) {
					skipUnreachable(err)
					break batchLoop
				}
				l.Error(err, "reaper: delete conversation object", "action", "gc_conversation", "key", key)
				s.Metrics.ConversationGC("error")
				continue
			}
			l.Info("garbage-collected conversation object", "action", "gc_conversation", "key", key)
			s.Metrics.ConversationGC("deleted")
		}
	}
}

// conversationKeyForGC returns the Task's recorded conversation key, falling back
// to the deterministic derivation when none was recorded yet.
func conversationKeyForGC(tk *tatarav1alpha1.Task) string {
	if tk.Status.ConversationObjectKey != "" {
		return tk.Status.ConversationObjectKey
	}
	return agent.ConversationKey(tk)
}

// terminalConversationPastGrace reports whether tk is terminal and its last
// activity (fallback: creation) is older than grace.
func terminalConversationPastGrace(tk *tatarav1alpha1.Task, grace time.Duration) bool {
	if !tatarav1alpha1.TaskTerminal(tk) {
		return false
	}
	ts := tk.CreationTimestamp.Time
	if tk.Status.LastActivityAt != nil {
		ts = tk.Status.LastActivityAt.Time
	}
	return time.Since(ts) > grace
}

// gcTerminalTasks deletes Tasks that are terminal (TaskTerminal: Succeeded/Failed
// phase or Done/Stopped/Parked lifecycle) AND older than the retention window.
// Subtasks carry a controller OwnerReference to their Task, so background-
// propagation delete cascades to them through normal Kubernetes garbage
// collection - the GC only has to delete the Tasks.
//
// Without GC the operator's hot paths (reaper, projectscan max-open/dedup,
// incident dedup) do unindexed full-namespace Task Lists whose cost grows with
// every terminal Task ever created; this bounds that set. Non-terminal Tasks are
// never touched. A zero/negative retention disables the pass entirely.
func (s *CallbackServer) gcTerminalTasks(ctx context.Context, tasks []tatarav1alpha1.Task) {
	if s.TaskRetention <= 0 {
		return
	}
	l := log.FromContext(ctx)
	cutoff := time.Now().Add(-s.TaskRetention)
	policy := metav1.DeletePropagationBackground
	for i := range tasks {
		if ctx.Err() != nil {
			return
		}
		tk := &tasks[i]
		if !tatarav1alpha1.TaskTerminal(tk) {
			continue
		}
		if tk.CreationTimestamp.After(cutoff) {
			continue // younger than the retention window
		}
		// Spare a recoverable give-up task while its issue is still open: the
		// ImplementGiveUps counter must outlive the retention window so
		// recoverOrphans can reroll it (under cap) or keep it blocked without
		// restarting the count from zero (at cap). recoverOrphans transitions a
		// give-up task whose issue has closed to Done ("issue-closed"), so a
		// still-Parked recoverable give-up here means the issue is open.
		if tk.Status.DeployState == "Parked" &&
			tatarav1alpha1.IsRecoverableGiveup(tk.Status.ParkReason) &&
			tk.Status.ImplementGiveUps > 0 {
			continue
		}
		age := time.Since(tk.CreationTimestamp.Time)
		// Clean per-issue token and turn series before deletion so Prometheus does
		// not accumulate stale series forever (bounded cardinality). Skip project-
		// scoped tasks (empty issue) to avoid clearing shared label-value buckets.
		if s.Metrics != nil {
			project, repo, kind, issue, model := taskTokenLabels(tk)
			if issue != "" {
				s.Metrics.DeleteTaskSeries(project, repo, kind, issue, model)
			}
		}
		del := tk.DeepCopy()
		if err := s.Client.Delete(ctx, del, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
			l.Error(err, "reaper: gc terminal task", "action", "gc_task",
				"resource_id", tk.Name, "kind", tk.Spec.Kind)
			s.Metrics.ReapDeleteError("task")
			continue
		}
		l.Info("garbage-collected terminal task", "action", "gc_task",
			"resource_id", tk.Name, "kind", tk.Spec.Kind, "age_seconds", age.Seconds())
		s.Metrics.TasksGC(tk.Spec.Kind)
	}
}
