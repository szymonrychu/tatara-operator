package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
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
	for i := range svcs.Items {
		if ctx.Err() != nil {
			return
		}
		svc := &svcs.Items[i]
		if _, alive := alivePods[svc.Name]; alive {
			// Pod is present and non-orphan; Service is fine.
			continue
		}
		// No live Pod backs this Service; delete it.
		del := svc.DeepCopy()
		if err := s.Client.Delete(ctx, del); err != nil && !apierrors.IsNotFound(err) {
			l.Error(err, "reaper: delete orphan service", "action", "reap_orphan_service", "resource_id", svc.Name)
			s.Metrics.ReapDeleteError("service")
		} else {
			l.Info("reaped orphan wrapper service", "action", "reap_orphan_service", "resource_id", svc.Name)
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
	if isLifecycleTerminal(task.Status.LifecycleState) {
		return fmt.Sprintf("task lifecycle %s", task.Status.LifecycleState), true
	}
	return "", false
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
