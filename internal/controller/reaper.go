package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
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

	for i := range pods.Items {
		pod := &pods.Items[i]
		reason, orphan := s.orphanReason(ctx, pod)
		if !orphan {
			continue
		}
		l.Info("reaping orphan wrapper pod", "action", "reap_orphan",
			"resource_id", pod.Name, "task", pod.Labels[agent.LabelTask], "reason", reason)
		s.reapWrapper(ctx, pod.Name)
	}
}

// orphanReason decides whether a wrapper pod should be reaped and why. It returns
// (reason, true) when the pod is an orphan, or ("", false) when it backs a live,
// non-terminal Task and must be left running.
func (s *CallbackServer) orphanReason(ctx context.Context, pod *corev1.Pod) (string, bool) {
	taskName := pod.Labels[agent.LabelTask]
	if taskName == "" {
		// Unlabelled wrapper pod: cannot correlate, leave it for a human.
		return "", false
	}

	var task tatarav1alpha1.Task
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: taskName}, &task); err != nil {
		if apierrors.IsNotFound(err) {
			return "task absent", true
		}
		// Transient API error: do not reap on uncertainty.
		return "", false
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
// name. A missing object is not an error.
func (s *CallbackServer) reapWrapper(ctx context.Context, name string) {
	l := log.FromContext(ctx)
	pod := &corev1.Pod{}
	pod.Name = name
	pod.Namespace = s.Namespace
	if err := s.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		l.Error(err, "reaper: delete pod", "resource_id", name)
	}
	svc := &corev1.Service{}
	svc.Name = name
	svc.Namespace = s.Namespace
	if err := s.Client.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		l.Error(err, "reaper: delete service", "resource_id", name)
	}
}
