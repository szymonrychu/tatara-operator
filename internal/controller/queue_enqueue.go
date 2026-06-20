package controller

import (
	"context"
	"fmt"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	LabelQueuedEvent = "tatara.dev/queued-event"
	LabelDedupKey    = "tatara.dev/dedup-key"
)

// dedupExists reports whether a non-Done QueuedEvent or a non-terminal Task
// with dedupKey already exists for the project.
func dedupExists(ctx context.Context, c client.Client, ns, projectRef, dedupKey string) (bool, error) {
	if dedupKey == "" {
		return false, nil
	}
	var qel tatarav1alpha1.QueuedEventList
	if err := c.List(ctx, &qel, client.InNamespace(ns), client.MatchingLabels{LabelDedupKey: dedupKey}); err != nil {
		return false, err
	}
	for i := range qel.Items {
		if qel.Items[i].Spec.ProjectRef == projectRef && qel.Items[i].Status.State != tatarav1alpha1.QueueStateDone {
			return true, nil
		}
	}
	var tl tatarav1alpha1.TaskList
	if err := c.List(ctx, &tl, client.InNamespace(ns), client.MatchingLabels{LabelDedupKey: dedupKey}); err != nil {
		return false, err
	}
	for i := range tl.Items {
		if tl.Items[i].Spec.ProjectRef == projectRef && !tatarav1alpha1.TaskTerminal(&tl.Items[i]) {
			return true, nil
		}
	}
	return false, nil
}

// EnqueueEvent writes a QueuedEvent (seq-assigned, owned by Project, state=Queued).
// Returns created=false when dedupKey already has live work.
func EnqueueEvent(ctx context.Context, c client.Client, alloc *queue.SeqAllocator, proj *tatarav1alpha1.Project,
	class string, autonomous bool, dedupKey string, payload tatarav1alpha1.QueuedEventPayload) (*tatarav1alpha1.QueuedEvent, bool, error) {

	dup, err := dedupExists(ctx, c, proj.Namespace, proj.Name, dedupKey)
	if err != nil {
		return nil, false, err
	}
	if dup {
		return nil, false, nil
	}
	labels := map[string]string{}
	if dedupKey != "" {
		labels[LabelDedupKey] = dedupKey
	}
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "qe-",
			Namespace:    proj.Namespace,
			Labels:       labels,
		},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq:           alloc.Next(),
			Class:         class,
			Kind:          payload.Kind,
			Autonomous:    autonomous,
			ProjectRef:    proj.Name,
			RepositoryRef: payload.RepositoryRef,
			DedupKey:      dedupKey,
			Payload:       payload,
		},
	}
	if err := controllerutil.SetControllerReference(proj, qe, c.Scheme()); err != nil {
		return nil, false, fmt.Errorf("enqueue: set ownerref: %w", err)
	}
	if err := c.Create(ctx, qe); err != nil {
		return nil, false, fmt.Errorf("enqueue: create queuedevent: %w", err)
	}
	qe.Status.State = tatarav1alpha1.QueueStateQueued
	if err := c.Status().Update(ctx, qe); err != nil {
		return nil, false, fmt.Errorf("enqueue: set state: %w", err)
	}
	return qe, true, nil
}

// buildTaskFromQueuedEvent reconstructs the Task the producer described, labelled
// with the QueuedEvent name (dispatcher completion mapping) and dedup key.
func buildTaskFromQueuedEvent(qe *tatarav1alpha1.QueuedEvent, proj *tatarav1alpha1.Project, scheme *runtime.Scheme) (*tatarav1alpha1.Task, error) {
	p := qe.Spec.Payload
	labels := map[string]string{}
	for k, v := range p.Labels {
		labels[k] = v
	}
	labels[LabelQueuedEvent] = qe.Name
	if qe.Spec.DedupKey != "" {
		labels[LabelDedupKey] = qe.Spec.DedupKey
	}
	om := metav1.ObjectMeta{
		Namespace:   qe.Namespace,
		Labels:      labels,
		Annotations: p.Annotations,
	}
	if p.Name != "" {
		om.Name = p.Name
	} else {
		om.GenerateName = p.GenerateName
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: om,
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: p.RepositoryRef,
			Goal:          p.Goal,
			Kind:          p.Kind,
			Source:        p.Source,
		},
	}
	agent.StampPodName(task, proj.Name, p.Provider, p.PodRepo)
	if err := controllerutil.SetControllerReference(proj, task, scheme); err != nil {
		return nil, fmt.Errorf("build task: set ownerref: %w", err)
	}
	return task, nil
}
