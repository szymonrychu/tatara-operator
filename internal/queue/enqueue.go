package queue

import (
	"context"
	"crypto/sha256"
	"fmt"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	LabelQueuedEvent = "tatara.dev/queued-event"
	LabelDedupKey    = "tatara.dev/dedup-key"
)

// dedupLabel converts a dedup key into a K8s-label-safe value.
// K8s label values must match [a-zA-Z0-9][-_.a-zA-Z0-9]* and be <= 63 chars.
// Keys that contain '/', '#', '\x00', or other unsafe chars are sha256-hashed.
// Simple alphanumeric keys (e.g. "grp1", "brainstorm-myproj") pass through unchanged.
func dedupLabel(key string) string {
	if key == "" {
		return ""
	}
	if len(key) <= 63 && isLabelSafe(key) {
		return key
	}
	h := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%x", h[:16])
}

// isLabelSafe returns true when key matches the K8s label-value format:
// [a-zA-Z0-9][-_.a-zA-Z0-9]* with an optional leading char requirement.
// Empty string is handled by the caller.
func isLabelSafe(key string) bool {
	for i, ch := range key {
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= 'A' && ch <= 'Z':
		case ch >= '0' && ch <= '9':
		case ch == '-' || ch == '_' || ch == '.':
			if i == 0 {
				return false // must start with alphanumeric
			}
		default:
			return false
		}
	}
	return true
}

// dedupExists reports whether a non-Done QueuedEvent or a non-terminal Task
// with dedupKey already exists for the project. In practice no QE ever reaches
// state Done (reconcileDone GC-deletes them on Task completion), so the != Done
// guard is defensive; do not remove it as callers may set Done in tests.
func dedupExists(ctx context.Context, c client.Client, ns, projectRef, dedupKey string) (bool, error) {
	if dedupKey == "" {
		return false, nil
	}
	label := dedupLabel(dedupKey)
	var qel tatarav1alpha1.QueuedEventList
	if err := c.List(ctx, &qel, client.InNamespace(ns), client.MatchingLabels{LabelDedupKey: label}); err != nil {
		return false, err
	}
	for i := range qel.Items {
		if qel.Items[i].Spec.ProjectRef == projectRef && qel.Items[i].Status.State != tatarav1alpha1.QueueStateDone {
			return true, nil
		}
	}
	var tl tatarav1alpha1.TaskList
	if err := c.List(ctx, &tl, client.InNamespace(ns), client.MatchingLabels{LabelDedupKey: label}); err != nil {
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
// Returns created=false when dedupKey already has live work. The per-project seq
// is allocated durably (per-project ConfigMap CAS) AFTER the dedup check so a
// deduped event never burns a sequence number.
func EnqueueEvent(ctx context.Context, c client.Client, seq *SeqSource, proj *tatarav1alpha1.Project,
	class string, autonomous bool, dedupKey string, payload tatarav1alpha1.QueuedEventPayload) (*tatarav1alpha1.QueuedEvent, bool, error) {

	dup, err := dedupExists(ctx, c, proj.Namespace, proj.Name, dedupKey)
	if err != nil {
		return nil, false, err
	}
	if dup {
		return nil, false, nil
	}
	seqNum, err := seq.Next(ctx, proj.Name)
	if err != nil {
		return nil, false, fmt.Errorf("enqueue: allocate seq: %w", err)
	}
	labels := map[string]string{}
	if dedupKey != "" {
		labels[LabelDedupKey] = dedupLabel(dedupKey)
	}
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "qe-",
			Namespace:    proj.Namespace,
			Labels:       labels,
		},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq:           seqNum,
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

// BuildTaskFromQueuedEvent reconstructs the Task the producer described, labelled
// with the QueuedEvent name (dispatcher completion mapping) and dedup key.
// When payload.Name is empty, task.Name is set to payload.GenerateName+qe.Name.
func BuildTaskFromQueuedEvent(qe *tatarav1alpha1.QueuedEvent, proj *tatarav1alpha1.Project, scheme *runtime.Scheme) (*tatarav1alpha1.Task, error) {
	p := qe.Spec.Payload
	labels := map[string]string{}
	for k, v := range p.Labels {
		labels[k] = v
	}
	labels[LabelQueuedEvent] = qe.Name
	if qe.Spec.DedupKey != "" {
		labels[LabelDedupKey] = dedupLabel(qe.Spec.DedupKey)
	}
	om := metav1.ObjectMeta{
		Namespace:   qe.Namespace,
		Labels:      labels,
		Annotations: p.Annotations,
	}
	if p.Name != "" {
		om.Name = p.Name
	} else {
		om.Name = p.GenerateName + qe.Name
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
