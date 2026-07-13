package queue

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

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

	// DedupKeyIndex is the field index key for QueuedEvent.Spec.DedupKey
	// (contract B.7 addendum 7: "queuedEventDedupKey" on QueuedEvent ->
	// spec.dedupKey). It is the AUTHORITATIVE in-flight QueuedEvent dedup
	// lookup ("is an identical event already Queued but not yet Admitted?"),
	// registered by DispatcherReconciler.SetupWithManager
	// (internal/controller/queue_controller.go) - NEVER a label selector.
	DedupKeyIndex = ".spec.dedupKey"
)

// DedupKeyIndexer is the client.IndexerFunc for DedupKeyIndex.
func DedupKeyIndexer(obj client.Object) []string {
	q, ok := obj.(*tatarav1alpha1.QueuedEvent)
	if !ok || q.Spec.DedupKey == "" {
		return nil
	}
	return []string{q.Spec.DedupKey}
}

// fieldSelectorUnsupported reports whether a list error is "field label not
// supported", which happens when a direct (non-cached) client is used without
// a registered field index (e.g. this package's unit tests, or an envtest
// suite with no manager). Mirrors internal/controller.isFieldSelectorUnsupported;
// duplicated rather than imported to avoid a controller->queue->controller cycle.
func fieldSelectorUnsupported(err error) bool {
	return err != nil && strings.Contains(err.Error(), "field label not supported")
}

// QueuedEventStillQueued reports whether a QueuedEvent with the given
// dedupKey is still Queued (not yet Admitted) for projectRef, via the
// DedupKeyIndex field index - the NEW in-flight dedup mechanism (contract B.7
// addendum 7, item 3). Falls back to a full-namespace scan filtered in Go
// when the index is unregistered. This is additive: EnqueueEvent/dedupExists
// below remain the OLD label-based mechanism until Task 20/21 rewires their
// callers (webhook/server.go and friends).
func QueuedEventStillQueued(ctx context.Context, c client.Client, ns, projectRef, dedupKey string) (bool, error) {
	if dedupKey == "" {
		return false, nil
	}
	var qel tatarav1alpha1.QueuedEventList
	err := c.List(ctx, &qel, client.InNamespace(ns), client.MatchingFields{DedupKeyIndex: dedupKey})
	if err != nil {
		if !fieldSelectorUnsupported(err) {
			return false, err
		}
		qel = tatarav1alpha1.QueuedEventList{}
		if err := c.List(ctx, &qel, client.InNamespace(ns)); err != nil {
			return false, err
		}
	}
	for i := range qel.Items {
		q := &qel.Items[i]
		if q.Spec.DedupKey != dedupKey || q.Spec.ProjectRef != projectRef {
			continue
		}
		if q.Status.State == "" || q.Status.State == tatarav1alpha1.QueueStateQueued {
			return true, nil
		}
	}
	return false, nil
}

// dedupLabel converts a dedup key into a K8s-label-safe value.
// K8s label values must match [a-zA-Z0-9][-_.a-zA-Z0-9]* and be <= 63 chars.
// Keys that contain '/', '#', '\x00', or other unsafe chars are sha256-hashed.
// Simple alphanumeric keys (e.g. "grp1", "brainstorm-myproj") pass through unchanged.
//
// Deprecated: this is the OLD dedup mechanism (contract B.7 addendum 7 deletes
// it), kept because EnqueueEvent/dedupExists/BuildTaskFromQueuedEvent below are
// still the live path for every webhook producer (internal/webhook/server.go)
// and cannot be repointed without rewiring those callers - out of scope for
// task 15 (the greenness rule, tasks 1-19 are purely additive). Task 20/21
// rewires the callers and deletes this.
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
		if tl.Items[i].Spec.ProjectRef == projectRef && !tatarav1alpha1.TaskDone(&tl.Items[i]) {
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

// IsAdmissionTicket reports whether a QueuedEvent ADMITS AN EXISTING Task
// (contract B.7: payload.taskRef) rather than minting one. In the task-centric
// model a Task already exists by the time it reaches a pod stage, so its
// QueuedEvent is an admission ticket for that Task's POD - not a Task-creation
// request. The dispatcher keys off this: a ticket resolves the Task, a mint
// (payload.newTask, or a legacy flat payload) calls BuildTaskFromQueuedEvent.
func IsAdmissionTicket(qe *tatarav1alpha1.QueuedEvent) bool {
	return qe.Spec.Payload.TaskRef != ""
}

// BuildTaskFromQueuedEvent reconstructs the Task the producer described, labelled
// with the QueuedEvent name (dispatcher completion mapping) and dedup key.
//
// Two MINT shapes are accepted: the B.7 blueprint (payload.newTask), and the
// legacy flat payload the sweep/webhook still produce. A payload carrying
// payload.taskRef is NOT a mint (see IsAdmissionTicket) and is refused: minting
// from it would create a SECOND Task alongside the one the ticket names.
//
// When payload.Name is empty, task.Name is set to payload.GenerateName+qe.Name.
func BuildTaskFromQueuedEvent(qe *tatarav1alpha1.QueuedEvent, proj *tatarav1alpha1.Project, scheme *runtime.Scheme) (*tatarav1alpha1.Task, error) {
	p := qe.Spec.Payload
	if p.TaskRef != "" {
		return nil, fmt.Errorf("build task: queuedevent %s admits existing task %q; it mints nothing", qe.Name, p.TaskRef)
	}
	labels := map[string]string{}
	for k, v := range p.Labels {
		labels[k] = v
	}
	om := metav1.ObjectMeta{Namespace: qe.Namespace, Annotations: p.Annotations}
	spec := tatarav1alpha1.TaskSpec{
		ProjectRef:    proj.Name,
		RepositoryRef: p.RepositoryRef,
		Goal:          p.Goal,
		Kind:          p.Kind,
		DedupKey:      p.DedupKey,
		Source:        p.Source,
	}
	if p.AlertRule != "" {
		spec.AlertRules = []string{p.AlertRule}
	}
	if p.Name != "" {
		om.Name = p.Name
	} else {
		om.Name = p.GenerateName + qe.Name
	}
	if bp := p.NewTask; bp != nil {
		// The B.7 mint blueprint WINS over the flat legacy fields: a stage-driven
		// producer fills only the blueprint. bp.IssueKeys has NO home on TaskSpec
		// (the contract defines the field but no Task field to land it in); Issue
		// CRs are minted from the forge mirror in triaging, not from the payload.
		labels = map[string]string{}
		for k, v := range bp.Labels {
			labels[k] = v
		}
		if bp.Annotations != nil {
			om.Annotations = bp.Annotations
		}
		om.Name = bp.Name
		spec.RepositoryRef = bp.RepositoryRef
		spec.Goal = bp.Goal
		spec.Kind = bp.Kind
		spec.AlertRules = bp.AlertRules
	}
	labels[LabelQueuedEvent] = qe.Name
	if qe.Spec.DedupKey != "" {
		labels[LabelDedupKey] = dedupLabel(qe.Spec.DedupKey)
	}
	om.Labels = labels
	task := &tatarav1alpha1.Task{ObjectMeta: om, Spec: spec}
	agent.StampPodName(task, proj.Name, p.Provider, p.PodRepo)
	if err := controllerutil.SetControllerReference(proj, task, scheme); err != nil {
		return nil, fmt.Errorf("build task: set ownerref: %w", err)
	}
	return task, nil
}
