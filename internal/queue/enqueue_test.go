package queue

import (
	"context"
	"errors"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newEnqueueTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := tatarav1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	return s
}

func testProj(name, ns string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
}

func TestEnqueueEvent_AssignsSeqAndFields(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.QueuedEvent{}).Build()
	alloc := NewSeqAllocator()
	alloc.Recover(0)
	proj := testProj("p", "tatara")
	pl := tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "incident-"}
	qe, created, err := EnqueueEvent(context.Background(), c, alloc, proj, tatarav1alpha1.QueueClassAlert, false, "grp1", pl)
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if qe.Spec.Seq != 1 || qe.Spec.Class != tatarav1alpha1.QueueClassAlert || qe.Spec.Kind != "incident" {
		t.Fatalf("bad spec: %+v", qe.Spec)
	}
	if qe.Labels[LabelDedupKey] != "grp1" || qe.Status.State != tatarav1alpha1.QueueStateQueued {
		t.Fatalf("bad labels/state: %v %q", qe.Labels, qe.Status.State)
	}
}

func TestEnqueueEvent_DedupSkips(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.QueuedEvent{}).Build()
	alloc := NewSeqAllocator()
	alloc.Recover(0)
	proj := testProj("p", "tatara")
	pl := tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "incident-"}
	if _, created, _ := EnqueueEvent(context.Background(), c, alloc, proj, tatarav1alpha1.QueueClassAlert, false, "grp1", pl); !created {
		t.Fatal("first enqueue should create")
	}
	_, created, err := EnqueueEvent(context.Background(), c, alloc, proj, tatarav1alpha1.QueueClassAlert, false, "grp1", pl)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("duplicate dedupKey should skip")
	}
}

func TestBuildTaskFromQueuedEvent(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	proj := testProj("p", "tatara")
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-1", Namespace: "tatara"},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 1, Class: tatarav1alpha1.QueueClassNormal, Kind: "review", ProjectRef: "p", RepositoryRef: "r",
			Payload: tatarav1alpha1.QueuedEventPayload{
				Kind: "review", RepositoryRef: "r", Goal: "g", GenerateName: "scan-",
				Labels: map[string]string{"x": "y"}, Provider: "github", PodRepo: "r",
			},
		},
	}
	task, err := BuildTaskFromQueuedEvent(qe, proj, scheme)
	if err != nil {
		t.Fatal(err)
	}
	if task.Spec.Kind != "review" || task.Spec.Goal != "g" || task.Spec.RepositoryRef != "r" {
		t.Fatalf("bad task spec: %+v", task.Spec)
	}
	if task.Labels[LabelQueuedEvent] != "qe-1" || task.Labels["x"] != "y" {
		t.Fatalf("missing labels: %v", task.Labels)
	}
	if task.Name != "scan-qe-1" {
		t.Fatalf("expected task.Name == GenerateName+qe.Name, got %q", task.Name)
	}
	if task.GenerateName != "" {
		t.Fatalf("expected empty generateName, got %q", task.GenerateName)
	}
}

func TestEnqueueEvent_DedupAllowsAfterDone(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.QueuedEvent{}).Build()
	alloc := NewSeqAllocator()
	alloc.Recover(0)
	proj := testProj("p", "tatara")
	pl := tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "incident-"}

	// Pre-insert a Done QueuedEvent with the dedup label.
	existing := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "qe-done",
			Namespace: proj.Namespace,
			Labels:    map[string]string{LabelDedupKey: "grp2"},
		},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 0, Class: tatarav1alpha1.QueueClassAlert, Kind: "incident", ProjectRef: proj.Name,
			DedupKey: "grp2",
		},
	}
	if err := c.Create(context.Background(), existing); err != nil {
		t.Fatalf("pre-insert: %v", err)
	}
	existing.Status.State = tatarav1alpha1.QueueStateDone
	if err := c.Status().Update(context.Background(), existing); err != nil {
		t.Fatalf("pre-insert status: %v", err)
	}

	_, created, err := EnqueueEvent(context.Background(), c, alloc, proj, tatarav1alpha1.QueueClassAlert, false, "grp2", pl)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("should allow enqueue when existing dedupKey event is Done")
	}
}

func TestEnqueueEvent_ErrSeqNotReady(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.QueuedEvent{}).Build()
	alloc := NewSeqAllocator() // NOT recovered
	proj := testProj("p", "tatara")
	pl := tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "incident-"}
	qe, created, err := EnqueueEvent(context.Background(), c, alloc, proj, tatarav1alpha1.QueueClassAlert, false, "grp-notready", pl)
	if !errors.Is(err, ErrSeqNotReady) {
		t.Fatalf("expected ErrSeqNotReady, got err=%v qe=%v created=%v", err, qe, created)
	}
	if created || qe != nil {
		t.Fatalf("must not create when not ready: created=%v qe=%v", created, qe)
	}
	// Verify no QueuedEvent was persisted.
	var list tatarav1alpha1.QueuedEventList
	if err2 := c.List(context.Background(), &list); err2 != nil {
		t.Fatal(err2)
	}
	if len(list.Items) != 0 {
		t.Fatalf("expected 0 persisted QEs, got %d", len(list.Items))
	}
}
