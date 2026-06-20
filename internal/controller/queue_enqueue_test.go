package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testProject(name, ns string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
}

func TestEnqueueEvent_AssignsSeqAndFields(t *testing.T) {
	scheme := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.QueuedEvent{}).Build()
	alloc := queue.NewSeqAllocator()
	proj := testProject("p", "tatara")
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
	scheme := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.QueuedEvent{}).Build()
	alloc := queue.NewSeqAllocator()
	proj := testProject("p", "tatara")
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
	scheme := newTestScheme(t)
	proj := testProject("p", "tatara")
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
	task, err := buildTaskFromQueuedEvent(qe, proj, scheme)
	if err != nil {
		t.Fatal(err)
	}
	if task.Spec.Kind != "review" || task.Spec.Goal != "g" || task.Spec.RepositoryRef != "r" {
		t.Fatalf("bad task spec: %+v", task.Spec)
	}
	if task.Labels[LabelQueuedEvent] != "qe-1" || task.Labels["x"] != "y" {
		t.Fatalf("missing labels: %v", task.Labels)
	}
	if task.GenerateName != "scan-" {
		t.Fatalf("bad generateName: %q", task.GenerateName)
	}
}
