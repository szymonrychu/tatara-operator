package queue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newEnqueueTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := tatarav1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1 scheme: %v", err)
	}
	return s
}

func testProj(name, ns string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
}

func TestEnqueueEvent_AssignsSeqAndFields(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.QueuedEvent{}).Build()
	seq := &SeqSource{Client: c, Namespace: "tatara"}
	proj := testProj("p", "tatara")
	pl := tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "incident-"}
	qe, created, err := EnqueueEvent(context.Background(), c, seq, proj, tatarav1alpha1.QueueClassAlert, false, "grp1", pl)
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
	seq := &SeqSource{Client: c, Namespace: "tatara"}
	proj := testProj("p", "tatara")
	pl := tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "incident-"}
	if _, created, _ := EnqueueEvent(context.Background(), c, seq, proj, tatarav1alpha1.QueueClassAlert, false, "grp1", pl); !created {
		t.Fatal("first enqueue should create")
	}
	_, created, err := EnqueueEvent(context.Background(), c, seq, proj, tatarav1alpha1.QueueClassAlert, false, "grp1", pl)
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

func TestBuildTaskFromQueuedEvent_SystemicGroup(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	proj := testProj("p", "ns")
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe1", Namespace: "ns"},
		Spec: tatarav1alpha1.QueuedEventSpec{Payload: tatarav1alpha1.QueuedEventPayload{
			Kind: "issueLifecycle", RepositoryRef: "r", Goal: "g", GenerateName: "scan-",
			SystemicGroup: &tatarav1alpha1.SystemicGroup{
				SystemicID: "abc", SameRepoSiblings: []int{12, 15}, CrossRepo: []string{"o/r2#3 - x"},
			},
		}},
	}
	task, err := BuildTaskFromQueuedEvent(qe, proj, scheme)
	if err != nil {
		t.Fatal(err)
	}
	if task.Spec.SystemicGroup == nil || task.Spec.SystemicGroup.SystemicID != "abc" ||
		len(task.Spec.SystemicGroup.SameRepoSiblings) != 2 || len(task.Spec.SystemicGroup.CrossRepo) != 1 {
		t.Fatalf("SystemicGroup not mapped: %+v", task.Spec.SystemicGroup)
	}
}

func TestBuildTaskFromQueuedEvent_CopiesAlertRule(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	proj := testProj("p", "ns")
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-x", Namespace: "ns"},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "incident", AlertRule: "HighCPU", GenerateName: "incident-"},
		},
	}
	task, err := BuildTaskFromQueuedEvent(qe, proj, scheme)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if task.Spec.AlertRule != "HighCPU" {
		t.Fatalf("want AlertRule=HighCPU, got %q", task.Spec.AlertRule)
	}
}

func TestBuildTaskFromQueuedEvent_CopiesDedupKey(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	proj := testProj("p", "ns")
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-y", Namespace: "ns"},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "incident", DedupKey: "deadbeefcafe1234", GenerateName: "incident-"},
		},
	}
	task, err := BuildTaskFromQueuedEvent(qe, proj, scheme)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if task.Spec.DedupKey != "deadbeefcafe1234" {
		t.Fatalf("want DedupKey=deadbeefcafe1234, got %q", task.Spec.DedupKey)
	}
}

func TestEnqueueEvent_DedupGatedByTaskTerminalState(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&tatarav1alpha1.QueuedEvent{}, &tatarav1alpha1.Task{}).
		Build()
	seq := &SeqSource{Client: c, Namespace: "tatara"}
	proj := testProj("p", "tatara")
	require.NoError(t, c.Create(context.Background(), proj))
	pay := tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "incident-"}

	// First firing creates a QueuedEvent.
	_, created1, err := EnqueueEvent(context.Background(), c, seq, proj, tatarav1alpha1.QueueClassAlert, false, "rulehash", pay)
	require.NoError(t, err)
	require.True(t, created1)

	// List the created QueuedEvent.
	var qel tatarav1alpha1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace("tatara")))
	require.Len(t, qel.Items, 1)
	qe := &qel.Items[0]

	// Simulate consumption: build the Task (carries the dedup label) and mark it
	// non-terminal (Running), then delete the QueuedEvent so only the Task gates.
	task, err := BuildTaskFromQueuedEvent(qe, proj, c.Scheme())
	require.NoError(t, err)
	require.NoError(t, c.Create(context.Background(), task))
	task.Status.Phase = "Running"
	require.NoError(t, c.Status().Update(context.Background(), task))
	require.NoError(t, c.Delete(context.Background(), qe))

	// Second firing: non-terminal Task with same dedup key -> NO new event.
	_, created2, err := EnqueueEvent(context.Background(), c, seq, proj, tatarav1alpha1.QueueClassAlert, false, "rulehash", pay)
	require.NoError(t, err)
	require.False(t, created2, "dedup while incident Task non-terminal")

	// Mark the Task terminal; third firing -> fresh event created.
	task.Status.Phase = "Succeeded"
	require.NoError(t, c.Status().Update(context.Background(), task))
	_, created3, err := EnqueueEvent(context.Background(), c, seq, proj, tatarav1alpha1.QueueClassAlert, false, "rulehash", pay)
	require.NoError(t, err)
	require.True(t, created3, "re-investigate once prior incident Task is terminal")
}

func TestEnqueueEvent_DedupAllowsAfterDone(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.QueuedEvent{}).Build()
	seq := &SeqSource{Client: c, Namespace: "tatara"}
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

	_, created, err := EnqueueEvent(context.Background(), c, seq, proj, tatarav1alpha1.QueueClassAlert, false, "grp2", pl)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("should allow enqueue when existing dedupKey event is Done")
	}
}
