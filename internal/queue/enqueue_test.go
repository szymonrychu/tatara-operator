package queue

import (
	"context"
	"sync"
	"testing"
	"time"

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
	if len(task.Spec.AlertRules) != 1 || task.Spec.AlertRules[0] != "HighCPU" {
		t.Fatalf("want AlertRules=[HighCPU], got %v", task.Spec.AlertRules)
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
	task.Status.Stage = tatarav1alpha1.StageInvestigating
	require.NoError(t, c.Status().Update(context.Background(), task))
	require.NoError(t, c.Delete(context.Background(), qe))

	// Second firing: non-terminal Task with same dedup key -> NO new event.
	_, created2, err := EnqueueEvent(context.Background(), c, seq, proj, tatarav1alpha1.QueueClassAlert, false, "rulehash", pay)
	require.NoError(t, err)
	require.False(t, created2, "dedup while incident Task non-terminal")

	// Mark the Task terminal; third firing -> fresh event created.
	task.Status.Stage = tatarav1alpha1.StageFailed
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

func TestEnqueueEvent_ConcurrentSameKeyMintsOnce(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&tatarav1alpha1.QueuedEvent{}).Build()
	seq := &SeqSource{Client: c, Namespace: "tatara"}
	proj := testProj("p", "tatara")
	pl := tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "incident-"}

	const n = 8
	var wg sync.WaitGroup
	created := make([]bool, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, ok, err := EnqueueEvent(context.Background(), c, seq, proj,
				tatarav1alpha1.QueueClassAlert, false, "grp-race", pl)
			created[i], errs[i] = ok, err
		}(i)
	}
	wg.Wait()

	wins := 0
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i], "AlreadyExists must be swallowed, never surfaced")
		if created[i] {
			wins++
		}
	}
	require.Equal(t, 1, wins, "exactly one concurrent mint may win")

	var qel tatarav1alpha1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel))
	require.Len(t, qel.Items, 1, "exactly one QueuedEvent for the natural key")
	require.Equal(t, QueuedEventName("p", "grp-race"), qel.Items[0].Name)
}

// --- dedupExists scope: live QueuedEvent, live Task, open Issue -----------

func TestDedupExists_LiveQueuedEventSuppresses(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.QueuedEvent{}).Build()
	ns, proj, key := "tns", "p1", "abc123def4567890"
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe1", Namespace: ns, Labels: map[string]string{LabelDedupKey: dedupLabel(key)}},
		Spec:       tatarav1alpha1.QueuedEventSpec{ProjectRef: proj, DedupKey: key},
	}
	require.NoError(t, c.Create(context.Background(), qe))
	qe.Status.State = tatarav1alpha1.QueueStateQueued
	require.NoError(t, c.Status().Update(context.Background(), qe))

	got, err := dedupExists(context.Background(), c, ns, proj, key, true)
	require.NoError(t, err)
	require.True(t, got, "a live (Queued) QueuedEvent for the dedup key must suppress")
}

func TestDedupExists_LiveTaskSuppresses(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.Task{}).Build()
	ns, proj, key := "tns", "p1", "abc123def4567890"
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1", Namespace: ns, Labels: map[string]string{LabelDedupKey: dedupLabel(key)}},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj},
	}
	require.NoError(t, c.Create(context.Background(), task))
	task.Status.Stage = tatarav1alpha1.StageInvestigating
	require.NoError(t, c.Status().Update(context.Background(), task))

	got, err := dedupExists(context.Background(), c, ns, proj, key, true)
	require.NoError(t, err)
	require.True(t, got, "a non-terminal Task for the dedup key must suppress")
}

func TestDedupExists_OpenIssueSuppresses(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.Issue{}).Build()
	ns, proj, key := "tns", "p1", "abc123def4567890"
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "iss-open", Namespace: ns,
			Labels: map[string]string{LabelAlertRuleKey: key},
		},
		Spec: tatarav1alpha1.IssueSpec{ProjectRef: proj, RepositoryRef: "r"},
	}
	require.NoError(t, c.Create(context.Background(), iss))
	iss.Status.State = "open"
	require.NoError(t, c.Status().Update(context.Background(), iss))

	got, err := dedupExists(context.Background(), c, ns, proj, key, true)
	require.NoError(t, err)
	require.True(t, got, "an OPEN incident Issue for the rule-key must suppress")
}

func TestDedupExists_ClosedIssueDoesNotSuppress(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.Issue{}).Build()
	ns, proj, key := "tns", "p1", "abc123def4567890"
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "iss-closed", Namespace: ns,
			Labels: map[string]string{LabelAlertRuleKey: key},
		},
		Spec: tatarav1alpha1.IssueSpec{ProjectRef: proj, RepositoryRef: "r"},
	}
	require.NoError(t, c.Create(context.Background(), iss))
	iss.Status.State = "closed"
	require.NoError(t, c.Status().Update(context.Background(), iss))

	got, err := dedupExists(context.Background(), c, ns, proj, key, true)
	require.NoError(t, err)
	require.False(t, got, "a CLOSED incident Issue must NOT suppress - refire is fresh")
}

// The escalation path passes checkOpenIssue=false: an open tracker Issue must
// NOT suppress (it is what escalation re-investigates), while a live Task still
// does.
func TestDedupExists_IgnoreOpenIssueBypassesTrackerButNotLiveTask(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.Issue{}).Build()
	ns, proj, key := "tns", "p1", "abc123def4567890"
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: "iss-open", Namespace: ns,
			Labels: map[string]string{LabelAlertRuleKey: key}},
		Spec: tatarav1alpha1.IssueSpec{ProjectRef: proj, RepositoryRef: "r"},
	}
	require.NoError(t, c.Create(context.Background(), iss))
	iss.Status.State = "open"
	require.NoError(t, c.Status().Update(context.Background(), iss))

	got, err := dedupExists(context.Background(), c, ns, proj, key, false)
	require.NoError(t, err)
	require.False(t, got, "with checkOpenIssue=false, an open tracker must NOT suppress")

	// But a live escalation Task still single-flights: it must suppress.
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-esc", Namespace: ns,
			Labels: map[string]string{LabelDedupKey: dedupLabel(key)}},
		Spec: tatarav1alpha1.TaskSpec{ProjectRef: proj},
	}
	require.NoError(t, c.Create(context.Background(), task))
	got, err = dedupExists(context.Background(), c, ns, proj, key, false)
	require.NoError(t, err)
	require.True(t, got, "a live escalation Task must still suppress a second escalation")
}

func TestFindOldestOpenGroupSibling(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.Issue{}).Build()
	ns, proj, grp := "tns", "p1", "group0123456789a"

	// An older sibling (different rule-key, same group) and our own new tracker
	// (excluded by rule-key), both open.
	sib := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: "iss-sibling", Namespace: ns,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
			Labels: map[string]string{
				LabelAlertGroupKey: dedupLabel(grp), LabelAlertRuleKey: "rulekeyB0000000"}},
		Spec: tatarav1alpha1.IssueSpec{ProjectRef: proj, RepositoryRef: "tatara-memory", Number: 7},
	}
	self := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: "iss-self", Namespace: ns,
			Labels: map[string]string{
				LabelAlertGroupKey: dedupLabel(grp), LabelAlertRuleKey: "rulekeyA0000000"}},
		Spec: tatarav1alpha1.IssueSpec{ProjectRef: proj, RepositoryRef: "tatara-operator", Number: 9},
	}
	require.NoError(t, c.Create(context.Background(), sib))
	require.NoError(t, c.Create(context.Background(), self))

	got, ok, err := FindOldestOpenGroupSibling(context.Background(), c, ns, proj, grp, "rulekeyA0000000")
	require.NoError(t, err)
	require.True(t, ok, "an open sibling with a different rule-key in the same group must match")
	require.Equal(t, "iss-sibling", got.Name)
	require.Equal(t, 7, got.Spec.Number)

	// Empty group key => no correlation.
	_, ok, err = FindOldestOpenGroupSibling(context.Background(), c, ns, proj, "", "rulekeyA0000000")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestFindOldestOpenGroupSibling_ClosedAndSameRuleExcluded(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.Issue{}).Build()
	ns, proj, grp := "tns", "p1", "group0123456789a"

	closed := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: "iss-closed-sib", Namespace: ns,
			Labels: map[string]string{LabelAlertGroupKey: dedupLabel(grp), LabelAlertRuleKey: "rulekeyB0000000"}},
		Spec: tatarav1alpha1.IssueSpec{ProjectRef: proj, RepositoryRef: "tatara-memory", Number: 7},
	}
	require.NoError(t, c.Create(context.Background(), closed))
	closed.Status.State = "closed"
	require.NoError(t, c.Status().Update(context.Background(), closed))

	// Same-rule sibling (must be excluded even though open).
	sameRule := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: "iss-same-rule", Namespace: ns,
			Labels: map[string]string{LabelAlertGroupKey: dedupLabel(grp), LabelAlertRuleKey: "rulekeyA0000000"}},
		Spec: tatarav1alpha1.IssueSpec{ProjectRef: proj, RepositoryRef: "tatara-operator", Number: 9},
	}
	require.NoError(t, c.Create(context.Background(), sameRule))

	_, ok, err := FindOldestOpenGroupSibling(context.Background(), c, ns, proj, grp, "rulekeyA0000000")
	require.NoError(t, err)
	require.False(t, ok, "a closed sibling and a same-rule sibling must both be excluded")
}

func TestFindOpenIncidentIssue_EmptyKeyReturnsNotFound(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	_, ok, err := FindOpenIncidentIssue(context.Background(), c, "ns", "p", "")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestFindOpenIncidentIssue_DifferentProjectDoesNotMatch(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tatarav1alpha1.Issue{}).Build()
	ns, key := "tns", "abc123def4567890"
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: "iss1", Namespace: ns, Labels: map[string]string{LabelAlertRuleKey: key}},
		Spec:       tatarav1alpha1.IssueSpec{ProjectRef: "other-project", RepositoryRef: "r"},
	}
	require.NoError(t, c.Create(context.Background(), iss))
	iss.Status.State = "open"
	require.NoError(t, c.Status().Update(context.Background(), iss))

	_, ok, err := FindOpenIncidentIssue(context.Background(), c, ns, "p1", key)
	require.NoError(t, err)
	require.False(t, ok, "an open Issue under a DIFFERENT project must not match")
}

// TestDedupKeyIndexer_RoundTripsThroughSpecField verifies the NEW dedup
// mechanism (contract B.7 addendum 7): the natural key round-trips through
// QueuedEvent.spec.dedupKey and the DedupKeyIndex field index, with NO label
// involved anywhere.
func TestDedupKeyIndexer_RoundTripsThroughSpecField(t *testing.T) {
	qe := &tatarav1alpha1.QueuedEvent{
		Spec: tatarav1alpha1.QueuedEventSpec{DedupKey: "iss:o/r#42"},
	}
	got := DedupKeyIndexer(qe)
	require.Equal(t, []string{"iss:o/r#42"}, got)
	if qe.Labels != nil {
		t.Fatalf("DedupKeyIndexer must not require or inspect labels, got %v", qe.Labels)
	}
}

func TestDedupKeyIndexer_EmptyDedupKeyIndexesNothing(t *testing.T) {
	qe := &tatarav1alpha1.QueuedEvent{}
	if got := DedupKeyIndexer(qe); got != nil {
		t.Fatalf("want nil for empty dedupKey, got %v", got)
	}
}

// TestQueuedEventStillQueued_FindsByFieldIndex_NoLabel proves the natural key
// "iss:<repo>#<number>" - which CANNOT be a label value (':' and '#' are not
// label-safe) - is found via the DedupKeyIndex field lookup on a QueuedEvent
// that carries NO tatara.dev/dedup-key label at all.
func TestQueuedEventStillQueued_FindsByFieldIndex_NoLabel(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&tatarav1alpha1.QueuedEvent{}).
		WithIndex(&tatarav1alpha1.QueuedEvent{}, DedupKeyIndex, DedupKeyIndexer).
		Build()

	natural := "iss:o/r#42"
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-field-dedup", Namespace: "tatara"},
		Spec:       tatarav1alpha1.QueuedEventSpec{Seq: 1, Class: tatarav1alpha1.QueueClassNormal, Kind: "review", ProjectRef: "p", DedupKey: natural},
	}
	require.NoError(t, c.Create(context.Background(), qe))
	qe.Status.State = tatarav1alpha1.QueueStateQueued
	require.NoError(t, c.Status().Update(context.Background(), qe))

	if len(qe.Labels) != 0 {
		t.Fatalf("QueuedEvent built without EnqueueEvent must carry no labels, got %v", qe.Labels)
	}

	found, err := QueuedEventStillQueued(context.Background(), c, "tatara", "p", natural)
	require.NoError(t, err)
	if !found {
		t.Fatal("want QueuedEventStillQueued true: natural key is Queued, found via the field index, no label involved")
	}

	notFound, err := QueuedEventStillQueued(context.Background(), c, "tatara", "p", "mr:o/r!7")
	require.NoError(t, err)
	if notFound {
		t.Fatal("want false for a dedupKey that has no matching QueuedEvent")
	}
}

func TestQueuedEventStillQueued_AdmittedDoesNotCount(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&tatarav1alpha1.QueuedEvent{}).
		WithIndex(&tatarav1alpha1.QueuedEvent{}, DedupKeyIndex, DedupKeyIndexer).
		Build()

	natural := "iss:o/r#43"
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-field-dedup-admitted", Namespace: "tatara"},
		Spec:       tatarav1alpha1.QueuedEventSpec{Seq: 1, Class: tatarav1alpha1.QueueClassNormal, Kind: "review", ProjectRef: "p", DedupKey: natural},
	}
	require.NoError(t, c.Create(context.Background(), qe))
	qe.Status.State = tatarav1alpha1.QueueStateAdmitted
	require.NoError(t, c.Status().Update(context.Background(), qe))

	found, err := QueuedEventStillQueued(context.Background(), c, "tatara", "p", natural)
	require.NoError(t, err)
	if found {
		t.Fatal("an Admitted QueuedEvent is no longer 'Queued but not yet Admitted' - must not count")
	}
}

// TestBuildTaskFromQueuedEvent_TaskRefIsNotAMint: a TaskRef payload is an
// ADMISSION TICKET for an existing Task (contract B.7), never a mint. Building a
// Task from it is a programming error and must be refused loudly rather than
// silently creating a second Task alongside the one the ticket names.
func TestBuildTaskFromQueuedEvent_TaskRefIsNotAMint(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	proj := testProj("p", "ns")
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-ticket", Namespace: "ns"},
		Spec: tatarav1alpha1.QueuedEventSpec{Payload: tatarav1alpha1.QueuedEventPayload{
			Kind: "implement", AgentKind: "implement", TaskRef: "existing-task",
		}},
	}
	if _, err := BuildTaskFromQueuedEvent(qe, proj, scheme); err == nil {
		t.Fatal("BuildTaskFromQueuedEvent must refuse a taskRef payload")
	}
}

// TestBuildTaskFromQueuedEvent_NewTaskBlueprint: the B.7 mint shape. The
// blueprint - not the flat legacy fields - describes the Task.
func TestBuildTaskFromQueuedEvent_NewTaskBlueprint(t *testing.T) {
	scheme := newEnqueueTestScheme(t)
	proj := testProj("p", "ns")
	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-mint", Namespace: "ns"},
		Spec: tatarav1alpha1.QueuedEventSpec{
			DedupKey: "iss:o/r#7",
			Payload: tatarav1alpha1.QueuedEventPayload{
				Kind: "clarify", AgentKind: "clarify",
				NewTask: &tatarav1alpha1.QueuedTaskBlueprint{
					Name: "p-clarify-2026-07-13-a1b2c", Kind: "clarify", Goal: "fix it",
					ProjectRef: "p", RepositoryRef: "r",
					AlertRules: []string{"HighCPU"},
					Labels:     map[string]string{"x": "y"},
				},
			},
		},
	}
	task, err := BuildTaskFromQueuedEvent(qe, proj, scheme)
	if err != nil {
		t.Fatal(err)
	}
	if task.Name != "p-clarify-2026-07-13-a1b2c" {
		t.Fatalf("blueprint name not used: %q", task.Name)
	}
	if task.Spec.Kind != "clarify" || task.Spec.Goal != "fix it" || task.Spec.RepositoryRef != "r" {
		t.Fatalf("blueprint not mapped onto spec: %+v", task.Spec)
	}
	if len(task.Spec.AlertRules) != 1 || task.Spec.AlertRules[0] != "HighCPU" {
		t.Fatalf("blueprint alertRules not mapped: %+v", task.Spec.AlertRules)
	}
	if task.Labels["x"] != "y" || task.Labels[LabelQueuedEvent] != "qe-mint" {
		t.Fatalf("labels: %v", task.Labels)
	}
}
