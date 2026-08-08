package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// mkQueuedEvent creates a Queued mint-payload QueuedEvent in the given
// project and pool class, at the given seq.
func mkQueuedEvent(t *testing.T, ctx context.Context, project, class string, seq int64) *tatarav1alpha1.QueuedEvent {
	t.Helper()
	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-amp-", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: seq, Class: class, Kind: "review", ProjectRef: project,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "review", GenerateName: "amp-"},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q)
	return q
}

// TestBlockedAdmission_DoesNotSelfPoll is the regression guard for issue #496
// finding 2 (retry amplification). While the pool is full, EVERY Queued event
// used to schedule its own fixed 30s RequeueAfter, so a queue of depth N cost
// N re-attempts every 30s - 40/min at the observed depth of 20 - each running
// the IDENTICAL full-project admit pass and each writing one INFO line. The
// waste is O(queue depth) and grows exactly when the operator is most loaded.
//
// A reconcile of ANY QueuedEvent in a project walks and admits the WHOLE
// project pool, so N blocked events polling produce N copies of one answer.
// With the leader-only admission backstop wired (production, wire.go), a
// blocked reconcile must schedule NO self-poll at all: a freed slot wakes
// admission through the Task watch, and the backstop is the safety net.
func TestBlockedAdmission_DoesNotSelfPoll(t *testing.T) {
	ctx := context.Background()
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-amp-nopoll", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	// Capacity 1 with four queued events: the first admit fills the pool and
	// the remaining three stay Queued behind it.
	qes := []*tatarav1alpha1.QueuedEvent{
		mkQueuedEvent(t, ctx, proj.Name, tatarav1alpha1.QueueClassNormal, 1),
		mkQueuedEvent(t, ctx, proj.Name, tatarav1alpha1.QueueClassNormal, 2),
		mkQueuedEvent(t, ctx, proj.Name, tatarav1alpha1.QueueClassNormal, 3),
		mkQueuedEvent(t, ctx, proj.Name, tatarav1alpha1.QueueClassNormal, 4),
	}

	events := make(chan event.GenericEvent, 256)
	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: metrics, BackstopEvents: events}

	for _, q := range qes {
		res, err := r.Reconcile(ctx, reqFor(q))
		if err != nil {
			t.Fatalf("reconcile %s: %v", q.Name, err)
		}
		if res.RequeueAfter != 0 {
			t.Fatalf("blocked reconcile of %s scheduled RequeueAfter=%v, want 0: with the backstop wired a pool_full hold must not self-poll (O(depth) amplification, issue #496)", q.Name, res.RequeueAfter)
		}
	}

	// The pool really was full: exactly one admitted, the rest still Queued.
	admitted := 0
	for _, q := range qes {
		if refreshQE(t, ctx, q).Status.State == tatarav1alpha1.QueueStateAdmitted {
			admitted++
		}
	}
	if admitted != 1 {
		t.Fatalf("admitted %d events against capacity 1, want 1 (the test never saturated the pool)", admitted)
	}
	if got := testutil.ToFloat64(metrics.AdmissionBlockedCounter(proj.Name, tatarav1alpha1.QueueClassNormal, "", "pool_full")); got < 1 {
		t.Fatalf("admission_blocked{normal,pool_full} = %v, want >= 1 (the saturation signal must survive the fix)", got)
	}
}

// TestBlockedAdmission_PollsWhenNoBackstopWired is the safety net for the
// safety net. BackstopEvents nil is a supported construction (every
// pre-existing unit test, and any wiring that does not run the leader-only
// sweep): with no backstop there is nothing to heal a lost watch event, so the
// blocked reconcile MUST keep its own slow poll. Removing the poll
// unconditionally would trade an amplification defect for a stall.
func TestBlockedAdmission_PollsWhenNoBackstopWired(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-amp-nobackstop", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)
	mkQueuedEvent(t, ctx, proj.Name, tatarav1alpha1.QueueClassNormal, 1)
	blocked := mkQueuedEvent(t, ctx, proj.Name, tatarav1alpha1.QueueClassNormal, 2)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	if _, err := r.Reconcile(ctx, reqFor(blocked)); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	res, err := r.Reconcile(ctx, reqFor(blocked))
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if res.RequeueAfter != blockedPollInterval {
		t.Fatalf("blocked reconcile with no backstop wired scheduled RequeueAfter=%v, want %v", res.RequeueAfter, blockedPollInterval)
	}
}

// TestBlockedAdmission_ReleasedSlotAdmitsImmediately proves the wake that
// replaces the poll. When a Task finishes, the Task watch (mapTaskToQE) maps it
// onto the QueuedEvent it was admitted through; that reconcile GCs the spent
// ticket and admits the next waiting event in the SAME pass. No timer is
// involved, so dropping the 30s self-poll costs no latency on the happy path.
func TestBlockedAdmission_ReleasedSlotAdmitsImmediately(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-amp-release", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)
	first := mkQueuedEvent(t, ctx, proj.Name, tatarav1alpha1.QueueClassNormal, 1)
	waiting := mkQueuedEvent(t, ctx, proj.Name, tatarav1alpha1.QueueClassNormal, 2)

	events := make(chan event.GenericEvent, 256)
	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), BackstopEvents: events}
	if _, err := r.Reconcile(ctx, reqFor(first)); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	admittedFirst := refreshQE(t, ctx, first)
	if admittedFirst.Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("first QE state = %q, want Admitted", admittedFirst.Status.State)
	}
	if refreshQE(t, ctx, waiting).Status.State != tatarav1alpha1.QueueStateQueued {
		t.Fatal("second QE must still be Queued behind the full pool")
	}

	// The admitted Task finishes: this is the moment the slot frees.
	task, ok := mustGetTask(t, k8sClient, admittedFirst.Status.TaskRef)
	if !ok {
		t.Fatalf("minted task %q not found", admittedFirst.Status.TaskRef)
	}
	task.Status.State = tatarav1alpha1.StateDone
	mustStatusUpdate(t, ctx, task)

	// mapTaskToQE routes that Task change back to the QueuedEvent it was
	// admitted through.
	if _, err := r.Reconcile(ctx, reqFor(admittedFirst)); err != nil {
		t.Fatalf("release reconcile: %v", err)
	}
	if got := refreshQE(t, ctx, waiting); got.Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("waiting QE state = %q, want Admitted in the same pass the slot freed", got.Status.State)
	}
}

// TestBackstop_EnqueuesOneRepresentativePerPool is the other half of issue
// #496 finding 2. The backstop used to push a GenericEvent for EVERY pending
// QueuedEvent on every 60s sweep - 20/min at the observed depth of 20, each
// one triggering the same whole-project admit pass. One representative per
// pool per sweep drives an identical, complete pass at O(1) cost.
func TestBackstop_EnqueuesOneRepresentativePerPool(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "p-amp-backstop-", Namespace: testNS},
	}
	mustCreate(t, ctx, proj)

	mineNames := map[string]bool{}
	mk := func(class string, seq int64) *tatarav1alpha1.QueuedEvent {
		q := mkQueuedEvent(t, ctx, proj.Name, class, seq)
		mineNames[q.Name] = true
		return q
	}
	normalHead := mk(tatarav1alpha1.QueueClassNormal, 1)
	for seq := int64(2); seq <= 6; seq++ {
		mk(tatarav1alpha1.QueueClassNormal, seq)
	}
	alertHead := mk(tatarav1alpha1.QueueClassAlert, 7)
	mk(tatarav1alpha1.QueueClassAlert, 8)

	events := make(chan event.GenericEvent, 512)
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: metrics, BackstopEvents: events}

	n, err := r.EnqueuePending(ctx)
	if err != nil {
		t.Fatalf("EnqueuePending: %v", err)
	}
	// The suite shares one namespace, so other tests' projects contribute their
	// own representatives; count only this project's.
	mine := map[string]int{}
	for i := 0; i < n; i++ {
		select {
		case ev := <-events:
			mine[ev.Object.GetName()]++
		default:
			t.Fatalf("EnqueuePending reported n=%d but only %d landed on BackstopEvents", n, i)
		}
	}
	total := 0
	for name, count := range mine {
		if mineNames[name] {
			total += count
		}
	}
	if mine[normalHead.Name] != 1 {
		t.Fatalf("normal-pool head %s enqueued %d times, want exactly 1", normalHead.Name, mine[normalHead.Name])
	}
	if mine[alertHead.Name] != 1 {
		t.Fatalf("alert-pool head %s enqueued %d times, want exactly 1", alertHead.Name, mine[alertHead.Name])
	}
	if total != 2 {
		t.Fatalf("project %s contributed %d backstop enqueues, want 2 (one per pool with pending work)", proj.Name, total)
	}
	if got := testutil.ToFloat64(metrics.DispatcherBackstopEnqueuedCounter(proj.Name)); got != 2 {
		t.Fatalf("operator_dispatcher_backstop_enqueued_total{project=%s} = %v, want 2 (one per pool, not one per pending event)", proj.Name, got)
	}
}

// TestBackstop_DoesNotSkipASaturatedPool guards against the tempting variant of
// this fix - "make the backstop skip events already known blocked" - which
// would lose exactly the wakeup the backstop exists to recover: a slot that
// frees while the operator misses the Task watch event. The sweep must remain
// unconditional, so a saturated pool is still re-evaluated every sweep and no
// wakeup can be lost for longer than one interval.
func TestBackstop_DoesNotSkipASaturatedPool(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "p-amp-saturated-", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)
	first := mkQueuedEvent(t, ctx, proj.Name, tatarav1alpha1.QueueClassNormal, 1)
	blocked := mkQueuedEvent(t, ctx, proj.Name, tatarav1alpha1.QueueClassNormal, 2)

	events := make(chan event.GenericEvent, 512)
	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), BackstopEvents: events}
	if _, err := r.Reconcile(ctx, reqFor(first)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if refreshQE(t, ctx, blocked).Status.State != tatarav1alpha1.QueueStateQueued {
		t.Fatal("second QE must be Queued behind the full pool")
	}

	n, err := r.EnqueuePending(ctx)
	if err != nil {
		t.Fatalf("EnqueuePending: %v", err)
	}
	found := false
	for i := 0; i < n; i++ {
		select {
		case ev := <-events:
			if ev.Object.GetName() == blocked.Name {
				found = true
			}
		default:
			t.Fatalf("EnqueuePending reported n=%d but only %d landed on BackstopEvents", n, i)
		}
	}
	if !found {
		t.Fatalf("backstop skipped the blocked event %s in a saturated pool: a lost Task-watch wakeup would then stall admission forever", blocked.Name)
	}
}

// TestBlockedPollInterval covers the two wirings directly.
func TestBlockedPollInterval(t *testing.T) {
	withBackstop := &DispatcherReconciler{BackstopEvents: make(chan event.GenericEvent, 1)}
	if got := withBackstop.blockedPoll(); got != 0 {
		t.Fatalf("blockedPoll with the backstop wired = %v, want 0", got)
	}
	withoutBackstop := &DispatcherReconciler{}
	if got := withoutBackstop.blockedPoll(); got != blockedPollInterval {
		t.Fatalf("blockedPoll with no backstop = %v, want %v", got, blockedPollInterval)
	}
	if blockedPollInterval <= 0 || blockedPollInterval > time.Minute {
		t.Fatalf("blockedPollInterval = %v, want a bounded positive fallback", blockedPollInterval)
	}
}
