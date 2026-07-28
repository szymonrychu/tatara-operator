package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// ticketQE builds an admission-ticket QueuedEvent in the given state.
func ticketQE(taskRef, state string) *tatarav1alpha1.QueuedEvent {
	return &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-1", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Class:      tatarav1alpha1.QueueClassNormal,
			Kind:       "implement",
			ProjectRef: "proj",
			Payload: tatarav1alpha1.QueuedEventPayload{
				Kind: "implement", AgentKind: "implement", TaskRef: taskRef,
			},
		},
		Status: tatarav1alpha1.QueuedEventStatus{State: state},
	}
}

func TestTicketToTask(t *testing.T) {
	cases := []struct {
		name string
		obj  client.Object
		want string // "" means no request
	}{
		{
			name: "a ticket naming a Task maps to exactly that Task",
			obj:  ticketQE("task-abc", tatarav1alpha1.QueueStateAdmitted),
			want: "task-abc",
		},
		{
			name: "a MINT (no taskRef) is not a ticket and maps to nothing",
			obj:  ticketQE("", tatarav1alpha1.QueueStateAdmitted),
			want: "",
		},
		{
			name: "a non-QueuedEvent object maps to nothing",
			obj:  &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: testNS}},
			want: "",
		},
		{
			// EnqueueRequestsFromMapFunc.Update calls the map func on BOTH
			// ObjectOld and ObjectNew (enqueue_mapped.go): a Queued->Admitted
			// transition runs ticketToTask once with the OLD (still Queued)
			// object even though the predicate already gated the pair. If the
			// map func's own guard does not also check state, this old-object
			// call produces a request (and a wake count, and a log line) for a
			// ticket that is not actually admitted.
			name: "a ticket still in Queued state maps to nothing",
			obj:  ticketQE("task-abc", tatarav1alpha1.QueueStateQueued),
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &TaskReconciler{Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
			got := r.ticketToTask(context.Background(), tc.obj)
			if tc.want == "" {
				if len(got) != 0 {
					t.Fatalf("want no requests, got %+v", got)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("want exactly 1 request, got %+v", got)
			}
			if got[0].Name != tc.want || got[0].Namespace != testNS {
				t.Fatalf("want %s/%s, got %s/%s", testNS, tc.want, got[0].Namespace, got[0].Name)
			}
		})
	}
}

// The counter is the production proof the stall is gone, so it must fire on the
// wake itself and NOT on an event the map func declines.
func TestTicketToTaskCountsTheWake(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := &TaskReconciler{Metrics: m}

	r.ticketToTask(context.Background(), ticketQE("task-abc", tatarav1alpha1.QueueStateAdmitted))
	if got := testutil.ToFloat64(m.AdmissionWakeCounter(tatarav1alpha1.QueueClassNormal, "implement")); got != 1 {
		t.Fatalf("admission wake counter = %v, want 1", got)
	}

	r.ticketToTask(context.Background(), ticketQE("", tatarav1alpha1.QueueStateAdmitted))
	if got := testutil.ToFloat64(m.AdmissionWakeCounter(tatarav1alpha1.QueueClassNormal, "implement")); got != 1 {
		t.Fatalf("a mint must not count as a wake; counter = %v, want still 1", got)
	}
}

// A nil Metrics is the unit-test construction of TaskReconciler and must not
// panic through the promoted queueMetrics pointer.
func TestTicketToTaskNilMetricsDoesNotPanic(t *testing.T) {
	r := &TaskReconciler{}
	if got := r.ticketToTask(context.Background(), ticketQE("task-abc", tatarav1alpha1.QueueStateAdmitted)); len(got) != 1 {
		t.Fatalf("want 1 request, got %+v", got)
	}
}

func TestTicketAdmittedPredicate(t *testing.T) {
	p := ticketAdmittedPredicate()
	queued := ticketQE("task-abc", tatarav1alpha1.QueueStateQueued)
	admitted := ticketQE("task-abc", tatarav1alpha1.QueueStateAdmitted)

	t.Run("update Queued to Admitted is the wake", func(t *testing.T) {
		if !p.Update(event.UpdateEvent{ObjectOld: queued, ObjectNew: admitted}) {
			t.Fatal("the Queued->Admitted transition must pass")
		}
	})
	t.Run("update Admitted to Admitted is a resync, not a wake", func(t *testing.T) {
		if p.Update(event.UpdateEvent{ObjectOld: admitted, ObjectNew: admitted}) {
			t.Fatal("a resync of an already-Admitted ticket must be filtered")
		}
	})
	t.Run("update that stays Queued is not a wake", func(t *testing.T) {
		if p.Update(event.UpdateEvent{ObjectOld: queued, ObjectNew: queued}) {
			t.Fatal("a non-transition must be filtered")
		}
	})
	t.Run("create of an already-Admitted ticket is the restart replay", func(t *testing.T) {
		if !p.Create(event.CreateEvent{Object: admitted}) {
			t.Fatal("informer replay of a live Admitted ticket must pass, or the wake does not survive a restart")
		}
	})
	t.Run("create of a Queued ticket is not a wake", func(t *testing.T) {
		if p.Create(event.CreateEvent{Object: queued}) {
			t.Fatal("a freshly enqueued ticket must not wake anything")
		}
	})
	t.Run("delete never wakes", func(t *testing.T) {
		if p.Delete(event.DeleteEvent{Object: admitted}) {
			t.Fatal("DeleteFunc must be set explicitly to false; an unset func defaults to true")
		}
	})
	t.Run("generic never wakes", func(t *testing.T) {
		if p.Generic(event.GenericEvent{Object: admitted}) {
			t.Fatal("GenericFunc must be set explicitly to false; an unset func defaults to true")
		}
	})
	t.Run("a non-QueuedEvent object never wakes", func(t *testing.T) {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: testNS}}
		if p.Create(event.CreateEvent{Object: pod}) {
			t.Fatal("a type-assertion miss must be filtered, not passed")
		}
	})
}
