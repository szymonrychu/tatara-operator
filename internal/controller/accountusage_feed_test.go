package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/accountusage"
)

func mkFeedTask(t *testing.T, ctx context.Context, name string, au *tatarav1alpha1.TaskAccountUsage) *tatarav1alpha1.Task {
	t.Helper()
	tk := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS}}
	tk.Spec.ProjectRef = "feed-proj"
	tk.Spec.RepositoryRef = "feed-repo"
	tk.Spec.Goal = "ship the feature"
	mustCreate(t, ctx, tk)
	if au != nil {
		tk.Status.AccountUsage = au
		mustStatusUpdate(t, ctx, tk)
	}
	return tk
}

// The fleet store takes the NEWEST snapshot across every Task in the namespace,
// regardless of the order the Tasks reconcile in. That is what makes it
// fleet-wide rather than per-Project: a Project that falls quiet contributes an
// old observation that can never displace a live one.
func TestAccountUsageFeedReconciler(t *testing.T) {
	ctx := context.Background()
	store := &accountusage.Store{}
	r := &AccountUsageFeedReconciler{Client: k8sClient, Store: store, Namespace: testNS}

	t0 := metav1.NewTime(time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(t0.Add(time.Minute))
	reset := metav1.NewTime(time.Date(2026, 8, 22, 13, 0, 0, 0, time.UTC))

	older := mkFeedTask(t, ctx, "feed-old", &tatarav1alpha1.TaskAccountUsage{
		ObservedAt: t0, FiveHourPercent: 20, WeeklyPercent: 30,
	})
	newer := mkFeedTask(t, ctx, "feed-new", &tatarav1alpha1.TaskAccountUsage{
		ObservedAt: t1, FiveHourPercent: 61, WeeklyPercent: 72, FiveHourReset: &reset,
	})

	// Newer first, then older: the older must not win.
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(newer)}); err != nil {
		t.Fatalf("reconcile newer: %v", err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(older)}); err != nil {
		t.Fatalf("reconcile older: %v", err)
	}

	got := store.Get()
	if got.FiveHour.Percent != 61 || got.Weekly.Percent != 72 {
		t.Fatalf("store = %+v, want five_hour 61 / weekly 72", got)
	}
	if got.Source != accountusage.SourceWrapper {
		t.Fatalf("Source = %q, want %q", got.Source, accountusage.SourceWrapper)
	}
	if !got.Healthy {
		t.Fatal("Healthy = false, want true: the observation IS a successful read")
	}
	if !got.UpdatedAt.Equal(t1.UTC()) {
		t.Fatalf("UpdatedAt = %v, want %v", got.UpdatedAt, t1.UTC())
	}
	if !got.FiveHour.Reset.Equal(reset.UTC()) {
		t.Fatalf("FiveHour.Reset = %v, want %v", got.FiveHour.Reset, reset.UTC())
	}
	if !got.Weekly.Reset.IsZero() {
		t.Fatalf("Weekly.Reset = %v, want zero for an absent reset", got.Weekly.Reset)
	}
}

// A Task with no snapshot is the NORMAL case: the statusline reports nothing
// until a session's first API response, and a pod's first turn-complete
// callback legitimately carries no accountUsage. It must leave the store empty
// rather than seeding a zero-valued snapshot, which reads to the gate as
// "0% used" - the exact silent failure this feed exists to fix.
func TestAccountUsageFeedReconcilerNoSnapshot(t *testing.T) {
	ctx := context.Background()
	store := &accountusage.Store{}
	r := &AccountUsageFeedReconciler{Client: k8sClient, Store: store, Namespace: testNS}
	task := mkFeedTask(t, ctx, "feed-none", nil)
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(task)}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := store.Get(); !got.UpdatedAt.IsZero() {
		t.Fatalf("store = %+v, want empty", got)
	}
}

// The SCHEMA is the outer guard against a zero-valued snapshot: observedAt is
// required, so a snapshot whose only ordering key is missing can never be
// persisted at all. Without this, a "0% used with no observedAt" record would
// look to the gate exactly like a healthy, wide-open account. Reconcile's own
// ObservedAt.IsZero() check is the inner guard for the same hazard; this test
// pins the outer one so relaxing the schema cannot pass unnoticed.
func TestTaskAccountUsageRequiresObservedAt(t *testing.T) {
	ctx := context.Background()
	tk := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "feed-zero", Namespace: testNS}}
	tk.Spec.ProjectRef = "feed-proj"
	tk.Spec.RepositoryRef = "feed-repo"
	tk.Spec.Goal = "ship the feature"
	mustCreate(t, ctx, tk)
	tk.Status.AccountUsage = &tatarav1alpha1.TaskAccountUsage{FiveHourPercent: 44}
	err := k8sClient.Status().Update(ctx, tk)
	if err == nil {
		t.Fatal("status update with a zero observedAt was accepted, want rejected")
	}
	if !strings.Contains(err.Error(), "observedAt") {
		t.Fatalf("rejection did not name observedAt: %v", err)
	}
}

func TestAccountUsageFeedReconcilerMissingTask(t *testing.T) {
	ctx := context.Background()
	store := &accountusage.Store{}
	r := &AccountUsageFeedReconciler{Client: k8sClient, Store: store, Namespace: testNS}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "feed-gone"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := store.Get(); !got.UpdatedAt.IsZero() {
		t.Fatalf("store = %+v, want empty", got)
	}
}

// accountUsageChanged is a PREDICATE, not a map func, precisely so it can be
// evaluated any number of times per event with no consequence. This locks that:
// running every branch of it repeatedly must never touch the store, because a
// controller-runtime map func runs TWICE per Update and a side effect placed in
// one would double-fire. The store write lives in Reconcile.
func TestAccountUsageChangedPredicateIsPure(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(t0.Add(time.Minute))
	withUsage := func(name string, at *metav1.Time) *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS}}
		if at != nil {
			tk.Status.AccountUsage = &tatarav1alpha1.TaskAccountUsage{ObservedAt: *at, FiveHourPercent: 50}
		}
		return tk
	}
	none := withUsage("p-none", nil)
	old := withUsage("p-old", &t0)
	fresh := withUsage("p-new", &t1)

	p := accountUsageChanged()
	tests := []struct {
		name string
		run  func() bool
		want bool
	}{
		{"create with a snapshot passes", func() bool { return p.Create(event.CreateEvent{Object: fresh}) }, true},
		{"create without a snapshot is dropped", func() bool { return p.Create(event.CreateEvent{Object: none}) }, false},
		{"update to a newer snapshot passes", func() bool {
			return p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: fresh})
		}, true},
		{"update with an unchanged snapshot is dropped", func() bool {
			return p.Update(event.UpdateEvent{ObjectOld: fresh, ObjectNew: fresh})
		}, false},
		{"update to an older snapshot is dropped", func() bool {
			return p.Update(event.UpdateEvent{ObjectOld: fresh, ObjectNew: old})
		}, false},
		{"update that never had a snapshot is dropped", func() bool {
			return p.Update(event.UpdateEvent{ObjectOld: none, ObjectNew: none})
		}, false},
		{"delete is dropped", func() bool { return p.Delete(event.DeleteEvent{Object: fresh}) }, false},
		{"generic is dropped", func() bool { return p.Generic(event.GenericEvent{Object: fresh}) }, false},
	}
	store := &accountusage.Store{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Twice, mirroring the double-fire a map func would suffer.
			if got := tc.run(); got != tc.want {
				t.Fatalf("predicate = %v, want %v", got, tc.want)
			}
			if got := tc.run(); got != tc.want {
				t.Fatalf("predicate (second call) = %v, want %v", got, tc.want)
			}
			if got := store.Get(); !got.UpdatedAt.IsZero() {
				t.Fatalf("predicate wrote to the store: %+v", got)
			}
		})
	}
}

func TestSnapshotFromTaskAccountUsage(t *testing.T) {
	obs := metav1.NewTime(time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC))
	weekly := metav1.NewTime(time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC))
	got := SnapshotFromTaskAccountUsage(&tatarav1alpha1.TaskAccountUsage{
		ObservedAt: obs, FiveHourPercent: 41, WeeklyPercent: 73, WeeklyReset: &weekly,
	})
	if got.FiveHour.Percent != 41 || got.Weekly.Percent != 73 {
		t.Fatalf("percents = %+v", got)
	}
	if !got.FiveHour.Reset.IsZero() {
		t.Fatalf("FiveHour.Reset = %v, want zero", got.FiveHour.Reset)
	}
	if !got.Weekly.Reset.Equal(weekly.UTC()) {
		t.Fatalf("Weekly.Reset = %v, want %v", got.Weekly.Reset, weekly.UTC())
	}
	if got.Source != accountusage.SourceWrapper || !got.Healthy {
		t.Fatalf("source/healthy = %q/%v", got.Source, got.Healthy)
	}
}
