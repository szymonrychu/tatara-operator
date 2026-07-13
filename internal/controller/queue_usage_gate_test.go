package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/accountusage"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// subscriptionProject creates a claudeSubscription-mode Project (capacity 5/5,
// so capacity never gates) with the given per-kind spawn ceilings and pool-class
// thresholds. High thresholds let a test exercise the per-kind ceiling ladder in
// isolation from the coarse pool-class gate.
func subscriptionProject(t *testing.T, ctx context.Context, name string, proactive, emergency int, ceilings map[string]int32) *tatarav1alpha1.Project {
	t.Helper()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			MaxConcurrentAgents: 5,
			Queue:               &tatarav1alpha1.QueueSpec{Capacity: 5, AlertCapacity: 5},
			TokenBudget: &tatarav1alpha1.TokenBudgetSpec{
				Enabled: true, Mode: "claudeSubscription",
				ProactivePercent: proactive, EmergencyPercent: emergency,
				SpawnCeilingByKind: ceilings,
			},
		},
	}
	mustCreate(t, ctx, proj)
	return proj
}

// mkUsageQE enqueues a Queued event of the given class/kind with optional payload
// labels (used to carry the activity label for healthCheck-keyed ceilings).
func mkUsageQE(t *testing.T, ctx context.Context, projRef string, seq int64, class, kind string, labels map[string]string) *tatarav1alpha1.QueuedEvent {
	t.Helper()
	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: seq, Class: class, Kind: kind, ProjectRef: projRef,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: kind, GenerateName: kind + "-", Labels: labels},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q)
	return q
}

func usageStoreAt(fiveHourPercent float64) *accountusage.Store {
	st := &accountusage.Store{}
	st.Set(accountusage.Snapshot{
		FiveHour: accountusage.Window{Percent: fiveHourPercent, Reset: time.Now().Add(time.Hour)},
		Healthy:  true,
	})
	return st
}

// TestDispatcherSubscription_IncidentCeilingEscapesEmergencyGate is the sharp F1
// regression: with the DEFAULT pool-class thresholds (proactive 50, emergency
// 80) at 85% usage, the old coarse gate short-circuited the whole alert pool
// (85 >= emergency 80) and held incidents, so the incident:98 ceiling was inert.
// The per-event gate governs each kind by its own ceiling instead: incident (98)
// admits at 85% while brainstorm (40) is held.
func TestDispatcherSubscription_IncidentCeilingEscapesEmergencyGate(t *testing.T) {
	ctx := context.Background()
	proj := subscriptionProject(t, ctx, "p-sub-emergency", 50, 80,
		map[string]int32{"brainstorm": 40, "incident": 98})

	brainstorm := mkUsageQE(t, ctx, proj.Name, 1, tatarav1alpha1.QueueClassNormal, "brainstorm", nil)
	incident := mkUsageQE(t, ctx, proj.Name, 2, tatarav1alpha1.QueueClassAlert, "incident", nil)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Usage: usageStoreAt(85)}
	if _, err := r.Reconcile(ctx, reqFor(incident)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	assertQEAdmitted(t, ctx, incident, true)    // 85 < incident ceiling 98
	assertQEAdmitted(t, ctx, brainstorm, false) // 85 >= brainstorm ceiling 40
}

// TestDispatcherSubscription_PerKindHoldRequeuesAndResumes covers F1 (per-kind
// ceiling governs, not the coarse pool-class gate) and F2 (a per-kind hold
// schedules a resume requeue via the account-usage path and later admits when
// usage drops). Thresholds are high (proactive 90) so the pool-class Decision is
// NOT blocked at 55% - isolating the account-usage requeue (heldOnUsage) from the
// customWindow budgetHeld path - yet brainstorm (40) is held while incident (98)
// admits.
func TestDispatcherSubscription_PerKindHoldRequeuesAndResumes(t *testing.T) {
	ctx := context.Background()
	proj := subscriptionProject(t, ctx, "p-sub-ladder", 90, 95,
		map[string]int32{"brainstorm": 40, "incident": 98})

	brainstorm := mkUsageQE(t, ctx, proj.Name, 1, tatarav1alpha1.QueueClassNormal, "brainstorm", nil)
	incident := mkUsageQE(t, ctx, proj.Name, 2, tatarav1alpha1.QueueClassAlert, "incident", nil)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Usage: usageStoreAt(55)}

	res, err := r.Reconcile(ctx, reqFor(brainstorm))
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	assertQEAdmitted(t, ctx, brainstorm, false)
	assertQEAdmitted(t, ctx, incident, true)
	// F2: the per-kind hold must schedule a resume requeue. Pool-class is NOT
	// blocked here (budgetHeld=false), so the 60s delay comes only from the
	// account-usage path (usageRequeueAfter, 60s fallback with no poll interval).
	if res.RequeueAfter != 60*time.Second {
		t.Fatalf("held brainstorm must requeue at usage fallback 60s, got %v", res.RequeueAfter)
	}

	// Usage drops below the brainstorm ceiling: the previously-held event admits on
	// the next reconcile (the resume path the requeue drives in production).
	r.Usage = usageStoreAt(30)
	if _, err := r.Reconcile(ctx, reqFor(brainstorm)); err != nil {
		t.Fatalf("resume reconcile: %v", err)
	}
	assertQEAdmitted(t, ctx, brainstorm, true)
}

// TestDispatcherSubscription_HealthCheckActivityCeiling covers F5: healthCheck
// work is enqueued as Kind=brainstorm + activity=healthCheck, so the gate must
// key on activity-then-kind. With brainstorm:40 and healthCheck:60 at 50% usage,
// the healthCheck-activity event admits (governed by 60) while a plain brainstorm
// event is held (governed by 40) - proving the healthCheck ceiling governs, not
// brainstorm's.
func TestDispatcherSubscription_HealthCheckActivityCeiling(t *testing.T) {
	ctx := context.Background()
	proj := subscriptionProject(t, ctx, "p-sub-healthcheck", 90, 95,
		map[string]int32{"brainstorm": 40, "healthCheck": 60})

	healthCheck := mkUsageQE(t, ctx, proj.Name, 1, tatarav1alpha1.QueueClassNormal, "brainstorm",
		map[string]string{tatarav1alpha1.LabelActivity: "healthCheck"})
	plainBrainstorm := mkUsageQE(t, ctx, proj.Name, 2, tatarav1alpha1.QueueClassNormal, "brainstorm", nil)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Usage: usageStoreAt(50)}
	if _, err := r.Reconcile(ctx, reqFor(healthCheck)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	assertQEAdmitted(t, ctx, healthCheck, true)      // 50 < healthCheck ceiling 60
	assertQEAdmitted(t, ctx, plainBrainstorm, false) // 50 >= brainstorm ceiling 40
}

// TestDispatcherSubscription_RefineNeverGated covers the refine-barrier /
// usage-gate coupling: refine is a scan-pipeline BARRIER
// (projectscan.go runScans defers mrScan/issueScan/brainstorm/healthCheck until
// a refine Task reaches a terminal state). A refine event held Queued on the
// account-usage ceiling never runs, never becomes terminal, and wedges that
// barrier - and every scan behind it - forever. So refine must always admit
// regardless of any configured spawnCeilingByKind entry, while other kinds
// (e.g. brainstorm) stay governed by their own ceiling.
func TestDispatcherSubscription_RefineNeverGated(t *testing.T) {
	ctx := context.Background()
	proj := subscriptionProject(t, ctx, "p-sub-refine", 90, 95,
		map[string]int32{"refine": 55, "brainstorm": 40})

	refine := mkUsageQE(t, ctx, proj.Name, 1, tatarav1alpha1.QueueClassNormal, "refine",
		map[string]string{tatarav1alpha1.LabelActivity: "refine"})
	brainstorm := mkUsageQE(t, ctx, proj.Name, 2, tatarav1alpha1.QueueClassNormal, "brainstorm", nil)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Usage: usageStoreAt(60)}
	if _, err := r.Reconcile(ctx, reqFor(refine)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	assertQEAdmitted(t, ctx, refine, true)      // barrier: never held, even at 60 >= ceiling 55
	assertQEAdmitted(t, ctx, brainstorm, false) // 60 >= brainstorm ceiling 40
}
