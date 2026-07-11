package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestMatchIncidentByDedupKey_LegacyAlertRuleFallback verifies the FIX-9 dedup
// backstop: a legacy pre-migration incident Task (empty Spec.DedupKey, only
// Spec.AlertRule set) is still matched by a dedupKey that is itself the
// AlertRule-name fallback (incidentAlertGroup's degraded identity), so a
// recurring alert on an old in-flight incident still tracks onto its existing
// issue instead of spawning a near-duplicate.
func TestMatchIncidentByDedupKey_LegacyAlertRuleFallback(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "midk-proj", Namespace: testNS},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	legacy := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "midk-legacy", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, Kind: "incident",
			AlertRule: "HighErrorRate", // pre-migration: no DedupKey ever set
		},
	}
	if err := k8sClient.Create(ctx, legacy); err != nil {
		t.Fatalf("create legacy incident task: %v", err)
	}
	legacy.Status.DiscoveredIssues = []string{"https://github.com/o/r/issues/42"}
	if err := k8sClient.Status().Update(ctx, legacy); err != nil {
		t.Fatalf("seed discovered issue: %v", err)
	}

	r := &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	url, ok := r.matchIncidentByDedupKey(ctx, proj, "HighErrorRate", "midk-new")
	if !ok || url != "https://github.com/o/r/issues/42" {
		t.Fatalf("matchIncidentByDedupKey = (%q, %v), want the legacy task matched via its AlertRule fallback", url, ok)
	}
}

// TestMatchIncidentByDedupKey_HashDedupKeyStillMatches is a regression guard:
// the ordinary post-migration case (both tasks carry a real hash DedupKey)
// must keep matching exactly as before the fallback guard was added.
func TestMatchIncidentByDedupKey_HashDedupKeyStillMatches(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "midk-proj-hash", Namespace: testNS},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	existing := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "midk-hash-existing", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, Kind: "incident",
			DedupKey: "abc123hash", AlertRule: "HighErrorRate",
		},
	}
	if err := k8sClient.Create(ctx, existing); err != nil {
		t.Fatalf("create incident task: %v", err)
	}
	existing.Status.DiscoveredIssues = []string{"https://github.com/o/r/issues/43"}
	if err := k8sClient.Status().Update(ctx, existing); err != nil {
		t.Fatalf("seed discovered issue: %v", err)
	}

	r := &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	url, ok := r.matchIncidentByDedupKey(ctx, proj, "abc123hash", "midk-hash-new")
	if !ok || url != "https://github.com/o/r/issues/43" {
		t.Fatalf("matchIncidentByDedupKey = (%q, %v), want the hash-keyed task matched", url, ok)
	}

	// A DIFFERENT alert's AlertRule-name fallback must NOT accidentally match
	// this task's real hash DedupKey.
	if _, ok := r.matchIncidentByDedupKey(ctx, proj, "HighErrorRate", "midk-hash-new2"); ok {
		t.Fatal("an AlertRule-name dedupKey must not match a task with a real hash DedupKey set")
	}
}
