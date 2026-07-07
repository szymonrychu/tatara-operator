package controller

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// TestScanTaskLabels_NoSourceDedupeLabels verifies that scanTaskLabels no
// longer writes the three dedup labels (source-repo, source-number, head-sha).
// Kind/activity/is-pr labels are still written.
func TestScanTaskLabels_NoSourceDedupeLabels(t *testing.T) {
	got := scanTaskLabels(candidate{repo: "o/r", number: 5, headSHA: "abc"}, "mrScan", "review")
	// Use string literals: the consts are deleted in Phase 2 Task 9.
	for _, badKey := range []string{
		"tatara.io/source-repo",
		"tatara.io/source-number",
		"tatara.io/head-sha",
	} {
		if _, ok := got[badKey]; ok {
			t.Errorf("scanTaskLabels must not write %q any more; got %+v", badKey, got)
		}
	}
	// Must still carry kind + activity.
	if got[tatarav1alpha1.LabelSourceKind] != "review" {
		t.Errorf("LabelSourceKind missing or wrong: %+v", got)
	}
	if got[tatarav1alpha1.LabelActivity] != "mrScan" {
		t.Errorf("LabelActivity missing or wrong: %+v", got)
	}
}

// TestReconcile_SeedsLedgerFromSpec verifies that after one reconcile a Task
// with a populated Spec.Source has Status.WorkItems seeded from it.
func TestReconcile_SeedsLedgerFromSpec(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	reg := prometheus.NewRegistry()
	r := &TaskReconciler{
		Client:           k8sClient,
		Scheme:           k8sClient.Scheme(),
		Metrics:          obs.NewOperatorMetrics(reg),
		LifecycleMetrics: obs.NewLifecycleMetrics(reg),
		Session:          newFakeSession(),
	}

	mkSecret(t, "seed-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "seed-proj", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "seed-scm"},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	proj.Status.Memory = stableMemStatus("http://mem.svc:8080")
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "seed-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "seed-proj", Goal: "g", Kind: "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#5", Number: 5, IsPR: false,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}

	// Trigger one reconcile.
	_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "seed-task"}})

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "seed-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if len(got.Status.WorkItems) == 0 {
		t.Fatal("expected Status.WorkItems to be seeded after first reconcile; got empty")
	}
	if got.Status.WorkItems[0].Repo != "o/r" || got.Status.WorkItems[0].Number != 5 {
		t.Errorf("unexpected WorkItem: %+v", got.Status.WorkItems[0])
	}
}

// seedRaceOnFirstGet simulates another replica winning the race to seed the
// ledger between Reconcile's top-of-function Task Get and patchTaskStatus's
// internal fresh Get: on the first Get of a Task object it writes a
// concurrent WorkItems seed directly to the server (bypassing the object
// handed back to the caller, which stays stale - exactly what a genuine
// concurrent write would produce), then delegates normally thereafter.
type seedRaceOnFirstGet struct {
	client.Client
	seeded *atomic.Bool
	item   tatarav1alpha1.WorkItemRef
}

func (c *seedRaceOnFirstGet) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	if err := c.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	tk, ok := obj.(*tatarav1alpha1.Task)
	if !ok || c.seeded.Swap(true) {
		return nil
	}
	fresh := &tatarav1alpha1.Task{}
	if err := c.Client.Get(ctx, key, fresh); err != nil {
		return err
	}
	fresh.Status.WorkItems = []tatarav1alpha1.WorkItemRef{c.item}
	_ = tk // obj intentionally left stale, matching what Reconcile's own Get observed
	return c.Client.Status().Update(ctx, fresh)
}

// TestReconcile_SeedLedger_AdoptsFreshOnAlreadySeeded pins the site-153
// ledger-seed patch behavior: when patchTaskStatus's fresh Get observes that
// another replica already seeded Status.WorkItems (a concurrent write between
// Reconcile's top-of-function Get and this retry's Get), this replica's own
// seed attempt must be discarded (skip-write, mutate returns false) rather
// than appending a second, differently-keyed WorkItem - and the unconditional
// *task = *fresh copy-back on that skip path must still adopt the fresh
// resourceVersion (the #175 409-storm fix). Before the S17 patchTaskStatus
// refactor this lived as bespoke inline retry logic in Reconcile; collapsing
// away the "already seeded" check would silently double-seed the ledger.
func TestReconcile_SeedLedger_AdoptsFreshOnAlreadySeeded(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	mkSecret(t, "seed-race-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "seed-race-proj", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "seed-race-scm"},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	proj.Status.Memory = stableMemStatus("http://mem.svc:8080")
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "seed-race-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "seed-race-proj", Goal: "g", Kind: "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#5", Number: 5, IsPR: false,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}

	concurrent := tatarav1alpha1.WorkItemRef{
		Provider: "github", Repo: "o/other", Number: 99,
		Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource,
	}
	var seeded atomic.Bool
	cc := &seedRaceOnFirstGet{Client: k8sClient, seeded: &seeded, item: concurrent}
	reg := prometheus.NewRegistry()
	r := &TaskReconciler{
		Client:           cc,
		Scheme:           k8sClient.Scheme(),
		Metrics:          obs.NewOperatorMetrics(reg),
		LifecycleMetrics: obs.NewLifecycleMetrics(reg),
		Session:          newFakeSession(),
	}

	_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "seed-race-task"}})

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "seed-race-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if len(got.Status.WorkItems) != 1 {
		t.Fatalf("expected exactly the concurrently-seeded WorkItem to survive (no double-seed), got %+v", got.Status.WorkItems)
	}
	if got.Status.WorkItems[0].Repo != "o/other" || got.Status.WorkItems[0].Number != 99 {
		t.Errorf("expected the other replica's seed (o/other#99) to win over this replica's own re-seed; got %+v", got.Status.WorkItems[0])
	}
}
