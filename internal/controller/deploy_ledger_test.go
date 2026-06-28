package controller

import (
	"context"
	"strconv"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newLedgerClient(t *testing.T) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(s).Build()
}

func TestDeployLedger_AddListRoundTrip(t *testing.T) {
	l := &DeployLedger{Client: newLedgerClient(t), Namespace: "tatara"}
	ctx := context.Background()

	a := DeployLedgerEntry{Artifact: "tatara-operator", Version: "v1.4.0", SourceTaskRef: "t1", IssueRef: "o/r#5", HeadSHA: "abc", State: DeployStateDeploying}
	b := DeployLedgerEntry{Artifact: "tatara-memory", Version: "v0.2.0", SourceTaskRef: "t2", State: DeployStateDeploying}
	if err := l.Add(ctx, "alpha", a); err != nil {
		t.Fatalf("Add a: %v", err)
	}
	if err := l.Add(ctx, "alpha", b); err != nil {
		t.Fatalf("Add b: %v", err)
	}

	got, err := l.List(ctx, "alpha")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("List = %d entries, want 2: %+v", len(got), got)
	}
	if got[0] != a || got[1] != b {
		t.Fatalf("List = %+v, want [%+v %+v]", got, a, b)
	}
}

func TestDeployLedger_AddUpsertsBySourceTaskRef(t *testing.T) {
	l := &DeployLedger{Client: newLedgerClient(t), Namespace: "tatara"}
	ctx := context.Background()

	if err := l.Add(ctx, "alpha", DeployLedgerEntry{Artifact: "op", Version: "v1.0.0", SourceTaskRef: "t1", State: DeployStateDeploying}); err != nil {
		t.Fatal(err)
	}
	// Same Task re-reconciles with a newer version: upsert in place, not append.
	if err := l.Add(ctx, "alpha", DeployLedgerEntry{Artifact: "op", Version: "v1.1.0", SourceTaskRef: "t1", State: DeployStateDeploying}); err != nil {
		t.Fatal(err)
	}
	got, err := l.List(ctx, "alpha")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("List = %d entries, want 1 (upsert): %+v", len(got), got)
	}
	if got[0].Version != "v1.1.0" {
		t.Fatalf("upserted version = %q, want v1.1.0", got[0].Version)
	}
}

func TestDeployLedger_PerProjectIsolation(t *testing.T) {
	l := &DeployLedger{Client: newLedgerClient(t), Namespace: "tatara"}
	ctx := context.Background()

	if err := l.Add(ctx, "alpha", DeployLedgerEntry{Artifact: "op", Version: "v1", SourceTaskRef: "a1", State: DeployStateDeploying}); err != nil {
		t.Fatal(err)
	}
	if err := l.Add(ctx, "beta", DeployLedgerEntry{Artifact: "op", Version: "v1", SourceTaskRef: "b1", State: DeployStateDeploying}); err != nil {
		t.Fatal(err)
	}
	alpha, _ := l.List(ctx, "alpha")
	beta, _ := l.List(ctx, "beta")
	if len(alpha) != 1 || alpha[0].SourceTaskRef != "a1" {
		t.Fatalf("alpha = %+v", alpha)
	}
	if len(beta) != 1 || beta[0].SourceTaskRef != "b1" {
		t.Fatalf("beta = %+v", beta)
	}
}

func TestDeployLedger_SetState(t *testing.T) {
	l := &DeployLedger{Client: newLedgerClient(t), Namespace: "tatara"}
	ctx := context.Background()

	if err := l.Add(ctx, "alpha", DeployLedgerEntry{Artifact: "op", Version: "v1", SourceTaskRef: "t1", State: DeployStateDeploying}); err != nil {
		t.Fatal(err)
	}
	if err := l.SetState(ctx, "alpha", "t1", DeployStateApplied); err != nil {
		t.Fatalf("SetState: %v", err)
	}
	got, _ := l.List(ctx, "alpha")
	if got[0].State != DeployStateApplied {
		t.Fatalf("state = %q, want applied", got[0].State)
	}
	// SetState on an absent task is a no-op (no error, no append).
	if err := l.SetState(ctx, "alpha", "missing", DeployStateFailed); err != nil {
		t.Fatalf("SetState(missing): %v", err)
	}
	got, _ = l.List(ctx, "alpha")
	if len(got) != 1 {
		t.Fatalf("SetState(missing) mutated ledger: %+v", got)
	}
}

func TestDeployLedger_ListAbsentLedger(t *testing.T) {
	l := &DeployLedger{Client: newLedgerClient(t), Namespace: "tatara"}
	got, err := l.List(context.Background(), "nope")
	if err != nil {
		t.Fatalf("List(absent): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("List(absent) = %+v, want empty", got)
	}
}

func TestDeployLedger_ConcurrentAddNoLostWrites(t *testing.T) {
	l := &DeployLedger{Client: newLedgerClient(t), Namespace: "tatara"}
	ctx := context.Background()

	const n = 40
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := DeployLedgerEntry{Artifact: "op", Version: "v1", SourceTaskRef: "t" + strconv.Itoa(i), State: DeployStateDeploying}
			if err := l.Add(ctx, "race", e); err != nil {
				t.Errorf("Add(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	got, err := l.List(ctx, "race")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("concurrent Add lost writes: got %d entries, want %d", len(got), n)
	}
	seen := map[string]bool{}
	for _, e := range got {
		if seen[e.SourceTaskRef] {
			t.Fatalf("duplicate entry for %s", e.SourceTaskRef)
		}
		seen[e.SourceTaskRef] = true
	}
}

func TestMatchEntries(t *testing.T) {
	entries := []DeployLedgerEntry{
		{Artifact: "op", Version: "v1.4.0", SourceTaskRef: "t1"},
		{Artifact: "op", Version: "v1.4.0", SourceTaskRef: "t2"}, // two Tasks, one artifact@version
		{Artifact: "op", Version: "v1.3.0", SourceTaskRef: "t3"},
		{Artifact: "mem", Version: "v1.4.0", SourceTaskRef: "t4"},
	}
	got := MatchEntries(entries, "op", "v1.4.0")
	if len(got) != 2 {
		t.Fatalf("MatchEntries = %d, want 2: %+v", len(got), got)
	}
	if got[0].SourceTaskRef != "t1" || got[1].SourceTaskRef != "t2" {
		t.Fatalf("MatchEntries returned wrong entries: %+v", got)
	}
	if n := len(MatchEntries(entries, "op", "v9.9.9")); n != 0 {
		t.Fatalf("MatchEntries(no match) = %d, want 0", n)
	}
}

func TestDeployLedger_CorruptEntriesError(t *testing.T) {
	c := newLedgerClient(t)
	ctx := context.Background()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: deployLedgerName("p"), Namespace: "tatara"},
		Data:       map[string]string{deployLedgerKey: "{not-json"},
	}
	if err := c.Create(ctx, cm); err != nil {
		t.Fatalf("seed CM: %v", err)
	}
	l := &DeployLedger{Client: c, Namespace: "tatara"}
	if _, err := l.List(ctx, "p"); err == nil {
		t.Fatal("List: expected corrupt-entries error, got nil")
	}
	if err := l.Add(ctx, "p", DeployLedgerEntry{SourceTaskRef: "x"}); err == nil {
		t.Fatal("Add: expected corrupt-entries error, got nil")
	}
}

func TestDeployLedgerName_Sanitizes(t *testing.T) {
	if got := deployLedgerName("My/Project_Name"); got != "deploy-ledger-my-project-name" {
		t.Fatalf("deployLedgerName = %q", got)
	}
}
