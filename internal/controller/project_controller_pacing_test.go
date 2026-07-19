package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// listCountingClient wraps a client.Client and counts List calls, so a pacing
// test can assert the underlying full-namespace-List block ran exactly as
// often as the floor allows - not once per Reconcile pass (tatara-operator#367:
// a Project stuck on a fast Reconcile cadence, driven by owned-memory-stack
// watch events plus reconcileMemory's 10s provisioning requeue, re-ran
// computeProjectCounts/resumeNoReentryParks/ReapTerminal's full-namespace Lists
// on every single one of those passes).
type listCountingClient struct {
	client.Client
	mu    sync.Mutex
	lists int
}

func (c *listCountingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.mu.Lock()
	c.lists++
	c.mu.Unlock()
	return c.Client.List(ctx, list, opts...)
}

func (c *listCountingClient) ListCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lists
}

func pcRepo(name, projectRef string) *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mdNS},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: projectRef, URL: "https://example.test/" + name, ReingestSchedule: "0 6 * * *"},
	}
}

// Stream #367 (computeProjectCounts half): computeProjectCounts does two full
// namespace Lists (Repository, Task) EVERY Reconcile pass. computeProjectCountsPaced
// puts a floor under it, mirroring driveUnparksPaced (unpark.go): calling it twice
// inside the floor runs the underlying block once and returns the residual wait;
// a third call past the floor runs it again.
func TestComputeProjectCountsPaced_SkipsWithinFloor_FoldsResidualIntoRequeue(t *testing.T) {
	repo := pcRepo("r1", "proj")
	base := newMirrorClient(t, repo)
	cc := &listCountingClient{Client: base}
	r := &ProjectReconciler{Client: cc, Scheme: base.Scheme()}
	proj := wfProject()
	ctx := context.Background()

	t0 := time.Now()
	requeue1, err := r.computeProjectCountsPaced(ctx, proj, t0)
	if err != nil {
		t.Fatalf("computeProjectCountsPaced (first pass): %v", err)
	}
	if requeue1 != defaultComputeProjectCountsInterval {
		t.Fatalf("first pass requeue = %v, want the full %v floor", requeue1, defaultComputeProjectCountsInterval)
	}
	if proj.Status.RepositoryCount != 1 {
		t.Fatalf("first pass must run computeProjectCounts, got RepositoryCount=%d", proj.Status.RepositoryCount)
	}
	listsAfterFirst := cc.ListCount()
	if listsAfterFirst != 2 {
		t.Fatalf("first pass must List Repository+Task once each, got %d list calls", listsAfterFirst)
	}

	// A second call well inside the floor: no second sweep, and the returned
	// duration is the RESIDUAL wait, not zero and not the full interval.
	t1 := t0.Add(5 * time.Second)
	proj.Status.RepositoryCount = 999 // prove a skip does not touch the counts at all
	requeue2, err := r.computeProjectCountsPaced(ctx, proj, t1)
	if err != nil {
		t.Fatalf("computeProjectCountsPaced (paced pass): %v", err)
	}
	want2 := defaultComputeProjectCountsInterval - 5*time.Second
	if requeue2 != want2 {
		t.Fatalf("paced pass residual requeue = %v, want %v", requeue2, want2)
	}
	if proj.Status.RepositoryCount != 999 {
		t.Fatalf("a paced-out pass must leave Status counts untouched, got RepositoryCount=%d", proj.Status.RepositoryCount)
	}
	if got := cc.ListCount(); got != listsAfterFirst {
		t.Fatalf("a paced-out pass must not re-list, got %d list calls (want %d)", got, listsAfterFirst)
	}

	// A third call once the floor has fully elapsed must run again.
	t2 := t0.Add(defaultComputeProjectCountsInterval + time.Second)
	requeue3, err := r.computeProjectCountsPaced(ctx, proj, t2)
	if err != nil {
		t.Fatalf("computeProjectCountsPaced (post-floor pass): %v", err)
	}
	if requeue3 != defaultComputeProjectCountsInterval {
		t.Fatalf("post-floor pass requeue = %v, want the full %v floor again", requeue3, defaultComputeProjectCountsInterval)
	}
	if proj.Status.RepositoryCount != 1 {
		t.Fatalf("post-floor pass must re-run computeProjectCounts, got RepositoryCount=%d", proj.Status.RepositoryCount)
	}
	if got := cc.ListCount(); got != listsAfterFirst*2 {
		t.Fatalf("post-floor pass must re-list, got %d list calls (want %d)", got, listsAfterFirst*2)
	}
}

// Two live Projects must not throttle each other: computeProjectCountsPaced is
// keyed per-project (like lastDriveUnparks), not a single cluster-wide clock.
func TestComputeProjectCountsPaced_PerProjectFloor_DoesNotCrossThrottle(t *testing.T) {
	repoA := pcRepo("ra", "proj")
	repoB := pcRepo("rb", "proj-b")
	base := newMirrorClient(t, repoA, repoB)
	cc := &listCountingClient{Client: base}
	r := &ProjectReconciler{Client: cc, Scheme: base.Scheme()}
	projA := wfProject()
	projB := wfProject()
	projB.Name = "proj-b"
	ctx := context.Background()
	t0 := time.Now()

	if _, err := r.computeProjectCountsPaced(ctx, projA, t0); err != nil {
		t.Fatalf("computeProjectCountsPaced (project A): %v", err)
	}
	// Project B, 1s later, well inside project A's floor: must still run its own
	// first pass in full.
	requeueB, err := r.computeProjectCountsPaced(ctx, projB, t0.Add(time.Second))
	if err != nil {
		t.Fatalf("computeProjectCountsPaced (project B): %v", err)
	}
	if requeueB != defaultComputeProjectCountsInterval {
		t.Fatalf("project B's first pass requeue = %v, want the full floor (must not inherit project A's clock)", requeueB)
	}
	if projB.Status.RepositoryCount != 1 {
		t.Fatalf("project B's first pass must run in full, got RepositoryCount=%d", projB.Status.RepositoryCount)
	}
	if got := cc.ListCount(); got != 4 {
		t.Fatalf("both projects' first passes must each List Repository+Task once, got %d list calls", got)
	}
}

// A rapid trigger loop (the #367 amplification: sub-10s Reconcile cadence from
// the owned-memory-stack watch + the 10s provisioning requeue) must not turn
// into an unbounded number of full-namespace Lists. 150 calls spanning 149
// synthetic seconds cross the 60s floor exactly twice (at t=60s and t=120s), so
// exactly 3 runs (t=0, t=60, t=120) - 6 List calls, not 300.
func TestComputeProjectCountsPaced_ListCallsBoundedUnderRapidTriggerLoop(t *testing.T) {
	repo := pcRepo("r1", "proj")
	base := newMirrorClient(t, repo)
	cc := &listCountingClient{Client: base}
	r := &ProjectReconciler{Client: cc, Scheme: base.Scheme()}
	proj := wfProject()
	ctx := context.Background()
	t0 := time.Now()

	for i := 0; i < 150; i++ {
		if _, err := r.computeProjectCountsPaced(ctx, proj, t0.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("computeProjectCountsPaced (iteration %d): %v", i, err)
		}
	}
	if got := cc.ListCount(); got != 6 {
		t.Fatalf("rapid trigger loop must bound List calls to 3 runs x 2 lists = 6, got %d", got)
	}
}
