package queue

import (
	"sync"
	"testing"
)

func TestSeqAllocator_NotReadyBeforeRecover(t *testing.T) {
	a := NewSeqAllocator()
	seq, ok := a.Next("p")
	if ok {
		t.Fatalf("Next() before MarkReady should return ok=false, got seq=%d ok=%v", seq, ok)
	}
}

func TestSeqAllocator_ReadyAfterMarkReady(t *testing.T) {
	a := NewSeqAllocator()
	a.MarkReady()
	seq, ok := a.Next("p")
	if !ok || seq != 1 {
		t.Fatalf("Next() after MarkReady = (%d,%v), want (1,true)", seq, ok)
	}
}

func TestSeqAllocator_MonotonicFromZero(t *testing.T) {
	a := NewSeqAllocator()
	a.MarkReady()
	s1, ok1 := a.Next("p")
	s2, ok2 := a.Next("p")
	s3, ok3 := a.Next("p")
	if !ok1 || !ok2 || !ok3 {
		t.Fatal("expected ok=true after MarkReady")
	}
	if s1 != 1 || s2 != 2 || s3 != 3 {
		t.Fatalf("expected 1,2,3 got %d,%d,%d", s1, s2, s3)
	}
}

func TestSeqAllocator_RecoverMaxPlusOne(t *testing.T) {
	a := NewSeqAllocator()
	a.RecoverProject("p", 41)
	a.MarkReady()
	got, ok := a.Next("p")
	if !ok {
		t.Fatal("expected ok=true after MarkReady")
	}
	if got != 42 {
		t.Fatalf("Next after RecoverProject(p,41) = %d, want 42", got)
	}
}

// TestSeqAllocator_PerProjectIndependence proves that each project has its own
// monotonic line: Next("b") starts at 1 even after two calls to Next("a").
func TestSeqAllocator_PerProjectIndependence(t *testing.T) {
	a := NewSeqAllocator()
	a.MarkReady()
	got1, _ := a.Next("a")
	got2, _ := a.Next("a")
	got3, _ := a.Next("b")
	if got1 != 1 {
		t.Fatalf("first Next(a) = %d, want 1", got1)
	}
	if got2 != 2 {
		t.Fatalf("second Next(a) = %d, want 2", got2)
	}
	if got3 != 1 {
		t.Fatalf("first Next(b) = %d, want 1 (independent of a)", got3)
	}
}

func TestSeqAllocator_RecoverProject_PerProject(t *testing.T) {
	a := NewSeqAllocator()
	a.RecoverProject("proj-a", 100)
	a.RecoverProject("proj-b", 50)
	a.MarkReady()
	// proj-a should resume at 101, proj-b at 51, independently
	if got, _ := a.Next("proj-a"); got != 101 {
		t.Fatalf("Next(proj-a) after RecoverProject(proj-a,100) = %d, want 101", got)
	}
	if got, _ := a.Next("proj-b"); got != 51 {
		t.Fatalf("Next(proj-b) after RecoverProject(proj-b,50) = %d, want 51", got)
	}
}

func TestSeqAllocator_RecoverProject_OnlyRaises(t *testing.T) {
	a := NewSeqAllocator()
	a.RecoverProject("p", 100)
	a.RecoverProject("p", 10) // lower value must not lower the counter
	a.MarkReady()
	if got, _ := a.Next("p"); got != 101 {
		t.Fatalf("Next after second RecoverProject with lower value = %d, want 101", got)
	}
}

func TestSeqAllocator_ConcurrentUnique(t *testing.T) {
	a := NewSeqAllocator()
	a.MarkReady()
	const n = 1000
	seen := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		// All goroutines target the same project so we verify mutex correctness.
		go func(i int) { defer wg.Done(); seq, _ := a.Next("proj-concurrent"); seen[i] = seq }(i)
	}
	wg.Wait()
	set := map[int64]bool{}
	for _, v := range seen {
		if v < 1 || v > n {
			t.Fatalf("seq %d out of expected range [1, %d]", v, n)
		}
		if set[v] {
			t.Fatalf("duplicate seq %d", v)
		}
		set[v] = true
	}
}
