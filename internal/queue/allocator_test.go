package queue

import (
	"sync"
	"testing"
)

func TestSeqAllocator_NotReadyBeforeRecover(t *testing.T) {
	a := NewSeqAllocator()
	seq, ok := a.Next()
	if ok {
		t.Fatalf("Next() before Recover should return ok=false, got seq=%d ok=%v", seq, ok)
	}
}

func TestSeqAllocator_ReadyAfterRecover(t *testing.T) {
	a := NewSeqAllocator()
	a.Recover(0)
	seq, ok := a.Next()
	if !ok || seq != 1 {
		t.Fatalf("Next() after Recover(0) = (%d,%v), want (1,true)", seq, ok)
	}
}

func TestSeqAllocator_MonotonicFromZero(t *testing.T) {
	a := NewSeqAllocator()
	a.Recover(0)
	s1, ok1 := a.Next()
	s2, ok2 := a.Next()
	s3, ok3 := a.Next()
	if !ok1 || !ok2 || !ok3 {
		t.Fatal("expected ok=true after Recover")
	}
	if s1 != 1 || s2 != 2 || s3 != 3 {
		t.Fatalf("expected 1,2,3 got %d,%d,%d", s1, s2, s3)
	}
}

func TestSeqAllocator_RecoverMaxPlusOne(t *testing.T) {
	a := NewSeqAllocator()
	a.Recover(41)
	got, ok := a.Next()
	if !ok {
		t.Fatal("expected ok=true after Recover(41)")
	}
	if got != 42 {
		t.Fatalf("Next after Recover(41) = %d, want 42", got)
	}
}

func TestSeqAllocator_ConcurrentUnique(t *testing.T) {
	a := NewSeqAllocator()
	a.Recover(0)
	const n = 1000
	seen := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); seq, _ := a.Next(); seen[i] = seq }(i)
	}
	wg.Wait()
	set := map[int64]bool{}
	for _, v := range seen {
		if set[v] {
			t.Fatalf("duplicate seq %d", v)
		}
		set[v] = true
	}
}
