package queue

import (
	"sync"
	"testing"
)

func TestSeqAllocator_MonotonicFromZero(t *testing.T) {
	a := NewSeqAllocator()
	if a.Next() != 1 || a.Next() != 2 || a.Next() != 3 {
		t.Fatal("expected 1,2,3 from a fresh allocator")
	}
}

func TestSeqAllocator_RecoverMaxPlusOne(t *testing.T) {
	a := NewSeqAllocator()
	a.Recover(41)
	if got := a.Next(); got != 42 {
		t.Fatalf("Next after Recover(41) = %d, want 42", got)
	}
}

func TestSeqAllocator_ConcurrentUnique(t *testing.T) {
	a := NewSeqAllocator()
	const n = 1000
	seen := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); seen[i] = a.Next() }(i)
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
