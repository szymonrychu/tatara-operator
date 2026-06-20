package queue

import (
	"context"
	"sync"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// SeqAllocator hands out a strictly increasing int64 sequence. Correctness
// relies on a single leader-elected active operator (one allocator instance).
type SeqAllocator struct {
	mu   sync.Mutex
	next int64
}

func NewSeqAllocator() *SeqAllocator { return &SeqAllocator{next: 0} }

// Recover sets the counter so the next allocation is maxSeq+1. Call once at boot
// with the max Seq of existing QueuedEvents.
func (a *SeqAllocator) Recover(maxSeq int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if maxSeq > a.next {
		a.next = maxSeq
	}
}

func (a *SeqAllocator) Next() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.next++
	return a.next
}

// SeqRecoverer is a manager.Runnable that recovers the allocator high-water mark
// from existing QueuedEvents after the cache syncs.
type SeqRecoverer struct {
	Client    client.Client
	Alloc     *SeqAllocator
	Namespace string
}

func (s *SeqRecoverer) Start(ctx context.Context) error {
	var list tatarav1alpha1.QueuedEventList
	if err := s.Client.List(ctx, &list, client.InNamespace(s.Namespace)); err != nil {
		return err
	}
	var maxSeq int64
	for i := range list.Items {
		if list.Items[i].Spec.Seq > maxSeq {
			maxSeq = list.Items[i].Spec.Seq
		}
	}
	s.Alloc.Recover(maxSeq)
	log.FromContext(ctx).Info("queue: seq recovered", "action", "seq_recover", "max", maxSeq)
	return nil
}
