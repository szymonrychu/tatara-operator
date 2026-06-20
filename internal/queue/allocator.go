package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ErrSeqNotReady is returned by EnqueueEvent when the seq allocator has not
// yet recovered the high-water marks from existing QueuedEvents.
var ErrSeqNotReady = errors.New("queue: seq allocator not yet recovered")

// SeqAllocator hands out a strictly increasing int64 sequence per project.
// Correctness relies on a single leader-elected active operator (one allocator
// instance). The ready flag is global (set once after the boot recovery scans
// every project) so a project with no existing QueuedEvents is still allocatable.
type SeqAllocator struct {
	mu    sync.Mutex
	next  map[string]int64
	ready atomic.Bool
}

func NewSeqAllocator() *SeqAllocator { return &SeqAllocator{next: make(map[string]int64)} }

// RecoverProject sets the per-project counter so the next allocation is maxSeq+1.
// Only raises the counter; never lowers it. Call at boot per project with the max
// Seq of existing QueuedEvents for that project. Does not flip ready; the recoverer
// calls MarkReady once after scanning all projects so an empty cluster is still ready.
func (a *SeqAllocator) RecoverProject(project string, maxSeq int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if maxSeq > a.next[project] {
		a.next[project] = maxSeq
	}
}

// MarkReady marks the allocator recovered. Called once by the recoverer after it
// has scanned existing QueuedEvents (even when there are none), so Next admits.
func (a *SeqAllocator) MarkReady() { a.ready.Store(true) }

// Next returns the next sequence number for the given project and ok=true. Each
// project has an independent monotonic counter starting at 1. Returns (0, false)
// when recovery has not completed yet.
func (a *SeqAllocator) Next(project string) (int64, bool) {
	if !a.ready.Load() {
		return 0, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.next[project]++
	return a.next[project], true
}

// SeqRecoverer is a manager.Runnable that recovers the allocator high-water marks
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
	// Group max seq per project, then recover each project independently.
	maxByProject := make(map[string]int64)
	for i := range list.Items {
		ev := &list.Items[i]
		if ev.Spec.Seq > maxByProject[ev.Spec.ProjectRef] {
			maxByProject[ev.Spec.ProjectRef] = ev.Spec.Seq
		}
	}
	for proj, maxSeq := range maxByProject {
		s.Alloc.RecoverProject(proj, maxSeq)
	}
	// Mark ready unconditionally (even with zero events) so enqueue admits after boot.
	s.Alloc.MarkReady()
	log.FromContext(ctx).Info("queue: seq recovered", "action", "seq_recover", "namespace", s.Namespace, "projects", len(maxByProject))
	return nil
}
