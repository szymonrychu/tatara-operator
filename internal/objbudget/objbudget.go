// Package objbudget implements the etcd byte-budget pre-write guard
// (contract A.7): a byte-exact check, run BEFORE every write to an Issue,
// MergeRequest, or Task, that evicts the oldest comments/notes to
// tatara-memory when the marshalled object would exceed ObjectByteBudget.
//
// ObjectByteBudget (800,000 bytes) is HALF the ~1.5MiB etcd object ceiling,
// not 90% of it. The headroom is not slack - it is reserved for
// metadata.managedFields, which grows unboundedly under repeated
// server-side-apply status patches on a hot object and is counted against
// the SAME limit, entirely outside this package's control.
//
// The guard is byte-exact, not count-exact: "409 when len(notes) >= 200" is
// a count cap, and 200 notes of 4 KB is 800 KB while 200 notes of 40 KB is
// 8 MB. Only bytes are bytes.
//
// A 413 from the API server is NOT retried by retry.RetryOnConflict, so
// every writer of an over-budget object fails, the object becomes
// permanently unwritable, and anything it owns (an Issue/MR pinned open by
// ownership) is stuck forever. That is why the eviction decision AND the
// (non-idempotent) network spill happen ONCE, OUTSIDE any retry loop
// (Phase 1), and only the pure, idempotent trim + Update re-run on conflict
// (Phase 2). Putting the spill inside the retry closure re-fires it on
// every conflict; committing the trimmed object before the spill succeeds
// loses the evicted comments/notes if the spill then fails. The ordering
// here is SPILL FIRST, DROP ONLY ON SPILL SUCCESS.
package objbudget

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ErrObjectTooLarge means the object exceeds ObjectByteBudget with nothing
// left to evict (an oversized spec.goal, a wall of conditions): the eviction
// loop ran the evictable list to empty and the object still does not fit.
var ErrObjectTooLarge = errors.New("object exceeds byte budget with nothing left to evict")

// Spiller sends one eviction batch to tatara-memory and returns the durable
// track_id the caller records. A concrete implementation lives over
// internal/memory; tests inject a fake.
type Spiller interface {
	Spill(ctx context.Context, kind, name string, payload any) (trackID string, err error)
}

// Metrics is the recorder Fit* functions call on every guarded write. It
// defaults to a no-op; call SetMetrics once at startup (see
// internal/obs/objbudget_metrics.go for the Prometheus-backed
// implementation) to make the calls observable.
type Metrics interface {
	// ObserveObjectSize records the marshalled size, in bytes, of an object
	// this package just guarded (whether or not eviction happened).
	ObserveObjectSize(kind string, bytes int)
	// IncObjectTooLarge records an ErrObjectTooLarge outcome.
	IncObjectTooLarge(kind, name string)
	// IncCommentSpill records one eviction batch spilled to tatara-memory.
	IncCommentSpill(kind string)
}

type noopMetrics struct{}

func (noopMetrics) ObserveObjectSize(string, int)    {}
func (noopMetrics) IncObjectTooLarge(string, string) {}
func (noopMetrics) IncCommentSpill(string)           {}

var metricsRecorder Metrics = noopMetrics{}

// SetMetrics installs m as the process-wide recorder every Fit* call uses.
// Call once at startup, before any Fit* call can run concurrently; nil
// resets to the no-op default.
func SetMetrics(m Metrics) {
	if m == nil {
		m = noopMetrics{}
	}
	metricsRecorder = m
}

// sizeOf returns the marshalled JSON size of obj in bytes.
func sizeOf(obj any) (int, error) {
	b, err := json.Marshal(obj)
	if err != nil {
		return 0, fmt.Errorf("objbudget: marshal: %w", err)
	}
	return len(b), nil
}

// evictOldest pops items from the front of items - already ordered
// oldest-first, the invariant every append-only journal in this codebase
// (Comments, Notes) holds - remarshalling the full candidate object via
// setAndSize after each pop, until the marshalled size fits
// ObjectByteBudget or items is exhausted. setAndSize both installs the
// trimmed slice on the candidate object and returns its new marshalled
// size, so eviction always measures the WHOLE object, not just the list
// being trimmed.
func evictOldest[T any](items []T, setAndSize func([]T) (int, error)) (survivors, evicted []T, finalSize int, err error) {
	survivors = items
	for {
		finalSize, err = setAndSize(survivors)
		if err != nil {
			return nil, nil, 0, err
		}
		if finalSize <= tatarav1alpha1.ObjectByteBudget || len(survivors) == 0 {
			return survivors, evicted, finalSize, nil
		}
		evicted = append(evicted, survivors[0])
		survivors = survivors[1:]
	}
}

// filterCommentsFrom keeps only comments whose CreatedAt is at or after
// retainFrom - the PURE, idempotent half of the trim, safe to re-run on
// every RetryOnConflict attempt.
func filterCommentsFrom(comments []tatarav1alpha1.Comment, retainFrom metav1.Time) []tatarav1alpha1.Comment {
	out := make([]tatarav1alpha1.Comment, 0, len(comments))
	for _, c := range comments {
		if !c.CreatedAt.Time.Before(retainFrom.Time) {
			out = append(out, c)
		}
	}
	return out
}

// filterNotesFrom is the Note counterpart of filterCommentsFrom.
func filterNotesFrom(notes []tatarav1alpha1.Note, retainFrom metav1.Time) []tatarav1alpha1.Note {
	out := make([]tatarav1alpha1.Note, 0, len(notes))
	for _, n := range notes {
		if !n.At.Time.Before(retainFrom.Time) {
			out = append(out, n)
		}
	}
	return out
}

// FitIssue applies mutate to the Issue at key, evicting the oldest comments
// to tatara-memory (via sp) first if the result would exceed
// ObjectByteBudget, then persists it. mutate must be pure/idempotent: it is
// called once against a Phase-1 candidate to size the write, and again
// (possibly several times, on conflict) against a freshly re-Get'd object
// inside Phase 2.
func FitIssue(ctx context.Context, c client.Client, sp Spiller, key types.NamespacedName, mutate func(*tatarav1alpha1.Issue)) error {
	const kind = "Issue"

	cur := &tatarav1alpha1.Issue{}
	if err := c.Get(ctx, key, cur); err != nil {
		return fmt.Errorf("objbudget: get issue %s: %w", key, err)
	}
	candidate := cur.DeepCopy()
	mutate(candidate)

	survivors, evicted, finalSize, err := evictOldest(candidate.Status.Comments, func(items []tatarav1alpha1.Comment) (int, error) {
		candidate.Status.Comments = items
		return sizeOf(candidate)
	})
	if err != nil {
		return err
	}
	if finalSize > tatarav1alpha1.ObjectByteBudget {
		metricsRecorder.IncObjectTooLarge(kind, key.Name)
		return ErrObjectTooLarge
	}

	var retainFrom *metav1.Time
	var trackID string
	evictedN := len(evicted)
	if evictedN > 0 {
		trackID, err = sp.Spill(ctx, kind, key.Name, evicted)
		if err != nil {
			return fmt.Errorf("objbudget: spill %d comments for issue %s: %w", evictedN, key.Name, err)
		}
		metricsRecorder.IncCommentSpill(kind)
		if len(survivors) > 0 {
			t := survivors[0].CreatedAt
			retainFrom = &t
		} else {
			t := evicted[evictedN-1].CreatedAt
			retainFrom = &t
		}
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Issue{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		mutate(fresh)
		if retainFrom != nil {
			fresh.Status.Comments = filterCommentsFrom(fresh.Status.Comments, *retainFrom)
			fresh.Status.CommentsRetainedFrom = retainFrom
			fresh.Status.SpilledComments += evictedN
			fresh.Status.SpilledCommentsRefs = append(fresh.Status.SpilledCommentsRefs, trackID)
		}
		fresh.Status.CommentCount = len(fresh.Status.Comments) + fresh.Status.SpilledComments
		sz, err := sizeOf(fresh)
		if err != nil {
			return err
		}
		metricsRecorder.ObserveObjectSize(kind, sz)
		if sz > tatarav1alpha1.ObjectByteBudget {
			return ErrObjectTooLarge
		}
		return c.Status().Update(ctx, fresh)
	})
}

// FitMergeRequest is the MergeRequest counterpart of FitIssue: same
// two-phase guard over Status.Comments.
func FitMergeRequest(ctx context.Context, c client.Client, sp Spiller, key types.NamespacedName, mutate func(*tatarav1alpha1.MergeRequest)) error {
	const kind = "MergeRequest"

	cur := &tatarav1alpha1.MergeRequest{}
	if err := c.Get(ctx, key, cur); err != nil {
		return fmt.Errorf("objbudget: get mergerequest %s: %w", key, err)
	}
	candidate := cur.DeepCopy()
	mutate(candidate)

	survivors, evicted, finalSize, err := evictOldest(candidate.Status.Comments, func(items []tatarav1alpha1.Comment) (int, error) {
		candidate.Status.Comments = items
		return sizeOf(candidate)
	})
	if err != nil {
		return err
	}
	if finalSize > tatarav1alpha1.ObjectByteBudget {
		metricsRecorder.IncObjectTooLarge(kind, key.Name)
		return ErrObjectTooLarge
	}

	var retainFrom *metav1.Time
	var trackID string
	evictedN := len(evicted)
	if evictedN > 0 {
		trackID, err = sp.Spill(ctx, kind, key.Name, evicted)
		if err != nil {
			return fmt.Errorf("objbudget: spill %d comments for mergerequest %s: %w", evictedN, key.Name, err)
		}
		metricsRecorder.IncCommentSpill(kind)
		if len(survivors) > 0 {
			t := survivors[0].CreatedAt
			retainFrom = &t
		} else {
			t := evicted[evictedN-1].CreatedAt
			retainFrom = &t
		}
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.MergeRequest{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		mutate(fresh)
		if retainFrom != nil {
			fresh.Status.Comments = filterCommentsFrom(fresh.Status.Comments, *retainFrom)
			fresh.Status.CommentsRetainedFrom = retainFrom
			fresh.Status.SpilledComments += evictedN
			fresh.Status.SpilledCommentsRefs = append(fresh.Status.SpilledCommentsRefs, trackID)
		}
		fresh.Status.CommentCount = len(fresh.Status.Comments) + fresh.Status.SpilledComments
		sz, err := sizeOf(fresh)
		if err != nil {
			return err
		}
		metricsRecorder.ObserveObjectSize(kind, sz)
		if sz > tatarav1alpha1.ObjectByteBudget {
			return ErrObjectTooLarge
		}
		return c.Status().Update(ctx, fresh)
	})
}

// FitTask is the Task counterpart of FitIssue/FitMergeRequest: the
// evictable list is Status.Notes (spilled to Status.Stats.NotesSpilled /
// NotesSpilledRefs). Notes carry no re-ingest watermark field - unlike
// Issue/MergeRequest comments, notes are never re-synced from an external
// source, so there is no re-fetch/re-evict loop to guard against.
func FitTask(ctx context.Context, c client.Client, sp Spiller, key types.NamespacedName, mutate func(*tatarav1alpha1.Task)) error {
	const kind = "Task"

	cur := &tatarav1alpha1.Task{}
	if err := c.Get(ctx, key, cur); err != nil {
		return fmt.Errorf("objbudget: get task %s: %w", key, err)
	}
	candidate := cur.DeepCopy()
	mutate(candidate)

	survivors, evicted, finalSize, err := evictOldest(candidate.Status.Notes, func(items []tatarav1alpha1.Note) (int, error) {
		candidate.Status.Notes = items
		return sizeOf(candidate)
	})
	if err != nil {
		return err
	}
	if finalSize > tatarav1alpha1.ObjectByteBudget {
		metricsRecorder.IncObjectTooLarge(kind, key.Name)
		return ErrObjectTooLarge
	}

	var retainFrom *metav1.Time
	var trackID string
	evictedN := len(evicted)
	if evictedN > 0 {
		trackID, err = sp.Spill(ctx, kind, key.Name, evicted)
		if err != nil {
			return fmt.Errorf("objbudget: spill %d notes for task %s: %w", evictedN, key.Name, err)
		}
		metricsRecorder.IncCommentSpill(kind)
		if len(survivors) > 0 {
			t := survivors[0].At
			retainFrom = &t
		} else {
			t := evicted[evictedN-1].At
			retainFrom = &t
		}
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		mutate(fresh)
		if retainFrom != nil {
			fresh.Status.Notes = filterNotesFrom(fresh.Status.Notes, *retainFrom)
			fresh.Status.Stats.NotesSpilled += evictedN
			fresh.Status.Stats.NotesSpilledRefs = append(fresh.Status.Stats.NotesSpilledRefs, trackID)
		}
		sz, err := sizeOf(fresh)
		if err != nil {
			return err
		}
		metricsRecorder.ObserveObjectSize(kind, sz)
		if sz > tatarav1alpha1.ObjectByteBudget {
			return ErrObjectTooLarge
		}
		return c.Status().Update(ctx, fresh)
	})
}

// MinimalFailPatch fails task with the given stageReason via a JSON merge
// patch whose body carries ONLY .status.stage and .status.stageReason -
// never the object itself. This is the way out of the circularity
// ErrObjectTooLarge creates: recording the failure by marshalling and
// writing the very object that is too large to write would 413 again.
func MinimalFailPatch(ctx context.Context, c client.Client, task *tatarav1alpha1.Task, reason string) error {
	body, err := json.Marshal(map[string]any{
		"status": map[string]any{
			"stage":       tatarav1alpha1.StageFailed,
			"stageReason": reason,
		},
	})
	if err != nil {
		return fmt.Errorf("objbudget: marshal minimal fail patch: %w", err)
	}
	patch := client.RawPatch(types.MergePatchType, body)
	if err := c.Status().Patch(ctx, task, patch); err != nil {
		return fmt.Errorf("objbudget: patch task %s to failed(%s): %w", task.Name, reason, err)
	}
	return nil
}
