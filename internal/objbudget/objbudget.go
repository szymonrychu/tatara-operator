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
//
// THAT ORDERING HAS EXACTLY ONE INVERSION, and it is deliberate: Discarding.
// SPILL FIRST protects evicted items from a REPAIRABLE outage. A Project with
// spec.memory.enabled=false has no memory stack, will never have one, and
// nothing is being repaired - so "block until the spill succeeds" is not a
// policy there, it is a permanent refusal (#616). Discarding therefore reports
// success with an EMPTY track_id, which every Fit* reads as "evicted, not
// stored": the items are dropped, no ref is recorded, and the outcome lands on
// operator_objbudget_evicted_dropped_total instead of
// operator_comment_spill_total. Recording a ref for a batch nothing stored is
// worse than recording none - task_context would rehydrate it and get a 404.
package objbudget

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ErrObjectTooLarge means the object exceeds ObjectByteBudget with nothing
// left to evict (an oversized spec.goal, a wall of conditions): the eviction
// loop ran the evictable list to empty and the object still does not fit.
var ErrObjectTooLarge = errors.New("object exceeds byte budget with nothing left to evict")

// ErrSpillerUnconfigured means eviction was needed but no Spiller was passed.
// It is a WIRING bug, not a runtime condition, and it is reported as a clean
// error rather than tolerated: the write still fails, exactly as it did when
// this path dereferenced a nil interface and panicked. See SpillBlocked.
var ErrSpillerUnconfigured = errors.New("no tatara-memory spiller configured")

// ErrSpillFailed means the Spiller was configured and reachable-looking but
// the spill call returned an error. It exists so a caller can tell a
// TRANSIENT spill fault (answer 503 + Retry-After: tatara-memory is down and
// will come back) from every other Fit* failure, without string-matching the
// wrapped error.
var ErrSpillFailed = errors.New("tatara-memory spill failed")

// The reasons SpillBlocked distinguishes. They are the label values of
// operator_objbudget_spill_blocked_total, so an alert can tell an outage
// (spill_error, self-healing once tatara-memory is back) from a wiring bug
// (unconfigured, which needs a deploy).
const (
	SpillBlockedReasonError        = "spill_error"
	SpillBlockedReasonUnconfigured = "unconfigured"
)

// Spiller sends one eviction batch to tatara-memory and returns the durable
// track_id the caller records. A concrete implementation lives over
// internal/memory; tests inject a fake.
//
// AN EMPTY track_id WITH A NIL ERROR MEANS DISCARDED, NOT STORED: the batch
// is gone and the caller must NOT record a ref for it. See Discarding and the
// package doc's inversion note.
type Spiller interface {
	Spill(ctx context.Context, kind, name string, payload any) (trackID string, err error)
}

// Discarding is the Spiller for a Project whose memory stack is switched off
// by configuration. It accepts every batch and stores nothing.
//
// It is a VALUE, not nil: nil means "no spiller was wired", which is a bug
// and blocks the write. Discarding means "there is deliberately nowhere to
// spill to", which permits it.
var Discarding Spiller = discardingSpiller{}

type discardingSpiller struct{}

func (discardingSpiller) Spill(context.Context, string, string, any) (string, error) {
	return "", nil
}

// FitOption tunes one Fit* call. The only one today is WithNoteCap.
type FitOption func(*fitOptions)

type fitOptions struct{ noteCap bool }

// WithNoteCap makes FitTask evict on COUNT as well as on bytes, down to
// tatarav1alpha1.MaxNotes.
//
// It is OPT-IN, and that is the whole design. Only the four note APPENDERS
// pass it; the dozen other FitTask callers (stage transitions, the merge
// cursor, CI gating) write status fields that have nothing to do with the
// journal, and making them evict on count would let a tatara-memory blip
// block a transition - a worse failure than the one the cap prevents.
func WithNoteCap() FitOption { return func(o *fitOptions) { o.noteCap = true } }

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
	// IncEvictedDropped records one eviction batch DISCARDED rather than
	// spilled, because the owning Project has no memory stack by
	// configuration. It is the counterpart of IncCommentSpill, not of
	// IncSpillBlocked: nothing was refused, the items are simply gone.
	IncEvictedDropped(kind string)
	// IncSpillBlocked records a write REFUSED because its eviction batch could
	// not be spilled, by kind and by one of the SpillBlockedReason* values.
	IncSpillBlocked(kind, reason string)
}

type noopMetrics struct{}

func (noopMetrics) ObserveObjectSize(string, int)    {}
func (noopMetrics) IncObjectTooLarge(string, string) {}
func (noopMetrics) IncCommentSpill(string)           {}
func (noopMetrics) IncEvictedDropped(string)         {}
func (noopMetrics) IncSpillBlocked(string, string)   {}

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

// createLagBackoff bounds how long a Fit* Get waits out informer cache lag.
// The mirror upserts Create an Issue/MergeRequest CR and immediately Fit* it
// through the CACHED client; a Get that races the watch event reports
// NotFound for an object that exists on the server, and an unretried NotFound
// here failed a whole webhook mint in production (2026-07-19,
// mr-tatara-operator-388: NotFound in the same second as the CR's
// creationTimestamp). ~3s of total sleep across 5 attempts, then the
// NotFound is real and surfaces.
var createLagBackoff = wait.Backoff{Steps: 5, Duration: 100 * time.Millisecond, Factor: 2}

// getWaitingOutCreateLag is the initial Fit* read: a Get that retries
// NotFound on createLagBackoff.
func getWaitingOutCreateLag(ctx context.Context, c client.Client, key types.NamespacedName, obj client.Object) error {
	return retry.OnError(createLagBackoff, apierrors.IsNotFound, func() error {
		return c.Get(ctx, key, obj)
	})
}

// SpillBlocked records that a guarded write was REFUSED because its eviction
// batch could not be spilled to tatara-memory. Blocking is the deliberate
// policy - see the package doc's SPILL FIRST, DROP ONLY ON SPILL SUCCESS
// ordering - so this does not change any behaviour; it only makes the block
// visible. Before it the block was completely silent: no metric, no
// distinguishable log, so an object wedged by a memory outage was
// indistinguishable from a healthy one and there was nothing to alert on.
//
// Exported because internal/restapi's note-cap path spills directly (outside
// Fit*) and must land on the SAME counter; a second counter for the same
// condition would split every alert query in two.
//
// items is a COUNT. Comment and note bodies are never logged here: they are
// arbitrary forge/agent text and a spill-blocked object is exactly the case
// where they are largest.
func SpillBlocked(ctx context.Context, kind, name, reason string, items int) {
	metricsRecorder.IncSpillBlocked(kind, reason)
	slog.WarnContext(ctx, "objbudget: cannot spill to tatara-memory; refusing the write rather than dropping evicted items",
		"action", "objbudget_spill_blocked", "kind", kind, "resource_id", name,
		"reason", reason, "items", items)
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

// spillEvicted sends ONE eviction batch and reports whether tatara-memory
// stored it. It is the single place the three Fit* functions decide what an
// unspillable batch means, so the unconfigured/failed/discarded split cannot
// drift between them.
//
// stored=false with err=nil is the deliberate-discard case (see Discarding):
// the caller drops the items and records NO ref.
func spillEvicted(ctx context.Context, sp Spiller, kind, name string, batch any, items int) (trackID string, stored bool, err error) {
	if sp == nil {
		SpillBlocked(ctx, kind, name, SpillBlockedReasonUnconfigured, items)
		return "", false, ErrSpillerUnconfigured
	}
	trackID, err = sp.Spill(ctx, kind, name, batch)
	if err != nil {
		SpillBlocked(ctx, kind, name, SpillBlockedReasonError, items)
		return "", false, fmt.Errorf("%w: %w", ErrSpillFailed, err)
	}
	if trackID == "" {
		metricsRecorder.IncEvictedDropped(kind)
		slog.WarnContext(ctx, "objbudget: evicted items discarded rather than spilled; the owning project has no memory stack by configuration",
			"action", "objbudget_evicted_dropped", "kind", kind, "resource_id", name, "items", items)
		return "", false, nil
	}
	metricsRecorder.IncCommentSpill(kind)
	return trackID, true, nil
}

// excludeComments drops every comment whose ExternalID is in evicted.
//
// It replaces a "retain CreatedAt >= retainFrom" filter, which was wrong on a
// TIE: metav1.Time round-trips at second granularity, so a comment sharing its
// second with the oldest survivor was both evicted AND retained. The object
// then never came under budget, and every later write re-spilled the same
// batch and appended another track_id. Identity has no ties.
func excludeComments(comments []tatarav1alpha1.Comment, evicted map[string]struct{}) []tatarav1alpha1.Comment {
	out := make([]tatarav1alpha1.Comment, 0, len(comments))
	for _, c := range comments {
		if _, gone := evicted[c.ExternalID]; gone {
			continue
		}
		out = append(out, c)
	}
	return out
}

// evictedCommentIDs is excludeComments' key set.
func evictedCommentIDs(evicted []tatarav1alpha1.Comment) map[string]struct{} {
	ids := make(map[string]struct{}, len(evicted))
	for _, c := range evicted {
		ids[c.ExternalID] = struct{}{}
	}
	return ids
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
	if err := getWaitingOutCreateLag(ctx, c, key, cur); err != nil {
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
	var stored bool
	evictedN := len(evicted)
	if evictedN > 0 {
		trackID, stored, err = spillEvicted(ctx, sp, kind, key.Name, evicted, evictedN)
		if err != nil {
			return fmt.Errorf("objbudget: spill %d comments for issue %s: %w", evictedN, key.Name, err)
		}
		if len(survivors) > 0 {
			t := survivors[0].CreatedAt
			retainFrom = &t
		} else {
			t := evicted[evictedN-1].CreatedAt
			retainFrom = &t
		}
	}
	goneIDs := evictedCommentIDs(evicted)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Issue{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		mutate(fresh)
		if retainFrom != nil {
			was := len(fresh.Status.Comments)
			fresh.Status.Comments = excludeComments(fresh.Status.Comments, goneIDs)
			// CommentsRetainedFrom no longer drives the exclusion, but it is
			// still the A.1 re-ingest watermark: without it the mirror re-fetches
			// and re-appends everything this call just evicted.
			fresh.Status.CommentsRetainedFrom = retainFrom
			fresh.Status.SpilledComments += was - len(fresh.Status.Comments)
			if stored {
				fresh.Status.SpilledCommentsRefs = append(fresh.Status.SpilledCommentsRefs, trackID)
			}
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
	if err := getWaitingOutCreateLag(ctx, c, key, cur); err != nil {
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
	var stored bool
	evictedN := len(evicted)
	if evictedN > 0 {
		trackID, stored, err = spillEvicted(ctx, sp, kind, key.Name, evicted, evictedN)
		if err != nil {
			return fmt.Errorf("objbudget: spill %d comments for mergerequest %s: %w", evictedN, key.Name, err)
		}
		if len(survivors) > 0 {
			t := survivors[0].CreatedAt
			retainFrom = &t
		} else {
			t := evicted[evictedN-1].CreatedAt
			retainFrom = &t
		}
	}
	goneIDs := evictedCommentIDs(evicted)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.MergeRequest{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		mutate(fresh)
		if retainFrom != nil {
			was := len(fresh.Status.Comments)
			fresh.Status.Comments = excludeComments(fresh.Status.Comments, goneIDs)
			fresh.Status.CommentsRetainedFrom = retainFrom
			fresh.Status.SpilledComments += was - len(fresh.Status.Comments)
			if stored {
				fresh.Status.SpilledCommentsRefs = append(fresh.Status.SpilledCommentsRefs, trackID)
			}
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
func FitTask(ctx context.Context, c client.Client, sp Spiller, key types.NamespacedName, mutate func(*tatarav1alpha1.Task), opts ...FitOption) error {
	const kind = "Task"

	var o fitOptions
	for _, apply := range opts {
		apply(&o)
	}

	cur := &tatarav1alpha1.Task{}
	if err := getWaitingOutCreateLag(ctx, c, key, cur); err != nil {
		return fmt.Errorf("objbudget: get task %s: %w", key, err)
	}
	candidate := cur.DeepCopy()
	mutate(candidate)

	// The COUNT cap runs first and unconditionally; the byte guard then runs on
	// what is left. Doing it the other way round would let a journal of 500
	// small notes pass the byte check and never reach the cap.
	notes := candidate.Status.Notes
	var capEvicted []tatarav1alpha1.Note
	if o.noteCap && len(notes) > tatarav1alpha1.MaxNotes {
		n := len(notes) - tatarav1alpha1.MaxNotes
		capEvicted = notes[:n:n]
		notes = notes[n:]
	}

	_, evicted, finalSize, err := evictOldest(notes, func(items []tatarav1alpha1.Note) (int, error) {
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
	evicted = append(capEvicted, evicted...)

	var trackID string
	var stored bool
	evictedN := len(evicted)
	if evictedN > 0 {
		trackID, stored, err = spillEvicted(ctx, sp, kind, key.Name, evicted, evictedN)
		if err != nil {
			return fmt.Errorf("objbudget: spill %d notes for task %s: %w", evictedN, key.Name, err)
		}
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		mutate(fresh)
		if evictedN > 0 {
			// POSITIONAL, not "retain At >= retainFrom". Notes are appended
			// oldest-first and metav1.Time round-trips at second granularity, so
			// a timestamp filter both evicted and retained a note that shared its
			// second with the oldest survivor: the cap was never reached and every
			// later write re-spilled the same batch. Index arithmetic has no ties.
			//
			// The cap is re-applied against fresh, not just replayed at evictedN,
			// because an uncapped FitTask caller may have appended between the two
			// reads; without it the cap silently stops converging.
			drop := evictedN
			if o.noteCap {
				drop = max(drop, len(fresh.Status.Notes)-tatarav1alpha1.MaxNotes)
			}
			drop = min(drop, len(fresh.Status.Notes))
			fresh.Status.Notes = fresh.Status.Notes[drop:]
			fresh.Status.Stats.NotesSpilled += drop
			if stored {
				fresh.Status.Stats.NotesSpilledRefs = append(fresh.Status.Stats.NotesSpilledRefs, trackID)
			}
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

// MinimalFailPatch parks task with the given park reason via a JSON merge
// patch whose body carries ONLY .status.parkReason and .status.parkedAt -
// never the object itself. This is the way out of the circularity
// ErrObjectTooLarge creates: recording the failure by marshalling and
// writing the very object that is too large to write would 413 again.
//
// It writes the PARK FLAG and not a state, because under the 8-state model a
// failure does not move the Task: it stalls it where it is (#521). It cannot
// call stage.Park for the same reason it cannot marshal the object - the patch
// body has to stay minimal - so the two must not drift: the fields written here
// are exactly the ones Park stamps, minus parkedFromState (which needs a read
// of the current state this patch deliberately does not have).
func MinimalFailPatch(ctx context.Context, c client.Client, task *tatarav1alpha1.Task, reason string) error {
	body, err := json.Marshal(map[string]any{
		"status": map[string]any{
			"parkReason": reason,
			"parkedAt":   time.Now().UTC().Format(time.RFC3339),
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
