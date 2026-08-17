package objbudget

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// droppingMetrics captures the two eviction-outcome counters so a test can
// tell "spilled to tatara-memory" from "deliberately discarded".
type droppingMetrics struct {
	noopMetrics
	spilled []string
	dropped []string
}

func (d *droppingMetrics) IncCommentSpill(kind string)   { d.spilled = append(d.spilled, kind) }
func (d *droppingMetrics) IncEvictedDropped(kind string) { d.dropped = append(d.dropped, kind) }

func withDroppingMetrics(t *testing.T) *droppingMetrics {
	t.Helper()
	rec := &droppingMetrics{}
	SetMetrics(rec)
	t.Cleanup(func() { SetMetrics(nil) })
	return rec
}

// taskWithNotes builds a Task carrying n small notes, all stamped at the same
// second unless spaced is true. metav1.Time round-trips at SECOND
// granularity, so "same second" is the realistic case, not a contrived one.
func taskWithNotes(name string, n int, spaced bool) *tatarav1alpha1.Task {
	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	notes := make([]tatarav1alpha1.Note, 0, n)
	for i := 0; i < n; i++ {
		at := base
		if spaced {
			at = base.Add(time.Duration(i) * time.Minute)
		}
		notes = append(notes, tatarav1alpha1.Note{
			At: metav1.NewTime(at), Agent: "implement", Kind: "note",
			Body: "note-" + strconv.Itoa(i),
		})
	}
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Goal: "g"},
		Status:     tatarav1alpha1.TaskStatus{Notes: notes},
	}
}

// TestFitTask_NoteCapEvictsAtMaxNotes is the cap, moved off the restapi note
// path and onto FitTask so every note appender obeys it. Without
// WithNoteCap the count is not a budget at all - only bytes are.
func TestFitTask_NoteCapEvictsAtMaxNotes(t *testing.T) {
	ctx := context.Background()
	task := taskWithNotes("proj-implement-2026-08-17-cap", tatarav1alpha1.MaxNotes, true)
	s := newTestScheme(t)
	c := newFakeClient(t, s, task)
	sp := &fakeSpiller{}
	key := types.NamespacedName{Name: task.Name, Namespace: "tatara"}

	err := FitTask(ctx, c, sp, key, func(tk *tatarav1alpha1.Task) {
		tk.Status.Notes = append(tk.Status.Notes, tatarav1alpha1.Note{
			At:    metav1.NewTime(time.Date(2026, 8, 17, 13, 0, 0, 0, time.UTC)),
			Agent: "implement", Kind: "handoff", Body: "the newest",
		})
	}, WithNoteCap())
	if err != nil {
		t.Fatalf("FitTask: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got.Status.Notes) != tatarav1alpha1.MaxNotes {
		t.Fatalf("notes = %d, want %d", len(got.Status.Notes), tatarav1alpha1.MaxNotes)
	}
	if got.Status.Notes[len(got.Status.Notes)-1].Body != "the newest" {
		t.Fatalf("newest note was evicted: %q", got.Status.Notes[len(got.Status.Notes)-1].Body)
	}
	if got.Status.Notes[0].Body != "note-1" {
		t.Fatalf("oldest retained note = %q, want note-1 (note-0 is the eviction)", got.Status.Notes[0].Body)
	}
	if got.Status.Stats.NotesSpilled != 1 {
		t.Fatalf("NotesSpilled = %d, want 1", got.Status.Stats.NotesSpilled)
	}
	if sp.calls != 1 {
		t.Fatalf("Spill calls = %d, want 1", sp.calls)
	}
}

// TestFitTask_NoteCapSameSecondEvictsExactlyOnce is the boundary-tie
// regression. Eviction used to re-run as a timestamp filter (retain
// At >= retainFrom), so a note sharing its second with the oldest SURVIVOR
// was spilled and retained at the same time: the cap was never reached, and
// every later write re-spilled the same batch and appended another track_id.
// Positional eviction is tie-immune.
func TestFitTask_NoteCapSameSecondEvictsExactlyOnce(t *testing.T) {
	ctx := context.Background()
	task := taskWithNotes("proj-implement-2026-08-17-tie", tatarav1alpha1.MaxNotes, false)
	s := newTestScheme(t)
	c := newFakeClient(t, s, task)
	sp := &fakeSpiller{}
	key := types.NamespacedName{Name: task.Name, Namespace: "tatara"}

	err := FitTask(ctx, c, sp, key, func(tk *tatarav1alpha1.Task) {
		tk.Status.Notes = append(tk.Status.Notes, tatarav1alpha1.Note{
			At:    metav1.NewTime(time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)),
			Agent: "implement", Kind: "handoff", Body: "the newest",
		})
	}, WithNoteCap())
	if err != nil {
		t.Fatalf("FitTask: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got.Status.Notes) != tatarav1alpha1.MaxNotes {
		t.Fatalf("notes = %d, want %d (a same-second tie must not defeat the cap)",
			len(got.Status.Notes), tatarav1alpha1.MaxNotes)
	}
	if got.Status.Notes[0].Body != "note-1" {
		t.Fatalf("oldest retained note = %q, want note-1", got.Status.Notes[0].Body)
	}
}

// TestFitTask_WithoutNoteCapCountIsNotABudget pins the opt-in: the 15 FitTask
// call sites that are not note appenders must not evict on count, or a
// tatara-memory blip blocks stage transitions and the merge cursor.
func TestFitTask_WithoutNoteCapCountIsNotABudget(t *testing.T) {
	ctx := context.Background()
	task := taskWithNotes("proj-implement-2026-08-17-nocap", tatarav1alpha1.MaxNotes+10, true)
	s := newTestScheme(t)
	c := newFakeClient(t, s, task)
	sp := &fakeSpiller{}
	key := types.NamespacedName{Name: task.Name, Namespace: "tatara"}

	if err := FitTask(ctx, c, sp, key, func(*tatarav1alpha1.Task) {}); err != nil {
		t.Fatalf("FitTask: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got.Status.Notes) != tatarav1alpha1.MaxNotes+10 {
		t.Fatalf("notes = %d, want %d untouched", len(got.Status.Notes), tatarav1alpha1.MaxNotes+10)
	}
	if sp.calls != 0 {
		t.Fatalf("Spill calls = %d, want 0", sp.calls)
	}
}

// TestFitTask_DiscardingDropsWithoutRecordingARef is the memory-disabled
// policy. Discarding returns an EMPTY track_id, which the contract reads as
// "discarded, not stored": the notes go, no ref is recorded (a ref that
// resolves to nothing is worse than no ref), and the outcome counts as a
// drop rather than a spill.
func TestFitTask_DiscardingDropsWithoutRecordingARef(t *testing.T) {
	ctx := context.Background()
	rec := withDroppingMetrics(t)
	task := taskWithNotes("proj-implement-2026-08-17-disc", tatarav1alpha1.MaxNotes+3, true)
	s := newTestScheme(t)
	c := newFakeClient(t, s, task)
	key := types.NamespacedName{Name: task.Name, Namespace: "tatara"}

	err := FitTask(ctx, c, Discarding, key, func(tk *tatarav1alpha1.Task) {
		tk.Status.Notes = append(tk.Status.Notes, tatarav1alpha1.Note{
			At:    metav1.NewTime(time.Date(2026, 8, 17, 14, 0, 0, 0, time.UTC)),
			Agent: "implement", Kind: "handoff", Body: "the handoff that used to 503",
		})
	}, WithNoteCap())
	if err != nil {
		t.Fatalf("FitTask with Discarding: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got.Status.Notes) != tatarav1alpha1.MaxNotes {
		t.Fatalf("notes = %d, want %d", len(got.Status.Notes), tatarav1alpha1.MaxNotes)
	}
	if got.Status.Notes[len(got.Status.Notes)-1].Body != "the handoff that used to 503" {
		t.Fatal("the handoff note was not written")
	}
	if len(got.Status.Stats.NotesSpilledRefs) != 0 {
		t.Fatalf("NotesSpilledRefs = %v, want empty (nothing was stored)", got.Status.Stats.NotesSpilledRefs)
	}
	if got.Status.Stats.NotesSpilled != 4 {
		t.Fatalf("NotesSpilled = %d, want 4 (the journal really is 4 notes shorter)", got.Status.Stats.NotesSpilled)
	}
	if len(rec.spilled) != 0 {
		t.Fatalf("IncCommentSpill = %v, want none", rec.spilled)
	}
	if len(rec.dropped) != 1 || rec.dropped[0] != "Task" {
		t.Fatalf("IncEvictedDropped = %v, want [Task]", rec.dropped)
	}
}

// TestFitTask_SpillFailureIsErrSpillFailed lets the note path keep answering
// 503 + Retry-After on a real outage instead of falling through to a 500.
func TestFitTask_SpillFailureIsErrSpillFailed(t *testing.T) {
	ctx := context.Background()
	task := taskWithNotes("proj-implement-2026-08-17-fail", tatarav1alpha1.MaxNotes+1, true)
	s := newTestScheme(t)
	c := newFakeClient(t, s, task)
	sp := &fakeSpiller{err: errors.New("tatara-memory unreachable")}
	key := types.NamespacedName{Name: task.Name, Namespace: "tatara"}

	err := FitTask(ctx, c, sp, key, func(*tatarav1alpha1.Task) {}, WithNoteCap())
	if !errors.Is(err, ErrSpillFailed) {
		t.Fatalf("FitTask err = %v, want ErrSpillFailed", err)
	}
}

// TestFitIssue_EvictionIsByIdentityNotTimestamp is filterCommentsFrom's half
// of the same boundary-tie defect: two comments posted in the same second,
// one evicted and one retained, meant the evicted one survived its own
// eviction. Comments carry a provider id; exclusion goes by that.
func TestFitIssue_EvictionIsByIdentityNotTimestamp(t *testing.T) {
	ctx := context.Background()
	// Every comment shares one CreatedAt, so a timestamp filter retains all
	// 120 and the object stays over budget forever.
	at := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	comments := make([]tatarav1alpha1.Comment, 0, 120)
	for i := 0; i < 120; i++ {
		comments = append(comments, tatarav1alpha1.Comment{
			ExternalID: "c" + strconv.Itoa(i), Author: "someone",
			Body: strings.Repeat("x", 8000), CreatedAt: metav1.NewTime(at),
		})
	}
	issue := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: "iss-repo-tie", Namespace: "tatara"},
		Spec:       tatarav1alpha1.IssueSpec{RepositoryRef: "repo", Number: 1, URL: "https://example.invalid/1"},
		Status:     tatarav1alpha1.IssueStatus{Comments: comments, CommentCount: 120},
	}
	s := newTestScheme(t)
	c := newFakeClient(t, s, issue)
	sp := &fakeSpiller{}
	key := types.NamespacedName{Name: issue.Name, Namespace: "tatara"}

	if err := FitIssue(ctx, c, sp, key, func(*tatarav1alpha1.Issue) {}); err != nil {
		t.Fatalf("FitIssue: %v", err)
	}

	got := &tatarav1alpha1.Issue{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got.Status.Comments) >= 120 {
		t.Fatalf("comments = %d, want fewer than 120 (a same-second tie must not defeat eviction)",
			len(got.Status.Comments))
	}
	if got.Status.Comments[0].ExternalID == "c0" {
		t.Fatal("the oldest comment survived its own eviction")
	}
	// CommentsRetainedFrom stays load-bearing for re-ingest even though it no
	// longer drives the exclusion.
	if got.Status.CommentsRetainedFrom == nil {
		t.Fatal("CommentsRetainedFrom = nil, want the retain watermark still persisted")
	}
	if got.Status.CommentCount != len(got.Status.Comments)+got.Status.SpilledComments {
		t.Fatalf("CommentCount = %d, want %d", got.Status.CommentCount,
			len(got.Status.Comments)+got.Status.SpilledComments)
	}
}
