package objbudget

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := tatarav1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	return s
}

// newFakeClient builds a fake client with the status subresource enabled for
// Issue/MergeRequest/Task - without it the fake tracker's Status().Update
// unconditionally 404s (real apiserver semantics for a CRD that declares
// +kubebuilder:subresource:status, but a footgun for a guard that lives
// entirely behind Status().Update/Patch).
func newFakeClient(t *testing.T, s *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&tatarav1alpha1.Issue{}, &tatarav1alpha1.MergeRequest{}, &tatarav1alpha1.Task{}).
		Build()
}

// fakeSpiller records every call it receives; err, when set, is returned
// (with an empty track_id) instead of a synthesized one.
type fakeSpiller struct {
	calls int
	err   error
}

func (f *fakeSpiller) Spill(_ context.Context, _, _ string, _ any) (string, error) {
	f.calls++
	if f.err != nil {
		return "", f.err
	}
	return "track-" + strconv.Itoa(f.calls), nil
}

func bigComment(id string, at time.Time, n int) tatarav1alpha1.Comment {
	return tatarav1alpha1.Comment{
		ExternalID: id,
		Author:     "someone",
		Body:       strings.Repeat("x", n),
		CreatedAt:  metav1.NewTime(at),
	}
}

// overBudgetIssue builds an Issue whose stored Comments alone exceed
// ObjectByteBudget: n comments of ~8KB each.
func overBudgetIssue(name string, n int) *tatarav1alpha1.Issue {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	comments := make([]tatarav1alpha1.Comment, 0, n)
	for i := 0; i < n; i++ {
		comments = append(comments, bigComment("c"+strconv.Itoa(i), base.Add(time.Duration(i)*time.Minute), 8000))
	}
	return &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec:       tatarav1alpha1.IssueSpec{RepositoryRef: "repo", Number: 1, URL: "https://example.invalid/1"},
		Status:     tatarav1alpha1.IssueStatus{Comments: comments, CommentCount: n},
	}
}

// conflictThenSucceed intercepts SubResourceUpdate: the first n calls return
// a conflict, every call after that (and every non-Update subresource op)
// passes through to the real client.
func conflictThenSucceed(n int, calls *int) interceptor.Funcs {
	return interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			*calls++
			if *calls <= n {
				return apierrors.NewConflict(schema.GroupResource{Group: "tatara.dev", Resource: "issues"}, obj.GetName(), errors.New("injected conflict"))
			}
			return c.SubResource(subResourceName).Update(ctx, obj, opts...)
		},
	}
}

// TestFitIssue_SpillOnceDespiteConflictRetries prevents the v3 bug: the spill
// re-fired on every RetryOnConflict attempt because it lived inside the
// closure. Three Update attempts (two conflicts + one success) must still
// produce exactly one Spill call.
func TestFitIssue_SpillOnceDespiteConflictRetries(t *testing.T) {
	ctx := context.Background()
	issue := overBudgetIssue("iss-repo-1", 120)
	s := newTestScheme(t)
	updateCalls := 0
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(issue).
		WithStatusSubresource(&tatarav1alpha1.Issue{}).
		WithInterceptorFuncs(conflictThenSucceed(2, &updateCalls)).Build()
	sp := &fakeSpiller{}

	key := types.NamespacedName{Name: "iss-repo-1", Namespace: "tatara"}
	if err := FitIssue(ctx, c, sp, key, func(*tatarav1alpha1.Issue) {}); err != nil {
		t.Fatalf("FitIssue: %v", err)
	}

	if sp.calls != 1 {
		t.Fatalf("Spiller.Spill called %d times, want exactly 1 (spill must be OUTSIDE the retry closure)", sp.calls)
	}
	if updateCalls != 3 {
		t.Fatalf("SubResourceUpdate called %d times, want 3 (2 conflicts + 1 success)", updateCalls)
	}
}

// TestFitIssue_SpillFailureDropsNothing enforces the ordering rule: SPILL
// FIRST, DROP ONLY ON SPILL SUCCESS. A failing Spiller must leave the stored
// Issue byte-for-byte unchanged - update-ok + spill-fail would silently lose
// the evicted comments, which is the forbidden ordering.
func TestFitIssue_SpillFailureDropsNothing(t *testing.T) {
	ctx := context.Background()
	issue := overBudgetIssue("iss-repo-2", 120)
	s := newTestScheme(t)
	c := newFakeClient(t, s, issue)
	sp := &fakeSpiller{err: errors.New("tatara-memory unreachable")}

	key := types.NamespacedName{Name: "iss-repo-2", Namespace: "tatara"}

	before := &tatarav1alpha1.Issue{}
	if err := c.Get(ctx, key, before); err != nil {
		t.Fatalf("get before: %v", err)
	}
	beforeJSON, err := json.Marshal(before)
	if err != nil {
		t.Fatalf("marshal before: %v", err)
	}

	err = FitIssue(ctx, c, sp, key, func(*tatarav1alpha1.Issue) {})
	if err == nil {
		t.Fatal("FitIssue: want error on spill failure, got nil")
	}

	after := &tatarav1alpha1.Issue{}
	if err := c.Get(ctx, key, after); err != nil {
		t.Fatalf("get after: %v", err)
	}
	afterJSON, err := json.Marshal(after)
	if err != nil {
		t.Fatalf("marshal after: %v", err)
	}

	if string(beforeJSON) != string(afterJSON) {
		t.Fatalf("Issue changed despite spill failure:\nbefore: %s\nafter:  %s", beforeJSON, afterJSON)
	}
}

// TestFitIssue_SpilledCommentsRefsAccumulate proves the v3 M19 bug is fixed:
// a second spill batch must APPEND its track_id, not overwrite the first.
func TestFitIssue_SpilledCommentsRefsAccumulate(t *testing.T) {
	ctx := context.Background()
	issue := overBudgetIssue("iss-repo-3", 120)
	s := newTestScheme(t)
	c := newFakeClient(t, s, issue)
	sp := &fakeSpiller{}
	key := types.NamespacedName{Name: "iss-repo-3", Namespace: "tatara"}

	if err := FitIssue(ctx, c, sp, key, func(*tatarav1alpha1.Issue) {}); err != nil {
		t.Fatalf("first FitIssue: %v", err)
	}

	mid := &tatarav1alpha1.Issue{}
	if err := c.Get(ctx, key, mid); err != nil {
		t.Fatalf("get mid: %v", err)
	}
	if len(mid.Status.SpilledCommentsRefs) != 1 {
		t.Fatalf("after first spill: SpilledCommentsRefs = %v, want 1 entry", mid.Status.SpilledCommentsRefs)
	}
	firstRef := mid.Status.SpilledCommentsRefs[0]

	// A second round of overflow: append enough new large comments to push
	// back over budget, forcing a second eviction batch.
	appendMore := func(iss *tatarav1alpha1.Issue) {
		base := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
		for i := 0; i < 20; i++ {
			iss.Status.Comments = append(iss.Status.Comments, bigComment("new-"+strconv.Itoa(i), base.Add(time.Duration(i)*time.Minute), 8000))
		}
	}
	if err := FitIssue(ctx, c, sp, key, appendMore); err != nil {
		t.Fatalf("second FitIssue: %v", err)
	}

	final := &tatarav1alpha1.Issue{}
	if err := c.Get(ctx, key, final); err != nil {
		t.Fatalf("get final: %v", err)
	}
	if len(final.Status.SpilledCommentsRefs) != 2 {
		t.Fatalf("SpilledCommentsRefs = %v, want 2 accumulated entries", final.Status.SpilledCommentsRefs)
	}
	if final.Status.SpilledCommentsRefs[0] != firstRef {
		t.Fatalf("first ref overwritten: got %q, want %q preserved at index 0", final.Status.SpilledCommentsRefs[0], firstRef)
	}
	if final.Status.SpilledCommentsRefs[1] == firstRef {
		t.Fatalf("second ref equals first ref %q, want a distinct track_id", firstRef)
	}
	if sp.calls != 2 {
		t.Fatalf("Spiller.Spill called %d times across two overflow rounds, want 2", sp.calls)
	}
}

// TestFitIssue_CommentsRetainedFromAdvances proves fix M18: the watermark
// advances to the CreatedAt of the oldest SURVIVING comment, so a follow-up
// sync that ingests only comments newer than the watermark never re-ingests
// (and so never re-evicts) the ones just spilled.
func TestFitIssue_CommentsRetainedFromAdvances(t *testing.T) {
	ctx := context.Background()
	issue := overBudgetIssue("iss-repo-4", 130)
	s := newTestScheme(t)
	c := newFakeClient(t, s, issue)
	sp := &fakeSpiller{}
	key := types.NamespacedName{Name: "iss-repo-4", Namespace: "tatara"}

	if err := FitIssue(ctx, c, sp, key, func(*tatarav1alpha1.Issue) {}); err != nil {
		t.Fatalf("FitIssue: %v", err)
	}

	got := &tatarav1alpha1.Issue{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.CommentsRetainedFrom == nil {
		t.Fatal("CommentsRetainedFrom is nil, want it stamped after eviction")
	}
	if len(got.Status.Comments) == 0 {
		t.Fatal("expected at least one surviving comment after eviction to a fit size")
	}
	oldestSurvivor := got.Status.Comments[0].CreatedAt
	if !got.Status.CommentsRetainedFrom.Time.Equal(oldestSurvivor.Time) {
		t.Fatalf("CommentsRetainedFrom = %v, want the oldest surviving comment's CreatedAt %v", got.Status.CommentsRetainedFrom.Time, oldestSurvivor.Time)
	}
	// Nothing surviving is older than the watermark.
	for _, cm := range got.Status.Comments {
		if cm.CreatedAt.Time.Before(got.Status.CommentsRetainedFrom.Time) {
			t.Fatalf("surviving comment %s (createdAt %v) is older than the watermark %v", cm.ExternalID, cm.CreatedAt.Time, got.Status.CommentsRetainedFrom.Time)
		}
	}
}

// TestFitTask_ObjectTooLarge_MinimalFailPatch covers fix L32: a Task whose
// spec.goal alone exceeds the budget has nothing evictable, so FitTask must
// return ErrObjectTooLarge without ever calling Spill, and the recovery
// write (MinimalFailPatch) must be a merge patch whose BODY carries only
// stage/stageReason - never the oversized goal - so recording the failure
// cannot itself 413.
func TestFitTask_ObjectTooLarge_MinimalFailPatch(t *testing.T) {
	ctx := context.Background()
	hugeGoal := strings.Repeat("g", 900_000)
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "proj-implement-2026-07-12-abcde", Namespace: "tatara"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Goal: hugeGoal},
	}
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(task).Build()
	sp := &fakeSpiller{}
	key := types.NamespacedName{Name: task.Name, Namespace: "tatara"}

	err := FitTask(ctx, c, sp, key, func(*tatarav1alpha1.Task) {})
	if !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("FitTask err = %v, want ErrObjectTooLarge", err)
	}
	if sp.calls != 0 {
		t.Fatalf("Spiller.Spill called %d times, want 0 (nothing evictable, nothing to spill)", sp.calls)
	}

	var capturedPatch []byte
	pc := fake.NewClientBuilder().WithScheme(s).WithObjects(task).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cli client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				data, dErr := patch.Data(obj)
				if dErr != nil {
					return dErr
				}
				capturedPatch = data
				return nil
			},
		}).Build()

	if err := MinimalFailPatch(ctx, pc, task, "object-too-large"); err != nil {
		t.Fatalf("MinimalFailPatch: %v", err)
	}
	if capturedPatch == nil {
		t.Fatal("SubResourcePatch was never called")
	}
	if strings.Contains(string(capturedPatch), "g") && strings.Contains(string(capturedPatch), hugeGoal[:100]) {
		t.Fatalf("patch body carries the oversized goal: %d bytes", len(capturedPatch))
	}
	if len(capturedPatch) > 200 {
		t.Fatalf("patch body is %d bytes, want a MINIMAL patch (stage+stageReason only)", len(capturedPatch))
	}

	var decoded struct {
		Status struct {
			Stage       string `json:"stage"`
			StageReason string `json:"stageReason"`
		} `json:"status"`
	}
	if err := json.Unmarshal(capturedPatch, &decoded); err != nil {
		t.Fatalf("patch body is not valid JSON: %v (%s)", err, capturedPatch)
	}
	if decoded.Status.Stage != tatarav1alpha1.StageFailed {
		t.Fatalf("patch stage = %q, want %q", decoded.Status.Stage, tatarav1alpha1.StageFailed)
	}
	if decoded.Status.StageReason != "object-too-large" {
		t.Fatalf("patch stageReason = %q, want object-too-large", decoded.Status.StageReason)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(capturedPatch, &raw); err != nil {
		t.Fatalf("unmarshal top level: %v", err)
	}
	if len(raw) != 1 {
		t.Fatalf("patch has %d top-level keys, want exactly 1 (status)", len(raw))
	}
	var statusFields map[string]json.RawMessage
	if err := json.Unmarshal(raw["status"], &statusFields); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if len(statusFields) != 2 {
		t.Fatalf("patch .status has %d fields %v, want exactly 2 (stage, stageReason)", len(statusFields), statusFields)
	}
}

// notFoundThenSucceed intercepts Get: the first n calls for the named object
// return NotFound (the informer cache has not seen a just-created CR yet),
// every call after that passes through to the real client.
func notFoundThenSucceed(n int, name string, calls *int) interceptor.Funcs {
	return interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == name {
				*calls++
				if *calls <= n {
					return apierrors.NewNotFound(schema.GroupResource{Group: "tatara.dev", Resource: "mergerequests"}, key.Name)
				}
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}
}

// TestFitMergeRequest_RetriesGetThroughInformerCacheLag reproduces the
// 2026-07-19 mint failure: mirror.SyncMergeRequest Creates the CR and
// immediately calls FitMergeRequest through the CACHED client, whose informer
// had not observed the create yet - the unretried Get returned NotFound for an
// object that exists on the server ("objbudget: get mergerequest
// mr-tatara-operator-388 not found") and the whole webhook mint 500'd. The Get
// must wait out the cache lag with a short bounded retry.
func TestFitMergeRequest_RetriesGetThroughInformerCacheLag(t *testing.T) {
	ctx := context.Background()
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "mr-repo-388", Namespace: "tatara"},
		Spec:       tatarav1alpha1.MergeRequestSpec{RepositoryRef: "repo", Number: 388, URL: "https://example.invalid/388"},
	}
	s := newTestScheme(t)
	getCalls := 0
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(mr).
		WithStatusSubresource(&tatarav1alpha1.MergeRequest{}).
		WithInterceptorFuncs(notFoundThenSucceed(2, "mr-repo-388", &getCalls)).Build()

	key := types.NamespacedName{Name: "mr-repo-388", Namespace: "tatara"}
	err := FitMergeRequest(ctx, c, &fakeSpiller{}, key, func(m *tatarav1alpha1.MergeRequest) {
		m.Status.Title = "written through cache lag"
	})
	if err != nil {
		t.Fatalf("FitMergeRequest: %v (a NotFound that is informer cache lag after a Create must be retried)", err)
	}
	got := &tatarav1alpha1.MergeRequest{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.Title != "written through cache lag" {
		t.Fatalf("Status.Title = %q, want the mutation persisted", got.Status.Title)
	}
}

// TestFitIssue_RetriesGetThroughInformerCacheLag holds the same guarantee on
// the Issue sibling: SyncIssue has the identical Create-then-Fit shape.
func TestFitIssue_RetriesGetThroughInformerCacheLag(t *testing.T) {
	ctx := context.Background()
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: "iss-repo-41", Namespace: "tatara"},
		Spec:       tatarav1alpha1.IssueSpec{RepositoryRef: "repo", Number: 41, URL: "https://example.invalid/41"},
	}
	s := newTestScheme(t)
	getCalls := 0
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(iss).
		WithStatusSubresource(&tatarav1alpha1.Issue{}).
		WithInterceptorFuncs(notFoundThenSucceed(2, "iss-repo-41", &getCalls)).Build()

	key := types.NamespacedName{Name: "iss-repo-41", Namespace: "tatara"}
	if err := FitIssue(ctx, c, &fakeSpiller{}, key, func(i *tatarav1alpha1.Issue) {
		i.Status.Title = "written through cache lag"
	}); err != nil {
		t.Fatalf("FitIssue: %v", err)
	}
	got := &tatarav1alpha1.Issue{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.Title != "written through cache lag" {
		t.Fatalf("Status.Title = %q, want the mutation persisted", got.Status.Title)
	}
}

// TestFitMergeRequest_PersistentNotFoundStillSurfaces bounds the retry: a
// genuinely deleted object must come back as NotFound, not hang.
func TestFitMergeRequest_PersistentNotFoundStillSurfaces(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)
	c := newFakeClient(t, s) // the MR never exists
	key := types.NamespacedName{Name: "mr-gone-1", Namespace: "tatara"}
	err := FitMergeRequest(ctx, c, &fakeSpiller{}, key, func(*tatarav1alpha1.MergeRequest) {})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("err = %v, want NotFound after the bounded retry", err)
	}
}
