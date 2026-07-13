package controller

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// mirrorScheme builds a scheme carrying the tatara types for the fake-client
// mirror tests.
func mirrorScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := tatarav1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("scheme (corev1): %v", err)
	}
	return s
}

// newMirrorClient builds a fake client with the status subresource enabled for
// Issue/MergeRequest/Task (without it Status().Update unconditionally 404s) and
// the five contract A.3 field indexes registered.
func newMirrorClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(mirrorScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&tatarav1alpha1.Issue{}, &tatarav1alpha1.MergeRequest{}, &tatarav1alpha1.Task{}).
		WithIndex(&tatarav1alpha1.Issue{}, IssueKeyIndex, IssueKeyIndexer).
		WithIndex(&tatarav1alpha1.MergeRequest{}, MRKeyIndex, MRKeyIndexer).
		WithIndex(&tatarav1alpha1.Task{}, TaskProjectRefIndex, TaskProjectRefIndexer).
		WithIndex(&tatarav1alpha1.Task{}, TaskDocumentsTasksIndex, TaskDocumentsTasksIndexer).
		// The manager indexes Repository by spec.projectRef under this (misnamed)
		// key; projectRepos lists on it, so a fake client without it cannot reach the
		// pod path at all.
		WithIndex(&tatarav1alpha1.Repository{}, taskIndexRepositoryRef, func(obj client.Object) []string {
			repo := obj.(*tatarav1alpha1.Repository)
			if repo.Spec.ProjectRef == "" {
				return nil
			}
			return []string{repo.Spec.ProjectRef}
		}).
		Build()
}

// mirrorSpiller is a no-op objbudget.Spiller: the mirror tests never exceed the
// byte budget, so a spill here is itself a failure signal.
type mirrorSpiller struct{ calls int }

func (m *mirrorSpiller) Spill(context.Context, string, string, any) (string, error) {
	m.calls++
	return "track-1", nil
}

// mirrorReader counts thread reads so the on-demand-sync test can assert
// EXACTLY ONE forge read per released park.
type mirrorReader struct {
	scm.SCMReader
	comments   []scm.IssueComment
	prComments []scm.IssueComment
	calls      int
	prCalls    int
}

func (m *mirrorReader) ListIssueComments(context.Context, string, string, int) ([]scm.IssueComment, error) {
	m.calls++
	return m.comments, nil
}

func (m *mirrorReader) ListPRComments(context.Context, string, string, int) ([]scm.IssueComment, error) {
	m.prCalls++
	return m.prComments, nil
}

func mirrorProject(botLogin string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "proj", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "scm-secret",
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github",
				BotLogin: botLogin,
			},
		},
	}
}

func mirrorRepo() *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "tatara-operator", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: "proj",
			URL:        "https://github.com/szymonrychu/tatara-operator.git",
		},
	}
}

func getIssueCR(t *testing.T, c client.Client, name string) *tatarav1alpha1.Issue {
	t.Helper()
	var iss tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &iss); err != nil {
		t.Fatalf("get issue %s: %v", name, err)
	}
	return &iss
}

// TestMirrorTruncatesCommentBodies asserts contract A.1's ingest truncation: a
// comment body is cut at 8192 BYTES on a RUNE boundary and marked
// truncated=true. GitHub allows 65,536-char bodies; 25 of them is 1.6 MB, over
// the etcd ceiling on its own.
func TestMirrorTruncatesCommentBodies(t *testing.T) {
	// 3-byte runes: 4000 of them is 12000 bytes, so the cut lands mid-rune
	// unless the truncation is rune-aware.
	long := strings.Repeat("中", 4000)
	short := "lgtm"

	tests := []struct {
		name      string
		body      string
		truncated bool
	}{
		{name: "short body is untouched", body: short, truncated: false},
		{name: "oversized body is cut", body: long, truncated: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, truncated := truncateCommentBody(tc.body)
			if truncated != tc.truncated {
				t.Fatalf("truncated = %v, want %v", truncated, tc.truncated)
			}
			if len(got) > commentBodyLimit {
				t.Fatalf("body is %d bytes, over the %d-byte limit", len(got), commentBodyLimit)
			}
			if !utf8.ValidString(got) {
				t.Fatalf("truncation cut a rune in half: body is not valid UTF-8")
			}
			if !tc.truncated && got != tc.body {
				t.Fatalf("short body was modified: %q", got)
			}
		})
	}
}

// TestMirrorComputesIsBotFromBotLogin asserts the STRUCTURAL bot exclusion the
// approval grammar (C.6) and the pendingEvents enqueue filter (E.3) rely on:
// IsBot comes from Project.spec.scm.botLogin, never from a heuristic.
func TestMirrorComputesIsBotFromBotLogin(t *testing.T) {
	proj := mirrorProject("tatara-bot")
	now := time.Now()

	bot := mirrorCommentFrom(proj, scm.IssueComment{ExternalID: "1", Author: "tatara-bot", Body: "parked", CreatedAt: now})
	if !bot.IsBot {
		t.Fatalf("comment authored by botLogin must set IsBot=true")
	}
	human := mirrorCommentFrom(proj, scm.IssueComment{ExternalID: "2", Author: "szymonrychu", Body: "go ahead", CreatedAt: now})
	if human.IsBot {
		t.Fatalf("comment authored by a human must set IsBot=false")
	}
	// An empty author is NOT the bot (a deleted account must never pass an
	// equality gate as the bot).
	empty := mirrorCommentFrom(proj, scm.IssueComment{ExternalID: "3", Author: "", Body: "x", CreatedAt: now})
	if empty.IsBot {
		t.Fatalf("an empty author must never be treated as the bot")
	}
}

// TestSyncIssueUpsertsMirror asserts SyncIssue creates the Issue CR, mirrors the
// forge fields, and that a re-sync of the same thread is a SET-UNION on
// externalId - not a duplicate append.
func TestSyncIssueUpsertsMirror(t *testing.T) {
	ctx := context.Background()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	c := newMirrorClient(t, proj, repo)
	sp := &mirrorSpiller{}
	t0 := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)

	ext := scm.Issue{
		Number:    291,
		URL:       "https://github.com/szymonrychu/tatara-operator/issues/291",
		Title:     "the mirror",
		Author:    "szymonrychu",
		Body:      "body",
		State:     "open",
		Labels:    []string{"tatara"},
		CreatedAt: t0,
		UpdatedAt: t0.Add(time.Minute),
		Comments: []scm.IssueComment{
			{ExternalID: "10", Author: "szymonrychu", Body: "first", CreatedAt: t0},
			{ExternalID: "11", Author: "tatara-bot", Body: "second", CreatedAt: t0.Add(time.Minute)},
		},
	}
	if err := SyncIssue(ctx, c, sp, proj, repo, ext); err != nil {
		t.Fatalf("SyncIssue: %v", err)
	}

	name := tatarav1alpha1.IssueName(repo.Name, 291)
	iss := getIssueCR(t, c, name)
	if iss.Spec.Number != 291 || iss.Spec.RepositoryRef != repo.Name || iss.Spec.ProjectRef != proj.Name {
		t.Fatalf("spec not mirrored: %+v", iss.Spec)
	}
	if iss.Status.Title != "the mirror" || iss.Status.State != "open" || iss.Status.Author != "szymonrychu" {
		t.Fatalf("status not mirrored: %+v", iss.Status)
	}
	if len(iss.Status.Comments) != 2 || iss.Status.CommentCount != 2 {
		t.Fatalf("comments = %d (count %d), want 2", len(iss.Status.Comments), iss.Status.CommentCount)
	}
	if !iss.Status.Comments[1].IsBot {
		t.Fatalf("bot comment must carry IsBot=true")
	}
	if iss.Status.LastSyncedAt == nil {
		t.Fatalf("lastSyncedAt must be stamped by a sync")
	}
	// status.status is NOT written by the mirror sync: it is the platform's
	// decision state, owned by the approval grammar (C.6).
	if iss.Status.Status != "" {
		t.Fatalf("sync wrote status.status = %q; the mirror never decides platform state", iss.Status.Status)
	}

	// Re-sync with one NEW comment. The two existing ones must not duplicate.
	ext.Comments = append(ext.Comments, scm.IssueComment{
		ExternalID: "12", Author: "szymonrychu", Body: "third", CreatedAt: t0.Add(2 * time.Minute),
	})
	if err := SyncIssue(ctx, c, sp, proj, repo, ext); err != nil {
		t.Fatalf("SyncIssue (re-sync): %v", err)
	}
	iss = getIssueCR(t, c, name)
	if len(iss.Status.Comments) != 3 || iss.Status.CommentCount != 3 {
		t.Fatalf("re-sync produced %d comments (count %d), want 3 (set-union on externalId)",
			len(iss.Status.Comments), iss.Status.CommentCount)
	}
	if sp.calls != 0 {
		t.Fatalf("a 3-comment thread must not spill; got %d spills", sp.calls)
	}
}

// TestSyncIssueHonoursCommentsRetainedFrom is fix M18. After a spill sets
// status.commentsRetainedFrom, the evicted comments are STILL in the forge
// response. Re-ingesting them would re-evict them on the next fit: an
// evict/re-fetch/re-evict loop writing a duplicate spill record every hour,
// forever.
func TestSyncIssueHonoursCommentsRetainedFrom(t *testing.T) {
	ctx := context.Background()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()

	t0 := time.Now().Add(-4 * time.Hour).UTC().Truncate(time.Second)
	watermark := metav1.NewTime(t0.Add(2 * time.Hour))

	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.IssueName(repo.Name, 7), Namespace: testNS},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo.Name, Number: 7, ProjectRef: proj.Name,
			URL: "https://github.com/szymonrychu/tatara-operator/issues/7",
		},
		Status: tatarav1alpha1.IssueStatus{
			// The survivors of an earlier spill.
			Comments: []tatarav1alpha1.Comment{
				{ExternalID: "30", Author: "h", Body: "kept", CreatedAt: watermark},
			},
			CommentsRetainedFrom: &watermark,
			SpilledComments:      2,
			SpilledCommentsRefs:  []string{"track-1"},
			CommentCount:         3,
		},
	}
	c := newMirrorClient(t, proj, repo, iss)

	// The forge still returns the two EVICTED comments (ids 10, 11).
	ext := scm.Issue{
		Number: 7, State: "open", Title: "t",
		URL: "https://github.com/szymonrychu/tatara-operator/issues/7",
		Comments: []scm.IssueComment{
			{ExternalID: "10", Author: "h", Body: "evicted", CreatedAt: t0},
			{ExternalID: "11", Author: "h", Body: "evicted", CreatedAt: t0.Add(time.Hour)},
			{ExternalID: "30", Author: "h", Body: "kept", CreatedAt: watermark.Time},
			{ExternalID: "31", Author: "h", Body: "new", CreatedAt: watermark.Add(time.Minute)},
		},
	}
	if err := SyncIssue(ctx, c, &mirrorSpiller{}, proj, repo, ext); err != nil {
		t.Fatalf("SyncIssue: %v", err)
	}

	got := getIssueCR(t, c, iss.Name)
	for _, cm := range got.Status.Comments {
		if cm.ExternalID == "10" || cm.ExternalID == "11" {
			t.Fatalf("re-ingested an EVICTED comment (%s): the commentsRetainedFrom watermark was not honoured", cm.ExternalID)
		}
	}
	if len(got.Status.Comments) != 2 {
		t.Fatalf("comments = %d, want 2 (kept + new)", len(got.Status.Comments))
	}
	if got.Status.CommentCount != 4 {
		t.Fatalf("commentCount = %d, want 4 (2 retained + 2 spilled)", got.Status.CommentCount)
	}
}

// TestSyncMergeRequestUpsertsMirror asserts the MergeRequest half: forge fields
// mirrored, comments set-unioned, and the MIRROR's headSHA recorded (never
// trusted for a merge - fix 10 re-reads it live).
func TestSyncMergeRequestUpsertsMirror(t *testing.T) {
	ctx := context.Background()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	c := newMirrorClient(t, proj, repo)
	t0 := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)

	ext := scm.MergeRequest{
		Number:     42,
		URL:        "https://github.com/szymonrychu/tatara-operator/pull/42",
		Title:      "feat: mirror",
		Author:     "tatara-bot",
		Body:       "body",
		State:      "open",
		HeadBranch: "task/t-1",
		HeadSHA:    "abc123",
		CIStatus:   "green",
		Mergeable:  true,
		CreatedAt:  t0,
		Comments: []scm.IssueComment{
			{ExternalID: "90", Author: "tatara-bot", Body: "review", CreatedAt: t0},
		},
	}
	if err := SyncMergeRequest(ctx, c, &mirrorSpiller{}, proj, repo, ext); err != nil {
		t.Fatalf("SyncMergeRequest: %v", err)
	}

	var mr tatarav1alpha1.MergeRequest
	key := types.NamespacedName{Namespace: testNS, Name: tatarav1alpha1.MergeRequestName(repo.Name, 42)}
	if err := c.Get(ctx, key, &mr); err != nil {
		t.Fatalf("get mergerequest: %v", err)
	}
	if mr.Status.HeadSHA != "abc123" || mr.Status.HeadBranch != "task/t-1" || mr.Status.CIStatus != "green" || !mr.Status.Mergeable {
		t.Fatalf("mr status not mirrored: %+v", mr.Status)
	}
	if len(mr.Status.Comments) != 1 || mr.Status.CommentCount != 1 {
		t.Fatalf("mr comments = %d (count %d), want 1", len(mr.Status.Comments), mr.Status.CommentCount)
	}
	if mr.Status.Status != "" {
		t.Fatalf("sync wrote status.status = %q; only an ACCEPTED review outcome writes it", mr.Status.Status)
	}

	// Re-sync: no duplicate.
	if err := SyncMergeRequest(ctx, c, &mirrorSpiller{}, proj, repo, ext); err != nil {
		t.Fatalf("SyncMergeRequest (re-sync): %v", err)
	}
	if err := c.Get(ctx, key, &mr); err != nil {
		t.Fatalf("get mergerequest: %v", err)
	}
	if len(mr.Status.Comments) != 1 {
		t.Fatalf("re-sync duplicated a comment: %d", len(mr.Status.Comments))
	}
}

// TestMirrorAppendCommentToMirror is fix C5: every operator SCM write that
// produces a comment appends it to the CR mirror IN THE SAME HANDLER. Without
// it the next implement pod's bundle - rendered FROM THE MIRROR - does not
// contain the findings it is supposed to be fixing.
func TestMirrorAppendCommentToMirror(t *testing.T) {
	ctx := context.Background()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	now := metav1.NewTime(time.Now().UTC().Truncate(time.Second))

	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.MergeRequestName(repo.Name, 42), Namespace: testNS},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo.Name, Number: 42, ProjectRef: proj.Name,
			URL: "https://github.com/szymonrychu/tatara-operator/pull/42",
		},
		Status: tatarav1alpha1.MergeRequestStatus{
			Comments:     []tatarav1alpha1.Comment{{ExternalID: "1", Author: "h", Body: "hi", CreatedAt: now}},
			CommentCount: 1,
		},
	}
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.IssueName(repo.Name, 9), Namespace: testNS},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo.Name, Number: 9, ProjectRef: proj.Name,
			URL: "https://github.com/szymonrychu/tatara-operator/issues/9",
		},
	}
	c := newMirrorClient(t, proj, repo, mr, iss)
	sp := &mirrorSpiller{}

	finding := tatarav1alpha1.Comment{
		ExternalID:  "77",
		Author:      "tatara-bot",
		Body:        "this nil-derefs",
		CreatedAt:   now,
		IsBot:       true,
		Path:        "internal/controller/mirror.go",
		Line:        42,
		InReplyTo:   "1",
		ReviewRound: 2,
	}
	if err := AppendCommentToMirror(ctx, c, sp, mr, finding); err != nil {
		t.Fatalf("AppendCommentToMirror(mr): %v", err)
	}

	var got tatarav1alpha1.MergeRequest
	if err := c.Get(ctx, client.ObjectKeyFromObject(mr), &got); err != nil {
		t.Fatalf("get mr: %v", err)
	}
	if got.Status.CommentCount != 2 {
		t.Fatalf("commentCount = %d, want 2", got.Status.CommentCount)
	}
	last := got.Status.Comments[len(got.Status.Comments)-1]
	if last.Path != finding.Path || last.Line != 42 || last.InReplyTo != "1" || last.ReviewRound != 2 {
		t.Fatalf("inline fields lost on append: %+v", last)
	}

	// Idempotent: the same externalId twice is a no-op, not a duplicate (a crash
	// between the forge post and the mirror append re-runs the append).
	if err := AppendCommentToMirror(ctx, c, sp, mr, finding); err != nil {
		t.Fatalf("AppendCommentToMirror (re-run): %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(mr), &got); err != nil {
		t.Fatalf("get mr: %v", err)
	}
	if got.Status.CommentCount != 2 {
		t.Fatalf("re-run duplicated the comment: commentCount = %d, want 2", got.Status.CommentCount)
	}

	// The Issue half of the same helper.
	if err := AppendCommentToMirror(ctx, c, sp, iss, tatarav1alpha1.Comment{
		ExternalID: "78", Author: "tatara-bot", Body: "parked: identity unverified", CreatedAt: now, IsBot: true,
	}); err != nil {
		t.Fatalf("AppendCommentToMirror(issue): %v", err)
	}
	gotIss := getIssueCR(t, c, iss.Name)
	if gotIss.Status.CommentCount != 1 || len(gotIss.Status.Comments) != 1 {
		t.Fatalf("issue append: count = %d, comments = %d, want 1/1", gotIss.Status.CommentCount, len(gotIss.Status.Comments))
	}
}

// TestMirrorCadence is fix M27 completed by fix M11: an ACTIVE Task's Issues
// sync hourly; EVERY parked Task's Issues sync DAILY. v3 claimed a parked
// backlog Task "consumes no forge request" - false: its Issues are MIRRORED,
// and 150 backlog issues plus threads is several hundred requests per sweep,
// hourly, forever.
func TestMirrorCadence(t *testing.T) {
	tests := []struct {
		name  string
		task  *tatarav1alpha1.Task
		want  time.Duration
		notes string
	}{
		{name: "nil task (unowned mirror)", task: nil, want: MirrorCadenceActive},
		{name: "triaging", task: taskAtStage(tatarav1alpha1.StageTriaging, ""), want: MirrorCadenceActive},
		{name: "implementing", task: taskAtStage(tatarav1alpha1.StageImplementing, ""), want: MirrorCadenceActive},
		{name: "reviewing", task: taskAtStage(tatarav1alpha1.StageReviewing, ""), want: MirrorCadenceActive},
		{name: "merging", task: taskAtStage(tatarav1alpha1.StageMerging, ""), want: MirrorCadenceActive},
		{
			name: "parked(backlog-sweep)",
			task: taskAtStage(tatarav1alpha1.StageParked, "backlog-sweep"),
			want: MirrorCadenceParked,
		},
		{
			// EVERY parked Task, not just backlog-sweep (fix M11).
			name: "parked(identity-unverified)",
			task: taskAtStage(tatarav1alpha1.StageParked, "identity-unverified"),
			want: MirrorCadenceParked,
		},
		{
			name: "parked(review-loop-exhausted)",
			task: taskAtStage(tatarav1alpha1.StageParked, "review-loop-exhausted"),
			want: MirrorCadenceParked,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := MirrorCadence(tc.task); got != tc.want {
				t.Fatalf("MirrorCadence = %v, want %v", got, tc.want)
			}
		})
	}
	if MirrorCadenceActive != time.Hour {
		t.Fatalf("active cadence = %v, want 1h", MirrorCadenceActive)
	}
	if MirrorCadenceParked != 24*time.Hour {
		t.Fatalf("parked cadence = %v, want 24h", MirrorCadenceParked)
	}
}

func taskAtStage(stage, reason string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-1", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj"},
		Status:     tatarav1alpha1.TaskStatus{Stage: stage, StageReason: reason},
	}
}

// TestSyncIssueOnDemand is fix M11. A non-bot pendingEvent on a parked Task
// triggers EXACTLY ONE forge read of that thread, BEFORE the C.6 grammar runs.
// The grammar's clause 3d enforces single-use evidence against
// Comment.ExternalID; TaskEvent carries no externalId; and the parked cadence is
// DAILY. Without this sync the grammar re-runs against a thread that does not
// contain the comment that triggered it, and silently fails - restoring the
// exact 7-day dead end the redesign removes.
func TestSyncIssueOnDemand(t *testing.T) {
	ctx := context.Background()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()

	dayOld := metav1.NewTime(time.Now().Add(-25 * time.Hour).UTC().Truncate(time.Second))
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.IssueName(repo.Name, 291), Namespace: testNS},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo.Name, Number: 291, ProjectRef: proj.Name,
			URL: "https://github.com/szymonrychu/tatara-operator/issues/291",
		},
		Status: tatarav1alpha1.IssueStatus{
			// A mirror deliberately one day stale: the parked cadence is DAILY.
			LastSyncedAt: &dayOld,
			Comments: []tatarav1alpha1.Comment{
				{ExternalID: "1", Author: "tatara-bot", Body: "parked: identity unverified", CreatedAt: dayOld, IsBot: true},
			},
			CommentCount: 1,
		},
	}
	c := newMirrorClient(t, proj, repo, iss)

	approving := time.Now().UTC().Truncate(time.Second)
	rd := &mirrorReader{comments: []scm.IssueComment{
		{ExternalID: "1", Author: "tatara-bot", Body: "parked: identity unverified", CreatedAt: dayOld.Time},
		{ExternalID: "2", Author: "szymonrychu", Body: "go ahead", CreatedAt: approving},
	}}

	key := IssueKey(repo.Name, 291)
	if err := SyncIssueOnDemand(ctx, c, &mirrorSpiller{}, rd, proj, key); err != nil {
		t.Fatalf("SyncIssueOnDemand: %v", err)
	}
	if rd.calls != 1 {
		t.Fatalf("forge reads = %d, want EXACTLY 1", rd.calls)
	}

	got := getIssueCR(t, c, iss.Name)
	if len(got.Status.Comments) != 2 {
		t.Fatalf("comments = %d, want 2", len(got.Status.Comments))
	}
	last := got.Status.Comments[1]
	if last.ExternalID != "2" {
		t.Fatalf("approving comment has no ExternalID in the mirror: %+v", last)
	}
	if last.IsBot {
		t.Fatalf("the approving human comment was marked IsBot")
	}
	if got.Status.LastSyncedAt == nil || !got.Status.LastSyncedAt.After(dayOld.Time) {
		t.Fatalf("lastSyncedAt was not advanced by the on-demand sync")
	}
}

// TestIndexesRegistered asserts the FIVE contract A.3 field indexes are
// registered, by their EXACT contract names. Dedup is an indexed lookup on
// issueKey/mrKey, never a hashed Task name and never a label selector (label
// VALUES reject ':' and '#').
func TestIndexesRegistered(t *testing.T) {
	rec := &recordingIndexer{}
	if err := (&TaskReconciler{}).registerFieldIndexes(rec); err != nil {
		t.Fatalf("TaskReconciler.registerFieldIndexes: %v", err)
	}
	if err := (&DispatcherReconciler{}).registerFieldIndexes(rec); err != nil {
		t.Fatalf("DispatcherReconciler.registerFieldIndexes: %v", err)
	}
	for _, want := range []string{"issueKey", "mrKey", "projectRef", "documentsTasks", ".spec.dedupKey"} {
		if !rec.names[want] {
			t.Fatalf("field index %q was never registered; got %v", want, rec.names)
		}
	}
}

type recordingIndexer struct{ names map[string]bool }

func (r *recordingIndexer) IndexField(_ context.Context, _ client.Object, field string, _ client.IndexerFunc) error {
	if r.names == nil {
		r.names = map[string]bool{}
	}
	r.names[field] = true
	return nil
}

// TestIndexesResolve asserts a lookup by each index finds its CR.
func TestIndexesResolve(t *testing.T) {
	ctx := context.Background()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.IssueName(repo.Name, 291), Namespace: testNS},
		Spec:       tatarav1alpha1.IssueSpec{RepositoryRef: repo.Name, Number: 291, ProjectRef: proj.Name, URL: "u"},
	}
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.MergeRequestName(repo.Name, 42), Namespace: testNS},
		Spec:       tatarav1alpha1.MergeRequestSpec{RepositoryRef: repo.Name, Number: 42, ProjectRef: proj.Name, URL: "u"},
	}
	doc := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "doc-batch", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:     proj.Name,
			DocumentsTasks: []string{"t-1", "t-2"},
		},
	}
	c := newMirrorClient(t, proj, repo, iss, mr, doc)

	var issues tatarav1alpha1.IssueList
	if err := c.List(ctx, &issues, client.InNamespace(testNS),
		client.MatchingFields{IssueKeyIndex: IssueKey(repo.Name, 291)}); err != nil {
		t.Fatalf("list by issueKey: %v", err)
	}
	if len(issues.Items) != 1 || issues.Items[0].Name != iss.Name {
		t.Fatalf("issueKey lookup found %d issues, want 1", len(issues.Items))
	}
	if IssueKey(repo.Name, 291) != "tatara-operator#291" {
		t.Fatalf("issueKey = %q, want tatara-operator#291", IssueKey(repo.Name, 291))
	}

	var mrs tatarav1alpha1.MergeRequestList
	if err := c.List(ctx, &mrs, client.InNamespace(testNS),
		client.MatchingFields{MRKeyIndex: MRKey(repo.Name, 42)}); err != nil {
		t.Fatalf("list by mrKey: %v", err)
	}
	if len(mrs.Items) != 1 || mrs.Items[0].Name != mr.Name {
		t.Fatalf("mrKey lookup found %d mrs, want 1", len(mrs.Items))
	}
	if MRKey(repo.Name, 42) != "tatara-operator!42" {
		t.Fatalf("mrKey = %q, want tatara-operator!42", MRKey(repo.Name, 42))
	}

	var byProject tatarav1alpha1.TaskList
	if err := c.List(ctx, &byProject, client.InNamespace(testNS),
		client.MatchingFields{TaskProjectRefIndex: proj.Name}); err != nil {
		t.Fatalf("list by projectRef: %v", err)
	}
	if len(byProject.Items) != 1 {
		t.Fatalf("projectRef lookup found %d tasks, want 1", len(byProject.Items))
	}

	// documentsTasks indexes ONE ENTRY PER ELEMENT.
	for _, member := range []string{"t-1", "t-2"} {
		var covering tatarav1alpha1.TaskList
		if err := c.List(ctx, &covering, client.InNamespace(testNS),
			client.MatchingFields{TaskDocumentsTasksIndex: member}); err != nil {
			t.Fatalf("list by documentsTasks=%s: %v", member, err)
		}
		if len(covering.Items) != 1 || covering.Items[0].Name != "doc-batch" {
			t.Fatalf("documentsTasks lookup for %s found %d tasks, want the doc batch", member, len(covering.Items))
		}
	}
}
