package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// --- the fake forge -------------------------------------------------------
//
// scm.SCMWriter is EMBEDDED as a nil interface: any method this fake does not
// implement PANICS the moment it is called. That is deliberate - it is how the
// tests below assert that whole classes of call NEVER happen (Approve,
// RequestChanges, EnableAutoMerge), instead of hoping a grep catches them.

type fakeForge struct {
	scm.SCMWriter

	t *testing.T

	// head is the LIVE head per PR number. GetPRHead reads it.
	head map[int]string
	// state is the live PR state per number.
	state map[int]scm.PRState
	// mergeState is the live mergeability per number.
	mergeState map[int]scm.MergeState

	// reviews is what ListReviews returns per number: the FORGE IS THE LEDGER.
	reviews map[int][]scm.Review
	// comments is what ListReviewComments returns per review id.
	comments map[string][]scm.PostedComment
	// thread is what a plain comment listing returns per number.
	thread map[int][]scm.IssueComment

	// postReviewErr, when set, is what PostReview returns.
	postReviewErr error
	// mergeErr, when set, is what Merge returns.
	mergeErr error
	// mergePanics makes Merge panic. The fork-PR test uses it: a kind=review
	// Task must NEVER reach merging, unconditionally.
	mergePanics bool
	// closeHook runs INSIDE CloseIssue. The delivery-order test uses it to assert
	// that deliveredAt is still nil at the moment the Issue is closed (C.4).
	closeHook func()

	// call counters / recorders.
	postReviewCalls       int
	listReviewCommentCall int
	mergeCalls            int
	mergedRepos           []string
	mergedHeads           []string
	closedIssues          []string
	editedIssues          []string
	postedComments        []string
	disableAutoMergeCalls int

	// the semver:<level> label projection (H.4). addedLabels/removedLabels record
	// "<ref>|<label>" so the PROVIDER-CORRECT ref form is asserted, not just the
	// label; ensuredLabels records "<label>|<color>".
	addedLabels   []string
	removedLabels []string
	ensuredLabels []string
	// mergesAtLabel is f.mergeCalls sampled INSIDE AddLabel: the label must land
	// BEFORE the merge, or CI cuts the tag off a PR that has none.
	mergesAtLabel []int
	addLabelErr   error

	nextReviewID  int
	nextCommentID int
}

func (f *fakeForge) AddLabel(_ context.Context, _, ref, label string) error {
	f.mergesAtLabel = append(f.mergesAtLabel, f.mergeCalls)
	if f.addLabelErr != nil {
		return f.addLabelErr
	}
	f.addedLabels = append(f.addedLabels, ref+"|"+label)
	return nil
}

func (f *fakeForge) RemoveLabel(_ context.Context, _, ref, label string) error {
	f.removedLabels = append(f.removedLabels, ref+"|"+label)
	return nil
}

func (f *fakeForge) EnsureLabel(_ context.Context, _, _, name, color string) error {
	f.ensuredLabels = append(f.ensuredLabels, name+"|"+color)
	return nil
}

func newFakeForge(t *testing.T) *fakeForge {
	return &fakeForge{
		t:          t,
		head:       map[int]string{},
		state:      map[int]scm.PRState{},
		mergeState: map[int]scm.MergeState{},
		reviews:    map[int][]scm.Review{},
		comments:   map[string][]scm.PostedComment{},
		thread:     map[int][]scm.IssueComment{},
	}
}

func (f *fakeForge) GetPRHead(_ context.Context, _, _ string, number int) (string, error) {
	return f.head[number], nil
}

func (f *fakeForge) GetPRState(_ context.Context, _, _ string, number int) (scm.PRState, error) {
	st, ok := f.state[number]
	if !ok {
		st = scm.PRState{CIStatus: "success", HeadSHA: f.head[number]}
	}
	return st, nil
}

func (f *fakeForge) GetMergeState(_ context.Context, _, _ string, number int) (scm.MergeState, error) {
	ms, ok := f.mergeState[number]
	if !ok {
		return scm.MergeStateClean, nil
	}
	return ms, nil
}

func (f *fakeForge) ListReviews(_ context.Context, _, _ string, number int) ([]scm.Review, error) {
	return f.reviews[number], nil
}

func (f *fakeForge) PostReview(_ context.Context, _, _ string, number int, body string, findings []scm.ReviewFinding) (string, error) {
	f.postReviewCalls++
	if f.postReviewErr != nil {
		return "", f.postReviewErr
	}
	f.nextReviewID++
	id := fmt.Sprintf("rev-%d", f.nextReviewID)
	// The forge stores the body VERBATIM. If the caller forgot to prepend the
	// round marker, ListReviews cannot dedup and the crash test re-posts.
	f.reviews[number] = append(f.reviews[number], scm.Review{ID: id, Body: body, State: "COMMENTED"})
	// GitHub's create-review response carries NO comments array. The ids are a
	// SECOND read. Store them for ListReviewComments and return only the id.
	for _, fi := range findings {
		f.nextCommentID++
		f.comments[id] = append(f.comments[id], scm.PostedComment{
			ExternalID: fmt.Sprintf("c-%d", f.nextCommentID),
			Path:       fi.Path,
			Line:       fi.Line,
			Body:       fi.Body,
			CreatedAt:  time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC),
		})
	}
	return id, nil
}

func (f *fakeForge) ListReviewComments(_ context.Context, _, _ string, _ int, reviewID string) ([]scm.PostedComment, error) {
	f.listReviewCommentCall++
	return f.comments[reviewID], nil
}

func (f *fakeForge) Merge(_ context.Context, repoURL, _ string, number int, _, expectedHeadSHA string) (string, error) {
	if f.mergePanics {
		panic("fakeForge.Merge called: a kind=review Task must NEVER reach merging")
	}
	f.mergeCalls++
	f.mergedRepos = append(f.mergedRepos, repoURL)
	f.mergedHeads = append(f.mergedHeads, expectedHeadSHA)
	if f.mergeErr != nil {
		return "", f.mergeErr
	}
	st := f.state[number]
	st.Merged = true
	f.state[number] = st
	return "merge-" + expectedHeadSHA, nil
}

// parseIssueRef splits an "owner/repo#N" ref into its slug and number.
func parseIssueRef(ref string) (string, int) {
	i := strings.LastIndex(ref, "#")
	if i < 0 {
		return ref, 0
	}
	n, err := strconv.Atoi(ref[i+1:])
	if err != nil {
		return ref[:i], 0
	}
	return ref[:i], n
}

// Comment posts a plain thread comment. Like the real forge it returns NO id:
// the id comes from a SECOND read of the thread, which is also where the
// requestId marker dedups a re-run.
func (f *fakeForge) Comment(_ context.Context, _, issueRef, body string) error {
	f.postedComments = append(f.postedComments, issueRef+"|"+body)
	_, number := parseIssueRef(issueRef)
	f.nextCommentID++
	f.thread[number] = append(f.thread[number], scm.IssueComment{
		ExternalID: fmt.Sprintf("tc-%d", f.nextCommentID),
		Author:     "tatara-bot",
		Body:       body,
		CreatedAt:  time.Date(2026, 7, 12, 10, 30, 0, 0, time.UTC),
	})
	return nil
}

func (f *fakeForge) CloseIssue(_ context.Context, _, repo string, number int, comment string) error {
	if f.closeHook != nil {
		f.closeHook()
	}
	f.closedIssues = append(f.closedIssues, fmt.Sprintf("%s#%d|%s", repo, number, comment))
	return nil
}

func (f *fakeForge) EditIssue(_ context.Context, _, repo string, number int, req scm.EditIssueReq) error {
	title, body := "", ""
	if req.Title != nil {
		title = *req.Title
	}
	if req.Body != nil {
		body = *req.Body
	}
	f.editedIssues = append(f.editedIssues, fmt.Sprintf("%s#%d|%s|%s", repo, number, title, body))
	return nil
}

func (f *fakeForge) DisableAutoMerge(_ context.Context, _, _, _ string) error {
	f.disableAutoMergeCalls++
	return nil
}

// THE EVENT ENUM IS {COMMENT}. GitHub 422s the PR AUTHOR on BOTH review
// DECISIONS on a self-authored PR - APPROVE and REQUEST_CHANGES alike. Neither
// may ever be sent.
func (f *fakeForge) Approve(_ context.Context, _, _ string, _ int, _ string) error {
	f.t.Fatalf("fakeForge.Approve called: the review event enum is {COMMENT} (contract C.5.1b)")
	return nil
}

func (f *fakeForge) RequestChanges(_ context.Context, _, _ string, _ int, _ string) error {
	f.t.Fatalf("fakeForge.RequestChanges called: the review event enum is {COMMENT} (contract C.5.1b)")
	return nil
}

func (f *fakeForge) EnableAutoMerge(_ context.Context, _, _, _, _ string) error {
	f.t.Fatalf("fakeForge.EnableAutoMerge called: auto-merge is NEVER armed (contract C.5.2)")
	return nil
}

// mdReader serves the release-job CI status at a commit and the thread
// listings the pending-comment drain dedups against. It ALSO implements
// scm.DeployWatcher: the deploying stage polls it for the component's cut tag
// and for the terminal tatara-helmfile apply run + the pin state it applied.
type mdReader struct {
	scm.SCMReader
	f  *fakeForge
	ci map[string]string
	// tags is LatestSemverTag per repo name; a miss is "no tag cut yet".
	tags map[string]string
	// runs is LatestWorkflowRun per repo name; a miss is "no run".
	runs map[string]scm.WorkflowRun
	// pin is the applied helmfile pin state per ref (the run's head SHA, or
	// "main" for the is-this-artifact-pinned-at-all probe).
	pin map[string]string
}

func (r *mdReader) GetCommitCIStatus(_ context.Context, _, _, sha string) (string, error) {
	if st, ok := r.ci[sha]; ok {
		return st, nil
	}
	return "success", nil
}

func (r *mdReader) ListIssueComments(_ context.Context, _, _ string, number int) ([]scm.IssueComment, error) {
	return r.f.thread[number], nil
}

func (r *mdReader) LatestSemverTag(_ context.Context, _, repo string) (string, bool, error) {
	tag, ok := r.tags[repo]
	return tag, ok, nil
}

func (r *mdReader) LatestWorkflowRun(_ context.Context, _, repo, _, _ string) (scm.WorkflowRun, bool, error) {
	run, ok := r.runs[repo]
	return run, ok, nil
}

// GetFileContent serves the whole pin state on the FIRST pin file and "" (a
// 404) on the rest, exactly as the real GetFileContent does for a repo that
// carries only some of the candidate pin files.
func (r *mdReader) GetFileContent(_ context.Context, _, _, path, ref string) (string, error) {
	if path != deployPinFiles[0] {
		return "", nil
	}
	return r.pin[ref], nil
}

// --- fixtures -------------------------------------------------------------

const mdNS = "tatara"

func mdCtrlOwnerRefs(task *tatarav1alpha1.Task) []metav1.OwnerReference {
	yes := true
	return []metav1.OwnerReference{{
		APIVersion:         tatarav1alpha1.GroupVersion.String(),
		Kind:               "Task",
		Name:               task.Name,
		UID:                task.UID,
		Controller:         &yes,
		BlockOwnerDeletion: &yes,
	}}
}

func mdProject() *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "proj", Namespace: mdNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "scm-secret",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: "tatara-bot"},
			Agent:        tatarav1alpha1.AgentSpec{MaxReviewRounds: 3},
		},
	}
}

func mdSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "scm-secret", Namespace: mdNS},
		Data:       map[string][]byte{"token": []byte("tok")},
	}
}

func mdRepo(name string) *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mdNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: "proj",
			URL:        "https://github.com/szymonrychu/" + name, DefaultBranch: "main",
		},
	}
}

func mdTask(name, kind, stg string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mdNS, UID: types.UID("uid-" + name)},
		Spec:       tatarav1alpha1.TaskSpec{Kind: kind, ProjectRef: "proj"},
		Status:     tatarav1alpha1.TaskStatus{Stage: stg},
	}
}

func mdMR(task *tatarav1alpha1.Task, repo string, number int) *tatarav1alpha1.MergeRequest {
	return &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.MergeRequestName(repo, number), Namespace: mdNS,
			OwnerReferences: mdCtrlOwnerRefs(task),
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo, Number: number, ProjectRef: "proj",
			URL: fmt.Sprintf("https://github.com/szymonrychu/%s/pull/%d", repo, number),
		},
		Status: tatarav1alpha1.MergeRequestStatus{State: "open"},
	}
}

func mdIssue(task *tatarav1alpha1.Task, repo string, number int) *tatarav1alpha1.Issue {
	return &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.IssueName(repo, number), Namespace: mdNS,
			OwnerReferences: mdCtrlOwnerRefs(task),
		},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo, Number: number, ProjectRef: "proj",
			URL: fmt.Sprintf("https://github.com/szymonrychu/%s/issues/%d", repo, number),
		},
		Status: tatarav1alpha1.IssueStatus{State: "open"},
	}
}

func mdNewReader(f *fakeForge) *mdReader {
	return &mdReader{
		f: f, ci: map[string]string{},
		tags: map[string]string{},
		runs: map[string]scm.WorkflowRun{},
		pin:  map[string]string{},
	}
}

func mdNewDriver(t *testing.T, f *fakeForge, c client.Client) *StageDriver {
	t.Helper()
	return mdNewDriverWithReader(t, f, c, mdNewReader(f))
}

func mdNewDriverWithReader(t *testing.T, f *fakeForge, c client.Client, rd scm.SCMReader) *StageDriver {
	t.Helper()
	return &StageDriver{
		Client:    c,
		SCMFor:    func(string) (scm.SCMWriter, error) { return f, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return rd, nil },
		Now:       func() time.Time { return time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC) },
	}
}

func mdGetTask(t *testing.T, c client.Client, name string) *tatarav1alpha1.Task {
	t.Helper()
	var out tatarav1alpha1.Task
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: mdNS, Name: name}, &out); err != nil {
		t.Fatalf("get task %s: %v", name, err)
	}
	return &out
}

func mdGetMR(t *testing.T, c client.Client, name string) *tatarav1alpha1.MergeRequest {
	t.Helper()
	var out tatarav1alpha1.MergeRequest
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: mdNS, Name: name}, &out); err != nil {
		t.Fatalf("get mr %s: %v", name, err)
	}
	return &out
}

func mdGetIssue(t *testing.T, c client.Client, name string) *tatarav1alpha1.Issue {
	t.Helper()
	var out tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: mdNS, Name: name}, &out); err != nil {
		t.Fatalf("get issue %s: %v", name, err)
	}
	return &out
}

// --- the merge (C.5.2) ----------------------------------------------------

// A Task owning ONE MR in ONE repo merges. mergeOrder was resolved at /outcome
// (fix C2) - the single-repo case is the COMMON case and v3 could not merge it.
func TestMergeSingleRepoMerges(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.ReviewedSHA = "sha-a"
	mr.Status.Status = "approved"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.head[7] = "sha-a"
	d := mdNewDriver(t, f, c)

	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	if f.mergeCalls != 1 {
		t.Fatalf("merge calls = %d, want 1", f.mergeCalls)
	}
	if f.mergedHeads[0] != "sha-a" {
		t.Fatalf("merge pinned to %q, want the reviewed head sha-a", f.mergedHeads[0])
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.Stage != tatarav1alpha1.StageDeploying {
		t.Fatalf("stage = %q, want deploying", got.Status.Stage)
	}
	if got.Status.MergeCursor != 1 {
		t.Fatalf("mergeCursor = %d, want 1", got.Status.MergeCursor)
	}
	if gm := mdGetMR(t, c, mr.Name); gm.Status.State != "merged" || gm.Status.MergedAt == nil {
		t.Fatalf("mr state = %q mergedAt = %v, want merged/set", gm.Status.State, gm.Status.MergedAt)
	}
}

// merging entered with an EMPTY mergeOrder is a BUG, and it is treated as one.
func TestMergeEmptyMergeOrderFails(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
	mr := mdMR(task, "tatara-operator", 7)
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, c)
	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.Stage != tatarav1alpha1.StageFailed || got.Status.StageReason != stage.ReasonMergeOrderMissing {
		t.Fatalf("stage = %q/%q, want failed/merge-order-missing", got.Status.Stage, got.Status.StageReason)
	}
	if f.mergeCalls != 0 {
		t.Fatalf("merge calls = %d, want 0", f.mergeCalls)
	}
}

// mergeOrder is SEQUENTIAL and dependency-ordered: operator before cli, always.
func TestMergeSequentialOrder(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
	task.Spec.MergeOrder = []string{"tatara-operator", "tatara-cli"}
	mrA := mdMR(task, "tatara-operator", 7)
	mrA.Status.ReviewedSHA = "sha-a"
	mrB := mdMR(task, "tatara-cli", 9)
	mrB.Status.ReviewedSHA = "sha-b"
	c := newMirrorClient(t, mdProject(), mdSecret(),
		mdRepo("tatara-operator"), mdRepo("tatara-cli"), task, mrA, mrB)

	f := newFakeForge(t)
	f.head[7] = "sha-a"
	f.head[9] = "sha-b"
	d := mdNewDriver(t, f, c)

	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	if len(f.mergedRepos) != 2 {
		t.Fatalf("merged %d repos, want 2", len(f.mergedRepos))
	}
	if f.mergedRepos[0] != "https://github.com/szymonrychu/tatara-operator" {
		t.Fatalf("merged %q first, want tatara-operator (mergeOrder is dependency-ordered)", f.mergedRepos[0])
	}
	if mdGetTask(t, c, "t1").Status.Stage != tatarav1alpha1.StageDeploying {
		t.Fatalf("stage != deploying")
	}
}

// A head that moves between /outcome and Merge yields stage=reviewing, never a
// merged wrong SHA. Merge is NEVER called.
func TestMergeHeadMovedBeforeMergeReReviews(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.ReviewedSHA = "sha-a"
	mr.Status.Status = "approved"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.head[7] = "sha-MOVED"
	d := mdNewDriver(t, f, c)

	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	if f.mergeCalls != 0 {
		t.Fatalf("merge calls = %d, want 0: the head moved off the reviewed SHA", f.mergeCalls)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.Stage != tatarav1alpha1.StageReviewing {
		t.Fatalf("stage = %q, want reviewing", got.Status.Stage)
	}
	if got.Status.HeadMoveReentries != 1 {
		t.Fatalf("headMoveReentries = %d, want 1", got.Status.HeadMoveReentries)
	}
	gm := mdGetMR(t, c, mr.Name)
	if gm.Status.Status != "new" || gm.Status.ReviewedSHA != "" {
		t.Fatalf("mr status = %q reviewedSHA = %q, want new/empty", gm.Status.Status, gm.Status.ReviewedSHA)
	}
}

// The TOCTOU close: Merge itself 409s "head sha changed" -> re-review, never a
// merged wrong SHA.
func TestMergeHeadMoved409ReReviews(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.ReviewedSHA = "sha-a"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.head[7] = "sha-a"
	f.mergeErr = fmt.Errorf("merge: %w", scm.ErrHeadMoved)
	d := mdNewDriver(t, f, c)

	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.Stage != tatarav1alpha1.StageReviewing {
		t.Fatalf("stage = %q, want reviewing", got.Status.Stage)
	}
	if got.Status.HeadMoveReentries != 1 {
		t.Fatalf("headMoveReentries = %d, want 1", got.Status.HeadMoveReentries)
	}
	if gm := mdGetMR(t, c, mr.Name); gm.Status.State == "merged" {
		t.Fatalf("mr stamped merged after a 409")
	}
}

// The head-move cycle is BOUNDED: the fourth lap is refused.
func TestMergeHeadMovingExhausted(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	task.Status.HeadMoveReentries = tatarav1alpha1.MaxHeadMoveReentries
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.ReviewedSHA = "sha-a"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.head[7] = "sha-MOVED-AGAIN"
	d := mdNewDriver(t, f, c)

	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.Stage != tatarav1alpha1.StageFailed || got.Status.StageReason != stage.ReasonHeadMoving {
		t.Fatalf("stage = %q/%q, want failed/head-moving", got.Status.Stage, got.Status.StageReason)
	}
}

// An MR already merged on the forge resumes idempotently: the cursor advances,
// Merge is not called again.
func TestMergeIdempotentResumeOnMergedMR(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.State = "merged"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, c)
	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	if f.mergeCalls != 0 {
		t.Fatalf("merge calls = %d, want 0 (already merged)", f.mergeCalls)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.Stage != tatarav1alpha1.StageDeploying || got.Status.MergeCursor != 1 {
		t.Fatalf("stage = %q cursor = %d, want deploying/1", got.Status.Stage, got.Status.MergeCursor)
	}
}

// The C.9 accepted-risk DETECTOR: an MR found merged with no mergeCursor
// advance. One bot identity means the merge gate is operator logic, not a forge
// permission, so this is the only thing that can see it being bypassed.
func TestMergeUnexpectedMergeDetector(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
	task.Spec.MergeOrder = []string{"tatara-operator", "tatara-cli"}
	task.Status.MergeCursor = 1 // operator merged; cli has NOT been merged by us

	merged := mdMR(task, "tatara-cli", 9)
	merged.Status.State = "merged"
	if !UnexpectedMerge(task, merged) {
		t.Fatalf("an owned MR merged ahead of the cursor must be detected")
	}

	done := mdMR(task, "tatara-operator", 7)
	done.Status.State = "merged"
	if UnexpectedMerge(task, done) {
		t.Fatalf("an MR the cursor already passed is NOT an unexpected merge")
	}

	open := mdMR(task, "tatara-cli", 9)
	if UnexpectedMerge(task, open) {
		t.Fatalf("an open MR is not a merge at all")
	}

	before := testutil.ToFloat64(obs.UnexpectedMergeTotal.WithLabelValues("tatara-cli"))
	RecordUnexpectedMerge(task, merged)
	if after := testutil.ToFloat64(obs.UnexpectedMergeTotal.WithLabelValues("tatara-cli")); after != before+1 {
		t.Fatalf("operator_unexpected_merge_total{repo=tatara-cli} = %v, want %v", after, before+1)
	}
}

// --- delivery (C.4) -------------------------------------------------------

// deploying -> delivered CLOSES every owned Issue and ONLY THEN stamps
// deliveredAt. Nobody else can satisfy the precondition: issue_write(close) is
// gated to clarify + refine, and neither runs at deploying.
func TestDeliveryClosesIssuesThenStampsDeliveredAt(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageDeploying)
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.State = "merged"
	now := metav1.NewTime(time.Date(2026, 7, 12, 11, 0, 0, 0, time.UTC))
	mr.Status.DeployedAt = &now
	mr.Status.DeployedVersion = "v1.2.3"
	iss := mdIssue(task, "tatara-operator", 41)
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr, iss)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, c)
	if err := d.CloseIssuesOnDelivery(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("CloseIssuesOnDelivery: %v", err)
	}
	if len(f.closedIssues) != 1 {
		t.Fatalf("closed %d issues, want 1", len(f.closedIssues))
	}
	want := "szymonrychu/tatara-operator#41|Delivered in tatara-operator!7 (v1.2.3). Closed by tatara."
	if f.closedIssues[0] != want {
		t.Fatalf("close = %q, want %q", f.closedIssues[0], want)
	}
	gi := mdGetIssue(t, c, iss.Name)
	if gi.Status.State != "closed" || gi.Status.Status != "done" {
		t.Fatalf("issue state = %q/%q, want closed/done", gi.Status.State, gi.Status.Status)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.Stage != tatarav1alpha1.StageDelivered {
		t.Fatalf("stage = %q, want delivered", got.Status.Stage)
	}
	if got.Status.DeliveredAt == nil {
		t.Fatalf("deliveredAt not stamped")
	}
}

// deliveredAt is NOT stamped while an owned MR is unmerged or undeployed, and
// the EMPTY set is not a licence.
func TestDeliveryWaitsForEveryOwnedMR(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageDeploying)
	mrA := mdMR(task, "tatara-operator", 7)
	mrA.Status.State = "merged"
	now := metav1.NewTime(time.Date(2026, 7, 12, 11, 0, 0, 0, time.UTC))
	mrA.Status.DeployedAt = &now
	mrB := mdMR(task, "tatara-cli", 9)
	mrB.Status.State = "merged" // merged but NOT deployed
	iss := mdIssue(task, "tatara-operator", 41)
	c := newMirrorClient(t, mdProject(), mdSecret(),
		mdRepo("tatara-operator"), mdRepo("tatara-cli"), task, mrA, mrB, iss)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, c)
	if err := d.CloseIssuesOnDelivery(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("CloseIssuesOnDelivery: %v", err)
	}
	if len(f.closedIssues) != 0 {
		t.Fatalf("closed an issue before every owned MR deployed")
	}
	if got := mdGetTask(t, c, "t1"); got.Status.DeliveredAt != nil || got.Status.Stage != tatarav1alpha1.StageDeploying {
		t.Fatalf("delivered early: stage=%q deliveredAt=%v", got.Status.Stage, got.Status.DeliveredAt)
	}

	// The empty set is NOT a licence: a Task owning zero MRs never delivers here.
	bare := mdTask("t2", "implement", tatarav1alpha1.StageDeploying)
	c2 := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), bare)
	d2 := mdNewDriver(t, f, c2)
	if err := d2.CloseIssuesOnDelivery(context.Background(), mdProject(), bare); err != nil {
		t.Fatalf("CloseIssuesOnDelivery (bare): %v", err)
	}
	if mdGetTask(t, c2, "t2").Status.DeliveredAt != nil {
		t.Fatalf("a Task owning ZERO MergeRequests must not deliver: all([]) == true is not a licence")
	}
}
