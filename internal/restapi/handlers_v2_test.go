package restapi_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// --- fakes ----------------------------------------------------------------

// panicForge satisfies scm.SCMWriter with a nil embedded interface: ANY call on
// it panics. It is how the zero-forge-request tests prove the mirror endpoints
// and the /outcome handler never leave the cluster.
type panicForge struct{ scm.SCMWriter }

// panicReader is the same, for the SCMReader half.
type panicReader struct{ scm.SCMReader }

// recordingForge answers the reads and writes the write paths legitimately make
// and PANICS on everything else, which is the point: PostReview panicking is
// what proves /outcome never posts a review.
type recordingForge struct {
	scm.SCMWriter
	heads          map[string]string // "<number>" -> live head sha
	createdRefs    []string
	createdReqs    []scm.IssueReq // the FULL CreateIssue request (incl. Labels), one per call
	openedURLs     []string
	issueStates    map[int]scm.IssueState
	nextNumber     int
	comments       []recordedComment  // every Comment call, in order
	subIssueCalls  []recordedSubIssue // every AddSubIssue call, in order
	addSubIssueErr error              // returned by AddSubIssue when set (e.g. scm.ErrSubIssuesUnsupported)
	commentErr     error              // returned by Comment when set (e.g. cross-repo 403 on the parent)
	openChangeErr  error              // returned by OpenChange when set (e.g. a 422 or a transport error)
	createIssueErr error              // returned by CreateIssue when set (e.g. the GitLab 400 that #529 is about)

	// The B1 READINESS SURFACE. Both default to "ready" - CI success, merge state
	// clean - so that every pre-PR-B test that submits an implement outcome keeps
	// asserting what it always asserted. A test that wants a refusal names the
	// number it wants refused.
	ciStatuses    map[int]string         // number -> PRState.CIStatus ("" | pending | success | failure)
	mergeStates   map[int]scm.MergeState // number -> GetMergeState answer
	prStateErr    error                  // returned by GetPRState when set (the fail-open path)
	mergeStateErr error                  // returned by GetMergeState when set
}

// GetPRState answers the B1 readiness read. The head it reports is the SAME
// live head GetPRHead serves, because a fake whose two head reads disagree
// would make the head-moved and readiness paths untestable against each other.
func (f *recordingForge) GetPRState(_ context.Context, _, _ string, number int) (scm.PRState, error) {
	if f.prStateErr != nil {
		return scm.PRState{}, f.prStateErr
	}
	ci, ok := f.ciStatuses[number]
	if !ok {
		ci = "success"
	}
	return scm.PRState{Author: "tatara-agent", HeadSHA: f.heads[fmt.Sprint(number)], CIStatus: ci}, nil
}

func (f *recordingForge) GetMergeState(_ context.Context, _, _ string, number int) (scm.MergeState, error) {
	if f.mergeStateErr != nil {
		return "", f.mergeStateErr
	}
	if ms, ok := f.mergeStates[number]; ok {
		return ms, nil
	}
	return scm.MergeStateClean, nil
}

type recordedComment struct {
	Ref  string
	Body string
}

type recordedSubIssue struct {
	ParentRef   string
	ChildNumber int
}

func newRecordingForge() *recordingForge {
	return &recordingForge{
		heads: map[string]string{}, issueStates: map[int]scm.IssueState{}, nextNumber: 100,
		ciStatuses: map[int]string{}, mergeStates: map[int]scm.MergeState{},
	}
}

func (f *recordingForge) GetPRHead(_ context.Context, _, _ string, number int) (string, error) {
	sha, ok := f.heads[fmt.Sprint(number)]
	if !ok {
		return "", fmt.Errorf("no head for %d", number)
	}
	return sha, nil
}

func (f *recordingForge) CreateIssue(_ context.Context, repoURL, _ string, req scm.IssueReq) (scm.CreatedIssue, error) {
	if f.createIssueErr != nil {
		// Still recorded: a test asserting on the failure log needs to know what
		// the forge was actually handed. NOTE this is the one path on which
		// createdReqs and createdRefs stop being index-aligned - a failed call has
		// a request but no ref. Pair them by index only when createIssueErr is nil.
		f.createdReqs = append(f.createdReqs, req)
		return scm.CreatedIssue{}, f.createIssueErr
	}
	f.nextNumber++
	ref := fmt.Sprintf("acme/%s#%d", lastSeg(repoURL), f.nextNumber)
	f.createdRefs = append(f.createdRefs, ref)
	f.createdReqs = append(f.createdReqs, req)
	return scm.CreatedIssue{Ref: ref, URL: "https://forge/issues/" + fmt.Sprint(f.nextNumber)}, nil
}

func (f *recordingForge) OpenChange(_ context.Context, _, _, _, _, _, body string) (string, error) {
	if f.openChangeErr != nil {
		return "", f.openChangeErr
	}
	f.nextNumber++
	url := "https://forge/pull/" + fmt.Sprint(f.nextNumber)
	f.openedURLs = append(f.openedURLs, body)
	return url, nil
}

func (f *recordingForge) GetIssueState(_ context.Context, _, _ string, number int) (scm.IssueState, error) {
	return f.issueStates[number], nil
}

func (f *recordingForge) Comment(_ context.Context, _, issueRef, body string) error {
	f.comments = append(f.comments, recordedComment{Ref: issueRef, Body: body})
	return f.commentErr
}

func (f *recordingForge) AddSubIssue(_ context.Context, _, parentRef string, childNumber int) error {
	f.subIssueCalls = append(f.subIssueCalls, recordedSubIssue{ParentRef: parentRef, ChildNumber: childNumber})
	return f.addSubIssueErr
}

func lastSeg(s string) string {
	i := strings.LastIndexByte(s, '/')
	if i < 0 {
		return s
	}
	return s[i+1:]
}

type fakeSpiller struct {
	batches []any
	err     error
}

func (f *fakeSpiller) Spill(_ context.Context, _, _ string, payload any) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	f.batches = append(f.batches, payload)
	return fmt.Sprintf("track-%d", len(f.batches)), nil
}

type fakeMemory struct{ byTrack map[string]json.RawMessage }

func (f *fakeMemory) Fetch(_ context.Context, trackID string) (json.RawMessage, error) {
	raw, ok := f.byTrack[trackID]
	if !ok {
		return nil, fmt.Errorf("no such track %q", trackID)
	}
	return raw, nil
}

type fakeApproval struct {
	grant map[string]bool // issue CR name -> granted
	auto  bool            // when set, granted issues return Auto evidence
	// needCitation makes the fake behave like the real verifier's shape: it
	// refuses unless the agent cited something. It is what proves the citations
	// actually reach the seam.
	needCitation bool
	// grantWithNilEvidence exercises the defensive nil-evidence guard in the
	// clarify writer: ok=true with a nil evidence must never write an
	// approver-less approval.
	grantWithNilEvidence bool
	// refusalReason overrides the reason returned on a plain !grant refusal
	// (default controller.ApprovalRefusedNoMaintainer). needCitation's own
	// refusal is always controller.ApprovalRefusedNoCitation, matching the
	// real verifier's shape for that clause.
	refusalReason string
	gotCitations  [][]tatarav1alpha1.ApprovalCitation
	gotDeclared   []string
}

// VerifyApprovalDeclared is the restapi.ApprovalVerifier seam post-#521: it
// gains declared (the agent's approvingMaintainer, empty on the auto-approve
// path) and a third return, the refusal REASON, so the gate's 200 body can
// name which condition refused instead of a bare false.
func (f *fakeApproval) VerifyApprovalDeclared(_ context.Context, _ *tatarav1alpha1.Project,
	iss *tatarav1alpha1.Issue,
	citations []tatarav1alpha1.ApprovalCitation, declared string) (*tatarav1alpha1.ApprovalEvidence, bool, string) {
	f.gotCitations = append(f.gotCitations, citations)
	f.gotDeclared = append(f.gotDeclared, declared)
	// MIRRORS THE PRODUCTION SEAM, and it is load-bearing that it does. The real
	// verifier (controller.GrammarVerifier.VerifyApprovalDeclared) returns the
	// Issue's STORED approval with ok=TRUE for an out-of-scope Issue - it is not
	// pending approval, so it must not block the scope check. That stored
	// approval is routinely nil. A fake that refused instead would certify a
	// gate production does not have.
	if !controller.ApprovalInScope(iss) {
		return iss.Status.Approval, true, ""
	}
	// ALSO MIRRORS THE SEAM: verifyOneIssue's clause-2 idempotence returns an
	// already-approved Issue's STORED evidence before any citation clause runs,
	// so an Issue that already carries evidence needs no citation. Without this
	// arm needCitation was STRICTER than production for exactly that Issue, and a
	// test built on it would certify a refusal the real verifier never makes.
	if iss.Status.Status == "approved" && iss.Status.Approval != nil {
		return iss.Status.Approval, true, ""
	}
	if !f.grant[iss.Name] {
		reason := f.refusalReason
		if reason == "" {
			reason = controller.ApprovalRefusedNoMaintainer
		}
		return nil, false, reason
	}
	if f.needCitation && len(citations) == 0 {
		return nil, false, controller.ApprovalRefusedNoCitation
	}
	if f.grantWithNilEvidence {
		return nil, true, ""
	}
	if f.auto {
		return &tatarav1alpha1.ApprovalEvidence{
			Auto: true, Login: tatarav1alpha1.AutoApproveLogin,
			CreatedAt: metav1.NewTime(time.Unix(0, 0)),
		}, true, ""
	}
	return &tatarav1alpha1.ApprovalEvidence{
		Login: "maintainer", CommentID: "1", Phrase: "go ahead",
		CreatedAt: metav1.NewTime(time.Unix(0, 0)),
	}, true, ""
}

type fakeCI struct {
	res      scm.CIResult
	fetches  int
	logCalls []string
}

func (f *fakeCI) PRChecks(_ context.Context, _, _ string, _ int) (scm.CIResult, error) {
	f.fetches++
	return f.res, nil
}

func (f *fakeCI) JobLogTail(_ context.Context, _, _, jobID string, _ int) (string, error) {
	f.logCalls = append(f.logCalls, jobID)
	return "tail-of-" + jobID, nil
}

// --- fixtures --------------------------------------------------------------

const ns = "tatara"

var frozenNow = time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)

type v2Env struct {
	r       *chi.Mux
	c       client.Client
	forge   *recordingForge
	spiller *fakeSpiller
	memory  *fakeMemory
	ci      *fakeCI
	approve *fakeApproval
	now     time.Time
}

type v2Opts struct {
	writer     scm.SCMWriter
	reader     scm.SCMReader
	ci         *fakeCI
	memory     *fakeMemory
	approval   *fakeApproval
	metrics    *obs.OperatorMetrics
	logger     *slog.Logger
	spillerErr error
	now        func() time.Time
	// spillerFor and memoryFor override the default single-project resolver
	// (which always returns env.spiller / opts.memory regardless of the
	// Project passed in). Set these to prove per-project resolution: each
	// entry in the map is keyed by Project name.
	spillerFor func(proj *tatarav1alpha1.Project) objbudget.Spiller
	memoryFor  func(proj *tatarav1alpha1.Project) restapi.NoteFetcher
}

func buildV2(t *testing.T, opts v2Opts, objs ...client.Object) *v2Env {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&tatarav1alpha1.Project{}, &tatarav1alpha1.Repository{},
			&tatarav1alpha1.Task{},
			&tatarav1alpha1.Issue{}, &tatarav1alpha1.MergeRequest{}).
		Build()

	env := &v2Env{c: fc, now: frozenNow}
	env.forge = newRecordingForge()
	env.spiller = &fakeSpiller{err: opts.spillerErr}
	env.memory = opts.memory
	env.ci = opts.ci
	env.approve = opts.approval

	writer := opts.writer
	if writer == nil {
		writer = env.forge
	}
	spillerFor := opts.spillerFor
	if spillerFor == nil {
		spillerFor = func(*tatarav1alpha1.Project) objbudget.Spiller { return env.spiller }
	}
	cfg := restapi.Config{
		Client: fc, Namespace: ns,
		SCMFor:     func(string) (scm.SCMWriter, error) { return writer, nil },
		SpillerFor: spillerFor,
		Now:        func() time.Time { return frozenNow },
	}
	if opts.now != nil {
		cfg.Now = opts.now
	}
	if opts.reader != nil {
		cfg.ReaderFor = func(string, string) (scm.SCMReader, error) { return opts.reader, nil }
	}
	if opts.ci != nil {
		cfg.CIFor = func(string, string) (scm.CIReader, error) { return opts.ci, nil }
	}
	if opts.memoryFor != nil {
		cfg.MemoryFor = opts.memoryFor
	} else if opts.memory != nil {
		cfg.MemoryFor = func(*tatarav1alpha1.Project) restapi.NoteFetcher { return opts.memory }
	}
	if opts.approval != nil {
		cfg.Approval = opts.approval
	}
	if opts.metrics != nil {
		cfg.Metrics = opts.metrics
	}
	if opts.logger != nil {
		cfg.Logger = opts.logger
	}
	s := restapi.NewServer(cfg)
	r := chi.NewRouter()
	s.Mount(r, nil)
	env.r = r
	return env
}

func (e *v2Env) do(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rd *strings.Reader
	if body == "" {
		rd = strings.NewReader("")
	} else {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	w := httptest.NewRecorder()
	e.r.ServeHTTP(w, req)
	return w
}

func (e *v2Env) task(t *testing.T, name string) *tatarav1alpha1.Task {
	t.Helper()
	var out tatarav1alpha1.Task
	require.NoError(t, e.c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &out))
	return &out
}

func (e *v2Env) mr(t *testing.T, name string) *tatarav1alpha1.MergeRequest {
	t.Helper()
	var out tatarav1alpha1.MergeRequest
	require.NoError(t, e.c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &out))
	return &out
}

func (e *v2Env) issue(t *testing.T, name string) *tatarav1alpha1.Issue {
	t.Helper()
	var out tatarav1alpha1.Issue
	require.NoError(t, e.c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &out))
	return &out
}

// project re-reads a Project CR by name, for tests that assert on a status
// field a handler wrote (e.g. Status.BrainstormPausedAt).
func (e *v2Env) project(t *testing.T, name string) *tatarav1alpha1.Project {
	t.Helper()
	var out tatarav1alpha1.Project
	require.NoError(t, e.c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &out))
	return &out
}

func projectV2(name string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "tatara-scm", TriggerLabel: "tatara",
			MaxConcurrentAgents: 3, MaxOpenTasks: 6, MaxBundleBytes: 400000,
			AgentPodTTLSeconds: 3600,
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github", Owner: "acme", BotLogin: "tatara-bot",
			},
			Agent: tatarav1alpha1.AgentSpec{Model: "m"},
		},
	}
}

func scmSecretV2() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "tatara-scm", Namespace: ns},
		Data:       map[string][]byte{"token": []byte("t0ken")},
	}
}

func repoV2(name, projectRef string) *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: projectRef, URL: "https://github.com/acme/" + name, DefaultBranch: "main",
		},
	}
}

// taskV2 builds a Task already in a state, with its agentKind stamped.
func taskV2(name, projectRef, kind, state, agentKind string) *tatarav1alpha1.Task {
	entered := metav1.NewTime(frozenNow.Add(-time.Hour))
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: projectRef, RepositoryRef: "tatara-operator", Kind: kind,
			Goal: "Reaper phase race\n\nfix the reaper",
		},
		Status: tatarav1alpha1.TaskStatus{
			State: state, AgentKind: agentKind, StateEnteredAt: &entered,
		},
	}
}

// parkedTaskV2 is taskV2 plus a park: the fixture shape for a Task that is
// STALLED rather than merely resident in a state (#521: parked/failed are no
// longer states, they are the orthogonal park flag). state is where the Task
// was parked FROM and stays; it must be a live (non-terminal) state.
func parkedTaskV2(t *testing.T, name, projectRef, kind, state, agentKind, parkReason string) *tatarav1alpha1.Task {
	t.Helper()
	tk := taskV2(name, projectRef, kind, state, agentKind)
	if err := stage.Park(tk, parkReason, frozenNow); err != nil {
		t.Fatalf("parkedTaskV2: %v", err)
	}
	return tk
}

// taskBranchV2 is the branch agent.TaskBranch(t) derives for a taskV2
// fixture (no Spec.Source, so it always falls to the "tatara/task-<name>"
// form) - the single place every v2 test computes the expected head branch,
// so a future TaskBranch format change only needs one edit here.
func taskBranchV2(name string) string {
	return agent.TaskBranch(&tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: name}})
}

func ownerRef(task string, controller bool) metav1.OwnerReference {
	tr, fa := true, false
	c := &fa
	if controller {
		c = &tr
	}
	return metav1.OwnerReference{
		APIVersion: tatarav1alpha1.GroupVersion.String(), Kind: "Task",
		Name: task, UID: types.UID("uid-" + task), Controller: c, BlockOwnerDeletion: &tr,
	}
}

func issueV2(repo string, number int, owner string, opts ...func(*tatarav1alpha1.Issue)) *tatarav1alpha1.Issue {
	now := metav1.NewTime(frozenNow.Add(-2 * time.Hour))
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.IssueName(repo, number), Namespace: ns,
			OwnerReferences: []metav1.OwnerReference{ownerRef(owner, true)},
		},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo, Number: number, ProjectRef: "tatara",
			URL: fmt.Sprintf("https://github.com/acme/%s/issues/%d", repo, number),
		},
		Status: tatarav1alpha1.IssueStatus{
			Title: "an issue", Body: "body", Author: "human", State: "open", Status: "new",
			CreatedAt: &now, UpdatedAt: &now, LastSyncedAt: &now,
		},
	}
	for _, o := range opts {
		o(iss)
	}
	return iss
}

func mrV2(repo string, number int, owner string, opts ...func(*tatarav1alpha1.MergeRequest)) *tatarav1alpha1.MergeRequest {
	now := metav1.NewTime(frozenNow.Add(-2 * time.Hour))
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.MergeRequestName(repo, number), Namespace: ns,
			OwnerReferences: []metav1.OwnerReference{ownerRef(owner, true)},
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo, Number: number, ProjectRef: "tatara",
			URL: fmt.Sprintf("https://github.com/acme/%s/pull/%d", repo, number),
		},
		Status: tatarav1alpha1.MergeRequestStatus{
			Title: "an MR", Author: "tatara-bot", State: "open", Status: "new",
			HeadBranch: taskBranchV2(owner), HeadSHA: "sha1", CIStatus: "green",
			CreatedAt: &now, UpdatedAt: &now, LastSyncedAt: &now,
		},
	}
	for _, o := range opts {
		o(mr)
	}
	return mr
}

// --- 6. GET /tasks/{t}/context --------------------------------------------

func TestTaskContext_RendersBundle(t *testing.T) {
	e := buildV2(t, v2Opts{},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
		issueV2("tatara-operator", 291, "t1"),
	)
	w := e.do(t, http.MethodGet, "/tasks/t1/context", "")
	require.Equal(t, http.StatusOK, w.Code)
	var out struct {
		Task, Bundle string
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "t1", out.Task)
	require.Contains(t, out.Bundle, "<task_context")
	require.Contains(t, out.Bundle, "291")
}

func TestTaskContext_Index(t *testing.T) {
	e := buildV2(t, v2Opts{},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StateRefined, "refine"),
		taskV2("t2", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
	)
	w := e.do(t, http.MethodGet, "/tasks/t1/context?index=true", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "t2")
}

// notes=all rehydrates every spilled note from stats.notesSpilledRefs out of
// tatara-memory and renders them IN ORDER. This is the read path the
// <notes ... fetch=...> marker names (fix H10).
func TestTaskContext_NotesAll_RehydratesSpilledNotes(t *testing.T) {
	spilled, err := json.Marshal([]tatarav1alpha1.Note{
		{At: metav1.NewTime(frozenNow.Add(-3 * time.Hour)), Agent: "implement", Kind: "note", Body: "SPILLED-ONE"},
	})
	require.NoError(t, err)

	task := taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateUnderImplementation, "implement")
	task.Status.Notes = []tatarav1alpha1.Note{
		{At: metav1.NewTime(frozenNow), Agent: "implement", Kind: "handoff", Body: "LIVE-NOTE"},
	}
	task.Status.Stats.NotesSpilled = 1
	task.Status.Stats.NotesSpilledRefs = []string{"track-1"}

	e := buildV2(t, v2Opts{memory: &fakeMemory{byTrack: map[string]json.RawMessage{"track-1": spilled}}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	w := e.do(t, http.MethodGet, "/tasks/t1/context?notes=all", "")
	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, "SPILLED-ONE")
	require.Contains(t, body, "LIVE-NOTE")
	require.Less(t, strings.Index(body, "SPILLED-ONE"), strings.Index(body, "LIVE-NOTE"),
		"spilled notes render BEFORE the live ones: notes are an append-only journal")

	// The default read does NOT rehydrate.
	w = e.do(t, http.MethodGet, "/tasks/t1/context", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.NotContains(t, w.Body.String(), "SPILLED-ONE")
}

// TestTaskContext_NotesAll_ServesPartialOnRehydrateFailure: one unresolvable
// spilled ref used to abort the WHOLE response with a 502, so a tatara-memory
// outage made the entire note history unreadable - including the notes sitting
// right there in the CR. A read loses nothing by serving what it has, PROVIDED
// the partiality is unmistakable: the JSON carries notesTruncated plus the
// unavailable-ref count, and the bundle's <notes> marker carries
// unavailable="N" next to its total/rendered/elided.
func TestTaskContext_NotesAll_ServesPartialOnRehydrateFailure(t *testing.T) {
	okBatch, err := json.Marshal([]tatarav1alpha1.Note{
		{At: metav1.NewTime(frozenNow.Add(-3 * time.Hour)), Agent: "implement", Kind: "note", Body: "SPILLED-OK"},
	})
	require.NoError(t, err)

	task := taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateUnderImplementation, "implement")
	task.Status.Notes = []tatarav1alpha1.Note{
		{At: metav1.NewTime(frozenNow), Agent: "implement", Kind: "handoff", Body: "LIVE-NOTE"},
	}
	task.Status.Stats.NotesSpilled = 4
	task.Status.Stats.NotesSpilledRefs = []string{"track-1", "track-gone"}

	// track-gone is absent from the fake, so Fetch fails for it and only for it.
	e := buildV2(t, v2Opts{memory: &fakeMemory{byTrack: map[string]json.RawMessage{"track-1": okBatch}}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	w := e.do(t, http.MethodGet, "/tasks/t1/context?notes=all", "")
	require.Equal(t, http.StatusOK, w.Code, "a partial rehydrate must not 502 the whole read")

	var resp struct {
		Bundle               string `json:"bundle"`
		NotesTruncated       bool   `json:"notesTruncated"`
		NotesUnavailableRefs int    `json:"notesUnavailableRefs"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.True(t, resp.NotesTruncated)
	require.Equal(t, 1, resp.NotesUnavailableRefs)
	require.Contains(t, resp.Bundle, "SPILLED-OK", "the batch that DID resolve is still served")
	require.Contains(t, resp.Bundle, "LIVE-NOTE", "the in-CR notes are served regardless")
	// 4 spilled total, 1 came back: 3 notes are unreadable.
	require.Contains(t, resp.Bundle, `unavailable="3"`,
		"the bundle must say out loud that the history is incomplete")
}

// TestTaskContext_NotesAll_NoMemoryClientServesPartial: an unconfigured
// NoteFetcher is the same class of failure as an unreachable one. It must not
// 502 either; every spilled ref counts as unavailable.
func TestTaskContext_NotesAll_NoMemoryClientServesPartial(t *testing.T) {
	task := taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateUnderImplementation, "implement")
	task.Status.Notes = []tatarav1alpha1.Note{
		{At: metav1.NewTime(frozenNow), Agent: "implement", Kind: "handoff", Body: "LIVE-NOTE"},
	}
	task.Status.Stats.NotesSpilled = 2
	task.Status.Stats.NotesSpilledRefs = []string{"track-1", "track-2"}

	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	w := e.do(t, http.MethodGet, "/tasks/t1/context?notes=all", "")
	require.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Bundle               string `json:"bundle"`
		NotesTruncated       bool   `json:"notesTruncated"`
		NotesUnavailableRefs int    `json:"notesUnavailableRefs"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.True(t, resp.NotesTruncated)
	require.Equal(t, 2, resp.NotesUnavailableRefs)
	require.Contains(t, resp.Bundle, "LIVE-NOTE")
	require.Contains(t, resp.Bundle, `unavailable="2"`)
}

// TestTaskContext_NotesAll_CompleteHistoryIsNotFlaggedTruncated is the negative
// half: notesTruncated must be ABSENT on a fully-resolved read, or the flag is
// noise an agent learns to ignore.
func TestTaskContext_NotesAll_CompleteHistoryIsNotFlaggedTruncated(t *testing.T) {
	spilled, err := json.Marshal([]tatarav1alpha1.Note{
		{At: metav1.NewTime(frozenNow.Add(-time.Hour)), Agent: "implement", Kind: "note", Body: "SPILLED-ONE"},
	})
	require.NoError(t, err)

	task := taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateUnderImplementation, "implement")
	task.Status.Stats.NotesSpilled = 1
	task.Status.Stats.NotesSpilledRefs = []string{"track-1"}

	e := buildV2(t, v2Opts{memory: &fakeMemory{byTrack: map[string]json.RawMessage{"track-1": spilled}}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	w := e.do(t, http.MethodGet, "/tasks/t1/context?notes=all", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.NotContains(t, w.Body.String(), "notesTruncated")
	require.NotContains(t, w.Body.String(), "unavailable=")
}

// --- 7. POST /tasks/{t}/notes ---------------------------------------------

func TestPostNote_StampsAgentFromStatus(t *testing.T) {
	e := buildV2(t, v2Opts{},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateUnderImplementation, "implement"),
	)
	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"handoff","body":"pass it on"}`)
	require.Equal(t, http.StatusCreated, w.Code)

	notes := e.task(t, "t1").Status.Notes
	require.Len(t, notes, 1)
	require.Equal(t, "implement", notes[0].Agent, "the operator stamps agent from status.agentKind")
	require.Equal(t, "handoff", notes[0].Kind)
}

// TestPostNote_ReturnsTheNoteID: the response carries the id the write just
// stamped (NewNoteID(at, kind, body)), because the approval gate requires a
// planNoteId and an agent cannot quote back an id it was never given.
func TestPostNote_ReturnsTheNoteID(t *testing.T) {
	e := buildV2(t, v2Opts{},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateUnderImplementation, "implement"),
	)
	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"plan","body":"the plan"}`)
	require.Equal(t, http.StatusCreated, w.Code)

	var resp struct {
		NoteID string `json:"noteId"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Regexp(t, `^n-[0-9a-f]{16}$`, resp.NoteID)
}

// agent="operator" is UNREACHABLE from this endpoint: an `agent` body key is a
// 400 under DisallowUnknownFields, and an empty status.agentKind is a 409 -
// never a default (fix 19).
func TestPostNote_AgentIsNotABodyKey(t *testing.T) {
	e := buildV2(t, v2Opts{},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateUnderImplementation, "implement"),
	)
	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"note","body":"x","agent":"operator"}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "error")
}

func TestPostNote_NoAgentKindIs409NeverDefaulted(t *testing.T) {
	task := taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateNew, "")
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)
	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"note","body":"x"}`)
	require.Equal(t, http.StatusConflict, w.Code)
	require.Empty(t, e.task(t, "t1").Status.Notes)
}

// A parked Task is terminal for this endpoint (#521 folded the old `failed`
// stage into the park flag): TaskDone(t) || Parked(t) is the postNote gate.
func TestPostNote_TerminalStageIs409(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		parkedTaskV2(t, "t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify", stage.ReasonOperatorError))
	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"note","body":"x"}`)
	require.Equal(t, http.StatusConflict, w.Code)
}

func TestPostNote_BodyTruncatedOnARuneBoundary(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateUnderImplementation, "implement"))

	// 4095 bytes of ASCII plus a 3-byte rune straddles the 4096 cap.
	body := strings.Repeat("a", 4095) + "世"
	payload, err := json.Marshal(map[string]string{"kind": "note", "body": body})
	require.NoError(t, err)
	w := e.do(t, http.MethodPost, "/tasks/t1/notes", string(payload))
	require.Equal(t, http.StatusCreated, w.Code)

	got := e.task(t, "t1").Status.Notes[0].Body
	require.LessOrEqual(t, len(got), 4096)
	require.True(t, json.Valid([]byte(strconvQuote(got))), "body must remain valid UTF-8")
	require.Equal(t, 4095, len(got), "the straddling rune is dropped whole, not cut")
}

func strconvQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// THERE IS NO 409-ON-CAP. At 50 notes the OLDEST is spilled and dropped: an
// agent must ALWAYS be able to write its handoff.
func TestPostNote_At50TheOldestIsSpilledNot409(t *testing.T) {
	task := taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
	for i := 0; i < 50; i++ {
		task.Status.Notes = append(task.Status.Notes, tatarav1alpha1.Note{
			At:    metav1.NewTime(frozenNow.Add(time.Duration(i) * time.Minute)),
			Agent: "implement", Kind: "note", Body: fmt.Sprintf("note-%02d", i),
		})
	}
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"handoff","body":"THE HANDOFF"}`)
	require.Equal(t, http.StatusCreated, w.Code, "an agent must ALWAYS be able to write its handoff")

	got := e.task(t, "t1")
	require.Len(t, got.Status.Notes, 50)
	require.Equal(t, "note-01", got.Status.Notes[0].Body, "the OLDEST note was dropped")
	require.Equal(t, "THE HANDOFF", got.Status.Notes[49].Body)
	require.Equal(t, 1, got.Status.Stats.NotesSpilled)
	require.Equal(t, []string{"track-1"}, got.Status.Stats.NotesSpilledRefs)
	require.Len(t, e.spiller.batches, 1)
}

// TestPostNote_AtCapWithBrokenSpillerIs503 pins the reporting, not the policy.
// The write still FAILS when the oldest note cannot be spilled - that is the
// deliberate A.7 blocking decision, and nothing here drops a note - but a 500
// or a 502 reads as an operator bug and tells the caller to give up, while a
// 503 plus Retry-After says "the dependency is down, come back", which is
// exactly what a tatara-memory outage is.
func TestPostNote_AtCapWithBrokenSpillerIs503(t *testing.T) {
	capped := func() *tatarav1alpha1.Task {
		task := taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
		for i := 0; i < 50; i++ {
			task.Status.Notes = append(task.Status.Notes, tatarav1alpha1.Note{
				At:    metav1.NewTime(frozenNow.Add(time.Duration(i) * time.Minute)),
				Agent: "implement", Kind: "note", Body: fmt.Sprintf("note-%02d", i),
			})
		}
		return task
	}

	tests := []struct {
		name string
		opts v2Opts
	}{
		{name: "spill fails", opts: v2Opts{spillerErr: errors.New("tatara-memory unreachable")}},
		{name: "no spiller configured", opts: v2Opts{
			spillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return nil },
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := capped()
			e := buildV2(t, tc.opts, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

			w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"handoff","body":"THE HANDOFF"}`)
			require.Equal(t, http.StatusServiceUnavailable, w.Code)
			require.Equal(t, "10", w.Header().Get("Retry-After"))

			// The block loses NOTHING: the 50 stored notes are untouched and the
			// new one was not written either.
			got := e.task(t, "t1")
			require.Len(t, got.Status.Notes, 50)
			require.Equal(t, "note-00", got.Status.Notes[0].Body)
			require.Equal(t, 0, got.Status.Stats.NotesSpilled)
		})
	}
}

func TestPostNote_UnknownFieldIs400(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateUnderImplementation, "implement"))
	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"note","body":"x","extra":1}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// --- 9/10/11. the MIRROR reads make ZERO forge requests --------------------

// The implement-profile takeover skill must diff the remote head against the
// LAST BOT-PUSHED head, never the (possibly stale) mirror HeadSHA, before
// pushing on a taken-over MR. lastBotHeadSHA is how that field reaches the
// agent pod.
func TestScmRead_MRs_CarriesLastBotHeadSHAWhenSet(t *testing.T) {
	mr := mrV2("tatara-cli", 80, "t1", func(m *tatarav1alpha1.MergeRequest) {
		m.Status.LastBotHeadSHA = "bot-sha-1"
	})
	e := buildV2(t, v2Opts{},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
		mr,
	)

	w := e.do(t, http.MethodGet, "/projects/tatara/scm/mrs?repo=tatara-cli", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"lastBotHeadSHA":"bot-sha-1"`)
}

func TestScmRead_MRs_OmitsLastBotHeadSHAWhenEmpty(t *testing.T) {
	mr := mrV2("tatara-cli", 80, "t1")
	e := buildV2(t, v2Opts{},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
		mr,
	)

	w := e.do(t, http.MethodGet, "/projects/tatara/scm/mrs?repo=tatara-cli", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.NotContains(t, w.Body.String(), "lastBotHeadSHA")
}

func TestScmRead_Mirror_MakesZeroForgeRequests(t *testing.T) {
	created := metav1.NewTime(frozenNow.Add(-time.Hour))
	iss := issueV2("tatara-operator", 291, "t1", func(i *tatarav1alpha1.Issue) {
		i.Status.Labels = []string{"tatara-approved"}
		i.Status.Comments = []tatarav1alpha1.Comment{
			{ExternalID: "1234567", Author: "human", Body: "Go ahead.", CreatedAt: created},
		}
		i.Status.CommentCount = 1
	})
	mr := mrV2("tatara-cli", 80, "t1")

	// A forge client that PANICS on any use. Reaching it fails the test.
	e := buildV2(t, v2Opts{writer: panicForge{}, reader: panicReader{}},
		projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
		iss, mr,
	)

	w := e.do(t, http.MethodGet, "/projects/tatara/scm/issues?repo=tatara-operator", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"number":291`)
	require.Contains(t, w.Body.String(), `"taskRef":"t1"`)
	require.Contains(t, w.Body.String(), `"lastSyncedAt"`)

	w = e.do(t, http.MethodGet, "/projects/tatara/scm/mrs?repo=tatara-cli", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"number":80`)

	w = e.do(t, http.MethodGet, "/projects/tatara/scm/comments?repo=tatara-operator&number=291", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"externalId":"1234567"`)
	require.Contains(t, w.Body.String(), `"total":1`)
}

func TestScmRead_RepoIsRequiredOnEveryKind(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}, reader: panicReader{}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"))
	for _, path := range []string{
		"/projects/tatara/scm/issues",
		"/projects/tatara/scm/mrs",
		"/projects/tatara/scm/comments?number=1",
		"/projects/tatara/scm/commits",
		"/projects/tatara/scm/ci?number=1",
	} {
		t.Run(path, func(t *testing.T) {
			w := e.do(t, http.MethodGet, path, "")
			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Contains(t, w.Body.String(), "repo required")
		})
	}
}

func TestScmRead_Comments_IsPRReadsTheMRThread(t *testing.T) {
	created := metav1.NewTime(frozenNow)
	mr := mrV2("tatara-cli", 80, "t1", func(m *tatarav1alpha1.MergeRequest) {
		m.Status.Comments = []tatarav1alpha1.Comment{
			{ExternalID: "9", Author: "reviewer", Body: "nit", CreatedAt: created,
				Path: "internal/x.go", Line: 42, InReplyTo: "8"},
		}
		m.Status.CommentCount = 1
	})
	e := buildV2(t, v2Opts{writer: panicForge{}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"), mr)

	// isPR is CAMELCASE on the wire. A snake_case param here is a 400 on every
	// call from the cli.
	w := e.do(t, http.MethodGet, "/projects/tatara/scm/comments?repo=tatara-cli&number=80&isPR=true", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"path":"internal/x.go"`)
	require.Contains(t, w.Body.String(), `"inReplyTo":"8"`)
}

func TestScmRead_Issues_StateAndLabelFilters(t *testing.T) {
	open := issueV2("tatara-operator", 1, "t1")
	closed := issueV2("tatara-operator", 2, "t1", func(i *tatarav1alpha1.Issue) {
		i.Status.State = "closed"
	})
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"), open, closed)

	w := e.do(t, http.MethodGet, "/projects/tatara/scm/issues?repo=tatara-operator", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"number":1`)
	require.NotContains(t, w.Body.String(), `"number":2`)

	w = e.do(t, http.MethodGet, "/projects/tatara/scm/issues?repo=tatara-operator&state=all", "")
	require.Contains(t, w.Body.String(), `"number":2`)
}

// --- 12. commits (camelCase sinceDays) ------------------------------------

type commitReader struct {
	scm.SCMReader
	since time.Time
}

func (c *commitReader) ListCommits(_ context.Context, _, _ string, since time.Time) ([]scm.CommitRef, error) {
	c.since = since
	return []scm.CommitRef{{SHA: "695e96c", Message: "chore: bump", Author: "szymonrychu", Date: frozenNow}}, nil
}

func TestScmRead_Commits_SinceDaysIsCamelCase(t *testing.T) {
	cr := &commitReader{}
	e := buildV2(t, v2Opts{reader: cr}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"))
	w := e.do(t, http.MethodGet, "/projects/tatara/scm/commits?repo=tatara-operator&sinceDays=7", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "695e96c")
	require.WithinDuration(t, frozenNow.AddDate(0, 0, -7), cr.since, time.Second)
}

// --- 13. CI: paced, and logTail ONLY on a failing conclusion ---------------

func TestScmRead_CI_PacedAt20sPerRepoNumber(t *testing.T) {
	ci := &fakeCI{res: scm.CIResult{
		HeadSHA: "abc123", Status: "green", Mergeable: true,
		Checks: []scm.CICheck{{Name: "build", Status: "completed", Conclusion: "success", JobID: "1"}},
	}}
	e := buildV2(t, v2Opts{ci: ci}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-cli", "tatara"))

	w := e.do(t, http.MethodGet, "/projects/tatara/scm/ci?repo=tatara-cli&number=80", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"cached":false`)
	require.Equal(t, 1, ci.fetches)

	// A second call inside 20s is served from the last result and does NOT hit
	// the forge.
	w = e.do(t, http.MethodGet, "/projects/tatara/scm/ci?repo=tatara-cli&number=80", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"cached":true`)
	require.Equal(t, 1, ci.fetches, "a call inside the 20s window must not hit the forge")

	// A different PR is a different pacer key.
	w = e.do(t, http.MethodGet, "/projects/tatara/scm/ci?repo=tatara-cli&number=81", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, 2, ci.fetches)
}

func TestScmRead_CI_PacerExpiresAfter20s(t *testing.T) {
	ci := &fakeCI{res: scm.CIResult{HeadSHA: "abc", Status: "pending"}}
	now := frozenNow
	e := buildV2(t, v2Opts{ci: ci, now: func() time.Time { return now }},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-cli", "tatara"))

	require.Equal(t, http.StatusOK, e.do(t, http.MethodGet, "/projects/tatara/scm/ci?repo=tatara-cli&number=80", "").Code)
	require.Equal(t, 1, ci.fetches)
	now = now.Add(21 * time.Second)
	require.Equal(t, http.StatusOK, e.do(t, http.MethodGet, "/projects/tatara/scm/ci?repo=tatara-cli&number=80", "").Code)
	require.Equal(t, 2, ci.fetches, "past the 20s floor the read goes live again")
}

// logTail is served ONLY for a check whose conclusion is
// failure|timed_out|cancelled. A green run's logs are NEVER fetched.
func TestScmRead_CI_LogTailOnlyForAFailingCheck(t *testing.T) {
	ci := &fakeCI{res: scm.CIResult{
		HeadSHA: "abc123", Status: "red", Mergeable: false,
		Checks: []scm.CICheck{
			{Name: "lint", Status: "completed", Conclusion: "success", JobID: "green-job"},
			{Name: "build", Status: "completed", Conclusion: "failure", JobID: "red-job"},
			{Name: "e2e", Status: "in_progress", JobID: "running-job"},
		},
	}}
	e := buildV2(t, v2Opts{ci: ci}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-cli", "tatara"))

	w := e.do(t, http.MethodGet, "/projects/tatara/scm/ci?repo=tatara-cli&number=80", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, []string{"red-job"}, ci.logCalls, "only the FAILING check's log is fetched")
	require.Contains(t, w.Body.String(), "tail-of-red-job")
}

// --- 14. issue_write -------------------------------------------------------

func TestIssueWrite_Create_IsSynchronousAndReturnsTheNumber(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
		`{"task":"t1","action":"create","repo":"tatara-operator","title":"T","body":"B"}`)
	require.Equal(t, http.StatusOK, w.Code)

	var out struct {
		Number int    `json:"number"`
		URL    string `json:"url"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, 101, out.Number, "the agent NEEDS the number back")
	require.NotEmpty(t, out.URL)

	// The Issue CR exists, controller-owned by the calling Task.
	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	require.Equal(t, "t1", iss.OwnerReferences[0].Name)
	require.True(t, *iss.OwnerReferences[0].Controller)
	require.Contains(t, e.task(t, "t1").Status.IssueRefs, iss.Name)
	require.Empty(t, iss.Spec.ProposalKind,
		"a plain agent-filed issue is not a tatara proposal and must carry no provenance")
}

// The generic issue_write action=create endpoint is callable by ANY authenticated
// agent pod with an agent-supplied body, and mintIssueCR files it under the bot
// account. So body content must never be able to claim proposal provenance: an
// agent processing forge content (a prompt-injection surface) that later files an
// unrelated issue containing the literal marker string would otherwise mint a
// PERMANENT backlog slot, and - via the auto-approve carve-out's anchor - an
// auto-approvable one. Provenance comes from the CALLER, and this caller declares
// none.
func TestIssueWrite_Create_NeverClaimsProposalProvenanceFromTheBody(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"))

	body := tatarav1alpha1.StampProposalMarker("B", tatarav1alpha1.ProposalKindBrainstorm)
	w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
		`{"task":"t1","action":"create","repo":"tatara-operator","title":"T","body":`+
			strconv.Quote(body)+`}`)
	require.Equal(t, http.StatusOK, w.Code)

	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	require.Empty(t, iss.Spec.ProposalKind,
		"an agent-supplied body must never stamp durable proposal provenance")
	require.Empty(t, iss.Spec.ProposalBodyHash,
		"and must never mint the auto-approve integrity anchor either")
	require.Equal(t, tatarav1alpha1.ProposalKindBrainstorm,
		tatarav1alpha1.ProposalKindFromBody(iss.Status.Body),
		"the body is stored verbatim; it is the SPEC that refuses the claim")
}

// TestIssueWrite_Create_TitleIsClamped asserts that the generic issue_write
// action=create endpoint clamps over-long agent-supplied titles to
// IssueTitleMaxChars before filing on the forge. The clamp is applied
// uniformly across both GitHub and GitLab so a title valid on one is valid
// on both.
// The `want` values are written out from the CONTRACT (cap 255, a 14-rune
// marker, so a 241-rune cut) rather than from ClampIssueTitle. Calling the
// function under test to build its own expectation asserts nothing: it holds
// for any implementation, including one that returns the bare marker.
func TestIssueWrite_Create_TitleIsClamped(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		title    string
		want     string
	}{
		{"github_short", "github", "issue" + strings.Repeat("-X", 25),
			"issue" + strings.Repeat("-X", 25)},
		{"github_long", "github", "issue" + strings.Repeat("-X", 150),
			"issue" + strings.Repeat("-X", 118) + "...(truncated)"},
		{"gitlab_short", "gitlab", "issue" + strings.Repeat("-X", 25),
			"issue" + strings.Repeat("-X", 25)},
		{"gitlab_long", "gitlab", "issue" + strings.Repeat("-X", 150),
			"issue" + strings.Repeat("-X", 118) + "...(truncated)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"))
			p := e.project(t, "tatara")
			p.Spec.Scm.Provider = tc.provider
			require.NoError(t, e.c.Update(context.Background(), p))

			w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
				fmt.Sprintf(`{"task":"t1","action":"create","repo":"tatara-operator","title":%s,"body":"B"}`,
					strconv.Quote(tc.title)))
			require.Equal(t, http.StatusOK, w.Code)

			require.Len(t, e.forge.createdReqs, 1)
			require.Equal(t, tc.want, e.forge.createdReqs[0].Title,
				"forge must receive the clamped title, not the raw agent input")
			require.LessOrEqual(t, utf8.RuneCountInString(e.forge.createdReqs[0].Title),
				tatarav1alpha1.IssueTitleMaxChars,
				"clamped title must fit the character limit")

			// Assert the minted Issue CR's Status.Title matches the clamped title
			// (the CR is a mirror of what the forge stored).
			iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
			require.Equal(t, tc.want, iss.Status.Title,
				"Issue CR Status.Title must match the clamped title sent to the forge")
		})
	}
}

// issue_write(action=edit) defers the title through a pendingComments intent
// that reviewpost.go replays into EditIssue, so it hits the SAME forge title cap
// as the create path and has to be clamped in the SAME place. Its failure mode
// is the worse of the two: reviewpost returns on the edit error BEFORE draining
// the intent, so an over-long title requeues that Issue's reconcile forever and
// blocks every intent queued behind it. Once the 20-entry cap fills, further
// writes are dropped while the handler still answers 200.
func TestIssueWrite_Edit_TitleIsClamped(t *testing.T) {
	for _, provider := range []string{"github", "gitlab"} {
		t.Run(provider, func(t *testing.T) {
			title := "retitle" + strings.Repeat("-Z", 150)
			want := "retitle" + strings.Repeat("-Z", 117) + "...(truncated)"
			e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
				issueV2("tatara-operator", 291, "t1"))
			p := e.project(t, "tatara")
			p.Spec.Scm.Provider = provider
			require.NoError(t, e.c.Update(context.Background(), p))

			w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
				fmt.Sprintf(`{"task":"t1","action":"edit","repo":"tatara-operator","number":291,"title":%s}`,
					strconv.Quote(title)))
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())

			pcs := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)).Status.PendingComments
			require.Len(t, pcs, 1)
			require.Contains(t, pcs[0].Body, "title: "+want+"\n",
				"the queued edit intent must carry the clamped title")
			require.NotContains(t, pcs[0].Body, title,
				"the raw over-long title must not survive into the intent, or the replay 400s forever")
		})
	}
}

// action=edit validates title and body with an AND, so a whitespace-only title
// rides in on a legitimate body edit. It is UNDER the cap, so a clamp that
// length-checks before it trims passes it through untouched, parseEditIntent
// reads it back non-empty, and EditIssue is handed a blank title the forge
// rejects - forever, because the drain returns on that error before
// removePendingComments. The intent must carry no title line at all.
func TestIssueWrite_Edit_WhitespaceOnlyTitleIsDroppedFromTheIntent(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
		issueV2("tatara-operator", 291, "t1"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
		`{"task":"t1","action":"edit","repo":"tatara-operator","number":291,"title":"   ","body":"a real body edit"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	pcs := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)).Status.PendingComments
	require.Len(t, pcs, 1)
	require.Contains(t, pcs[0].Body, "title: \n",
		"the title line is always emitted, and a whitespace-only title must render it EMPTY: "+
			"the replay would otherwise send a blank title, 400 on every attempt, and requeue "+
			"that Issue's reconcile forever")
	require.Contains(t, pcs[0].Body, "a real body edit",
		"the body edit it rode in on must still be queued")
}

// The encoder half of the ambiguity parseEditIntent decodes. With the title line
// omitted for an empty title, an agent-supplied body beginning with "title: "
// landed on line 1 and was read back as a title that had never been clamped -
// #529 re-opened through the body field. The title line is now unconditional, so
// the body always starts on line 2.
func TestIssueWrite_Edit_BodyCannotImpersonateTheTitleLine(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
		issueV2("tatara-operator", 291, "t1"))

	// An empty title plus a body whose first line impersonates the encoding.
	smuggled := "title: " + strings.Repeat("z", 400) + "\nreal body"
	w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
		fmt.Sprintf(`{"task":"t1","action":"edit","repo":"tatara-operator","number":291,"body":%s}`,
			strconv.Quote(smuggled)))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	pcs := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)).Status.PendingComments
	require.Len(t, pcs, 1)
	require.True(t, strings.HasPrefix(pcs[0].Body, "<!-- tatara-edit -->\ntitle: \n"),
		"the empty title must still occupy line 1, or the body becomes the title: %q", pcs[0].Body)
}

// The controller-ownership gate on EVERY action that names a number (fix 7).
// It closes the hole where two Tasks that both own an issue could each spawn a
// pod and converse with EACH OTHER on a human's thread.
func TestIssueWrite_OwnershipGate(t *testing.T) {
	// t2 is only a PLAIN owner of the issue t1 controls.
	iss := issueV2("tatara-operator", 291, "t1", func(i *tatarav1alpha1.Issue) {
		i.OwnerReferences = append(i.OwnerReferences, ownerRef("t2", false))
	})
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
		taskV2("t2", "tatara", "refine", tatarav1alpha1.StateRefined, "refine"), iss)

	for _, body := range []string{
		`{"task":"t2","action":"comment","repo":"tatara-operator","number":291,"body":"hi"}`,
		`{"task":"t2","action":"close","repo":"tatara-operator","number":291,"comment":"done"}`,
		`{"task":"t2","action":"edit","repo":"tatara-operator","number":291,"title":"new"}`,
	} {
		w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write", body)
		require.Equal(t, http.StatusConflict, w.Code)
		require.Contains(t, w.Body.String(), "task does not own this issue")
	}
	require.Empty(t, e.issue(t, iss.Name).Status.PendingComments)
}

// edit / close / comment are DEFERRED: a pendingComments[] entry with a
// requestId, and a 200. The forge write and the mirror write cannot be made
// atomic in an HTTP handler.
func TestIssueWrite_CommentIsDeferred(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
		issueV2("tatara-operator", 291, "t1"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
		`{"task":"t1","action":"comment","repo":"tatara-operator","number":291,"body":"on it"}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"deferred":true`)

	pcs := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)).Status.PendingComments
	require.Len(t, pcs, 1)
	require.NotEmpty(t, pcs[0].RequestID)
	require.Equal(t, "comment", pcs[0].Action)
	require.Equal(t, "on it", pcs[0].Body)

	// The requestId is DERIVED, so the retry of an identical call is a no-op.
	w = e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
		`{"task":"t1","action":"comment","repo":"tatara-operator","number":291,"body":"on it"}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Len(t, e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)).Status.PendingComments, 1)
}

func TestIssueWrite_NoStatusAndNoLabelsParam(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
		issueV2("tatara-operator", 291, "t1"))
	for _, body := range []string{
		`{"task":"t1","action":"comment","repo":"tatara-operator","number":291,"body":"x","labels":["tatara"]}`,
		`{"task":"t1","action":"comment","repo":"tatara-operator","number":291,"body":"x","status":"approved"}`,
	} {
		w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write", body)
		require.Equal(t, http.StatusBadRequest, w.Code, "a labels/status key would let an agent self-escalate")
	}
}

// --- 15. mr_write ----------------------------------------------------------

func TestMRWrite_Open_Synchronous(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-cli","title":"T","body":"B"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var out struct {
		Number   int    `json:"number"`
		URL      string `json:"url"`
		Existing bool   `json:"existing"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, 101, out.Number)
	require.False(t, out.Existing)

	mr := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-cli", 101))
	require.Equal(t, taskBranchV2("t1"), mr.Status.HeadBranch)
	require.True(t, *mr.OwnerReferences[0].Controller)
	require.Contains(t, e.task(t, "t1").Status.MRRefs, mr.Name)
}

// TestMRWrite_Open_HeadBranchMatchesAgentTaskBranch pins fix A1: mrOpen must
// derive head from agent.TaskBranch(task), the SAME function that seeds the
// pod's TASK_BRANCH env and what the wrapper actually pushes to - not a
// hand-rolled "task/"+name that never exists on the forge (issue #381 bug A).
func TestMRWrite_Open_HeadBranchMatchesAgentTaskBranch(t *testing.T) {
	numbered := taskV2("t-num", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
	numbered.Spec.Source = &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "https://github.com/acme/tatara-cli/issues/42",
		Number: 42, Title: "Fix the thing",
	}
	docTask := taskV2("t-doc", "tatara", "documentation", tatarav1alpha1.StateUnderImplementation, "documentation")
	docTask.Annotations = map[string]string{tatarav1alpha1.AnnSourceHeadSHA: "deadbeefcafefeed"}
	fallback := taskV2("t-fb", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify")

	for _, tc := range []struct {
		name string
		task *tatarav1alpha1.Task
	}{
		{"numbered source", numbered},
		{"documentation SHA-keyed", docTask},
		{"no source (fallback)", fallback},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-cli", "tatara"), tc.task)

			w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
				fmt.Sprintf(`{"task":%q,"action":"open","repo":"tatara-cli","title":"T","body":"B"}`, tc.task.Name))
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())

			var out struct {
				Number int `json:"number"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
			mr := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-cli", out.Number))
			require.Equal(t, agent.TaskBranch(tc.task), mr.Status.HeadBranch)
		})
	}
}

// TestMRWrite_Open_4xxPassesThrough pins fix A2: a 422 head-invalid from the
// forge (exactly what mrOpen got every time before fix A1) must surface its
// real status and an actionable trimmed message, not a blanket 502 that
// erases the diagnostic (issue #381 bug A).
func TestMRWrite_Open_4xxPassesThrough(t *testing.T) {
	forge := newRecordingForge()
	forge.openChangeErr = &scm.HTTPError{
		Status: http.StatusUnprocessableEntity,
		Path:   "/repos/acme/tatara-cli/pulls",
		Body:   "{\"message\":\"Validation Failed\",\"errors\":[{\"field\":\"head\",\"code\":\"invalid\"}]}",
	}
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-cli","title":"T","body":"B"}`)
	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	// The message must carry the forge's raw body once, not the doubled
	// "scm rejected open (422): scm: <path> -> 422: <body>" that firstLine(he.Error())
	// produced (he.Error() already prefixes "scm: <path> -> <status>: <body>").
	var out struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, `scm rejected open (422): {"message":"Validation Failed","errors":[{"field":"head","code":"invalid"}]}`, out.Error)
}

// TestMRWrite_Open_TransportErrorStays502 pins fix A2's other half: a
// non-HTTPError (network/DNS/timeout - never reached the forge) still 502s,
// but now carries the real error text instead of a generic string.
func TestMRWrite_Open_TransportErrorStays502(t *testing.T) {
	forge := newRecordingForge()
	forge.openChangeErr = fmt.Errorf("dial tcp: connect: connection refused")
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-cli","title":"T","body":"B"}`)
	require.Equal(t, http.StatusBadGateway, w.Code)
	require.Contains(t, w.Body.String(), "connection refused")
}

// IDEMPOTENT (fix 13): an existing open MR on head branch task/<task> returns
// 200 existing=true and does NOT call the forge. A second open 422s on GitHub,
// and a TTL-stopped implement pod that already opened its MR must have a way
// forward.
func TestMRWrite_Open_IsIdempotent(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-cli", 80, "t1"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-cli","title":"T","body":"B"}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"existing":true`)
	require.Contains(t, w.Body.String(), `"number":80`)
}

// IDEMPOTENT for a Task that pushes to a branch it did not derive. mrOpen's
// idempotency key must be the branch the POD PUSHES TO, not the derived task
// branch: the two differ for any Task carrying AnnTakeoverHeadBranch - an
// adopted upgrade Task, and a takeover Task - and with the wrong key the
// handler falls through to a REAL OpenChange against a branch that was never
// pushed, opening a SECOND merge request.
func TestMRWrite_Open_IsIdempotentForATaskPushingToAnAdoptedBranch(t *testing.T) {
	adopted := taskV2("upg-charts-41", "tatara", "upgrade",
		tatarav1alpha1.StateUnderImplementation, "upgrade")
	adopted.Annotations = map[string]string{
		tatarav1alpha1.AnnTakeoverHeadBranch: "renovate/cilium",
	}
	adopted.Spec.Source = &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "https://github.com/acme/charts/pull/41",
		IsPR: true, Number: 41, Title: "Update cilium to v1.18.2",
	}
	bound := mrV2("charts", 41, adopted.Name, func(m *tatarav1alpha1.MergeRequest) {
		m.Status.HeadBranch = "renovate/cilium"
	})
	forge := newRecordingForge()
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("charts", "tatara"), adopted, bound)

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"upg-charts-41","action":"open","repo":"charts","title":"T","body":"B"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"existing":true`)
	require.Contains(t, w.Body.String(), `"number":41`)
	require.Empty(t, forge.openedURLs,
		"mrOpen opened a SECOND merge request from a branch nobody pushed to")
}

// The ordinary case must not change: a Task with no adopted branch still keys
// on its derived task branch, which for a numbered source is not the trivial
// tatara/task-<name> fallback.
func TestMRWrite_Open_StillKeysOnTheTaskBranchForAnOrdinaryTask(t *testing.T) {
	task := taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
	task.Spec.Source = &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "https://github.com/acme/tatara-cli/issues/42",
		Number: 42, Title: "Fix the thing",
	}
	bound := mrV2("tatara-cli", 80, task.Name, func(m *tatarav1alpha1.MergeRequest) {
		m.Status.HeadBranch = agent.TaskBranch(task)
	})
	forge := newRecordingForge()
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"), task, bound)

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-cli","title":"T","body":"B"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"existing":true`)
	require.Contains(t, w.Body.String(), `"number":80`)
	require.Empty(t, forge.openedURLs)
}

// REFUSED 409 when the Task already owns a MERGED MR for that repo (fix 2).
// This is the structural stop on the duplicate-PR path after a partial merge.
func TestMRWrite_Open_RefusedAfterAMerge(t *testing.T) {
	merged := mrV2("tatara-cli", 80, "t1", func(m *tatarav1alpha1.MergeRequest) {
		m.Status.State = "merged"
	})
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"), merged)

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-cli","title":"T","body":"B"}`)
	require.Equal(t, http.StatusConflict, w.Code)
	require.Contains(t, w.Body.String(), "task already merged an MR for this repo")
}

// mr_write has exactly three actions. A hallucinated merge call has nowhere to
// land.
func TestMRWrite_NoMergeNoApproveNoRequestChanges(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-cli", 80, "t1"))
	for _, action := range []string{"merge", "approve", "request_changes"} {
		w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
			fmt.Sprintf(`{"task":"t1","action":%q,"repo":"tatara-cli","number":80,"body":"x"}`, action))
		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Contains(t, w.Body.String(), "action must be one of open, comment, reply")
	}
}

func TestMRWrite_OwnershipGate(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		taskV2("t2", "tatara", "review", tatarav1alpha1.StateAwaitingReview, "review"),
		mrV2("tatara-cli", 80, "t1"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t2","action":"comment","repo":"tatara-cli","number":80,"body":"x"}`)
	require.Equal(t, http.StatusConflict, w.Code)
	require.Contains(t, w.Body.String(), "task does not own this merge request")
}

func TestMRWrite_ReplyIsDeferredAndCarriesInReplyTo(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-cli", 80, "t1"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"reply","repo":"tatara-cli","number":80,"body":"fixed","inReplyTo":"1234560"}`)
	require.Equal(t, http.StatusOK, w.Code)
	pcs := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-cli", 80)).Status.PendingComments
	require.Len(t, pcs, 1)
	require.Equal(t, "reply", pcs[0].Action)
	require.Equal(t, "1234560", pcs[0].InReplyTo)
}

// MIRROR WRITE-BACK, WITH THE SWEEP NEVER RUN. The synchronous writes seed the
// CR's mirror status in the same handler, so an artifact the platform ITSELF
// just created is visible to the NEXT pod immediately - not in an hour, when
// the sweep next runs. (The deferred writes' comment write-back is the Issue /
// MergeRequest reconciler's, by externalId set-union, once it has the forge's
// externalId to union ON - contract C.5.3.)
func TestSynchronousWrites_AreVisibleInTheMirrorWithTheSweepNeverRun(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"))

	require.Equal(t, http.StatusOK, e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
		`{"task":"t1","action":"create","repo":"tatara-operator","title":"just filed","body":"B"}`).Code)
	require.Equal(t, http.StatusOK, e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-cli","title":"just opened","body":"B"}`).Code)

	// No sweep has run. The mirror reads must already see both.
	w := e.do(t, http.MethodGet, "/projects/tatara/scm/issues?repo=tatara-operator", "")
	require.Contains(t, w.Body.String(), "just filed")
	require.Contains(t, w.Body.String(), `"taskRef":"t1"`)

	w = e.do(t, http.MethodGet, "/projects/tatara/scm/mrs?repo=tatara-cli", "")
	require.Contains(t, w.Body.String(), "just opened")
	require.Contains(t, w.Body.String(), fmt.Sprintf(`"headBranch":%q`, taskBranchV2("t1")))
}

// --- decode contract -------------------------------------------------------

func TestEveryV2Body_RejectsAnUnknownKey(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		issueV2("tatara-operator", 291, "t1"), mrV2("tatara-cli", 80, "t1"))

	cases := []struct{ path, body string }{
		{"/tasks/t1/notes", `{"kind":"note","body":"x","nope":1}`},
		{"/tasks/t1/outcome", `{"kind":"implement","payload":{},"nope":1}`},
		{"/tasks/t1/outcome", `{"kind":"implement","payload":{"action":"declined","reason":"r","nope":1}}`},
		{"/projects/tatara/scm/issue-write", `{"task":"t1","action":"comment","repo":"tatara-operator","number":291,"body":"x","nope":1}`},
		{"/projects/tatara/scm/mr-write", `{"task":"t1","action":"comment","repo":"tatara-cli","number":80,"body":"x","nope":1}`},
	}
	for i, c := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			w := e.do(t, http.MethodPost, c.path, c.body)
			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Contains(t, w.Body.String(), `"error"`)
		})
	}
}

// TestIssueWrite_Edit_MultiLineTitleCannotStealTheBody pins the whole queued
// intent, byte for byte, for a title carrying an interior line break.
//
// The intent encoding is line-oriented and its decoder cuts the title at the
// first "\n", so an unflattened title escaped its own line: the text after the
// break came back as the issue BODY and the drain handed it to EditIssue as a
// body replacement, silently overwriting the issue body on the forge with a
// fragment of its own title while the agent was answered 200 {"deferred":true}.
// ClampIssueTitle flattens line breaks, which is why these expectations are
// whole-body literals rather than a "no newline survives" property: the property
// holds for an encoder that drops the body entirely.
func TestIssueWrite_Edit_MultiLineTitleCannotStealTheBody(t *testing.T) {
	cases := []struct {
		name     string
		title    string
		body     string
		wantBody string
	}{
		{
			name:     "LF in the title with a body of its own",
			title:    "Fix crash\nSTATUS: done",
			body:     "real body",
			wantBody: "<!-- tatara-edit -->\ntitle: Fix crash STATUS: done\nreal body",
		},
		{
			// The shape that overwrote the issue body: no body field at all, so
			// everything the decoder found on line 2 came from the title.
			name:     "LF in the title with no body",
			title:    "Fix crash\nSTATUS: done",
			wantBody: "<!-- tatara-edit -->\ntitle: Fix crash STATUS: done\n",
		},
		{
			name:     "CRLF collapses to one space, not two",
			title:    "a\r\nb",
			wantBody: "<!-- tatara-edit -->\ntitle: a b\n",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
				issueV2("tatara-operator", 291, "t1"))

			req := fmt.Sprintf(`{"task":"t1","action":"edit","repo":"tatara-operator","number":291,"title":%s`,
				strconv.Quote(tc.title))
			if tc.body != "" {
				req += `,"body":` + strconv.Quote(tc.body)
			}
			req += "}"

			w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write", req)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())

			pcs := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)).Status.PendingComments
			require.Len(t, pcs, 1)
			require.Equal(t, tc.wantBody, pcs[0].Body)
		})
	}
}

// TestIssueWrite_Edit_IdempotencyKeyCoversTheTitle asserts that the requestId
// (idempotency key) for action=edit must include the title, not just the body.
// Without the title in the key, two edit requests with the same body but
// different titles collide: the second is silently dropped while the handler
// answers 200 {"deferred":true}, so the title change never reaches the forge.
func TestIssueWrite_Edit_IdempotencyKeyCoversTheTitle(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, e *v2Env)
	}{
		{
			name: "different_titles_same_body_create_distinct_entries",
			run: func(t *testing.T, e *v2Env) {
				// First edit: title="first title", body="same body"
				w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
					`{"task":"t1","action":"edit","repo":"tatara-operator","number":291,"title":"first title","body":"same body"}`)
				require.Equal(t, http.StatusOK, w.Code)

				// Second edit: title="SECOND title", body="same body"
				w = e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
					`{"task":"t1","action":"edit","repo":"tatara-operator","number":291,"title":"SECOND title","body":"same body"}`)
				require.Equal(t, http.StatusOK, w.Code)

				pcs := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)).Status.PendingComments
				require.Len(t, pcs, 2,
					"two edit requests with different titles but same body must create TWO pending comments, not collide")

				// Verify RequestIDs are distinct
				require.NotEqual(t, pcs[0].RequestID, pcs[1].RequestID,
					"distinct title+body combinations must have distinct requestIds")

				// Verify the second pending comment carries the second title
				require.Contains(t, pcs[1].Body, "title: SECOND title",
					"second edit's title must be in the queued intent")
			},
		},
		{
			name: "different_titles_no_body_create_distinct_entries",
			run: func(t *testing.T, e *v2Env) {
				// First title-only edit
				w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
					`{"task":"t1","action":"edit","repo":"tatara-operator","number":291,"title":"first"}`)
				require.Equal(t, http.StatusOK, w.Code)

				// Second title-only edit with different title
				w = e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
					`{"task":"t1","action":"edit","repo":"tatara-operator","number":291,"title":"second"}`)
				require.Equal(t, http.StatusOK, w.Code)

				pcs := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)).Status.PendingComments
				require.Len(t, pcs, 2,
					"two title-only edits with different titles must create distinct pending comments (this is the worse case for key collision)")

				require.NotEqual(t, pcs[0].RequestID, pcs[1].RequestID,
					"distinct titles must have distinct requestIds")
			},
		},
		{
			name: "identical_posts_still_deduplicate",
			run: func(t *testing.T, e *v2Env) {
				// First request
				w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
					`{"task":"t1","action":"edit","repo":"tatara-operator","number":291,"title":"new title","body":"new body"}`)
				require.Equal(t, http.StatusOK, w.Code)

				// Identical retry
				w = e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
					`{"task":"t1","action":"edit","repo":"tatara-operator","number":291,"title":"new title","body":"new body"}`)
				require.Equal(t, http.StatusOK, w.Code)

				pcs := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)).Status.PendingComments
				require.Len(t, pcs, 1,
					"byte-identical retry must still deduplicate (requestId logic must still work)")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
				issueV2("tatara-operator", 291, "t1"))
			tc.run(t, e)
		})
	}
}

// wantRequestID re-derives a deferred write's idempotency key from its
// documented formula - sha256 of the key parts joined by "|" - rather than
// calling the unexported newRequestID. That is deliberate: a test that asks the
// implementation what it computes cannot catch a part being added to, dropped
// from, or reordered in the key.
func wantRequestID(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return fmt.Sprintf("%x", sum)
}

// TestIssueWrite_KeyCompositionPerAction pins the idempotency key of all three
// deferred actions against the formula, not against itself.
//
// Only action=edit folds the title in, and it folds it in by APPENDING. Comment
// and close must hash the same three parts they always did, byte for byte, so a
// retry spanning this upgrade still dedups - a duplicate EditIssue is a PATCH to
// a value the forge already holds, a duplicate comment is a second comment on a
// human's thread. Asserting "the same request twice yields the same key" would
// not catch a title spliced into the comment key; asserting the composition does.
func TestIssueWrite_KeyCompositionPerAction(t *testing.T) {
	const issueNumber = 291
	name := tatarav1alpha1.IssueName("tatara-operator", issueNumber)
	cases := []struct {
		name    string
		req     string
		wantKey string
	}{
		{
			name:    "comment keys on the body alone",
			req:     `{"task":"t1","action":"comment","repo":"tatara-operator","number":291,"body":"a comment"}`,
			wantKey: wantRequestID("t1", "comment", name, "a comment"),
		},
		{
			name:    "close keys on the reason alone",
			req:     `{"task":"t1","action":"close","repo":"tatara-operator","number":291,"comment":"a reason"}`,
			wantKey: wantRequestID("t1", "close", name, "a reason"),
		},
		{
			name:    "edit appends the clamped title after the body",
			req:     `{"task":"t1","action":"edit","repo":"tatara-operator","number":291,"title":"a title","body":"a body"}`,
			wantKey: wantRequestID("t1", "edit", name, "a body", "a title"),
		},
		{
			// A title-only edit still keys on the title: with no body the first four
			// parts are identical for every retitle of this issue.
			name:    "title-only edit keys on the title with an empty body part",
			req:     `{"task":"t1","action":"edit","repo":"tatara-operator","number":291,"title":"a title"}`,
			wantKey: wantRequestID("t1", "edit", name, "", "a title"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
				issueV2("tatara-operator", issueNumber, "t1"))

			w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write", tc.req)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())

			var resp struct {
				RequestID string `json:"requestId"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			require.Equal(t, tc.wantKey, resp.RequestID)

			pcs := e.issue(t, name).Status.PendingComments
			require.Len(t, pcs, 1)
			require.Equal(t, tc.wantKey, pcs[0].RequestID,
				"the queued intent must carry the key the caller was handed")
		})
	}
}

// TestIssueWrite_Create_ClampEngagingIsObservable asserts that when the generic
// issue_write(action=create) path clamps an over-long title, the clamp event
// is observable via metrics and logs.
//
// The counter obs.RestTitleClampedTotal with label "issue_create" must
// increment by 1, and an INFO log line with message "restapi: issue title clamped"
// must be emitted carrying "site":"issue_create".
//
// This test is expected to have a COMPILE ERROR on obs.RestTitleClampedTotal
// because that counter does not exist yet.
func TestIssueWrite_Create_ClampEngagingIsObservable(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	e := buildV2(t, v2Opts{metrics: metrics, logger: logger}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"))

	longTitle := "issue" + strings.Repeat("-X", 150)
	before := testutil.ToFloat64(obs.RestTitleClampedTotal.WithLabelValues("issue_create"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
		fmt.Sprintf(`{"task":"t1","action":"create","repo":"tatara-operator","title":%s,"body":"B"}`,
			strconv.Quote(longTitle)))
	require.Equal(t, http.StatusOK, w.Code)

	after := testutil.ToFloat64(obs.RestTitleClampedTotal.WithLabelValues("issue_create"))
	require.Equal(t, before+1, after,
		"clamping an over-long title in issue_create must increment the RestTitleClampedTotal counter")

	logOutput := logBuf.String()
	require.Contains(t, logOutput, `"msg":"restapi: issue title clamped"`,
		"an INFO log line must be emitted when the title is clamped")
	require.Contains(t, logOutput, `"site":"issue_create"`,
		"the log line must carry the site label indicating this is an issue_create clamp")
}

func TestV2Body_OversizeIs413(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateUnderImplementation, "implement"))
	big := strings.Repeat("a", (1<<20)+10)
	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"note","body":"`+big+`"}`)
	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
}
