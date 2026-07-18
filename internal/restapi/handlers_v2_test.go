package restapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
	"github.com/szymonrychu/tatara-operator/internal/scm"
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
	f.nextNumber++
	ref := fmt.Sprintf("acme/%s#%d", lastSeg(repoURL), f.nextNumber)
	f.createdRefs = append(f.createdRefs, ref)
	f.createdReqs = append(f.createdReqs, req)
	return scm.CreatedIssue{Ref: ref, URL: "https://forge/issues/" + fmt.Sprint(f.nextNumber)}, nil
}

func (f *recordingForge) OpenChange(_ context.Context, _, _, _, _, _, body string) (string, error) {
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
}

func (f *fakeApproval) VerifyApproval(_ context.Context, _ *tatarav1alpha1.Project,
	iss *tatarav1alpha1.Issue) (*tatarav1alpha1.ApprovalEvidence, bool) {
	if !f.grant[iss.Name] {
		return nil, false
	}
	if f.auto {
		return &tatarav1alpha1.ApprovalEvidence{
			Auto: true, Login: tatarav1alpha1.AutoApproveLogin,
			CreatedAt: metav1.NewTime(time.Unix(0, 0)),
		}, true
	}
	return &tatarav1alpha1.ApprovalEvidence{
		Login: "maintainer", CommentID: "1", Phrase: "go ahead",
		CreatedAt: metav1.NewTime(time.Unix(0, 0)),
	}, true
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
	cfg := restapi.Config{
		Client: fc, Namespace: ns,
		SCMFor:  func(string) (scm.SCMWriter, error) { return writer, nil },
		Spiller: env.spiller,
		Now:     func() time.Time { return frozenNow },
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
	if opts.memory != nil {
		cfg.Memory = opts.memory
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
			Agent: tatarav1alpha1.AgentSpec{
				Model: "m", MaxTurnsPerPod: 40, MaxTurnsPerTask: 300,
				MaxReviewRounds: 3, MaxPodRecreations: 3,
			},
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

// taskV2 builds a Task already in a stage, with its agentKind stamped.
func taskV2(name, projectRef, kind, stg, agentKind string) *tatarav1alpha1.Task {
	entered := metav1.NewTime(frozenNow.Add(-time.Hour))
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: projectRef, RepositoryRef: "tatara-operator", Kind: kind,
			Goal: "Reaper phase race\n\nfix the reaper",
		},
		Status: tatarav1alpha1.TaskStatus{
			Stage: stg, AgentKind: agentKind, StageEnteredAt: &entered,
		},
	}
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
			HeadBranch: "task/" + owner, HeadSHA: "sha1", CIStatus: "green",
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
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"),
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
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StageRefining, "refine"),
		taskV2("t2", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"),
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

	task := taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageImplementing, "implement")
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

// --- 7. POST /tasks/{t}/notes ---------------------------------------------

func TestPostNote_StampsAgentFromStatus(t *testing.T) {
	e := buildV2(t, v2Opts{},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageImplementing, "implement"),
	)
	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"handoff","body":"pass it on"}`)
	require.Equal(t, http.StatusCreated, w.Code)

	notes := e.task(t, "t1").Status.Notes
	require.Len(t, notes, 1)
	require.Equal(t, "implement", notes[0].Agent, "the operator stamps agent from status.agentKind")
	require.Equal(t, "handoff", notes[0].Kind)
}

// agent="operator" is UNREACHABLE from this endpoint: an `agent` body key is a
// 400 under DisallowUnknownFields, and an empty status.agentKind is a 409 -
// never a default (fix 19).
func TestPostNote_AgentIsNotABodyKey(t *testing.T) {
	e := buildV2(t, v2Opts{},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageImplementing, "implement"),
	)
	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"note","body":"x","agent":"operator"}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "error")
}

func TestPostNote_NoAgentKindIs409NeverDefaulted(t *testing.T) {
	task := taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageTriaging, "")
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)
	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"note","body":"x"}`)
	require.Equal(t, http.StatusConflict, w.Code)
	require.Empty(t, e.task(t, "t1").Status.Notes)
}

func TestPostNote_TerminalStageIs409(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageFailed, "clarify"))
	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"note","body":"x"}`)
	require.Equal(t, http.StatusConflict, w.Code)
}

func TestPostNote_BodyTruncatedOnARuneBoundary(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageImplementing, "implement"))

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
	task := taskV2("t1", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement")
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

func TestPostNote_UnknownFieldIs400(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageImplementing, "implement"))
	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"note","body":"x","extra":1}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// --- 9/10/11. the MIRROR reads make ZERO forge requests --------------------

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
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"),
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
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageReviewing, "review"), mr)

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
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"), open, closed)

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
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"))

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
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"),
		taskV2("t2", "tatara", "refine", tatarav1alpha1.StageRefining, "refine"), iss)

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
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"),
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
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"),
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
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement"))

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
	require.Equal(t, "task/t1", mr.Status.HeadBranch)
	require.True(t, *mr.OwnerReferences[0].Controller)
	require.Contains(t, e.task(t, "t1").Status.MRRefs, mr.Name)
}

// IDEMPOTENT (fix 13): an existing open MR on head branch task/<task> returns
// 200 existing=true and does NOT call the forge. A second open 422s on GitHub,
// and a TTL-stopped implement pod that already opened its MR must have a way
// forward.
func TestMRWrite_Open_IsIdempotent(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement"),
		mrV2("tatara-cli", 80, "t1"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-cli","title":"T","body":"B"}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"existing":true`)
	require.Contains(t, w.Body.String(), `"number":80`)
}

// REFUSED 409 when the Task already owns a MERGED MR for that repo (fix 2).
// This is the structural stop on the duplicate-PR path after a partial merge.
func TestMRWrite_Open_RefusedAfterAMerge(t *testing.T) {
	merged := mrV2("tatara-cli", 80, "t1", func(m *tatarav1alpha1.MergeRequest) {
		m.Status.State = "merged"
	})
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement"), merged)

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
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement"),
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
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement"),
		taskV2("t2", "tatara", "review", tatarav1alpha1.StageReviewing, "review"),
		mrV2("tatara-cli", 80, "t1"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t2","action":"comment","repo":"tatara-cli","number":80,"body":"x"}`)
	require.Equal(t, http.StatusConflict, w.Code)
	require.Contains(t, w.Body.String(), "task does not own this merge request")
}

func TestMRWrite_ReplyIsDeferredAndCarriesInReplyTo(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement"),
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
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"))

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
	require.Contains(t, w.Body.String(), `"headBranch":"task/t1"`)
}

// --- decode contract -------------------------------------------------------

func TestEveryV2Body_RejectsAnUnknownKey(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement"),
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

func TestV2Body_OversizeIs413(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageImplementing, "implement"))
	big := strings.Repeat("a", (1<<20)+10)
	w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"note","body":"`+big+`"}`)
	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
}
