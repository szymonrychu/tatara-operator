package restapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/prometheus/client_golang/prometheus/testutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// takeoverNS / takeoverFrozenNow are this file's own namespace/clock fixtures:
// handlers_v2_test.go's ns/frozenNow live in package restapi_test, which this
// internal test file (package restapi, needed for takeoverTaskName/mrTakeover
// visibility) cannot see.
const takeoverNS = "tatara"

var takeoverFrozenNow = time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)

// ownerControllerName is own.ControllerOwner, named for readability at a test
// assertion's call site (mirrors internal/controller/takeover_mint_test.go's
// identically-named helper - this package cannot import that one).
func ownerControllerName(obj client.Object) (string, bool) {
	return own.ControllerOwner(obj)
}

// isPlainOwner reports whether name owns obj as a Task ref with the
// controller flag clear (present but not controller=true).
func isPlainOwner(obj client.Object, name string) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Kind != "Task" || ref.Name != name {
			continue
		}
		return ref.Controller == nil || !*ref.Controller
	}
	return false
}

// takeoverHarness is the self-contained fixture TestMRTakeover_* builds on: a
// Project + Repository + review Task that already controller-owns an open,
// external MergeRequest #9 mirroring one maintainer comment (extId "10",
// author "alice"). It is deliberately NOT shared with handlers_v2_test.go's
// v2Env (that lives in package restapi_test and cannot see this package's
// unexported takeoverTaskName/mrTakeover).
type takeoverHarness struct {
	t        *testing.T
	c        client.Client
	r        *chi.Mux
	proj     *tatarav1alpha1.Project
	repo     *tatarav1alpha1.Repository
	botLogin string
}

// takeoverHarnessOpts configures the parts of newTakeoverHarness that most
// tests don't need: an injectable SCM writer (FIX 2's live-head read) and a
// client interceptor (FIX 3's failure-injection tests). Both are wired in
// AFTER the fixture's own setup writes so intercepted/stubbed behavior only
// ever affects the request under test, never the harness building itself.
type takeoverHarnessOpts struct {
	scmWriter   scm.SCMWriter
	interceptor interceptor.Funcs
}

func newTakeoverHarness(t *testing.T) *takeoverHarness {
	return newTakeoverHarnessWithOpts(t, takeoverHarnessOpts{})
}

func newTakeoverHarnessWithOpts(t *testing.T, opts takeoverHarnessOpts) *takeoverHarness {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := tatarav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&tatarav1alpha1.Project{}, &tatarav1alpha1.Repository{},
			&tatarav1alpha1.Task{}, &tatarav1alpha1.MergeRequest{}).
		WithInterceptorFuncs(opts.interceptor).
		Build()

	h := &takeoverHarness{t: t, c: fc, botLogin: "tatara-bot"}
	ctx := context.Background()

	h.proj = &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "proj-a", Namespace: takeoverNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "tatara-scm",
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github", Owner: "acme", BotLogin: h.botLogin,
				// "alice" is a MAINTAINER (takeover is a privilege grant, gated
				// on IsMaintainer). "bob" is deliberately reporter-only: listed
				// for intake trust but NOT for the takeover grant - see
				// TestMRTakeover_RejectsReporterOnlyAuthor.
				MaintainerLogins: []string{"alice"},
				ReporterLogins:   []string{"bob"},
			},
		},
	}
	if err := fc.Create(ctx, h.proj); err != nil {
		t.Fatalf("create project: %v", err)
	}

	h.repo = &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "repo-a", Namespace: takeoverNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: h.proj.Name, URL: "https://github.com/acme/repo-a.git", DefaultBranch: "main",
		},
	}
	if err := fc.Create(ctx, h.repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}

	h.ensureTask(t, "review-task")

	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.MergeRequestName(h.repo.Name, 9), Namespace: takeoverNS,
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: h.repo.Name, ProjectRef: h.proj.Name, Number: 9,
			URL: "https://github.com/acme/repo-a/pull/9",
		},
	}
	own.AddPlainOwner(mr, h.task(t, "review-task"))
	if err := own.HandOverController(mr, nil, h.task(t, "review-task")); err != nil {
		t.Fatalf("handover: %v", err)
	}
	if err := fc.Create(ctx, mr); err != nil {
		t.Fatalf("create mr: %v", err)
	}
	mr.Status = tatarav1alpha1.MergeRequestStatus{
		Title: "external change", Author: "octocat", State: "open",
		HeadBranch: "octocat/patch-1", HeadSHA: "sha-9",
		Ownership: tatarav1alpha1.OwnershipExternal, OwnershipReason: "initial",
	}
	if err := fc.Status().Update(ctx, mr); err != nil {
		t.Fatalf("seed mr status: %v", err)
	}
	h.addComment(t, 9, "10", "alice", false)

	// The scm secret backing ScmSecretRef is always seeded (harmless when no
	// scmWriter is configured: scmFor stays nil below, so projectSCMWriterAndToken
	// never resolves the secret's token in the first place).
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: h.proj.Spec.ScmSecretRef, Namespace: takeoverNS},
		Data:       map[string][]byte{"token": []byte("tok")},
	}
	if err := fc.Create(ctx, secret); err != nil {
		t.Fatalf("create scm secret: %v", err)
	}

	var scmFor func(string) (scm.SCMWriter, error)
	if opts.scmWriter != nil {
		w := opts.scmWriter
		scmFor = func(string) (scm.SCMWriter, error) { return w, nil }
	}

	// SpillerFor is deliberately left unset: these fixtures never approach the
	// A.7 byte budget, so objbudget never calls Spill, and spillerForOrNil
	// already handles a nil resolver.
	s := NewServer(Config{
		Client: fc, Namespace: takeoverNS,
		Minter: &controller.Minter{Client: fc, APIReader: fc, Scheme: scheme},
		Now:    func() time.Time { return takeoverFrozenNow },
		SCMFor: scmFor,
	})
	r := chi.NewRouter()
	s.Mount(r, nil)
	h.r = r
	return h
}

// seedParkedTakeoverTask creates the deterministic-named takeover Task for
// (h.repo, number) already parked(ownership-lost) - the "re-take" shape
// MintOrUnparkTakeoverTask's UNPARK branch expects (see
// internal/controller/takeover_mint_test.go's identically-purposed
// TestMintOrUnparkTakeoverTask_UnparksExisting). Unlike a fresh mint, the
// unpark branch never touches the MR's owner refs - it only re-enters the
// Task's OWN stage - so tests that need to isolate takeover.go's OWN
// owner-ref-move / ownership-stamp writes from MintOrUnparkTakeoverTask's
// internal bindMRToTask writes (which a fresh mint also performs) seed this
// first.
func (h *takeoverHarness) seedParkedTakeoverTask(t *testing.T, number int) {
	t.Helper()
	ctx := context.Background()
	name := takeoverTaskName(h.proj, h.repo, number)
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: takeoverNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: h.proj.Name, RepositoryRef: h.repo.Name, Kind: "takeover",
			Goal: "take over", MergeOrder: []string{h.repo.Name},
		},
	}
	if err := h.c.Create(ctx, task); err != nil {
		t.Fatalf("create parked takeover task: %v", err)
	}
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonOwnershipLost
	if err := h.c.Status().Update(ctx, task); err != nil {
		t.Fatalf("park takeover task: %v", err)
	}
}

// ensureTask creates a minimal live Task CR named name if it does not already
// exist (idempotent - postAs calls it for a task the harness never seeded).
func (h *takeoverHarness) ensureTask(t *testing.T, name string) {
	t.Helper()
	var existing tatarav1alpha1.Task
	err := h.c.Get(context.Background(), types.NamespacedName{Namespace: takeoverNS, Name: name}, &existing)
	if err == nil {
		return
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: takeoverNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj-a", RepositoryRef: "repo-a", Kind: "review", Goal: "review it"},
	}
	if err := h.c.Create(context.Background(), task); err != nil {
		t.Fatalf("create task %s: %v", name, err)
	}
}

func (h *takeoverHarness) task(t *testing.T, name string) *tatarav1alpha1.Task {
	t.Helper()
	var task tatarav1alpha1.Task
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: takeoverNS, Name: name}, &task); err != nil {
		t.Fatalf("get task %s: %v", name, err)
	}
	return &task
}

// addComment appends a mirrored comment to MR number's Status.Comments.
func (h *takeoverHarness) addComment(t *testing.T, number int, extID, author string, isBot bool) {
	t.Helper()
	ctx := context.Background()
	key := types.NamespacedName{Namespace: takeoverNS, Name: tatarav1alpha1.MergeRequestName(h.repo.Name, number)}
	var mr tatarav1alpha1.MergeRequest
	if err := h.c.Get(ctx, key, &mr); err != nil {
		t.Fatalf("get mr %d: %v", number, err)
	}
	mr.Status.Comments = append(mr.Status.Comments, tatarav1alpha1.Comment{
		ExternalID: extID, Author: author, Body: "take over please", IsBot: isBot,
		CreatedAt: metav1.NewTime(takeoverFrozenNow),
	})
	if err := h.c.Status().Update(ctx, &mr); err != nil {
		t.Fatalf("append comment: %v", err)
	}
}

func (h *takeoverHarness) getMR(t *testing.T, number int) *tatarav1alpha1.MergeRequest {
	t.Helper()
	var mr tatarav1alpha1.MergeRequest
	key := types.NamespacedName{Namespace: takeoverNS, Name: tatarav1alpha1.MergeRequestName(h.repo.Name, number)}
	if err := h.c.Get(context.Background(), key, &mr); err != nil {
		t.Fatalf("get mr %d: %v", number, err)
	}
	return &mr
}

func (h *takeoverHarness) post(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/projects/"+h.proj.Name+"/scm/mr-takeover", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.r.ServeHTTP(w, req)
	return w
}

func (h *takeoverHarness) postAs(t *testing.T, taskName, body string) *httptest.ResponseRecorder {
	t.Helper()
	h.ensureTask(t, taskName)
	return h.post(t, body)
}

// --- tests ------------------------------------------------------------

func TestMRTakeover_RejectsCommentNotInMirror(t *testing.T) {
	h := newTakeoverHarness(t) // review task controller-owns an external MR with one mirrored maintainer comment (extId "10", author "alice")
	rr := h.post(t, `{"repo":"repo-a","number":9,"commentExternalId":"999","task":"review-task"}`)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("unknown comment id must be rejected, got %d", rr.Code)
	}
	if h.getMR(t, 9).Status.Ownership == "tatara" {
		t.Fatalf("no flip on rejection")
	}
}

func TestMRTakeover_RejectsNonMaintainerAuthor(t *testing.T) {
	h := newTakeoverHarness(t)
	h.addComment(t, 9, "11", "randocontributor", false) // not in reporter/maintainer lists
	rr := h.post(t, `{"repo":"repo-a","number":9,"commentExternalId":"11","task":"review-task"}`)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("non-maintainer author must be forbidden, got %d", rr.Code)
	}
}

// TestMRTakeover_RejectsReporterOnlyAuthor is the regression test for the
// authz gap flagged in whole-branch review: takeover grants the bot
// push+merge agency over a human's own MR - a privilege grant - and must gate
// on IsMaintainer, not the weaker IsTrustedAuthor (which also accepts a
// listed REPORTER). "bob" is reporter-listed but not a maintainer (see
// newTakeoverHarness); before the fix this comment would have been ACCEPTED.
func TestMRTakeover_RejectsReporterOnlyAuthor(t *testing.T) {
	h := newTakeoverHarness(t)
	h.addComment(t, 9, "13", "bob", false) // reporter-listed, NOT a maintainer
	rr := h.post(t, `{"repo":"repo-a","number":9,"commentExternalId":"13","task":"review-task"}`)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("reporter-only author must be forbidden (maintainer-only grant), got %d", rr.Code)
	}
	if h.getMR(t, 9).Status.Ownership == "tatara" {
		t.Fatalf("no flip on rejection")
	}
}

func TestMRTakeover_RejectsBotAuthoredComment(t *testing.T) {
	h := newTakeoverHarness(t)
	h.addComment(t, 9, "12", h.botLogin, true) // IsBot
	rr := h.post(t, `{"repo":"repo-a","number":9,"commentExternalId":"12","task":"review-task"}`)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("bot-authored comment must be forbidden, got %d", rr.Code)
	}
}

func TestMRTakeover_RejectsCallerNotOwningMR(t *testing.T) {
	h := newTakeoverHarness(t)
	rr := h.postAs(t, "some-other-task", `{"repo":"repo-a","number":9,"commentExternalId":"10","task":"some-other-task"}`)
	if rr.Code != http.StatusConflict {
		t.Fatalf("caller not owning the MR must 409, got %d", rr.Code)
	}
}

func TestMRTakeover_FlipsAndMovesOwnerRef(t *testing.T) {
	h := newTakeoverHarness(t) // maintainer comment extId "10" author "alice" already mirrored
	before := testutil.ToFloat64(obs.OwnershipFlipCounter("to-tatara", "takeover"))
	rr := h.post(t, `{"repo":"repo-a","number":9,"commentExternalId":"10","task":"review-task"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("valid takeover must 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if after := testutil.ToFloat64(obs.OwnershipFlipCounter("to-tatara", "takeover")); after != before+1 {
		t.Fatalf("operator_mr_ownership_flip_total{to-tatara,takeover} = %v, want %v", after, before+1)
	}
	mr := h.getMR(t, 9)
	if mr.Status.Ownership != "tatara" || mr.Status.OwnershipReason != "takeover-requested-by:alice" {
		t.Fatalf("flip not recorded: %q/%q", mr.Status.Ownership, mr.Status.OwnershipReason)
	}
	ctrl, ok := ownerControllerName(mr)
	if !ok || ctrl != takeoverTaskName(h.proj, h.repo, 9) {
		t.Fatalf("controller owner must be the takeover Task, got %q", ctrl)
	}
	// The review Task remains a (plain) owner.
	if !isPlainOwner(mr, "review-task") {
		t.Fatalf("review Task must survive as a plain owner")
	}
}

func TestMRTakeover_RejectsMRNotOpen(t *testing.T) {
	h := newTakeoverHarness(t)
	mr := h.getMR(t, 9)
	mr.Status.State = "merged"
	if err := h.c.Status().Update(context.Background(), mr); err != nil {
		t.Fatalf("close mr: %v", err)
	}
	rr := h.post(t, `{"repo":"repo-a","number":9,"commentExternalId":"10","task":"review-task"}`)
	if rr.Code != http.StatusConflict {
		t.Fatalf("takeover on a non-open MR must 409, got %d", rr.Code)
	}
	if h.getMR(t, 9).Status.Ownership == "tatara" {
		t.Fatalf("no flip on rejection")
	}
}

func TestMRTakeover_AlreadyTataraIsIdempotent(t *testing.T) {
	h := newTakeoverHarness(t)
	mr := h.getMR(t, 9)
	mr.Status.Ownership = tatarav1alpha1.OwnershipTatara
	mr.Status.OwnershipReason = "initial"
	if err := h.c.Status().Update(context.Background(), mr); err != nil {
		t.Fatalf("seed already-tatara: %v", err)
	}
	before := testutil.ToFloat64(obs.OwnershipFlipCounter("to-tatara", "takeover"))
	rr := h.post(t, `{"repo":"repo-a","number":9,"commentExternalId":"10","task":"review-task"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("already-tatara takeover must be idempotently OK, got %d: %s", rr.Code, rr.Body.String())
	}
	if after := testutil.ToFloat64(obs.OwnershipFlipCounter("to-tatara", "takeover")); after != before {
		t.Fatalf("idempotent no-op must not increment the flip metric: before=%v after=%v", before, after)
	}
	got := h.getMR(t, 9)
	if got.Status.OwnershipReason != "initial" {
		t.Fatalf("idempotent no-op must not rewrite the ownership reason, got %q", got.Status.OwnershipReason)
	}
	ctrl, ok := ownerControllerName(got)
	if !ok || ctrl != "review-task" {
		t.Fatalf("idempotent no-op must not move controller ownership, got %q", ctrl)
	}
}

// TestMRTakeover_SurvivesNextReconcileOwnership is the OP9->OP8 crossing
// regression test flagged in whole-branch review: the endpoint alone cannot
// see the bug because it never calls ReconcileOwnership, and OP8's own tests
// (internal/controller/ownership_test.go) pre-seed LastBotHeadSHA rather than
// going through a real takeover. The headline case is a never-bot-pushed MR
// (a Renovate PR) - exactly what newTakeoverHarness seeds: LastBotHeadSHA
// starts empty. Before the fix, the takeover endpoint left it empty, so the
// very next ReconcileOwnership convergence (webhook fast path or the OP12
// sweep) would see ownership==tatara && liveHead(unchanged) != "" and flip
// the just-taken-over MR straight back to external - parking the takeover
// Task before its agent pod ever ran.
func TestMRTakeover_SurvivesNextReconcileOwnership(t *testing.T) {
	h := newTakeoverHarness(t)
	rr := h.post(t, `{"repo":"repo-a","number":9,"commentExternalId":"10","task":"review-task"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("valid takeover must 200, got %d: %s", rr.Code, rr.Body.String())
	}
	mr := h.getMR(t, 9)
	if mr.Status.Ownership != tatarav1alpha1.OwnershipTatara {
		t.Fatalf("precondition: takeover must have flipped to tatara, got %q", mr.Status.Ownership)
	}
	liveHead := mr.Status.HeadSHA // unchanged since takeover: no new push happened
	if liveHead == "" || mr.Status.LastBotHeadSHA != liveHead {
		t.Fatalf("takeover must stamp LastBotHeadSHA to the current head, got head=%q lastBotHeadSHA=%q",
			liveHead, mr.Status.LastBotHeadSHA)
	}

	// Stamp the takeover Task into a live (non-parked) stage, as the real
	// controller would have on create, so "not parked" is a meaningful
	// assertion below rather than trivially true on a Status.Stage the fake
	// client never touched.
	tkName := takeoverTaskName(h.proj, h.repo, 9)
	var tk tatarav1alpha1.Task
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: takeoverNS, Name: tkName}, &tk); err != nil {
		t.Fatalf("get takeover task %s: %v", tkName, err)
	}
	tk.Status.Stage = tatarav1alpha1.StageApproved
	if err := h.c.Status().Update(context.Background(), &tk); err != nil {
		t.Fatalf("stamp takeover task stage: %v", err)
	}

	// Run the SAME convergence function the webhook fast path / OP12 sweep
	// call on every reconcile, with the live head unchanged from takeover.
	d := &controller.StageDriver{Client: h.c, APIReader: h.c}
	flipped, err := d.ReconcileOwnership(context.Background(), h.proj, h.repo, mr, liveHead, nil)
	if err != nil {
		t.Fatalf("ReconcileOwnership: %v", err)
	}
	if flipped {
		t.Fatalf("ReconcileOwnership must NOT flip a just-taken-over MR whose head has not moved")
	}

	after := h.getMR(t, 9)
	if after.Status.Ownership != tatarav1alpha1.OwnershipTatara {
		t.Fatalf("MR must stay tatara-owned, got %q", after.Status.Ownership)
	}

	var tkAfter tatarav1alpha1.Task
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: takeoverNS, Name: tkName}, &tkAfter); err != nil {
		t.Fatalf("get takeover task %s: %v", tkName, err)
	}
	if tkAfter.Status.Stage == tatarav1alpha1.StageParked {
		t.Fatalf("takeover task must not be parked by the next reconcile")
	}
}

// --- FIX 2: seed LastBotHeadSHA from the LIVE forge head, not the mirror ---

// stubForge answers ONLY GetPRHead with a canned (sha, err); every other
// scm.SCMWriter method panics via the nil embedded interface (the same
// pattern package restapi_test's panicForge uses) - the takeover endpoint's
// live-head read is the only forge call these tests exercise.
type stubForge struct {
	scm.SCMWriter
	head    string
	headErr error
}

func (f *stubForge) GetPRHead(_ context.Context, _, _ string, _ int) (string, error) {
	return f.head, f.headErr
}

// TestMRTakeover_SeedsLastBotHeadFromLiveForgeRead is the stale-mirror
// regression test: newTakeoverHarness seeds MR #9's mirrored HeadSHA to
// "sha-9", but the LIVE forge head (what mrTakeover now reads before the flip
// write) is different - simulating a mirror that lagged behind a push that
// landed between the last webhook/sweep sync and this takeover. Before the
// fix, LastBotHeadSHA seeded from the STALE mirror value; the very next
// ReconcileOwnership sweep would then see liveHead("live-sha-fresh") !=
// LastBotHeadSHA("sha-9") and instantly flip the fresh takeover back to
// external.
func TestMRTakeover_SeedsLastBotHeadFromLiveForgeRead(t *testing.T) {
	h := newTakeoverHarnessWithOpts(t, takeoverHarnessOpts{
		scmWriter: &stubForge{head: "live-sha-fresh"},
	})
	rr := h.post(t, `{"repo":"repo-a","number":9,"commentExternalId":"10","task":"review-task"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("valid takeover must 200, got %d: %s", rr.Code, rr.Body.String())
	}
	mr := h.getMR(t, 9)
	if mr.Status.LastBotHeadSHA != "live-sha-fresh" {
		t.Fatalf("LastBotHeadSHA must seed from the LIVE forge head, not the stale mirror (sha-9); got %q", mr.Status.LastBotHeadSHA)
	}
	if mr.Status.HeadSHA != "live-sha-fresh" {
		t.Fatalf("HeadSHA mirror must also refresh to the live head; got %q", mr.Status.HeadSHA)
	}
}

// TestMRTakeover_FallsBackToMirroredHeadOnLiveReadError proves the live-head
// read is best-effort: a transient forge error must never fail the takeover
// itself, and the seed falls back to the (possibly stale) mirrored HeadSHA
// exactly as before this fix.
func TestMRTakeover_FallsBackToMirroredHeadOnLiveReadError(t *testing.T) {
	h := newTakeoverHarnessWithOpts(t, takeoverHarnessOpts{
		scmWriter: &stubForge{headErr: errors.New("transient forge error")},
	})
	rr := h.post(t, `{"repo":"repo-a","number":9,"commentExternalId":"10","task":"review-task"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("a transient live-head read failure must never fail the takeover, got %d: %s", rr.Code, rr.Body.String())
	}
	mr := h.getMR(t, 9)
	if mr.Status.LastBotHeadSHA != "sha-9" { // the mirrored HeadSHA seeded by newTakeoverHarness
		t.Fatalf("fallback must seed from the mirrored head, got %q", mr.Status.LastBotHeadSHA)
	}
}

// --- FIX 3: operator_rest_takeover_error_total, one internal-error branch per stage ---

// takeoverErrStage counts obs.RestTakeoverErrorTotal{stage} before/after a
// takeover call and asserts it moved by exactly +1 for stage and +0 for every
// other of the four stages - so a wiring bug that increments the wrong stage
// (or double-counts) fails loudly instead of passing on a coincidental total.
func assertTakeoverErrorInc(t *testing.T, stage string, before map[string]float64) {
	t.Helper()
	for _, s := range []string{"demote", "mint", "ownerref", "stamp"} {
		want := before[s]
		if s == stage {
			want++
		}
		if got := testutil.ToFloat64(obs.RestTakeoverErrorTotal.WithLabelValues(s)); got != want {
			t.Fatalf("operator_rest_takeover_error_total{stage=%q} = %v, want %v (asserting stage=%q incremented)", s, got, want, stage)
		}
	}
}

func snapshotTakeoverErrors() map[string]float64 {
	snap := make(map[string]float64, 4)
	for _, s := range []string{"demote", "mint", "ownerref", "stamp"} {
		snap[s] = testutil.ToFloat64(obs.RestTakeoverErrorTotal.WithLabelValues(s))
	}
	return snap
}

// errInjected is the sentinel every failure-injection interceptor below
// returns, so a 500 body can be told apart from an unrelated fake-client bug.
var errInjected = errors.New("injected failure")

// TestMRTakeover_DemoteFailureIncrementsErrorMetric forces DemoteMRController
// (the first write mrTakeover makes past authz) to fail: the interceptor
// fails the FIRST Update on a MergeRequest object issued AFTER the harness
// finishes its own setup writes.
func TestMRTakeover_DemoteFailureIncrementsErrorMetric(t *testing.T) {
	armed := false
	mrUpdates := 0
	h := newTakeoverHarnessWithOpts(t, takeoverHarnessOpts{
		interceptor: interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if armed {
					if _, ok := obj.(*tatarav1alpha1.MergeRequest); ok {
						mrUpdates++
						if mrUpdates == 1 {
							return errInjected
						}
					}
				}
				return c.Update(ctx, obj, opts...)
			},
		},
	})
	before := snapshotTakeoverErrors()
	armed = true
	rr := h.post(t, `{"repo":"repo-a","number":9,"commentExternalId":"10","task":"review-task"}`)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("demote failure must 500, got %d: %s", rr.Code, rr.Body.String())
	}
	assertTakeoverErrorInc(t, "demote", before)
}

// TestMRTakeover_MintFailureIncrementsErrorMetric forces
// MintOrUnparkTakeoverTask's Create of the (not-yet-existing) takeover Task
// to fail, gated on the deterministic takeover Task name so the harness's own
// "review-task" Create (during setup, before arming) is never touched.
func TestMRTakeover_MintFailureIncrementsErrorMetric(t *testing.T) {
	armed := false
	var takeoverName string
	h := newTakeoverHarnessWithOpts(t, takeoverHarnessOpts{
		interceptor: interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if armed {
					if tk, ok := obj.(*tatarav1alpha1.Task); ok && tk.Name == takeoverName {
						return errInjected
					}
				}
				return c.Create(ctx, obj, opts...)
			},
		},
	})
	takeoverName = takeoverTaskName(h.proj, h.repo, 9)
	before := snapshotTakeoverErrors()
	armed = true
	rr := h.post(t, `{"repo":"repo-a","number":9,"commentExternalId":"10","task":"review-task"}`)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("mint failure must 500, got %d: %s", rr.Code, rr.Body.String())
	}
	assertTakeoverErrorInc(t, "mint", before)
}

// TestMRTakeover_OwnerRefFailureIncrementsErrorMetric forces the SECOND
// post-arm MergeRequest Update (demote is the first and must succeed; the
// owner-ref move onto the takeover Task is takeover.go's own explicit second
// one) to fail. A RE-TAKE (the takeover Task already parked ownership-lost,
// via seedParkedTakeoverTask) is required to isolate this: on a FRESH mint,
// MintOrUnparkTakeoverTask's own bindMRToTask ALSO writes the MR (sync +
// own) as part of minting, which would otherwise land as the "2nd" Update and
// misattribute the failure to the mint stage instead of this one.
func TestMRTakeover_OwnerRefFailureIncrementsErrorMetric(t *testing.T) {
	armed := false
	mrUpdates := 0
	h := newTakeoverHarnessWithOpts(t, takeoverHarnessOpts{
		interceptor: interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if armed {
					if _, ok := obj.(*tatarav1alpha1.MergeRequest); ok {
						mrUpdates++
						if mrUpdates == 2 {
							return errInjected
						}
					}
				}
				return c.Update(ctx, obj, opts...)
			},
		},
	})
	h.seedParkedTakeoverTask(t, 9)
	before := snapshotTakeoverErrors()
	armed = true
	rr := h.post(t, `{"repo":"repo-a","number":9,"commentExternalId":"10","task":"review-task"}`)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("owner-ref move failure must 500, got %d: %s", rr.Code, rr.Body.String())
	}
	assertTakeoverErrorInc(t, "ownerref", before)
}

// TestMRTakeover_StampFailureIncrementsErrorMetric forces the final
// ownership-flip status write (objbudget.FitMergeRequest's Status().Update)
// to fail - the SubResourceUpdate hook. A RE-TAKE (seedParkedTakeoverTask) is
// required for the same reason as the ownerref test: on a fresh mint,
// bindMRToTask's own SyncMergeRequest ALSO does a status Update on the MR as
// part of minting, which would otherwise be the first (and misattributed)
// SubResourceUpdate hit.
func TestMRTakeover_StampFailureIncrementsErrorMetric(t *testing.T) {
	armed := false
	h := newTakeoverHarnessWithOpts(t, takeoverHarnessOpts{
		interceptor: interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if armed && subResourceName == "status" {
					if _, ok := obj.(*tatarav1alpha1.MergeRequest); ok {
						return errInjected
					}
				}
				return c.SubResource(subResourceName).Update(ctx, obj, opts...)
			},
		},
	})
	h.seedParkedTakeoverTask(t, 9)
	before := snapshotTakeoverErrors()
	armed = true
	rr := h.post(t, `{"repo":"repo-a","number":9,"commentExternalId":"10","task":"review-task"}`)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("stamp failure must 500, got %d: %s", rr.Code, rr.Body.String())
	}
	assertTakeoverErrorInc(t, "stamp", before)
}
