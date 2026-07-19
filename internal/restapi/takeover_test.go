package restapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/prometheus/client_golang/prometheus/testutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
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

func newTakeoverHarness(t *testing.T) *takeoverHarness {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := tatarav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&tatarav1alpha1.Project{}, &tatarav1alpha1.Repository{},
			&tatarav1alpha1.Task{}, &tatarav1alpha1.MergeRequest{}).
		Build()

	h := &takeoverHarness{t: t, c: fc, botLogin: "tatara-bot"}
	ctx := context.Background()

	h.proj = &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "proj-a", Namespace: takeoverNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "tatara-scm",
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github", Owner: "acme", BotLogin: h.botLogin,
				ReporterLogins: []string{"alice"},
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

	// SpillerFor is deliberately left unset: these fixtures never approach the
	// A.7 byte budget, so objbudget never calls Spill, and spillerForOrNil
	// already handles a nil resolver.
	s := NewServer(Config{
		Client: fc, Namespace: takeoverNS,
		Minter: &controller.Minter{Client: fc, APIReader: fc, Scheme: scheme},
		Now:    func() time.Time { return takeoverFrozenNow },
	})
	r := chi.NewRouter()
	s.Mount(r, nil)
	h.r = r
	return h
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
