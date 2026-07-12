package restapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/auth"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/harness"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/titlecheck"
)

// maxBodyBytes caps the request body at 1 MB, matching the webhook server's
// approach and preventing unbounded memory reads on any POST/PATCH endpoint.
const maxBodyBytes = 1 << 20 // 1 MB

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// Headers already sent; log so the failure is visible server-side.
		log.Log.Error(err, "restapi: writeJSON encode failed")
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// writeClientErr returns a generic 500 for non-404 errors, avoiding leaking
// internal k8s error details to API callers.
func writeClientErr(w http.ResponseWriter, err error) {
	if apierrors.IsNotFound(err) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	// Log real error server-side; return generic message to caller.
	log.Log.Error(err, "restapi: client error")
	writeError(w, http.StatusInternalServerError, "internal error")
}

// authorizeCaller gates a mutating handler on the caller carrying a valid
// OIDC bearer token (a non-empty, verifier-validated Subject) for the operator
// audience. The auth middleware has already verified the issuer, audience and
// signature before this runs; this is the in-handler assertion that a verified
// identity is present.
//
// NOTE: per-task (object-level) authorization keyed on the agent Pod name is NOT
// enforceable under the current identity model. Every agent Pod mints its bearer
// token via a SINGLE shared OIDC client (CLI_OIDC_CLIENT_ID/SECRET, client-
// credentials grant), so the token's sub is the Keycloak service-account UUID
// and preferred_username is "service-account-<client-id>" - identical for every
// Pod and never equal to agent.PodName(t). Comparing claims to the Pod name
// would 403 every legitimate agent write. Tightening to per-task scope requires
// per-Pod identity (e.g. a projected ServiceAccount token whose sub is the Pod's
// ServiceAccount, or a token-exchange that stamps the Pod/Task into the sub),
// tracked in MEMORY/ROADMAP. When no Claims are present (middleware absent, e.g.
// tests) the check is skipped. Returns false and writes a 403 on failure.
func authorizeCaller(w http.ResponseWriter, r *http.Request) bool {
	claims, ok := auth.ClaimsFromContext(r.Context())
	if !ok {
		// No auth middleware in this path; skip enforcement.
		return true
	}
	if claims.Subject != "" {
		return true
	}
	writeError(w, http.StatusForbidden, "caller has no verified identity")
	return false
}

// reqLogFields returns the common structured log fields for an INFO business
// action: request_id (from chi middleware) and user (from OIDC claims).
// Hard rule 12 requires these on every InfoContext call.
func reqLogFields(r *http.Request) []any {
	rid := chiMiddleware.GetReqID(r.Context())
	user := ""
	if claims, ok := auth.ClaimsFromContext(r.Context()); ok {
		user = claims.Subject
		if user == "" {
			user = claims.PreferredUsername
		}
	}
	return []any{"request_id", rid, "user", user}
}

// pendingCommentsMaxLen caps the Task.Status.PendingComments queue.
// Beyond this limit postComment returns 409 to prevent unbounded status growth.
const pendingCommentsMaxLen = 20

func (s *Server) listProjects(w http.ResponseWriter, r *http.Request) {
	var list tatarav1alpha1.ProjectList
	if err := s.c.List(r.Context(), &list, client.InNamespace(s.ns)); err != nil {
		writeClientErr(w, err)
		return
	}
	out := make([]ProjectDTO, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, toProjectDTO(list.Items[i]))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) getProject(w http.ResponseWriter, r *http.Request) {
	var p tatarav1alpha1.Project
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "p")}
	if err := s.c.Get(r.Context(), key, &p); err != nil {
		writeClientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toProjectDTO(p))
}

func (s *Server) listRepositories(w http.ResponseWriter, r *http.Request) {
	projName := chi.URLParam(r, "p")
	var proj tatarav1alpha1.Project
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: projName}, &proj); err != nil {
		writeClientErr(w, err)
		return
	}
	var list tatarav1alpha1.RepositoryList
	if err := s.c.List(r.Context(), &list, client.InNamespace(s.ns)); err != nil {
		writeClientErr(w, err)
		return
	}
	out := make([]RepositoryDTO, 0)
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == projName {
			out = append(out, toRepositoryDTO(list.Items[i]))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	projName := chi.URLParam(r, "p")
	var proj tatarav1alpha1.Project
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: projName}, &proj); err != nil {
		writeClientErr(w, err)
		return
	}
	var list tatarav1alpha1.TaskList
	if err := s.c.List(r.Context(), &list, client.InNamespace(s.ns)); err != nil {
		writeClientErr(w, err)
		return
	}
	out := make([]TaskDTO, 0)
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == projName {
			out = append(out, toTaskDTO(list.Items[i]))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// --- Task 5: GET /tasks/{t} and PATCH /tasks/{t} ---

type taskPatchReq struct {
	ResultSummary *string `json:"resultSummary,omitempty"`
	Note          string  `json:"note,omitempty"`
}

func decodeJSON(r *http.Request, w http.ResponseWriter, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

// writeDecodeError writes the appropriate HTTP error for a decodeJSON failure.
// Oversized bodies become 413; all other decode errors become 400 with a generic
// message so internal json-decoder detail is not echoed to callers.
func writeDecodeError(w http.ResponseWriter, r *http.Request, err error) {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	// Log real error server-side; return generic message to caller.
	log.Log.Error(err, "restapi: decode body failed", "path", r.URL.Path)
	writeError(w, http.StatusBadRequest, "invalid JSON body")
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	var t tatarav1alpha1.Task
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

func (s *Server) patchTask(w http.ResponseWriter, r *http.Request) {
	var req taskPatchReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	var t tatarav1alpha1.Task
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeCaller(w, r) {
		return
	}
	// Reject writes to already-terminal tasks; late/replayed agent writes must not
	// overwrite a completed result summary (Finding 9).
	if t.Status.Phase == "Succeeded" || t.Status.Phase == "Failed" {
		writeError(w, http.StatusConflict, "task is already terminal")
		return
	}
	start := time.Now()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if gerr := s.c.Get(r.Context(), key, &t); gerr != nil {
			return gerr
		}
		if req.ResultSummary != nil {
			t.Status.ResultSummary = *req.ResultSummary
		}
		if req.Note != "" {
			apimeta.SetStatusCondition(&t.Status.Conditions, metav1.Condition{
				Type:               "AgentNote",
				Status:             metav1.ConditionTrue,
				Reason:             "AgentReport",
				Message:            req.Note,
				LastTransitionTime: metav1.NewTime(time.Now()),
			})
		}
		return s.c.Status().Update(r.Context(), &t)
	}); err != nil {
		if s.metrics != nil {
			s.metrics.RecordRESTRequest("patch_task", "error", time.Since(start).Seconds())
		}
		writeClientErr(w, err)
		return
	}
	elapsed := time.Since(start)
	if s.metrics != nil {
		s.metrics.RecordRESTRequest("patch_task", "ok", elapsed.Seconds())
	}
	s.log.InfoContext(r.Context(), "restapi: patchTask",
		append(reqLogFields(r),
			"action", "patch_task",
			"resource_id", key.Name,
			"duration_ms", elapsed.Milliseconds())...)
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

// --- Task 6: GET /tasks/{t}/subtasks and POST /tasks/{t}/subtasks ---

type subtaskCreateReq struct {
	Title  string `json:"title"`
	Detail string `json:"detail,omitempty"`
	Order  int    `json:"order,omitempty"`
}

func (s *Server) listSubtasks(w http.ResponseWriter, r *http.Request) {
	taskName := chi.URLParam(r, "t")
	var list tatarav1alpha1.SubtaskList
	if err := s.c.List(r.Context(), &list, client.InNamespace(s.ns)); err != nil {
		writeClientErr(w, err)
		return
	}
	items := make([]tatarav1alpha1.Subtask, 0)
	for i := range list.Items {
		if list.Items[i].Spec.TaskRef == taskName {
			items = append(items, list.Items[i])
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Spec.Order < items[j].Spec.Order })
	out := make([]SubtaskDTO, 0, len(items))
	for i := range items {
		out = append(out, toSubtaskDTO(items[i]))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) createSubtask(w http.ResponseWriter, r *http.Request) {
	var req subtaskCreateReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Title == "" {
		writeError(w, http.StatusBadRequest, "title required")
		return
	}
	taskName := chi.URLParam(r, "t")
	var parent tatarav1alpha1.Task
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: taskName}, &parent); err != nil {
		writeClientErr(w, err)
		return
	}
	st := &tatarav1alpha1.Subtask{
		ObjectMeta: metav1.ObjectMeta{
			// Use GenerateName so the API server assigns a collision-free suffix,
			// consistent with proposeIssue (Finding 12: timestamp suffix is collidable
			// under concurrent calls on coarse-clock platforms).
			GenerateName: fmt.Sprintf("%s-st-", taskName),
			Namespace:    s.ns,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(&parent, tatarav1alpha1.GroupVersion.WithKind("Task")),
			},
		},
		Spec: tatarav1alpha1.SubtaskSpec{
			TaskRef: taskName, Title: req.Title, Detail: req.Detail, Order: req.Order,
		},
	}
	start := time.Now()
	if err := s.c.Create(r.Context(), st); err != nil {
		if s.metrics != nil {
			s.metrics.RecordRESTRequest("create_subtask", "error", time.Since(start).Seconds())
		}
		writeClientErr(w, err)
		return
	}
	elapsed := time.Since(start)
	if s.metrics != nil {
		s.metrics.RecordRESTRequest("create_subtask", "ok", elapsed.Seconds())
	}
	// item 8: seed the durable rollup entry so it is visible via task_get even
	// before the subtask's first turn completes. Best-effort: a failure here
	// does not fail subtask creation itself.
	if uerr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if gerr := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: taskName}, fresh); gerr != nil {
			return gerr
		}
		controller.UpsertSubtaskRollup(&fresh.Status, tatarav1alpha1.SubtaskRef{
			Name: st.Name, Order: st.Spec.Order, Title: st.Spec.Title, Phase: "Pending",
		})
		return s.c.Status().Update(r.Context(), fresh)
	}); uerr != nil {
		s.log.ErrorContext(r.Context(), "createSubtask: rollup update (non-fatal)", "error", uerr)
	}
	s.log.InfoContext(r.Context(), "restapi: createSubtask",
		append(reqLogFields(r),
			"action", "create_subtask",
			"resource_id", st.Name,
			"task", taskName,
			"duration_ms", elapsed.Milliseconds())...)
	writeJSON(w, http.StatusCreated, toSubtaskDTO(*st))
}

// --- Task 11: POST /projects/{p}/issues, /tasks/{t}/review, /tasks/{t}/pr-outcome ---

type proposeIssueReq struct {
	RepositoryRef string `json:"repositoryRef"`
	Title         string `json:"title"`
	Body          string `json:"body"`
	Kind          string `json:"kind"`
	SystemicID    string `json:"systemicId,omitempty"`
}

// inflightBrainstormConversationKey returns the deterministic S3 conversation
// key of the project's in-flight brainstorm Task (the one whose agent is calling
// propose_issue), so a proposal it spawns can carry the parent key for
// conversation forking (issue #114 decision 3). Brainstorm is project-level (one
// in flight per cycle); the newest non-terminal one wins. Returns "" when none
// is in flight (then no fork pointer is stamped and the issue starts fresh).
func (s *Server) inflightBrainstormConversationKey(ctx context.Context, project string) string {
	var tasks tatarav1alpha1.TaskList
	if err := s.c.List(ctx, &tasks, client.InNamespace(s.ns)); err != nil {
		return ""
	}
	var newest *tatarav1alpha1.Task
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.ProjectRef != project || t.Spec.Kind != "brainstorm" {
			continue
		}
		if t.Status.Phase == "Succeeded" || t.Status.Phase == "Failed" {
			continue
		}
		if newest == nil || t.CreationTimestamp.After(newest.CreationTimestamp.Time) {
			newest = t
		}
	}
	if newest == nil {
		return ""
	}
	return agent.ConversationKey(newest)
}

// hasLiveTaskForIssue reports whether any non-terminal Task in the namespace is
// working the issue (repoSlug, number). Identity is spec/ledger via TaskMatchesItem
// (the same match the webhook dedup uses). Used by refine's close/edit tool layer
// to refuse mutating an issue an implement/review/clarify pod is mid-flight on.
// Fails open (returns false) only when the Task list itself cannot be read.
func (s *Server) hasLiveTaskForIssue(ctx context.Context, repoSlug string, number int) bool {
	var tasks tatarav1alpha1.TaskList
	if err := s.c.List(ctx, &tasks, client.InNamespace(s.ns)); err != nil {
		return false
	}
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if tatarav1alpha1.TaskTerminal(t) {
			continue
		}
		if tatarav1alpha1.TaskMatchesItem(t, repoSlug, number) {
			return true
		}
	}
	return false
}

// inflightIncidentTask returns the project's first non-terminal incident Task,
// or nil when none is in flight. Agent identity is shared OIDC, so an
// incident-investigation agent is inferred from the project's in-flight incident
// work (same project-level inference inflightBrainstormConversationKey uses).
// Returning the Task (not a bool) lets the caller carry its alert-group dedup
// identity onto the proposal.
func (s *Server) inflightIncidentTask(ctx context.Context, project string) *tatarav1alpha1.Task {
	var tasks tatarav1alpha1.TaskList
	if err := s.c.List(ctx, &tasks, client.InNamespace(s.ns)); err != nil {
		return nil
	}
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.ProjectRef != project || t.Spec.Kind != "incident" {
			continue
		}
		// Incident tasks leave Phase empty for life and signal completion via
		// DeployState; use the canonical terminal predicate (matches the
		// dedup gate in internal/queue) so a parked/stopped incident is not
		// mistaken for in-flight work.
		if tatarav1alpha1.TaskTerminal(t) {
			continue
		}
		return t
	}
	return nil
}

// incidentAlertGroup returns the alert-group dedup identity of an in-flight
// incident task: its Spec.DedupKey, falling back to the descriptive AlertRule
// name. Empty when t is nil (non-incident proposal).
func incidentAlertGroup(t *tatarav1alpha1.Task) string {
	if t == nil {
		return ""
	}
	if t.Spec.DedupKey != "" {
		return t.Spec.DedupKey
	}
	return t.Spec.AlertRule
}

func (s *Server) proposeIssue(w http.ResponseWriter, r *http.Request) {
	var req proposeIssueReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Title == "" || req.Body == "" || req.Kind == "" || req.RepositoryRef == "" {
		writeError(w, http.StatusBadRequest, "repositoryRef, title, body, kind required")
		return
	}
	switch req.Kind {
	case "bug", "improvement":
	default:
		writeError(w, http.StatusBadRequest, "kind must be bug or improvement")
		return
	}
	if weak, guidance := titlecheck.Weak(req.Title); weak {
		writeError(w, http.StatusBadRequest, "weak title: "+guidance)
		return
	}
	projName := chi.URLParam(r, "p")
	var proj tatarav1alpha1.Project
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: projName}, &proj); err != nil {
		writeClientErr(w, err)
		return
	}
	// Validate the repository: it must exist and belong to this project.
	var repo tatarav1alpha1.Repository
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: req.RepositoryRef}, &repo); err != nil {
		writeClientErr(w, err)
		return
	}
	if repo.Spec.ProjectRef != projName {
		writeError(w, http.StatusBadRequest, "repository does not belong to project")
		return
	}
	// Conversation forking (issue #114 decision 3): stamp the proposing
	// brainstorm's conversation key on the proposal so the eventual issueLifecycle
	// Task (correlated by repo+number) can fork it. The key is deterministic
	// (available before the brainstorm's first turn-complete records Status).
	var annotations map[string]string
	if parentKey := s.inflightBrainstormConversationKey(r.Context(), projName); parentKey != "" {
		annotations = map[string]string{tatarav1alpha1.AnnParentConversationKey: parentKey}
	}
	// An incident-investigation agent's proposal carries the in-flight incident
	// Task's alert-group identity so createProposal can dedup recurring alerts.
	incidentTask := s.inflightIncidentTask(r.Context(), projName)
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "task-",
			Namespace:    s.ns,
			Annotations:  annotations,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(&proj, tatarav1alpha1.GroupVersion.WithKind("Project")),
			},
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:       projName,
			RepositoryRef:    req.RepositoryRef,
			Goal:             req.Title,
			Kind:             "implement",
			ApprovalRequired: false,
			ProposedIssue: &tatarav1alpha1.ProposedIssueSpec{
				RepositoryRef: req.RepositoryRef, Title: req.Title, Body: req.Body, Kind: req.Kind,
				SystemicID: req.SystemicID,
				Incident:   incidentTask != nil,
				AlertGroup: incidentAlertGroup(incidentTask),
			},
		},
	}
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	agent.StampPodName(task, projName, provider, req.RepositoryRef)
	start := time.Now()
	if err := s.c.Create(r.Context(), task); err != nil {
		writeClientErr(w, err)
		return
	}
	elapsed := time.Since(start)
	if s.metrics != nil {
		// Use the REST metric family, not ScanTaskCreated: this is a REST-driven
		// creation, not a scan-loop creation. Conflating them pollutes scan-rate
		// dashboards (Finding 11).
		s.metrics.RecordRESTRequest("propose_issue", "ok", elapsed.Seconds())
	}
	s.log.InfoContext(r.Context(), "restapi: proposeIssue",
		append(reqLogFields(r),
			"action", "propose_issue",
			"resource_id", task.Name,
			"project", projName,
			"repository", req.RepositoryRef,
			"duration_ms", elapsed.Milliseconds())...)
	// The proposal Task starts Pending; the controller opens the idea-labelled
	// issue and completes the Task (Succeeded). No AwaitingApproval parking.
	writeJSON(w, http.StatusCreated, toTaskDTO(*task))
}

// --- POST /projects/{p}/issue-comment ---

type issueCommentOnProjectReq struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
	Body   string `json:"body"`
	// IsPR marks the target as a PR/MR rather than a plain issue: comments are
	// read via PRCommentLister (when available) instead of ListIssueComments,
	// and the ref uses the provider's PR/MR separator ('!' on GitLab).
	IsPR bool `json:"isPR,omitempty"`
}

// commentRefusedResp is the machine-readable body returned when the permission-
// layer self-comment guard refuses a comment. Refused/Reason let the pod's skill
// react (e.g. pick another action) rather than parsing a prose error.
type commentRefusedResp struct {
	Error   string `json:"error"`
	Refused bool   `json:"refused"`
	Reason  string `json:"reason"`
}

// callerKindForIssue resolves the task kind driving a comment_on_issue call on
// (repo, number), for the PermitComment refine carve-out. Agent identity is shared
// OIDC (see authorizeCaller), so the kind is inferred from in-flight work: a
// non-terminal Task whose Source is exactly this issue owns it (its Kind wins);
// otherwise a non-terminal refine Task in the project means the caller is the
// project-scoped refiner (which grooms every open issue and carries no per-issue
// Source). Returns "" when neither matches - the safe default (the guard then
// applies, since only "refine" is exempt). The residual ambiguity (a brainstorm
// commenting while a refine is also in flight) is the same shared-identity limit
// documented on authorizeCaller and is bounded by the refine-vs-brainstorm barrier.
func (s *Server) callerKindForIssue(ctx context.Context, projName, repoSlug string, number int) string {
	var tasks tatarav1alpha1.TaskList
	if err := s.c.List(ctx, &tasks, client.InNamespace(s.ns)); err != nil {
		return ""
	}
	ref := fmt.Sprintf("%s#%d", repoSlug, number)
	refineInFlight := false
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.ProjectRef != projName || tatarav1alpha1.TaskTerminal(t) {
			continue
		}
		if t.Spec.Source != nil && !t.Spec.Source.IsPR && t.Spec.Source.IssueRef == ref {
			return t.Spec.Kind
		}
		if t.Spec.Kind == "refine" {
			refineInFlight = true
		}
	}
	if refineInFlight {
		return "refine"
	}
	return ""
}

// commentOnIssue posts a comment on a specific issue via the Project's SCM
// provider and bot token. This is the egress-mediation path for the
// comment_on_issue tool call from a brainstorm or refine agent.
//
// Request body: { repo: "owner/repo", number: N, body: "..." }
// Response:     200 { status: "ok" }
// Errors:       400 missing/zero fields; 404 project/repo not found; 500 SCM error.
func (s *Server) commentOnIssue(w http.ResponseWriter, r *http.Request) {
	var req issueCommentOnProjectReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Body == "" {
		writeError(w, http.StatusBadRequest, "body required")
		return
	}
	if req.Number <= 0 {
		writeError(w, http.StatusBadRequest, "number must be > 0")
		return
	}
	if req.Repo == "" {
		writeError(w, http.StatusBadRequest, "repo required")
		return
	}

	projName := chi.URLParam(r, "p")
	var proj tatarav1alpha1.Project
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: projName}, &proj); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeCaller(w, r) {
		return
	}

	// Find the matching Repository by slug (owner/repo from URL).
	var repoList tatarav1alpha1.RepositoryList
	if err := s.c.List(r.Context(), &repoList, client.InNamespace(s.ns)); err != nil {
		writeClientErr(w, err)
		return
	}
	var matchedRepoURL string
	var matchedRepo *tatarav1alpha1.Repository
	for i := range repoList.Items {
		if repoList.Items[i].Spec.ProjectRef != projName {
			continue
		}
		o, n, err := scm.OwnerRepo(repoList.Items[i].Spec.URL)
		if err != nil {
			continue
		}
		if o+"/"+n == req.Repo {
			matchedRepoURL = repoList.Items[i].Spec.URL
			matchedRepo = &repoList.Items[i]
			break
		}
	}
	if matchedRepoURL == "" {
		writeError(w, http.StatusNotFound, "repo not found in project")
		return
	}

	// Resolve the SCM writer.
	if s.scmFor == nil {
		writeError(w, http.StatusNotImplemented, "scm writer not configured")
		return
	}
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	if provider == "" {
		writeError(w, http.StatusConflict, "project has no scm provider configured")
		return
	}
	writer, err := s.scmFor(provider)
	if err != nil {
		s.log.ErrorContext(r.Context(), "restapi: commentOnIssue scm factory failed",
			"err", err, "project", projName, "provider", provider)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// Fetch the project's SCM token from its secret.
	var sec corev1.Secret
	if err := s.c.Get(r.Context(), types.NamespacedName{Namespace: s.ns, Name: proj.Spec.ScmSecretRef}, &sec); err != nil {
		s.log.ErrorContext(r.Context(), "restapi: commentOnIssue secret fetch failed",
			"err", err, "project", projName, "secret", proj.Spec.ScmSecretRef)
		if apierrors.IsNotFound(err) {
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		writeClientErr(w, err)
		return
	}
	token := string(sec.Data["token"])
	if token == "" {
		s.log.ErrorContext(r.Context(), "restapi: commentOnIssue secret missing token key",
			"project", projName, "secret", proj.Spec.ScmSecretRef)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// Permission-layer self-comment guard (CROSS-REPO-CONTRACT): refuse the comment
	// when the last comment on the thread is tatara(bot)-authored, so the bot never
	// answers its own comment in a loop. The SOLE exceptions are the refine and
	// incident kinds (encoded in controller.PermitComment). Best-effort: an empty
	// BotLogin, no reader factory, or an SCM read error all fall open (post
	// proceeds) - matching decideCommentGate, so a lost webhook stays recoverable
	// by a later scan (never fail-closed on a read error).
	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	if botLogin != "" && s.readerFor != nil {
		if reader, rerr := s.readerFor(provider, token); rerr == nil {
			if owner, name, oerr := scm.OwnerRepo(matchedRepoURL); oerr == nil {
				var (
					comments []scm.IssueComment
					cerr     error
				)
				if req.IsPR {
					if pl, ok := reader.(scm.PRCommentLister); ok {
						comments, cerr = pl.ListPRComments(r.Context(), owner, name, req.Number)
					} else {
						comments, cerr = reader.ListIssueComments(r.Context(), owner, name, req.Number)
					}
				} else {
					comments, cerr = reader.ListIssueComments(r.Context(), owner, name, req.Number)
				}
				if cerr == nil {
					kind := s.callerKindForIssue(r.Context(), projName, req.Repo, req.Number)
					breakers := controller.CommentSilenceBreakers(&proj, matchedRepo)
					if permit, reason := controller.PermitComment(kind, comments, botLogin, breakers); !permit {
						if s.metrics != nil {
							s.metrics.SCMWrite(provider, "comment", "blocked")
						}
						s.log.InfoContext(r.Context(), "restapi: commentOnIssue refused",
							append(reqLogFields(r),
								"action", "scm_issue_comment_blocked",
								"reason", reason,
								"kind", kind,
								"project", projName,
								"repo", req.Repo,
								"number", req.Number)...)
						writeJSON(w, http.StatusConflict, commentRefusedResp{
							Error:   "comment refused: tatara has the last word on this thread; wait for a human reply before commenting again",
							Refused: true,
							Reason:  reason,
						})
						return
					}
					closed := false
					if req.IsPR {
						if st, perr := writer.GetPRState(r.Context(), matchedRepoURL, token, req.Number); perr == nil {
							closed = st.Closed || st.Merged
						}
					} else if st, ierr := writer.GetIssueState(r.Context(), matchedRepoURL, token, req.Number); ierr == nil {
						closed = st.Closed
					}
					if closed {
						writeJSON(w, http.StatusConflict, commentRefusedResp{
							Error:   "comment refused: target is closed",
							Refused: true,
							Reason:  "closed",
						})
						return
					}
					if controller.DuplicateRecentBotComment(comments, botLogin, req.Body) {
						writeJSON(w, http.StatusConflict, commentRefusedResp{
							Error:   "comment refused: duplicate of a recent bot comment",
							Refused: true,
							Reason:  "duplicate",
						})
						return
					}
				}
			}
		}
	}

	start := time.Now()
	sep := "#"
	if provider == "gitlab" && req.IsPR {
		sep = "!"
	}
	issueRef := fmt.Sprintf("%s%s%d", req.Repo, sep, req.Number)
	commentErr := writer.Comment(r.Context(), token, issueRef, req.Body)
	result := "ok"
	if commentErr != nil {
		result = "error"
	}
	if s.metrics != nil {
		s.metrics.SCMWrite(provider, "comment", result)
		if commentErr != nil {
			s.metrics.SCMRequestErrorByStatus(provider, "comment", scm.ErrorStatus(commentErr))
		}
	}
	if commentErr != nil {
		s.log.ErrorContext(r.Context(), "restapi: commentOnIssue scm write failed",
			"err", commentErr, "project", projName, "repo", req.Repo)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	s.log.InfoContext(r.Context(), "restapi: commentOnIssue",
		append(reqLogFields(r),
			"action", "scm_issue_comment",
			"project", projName,
			"repo", req.Repo,
			"number", req.Number,
			"duration_ms", time.Since(start).Milliseconds())...)

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type reviewVerdictReq struct {
	Decision    string                      `json:"decision"`
	Body        string                      `json:"body,omitempty"`
	Suggestions []tatarav1alpha1.Suggestion `json:"suggestions,omitempty"`
	// Semver carries the per-MR push-CD level the review agent assigns on approval
	// so the release tag can be cut for EVERY MR in the stream (human MRs otherwise
	// have no change_significance). Each Level is validated against the same closed
	// major|minor|patch set as change_summary (validChangeSignificance). Wire key
	// is exactly "semver" (decodeJSON DisallowUnknownFields freezes it).
	Semver []tatarav1alpha1.SemverAssignment `json:"semver,omitempty"`
}

// mutateTaskStatusParams bundles the per-handler pieces of the shared
// Get+authorize+kind-guard+RetryOnConflict(single Get+one-field mutate+
// Status().Update)+metrics+log+writeJSON(toTaskDTO) skeleton that
// reviewVerdict, prOutcome, implementOutcome, brainstormOutcome and
// issueOutcome all share byte-for-byte. Not used by patchTask (terminal-phase
// guard, not Kind), changeSummary/handover (no guard), postComment (custom
// mutate-with-sentinel-error contract) or patchSubtask (no pre-fetch, no
// authorize-with-object, no Kind concept) - those remain hand-written.
type mutateTaskStatusParams struct {
	// metricName is the RecordRESTRequest label (e.g. "review_verdict").
	metricName string
	// logMsg is the InfoContext message (e.g. "restapi: reviewVerdict").
	logMsg string
	// logAction is the structured "action" log field (e.g. "review_verdict").
	logAction string
	// kindOK reports whether t.Spec.Kind satisfies this handler's guard.
	kindOK func(kind string) bool
	// kindErrMsg is the 409 body written when kindOK returns false.
	kindErrMsg string
	// mutate applies this handler's one-field status write to the freshly
	// re-Get'd task inside the RetryOnConflict closure.
	mutate func(t *tatarav1alpha1.Task)
	// extraLogFields are inserted between resource_id and duration_ms.
	extraLogFields []any
	// onSuccess runs (only when s.metrics != nil, before RecordRESTRequest)
	// so issueOutcome's IssueOutcome(action) metric survives verbatim.
	onSuccess func(t *tatarav1alpha1.Task)
}

// mutateTaskStatus implements the shared skeleton described in
// mutateTaskStatusParams for the 5 handlers that share it exactly.
func (s *Server) mutateTaskStatus(w http.ResponseWriter, r *http.Request, p mutateTaskStatusParams) {
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	var t tatarav1alpha1.Task
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeCaller(w, r) {
		return
	}
	if !p.kindOK(t.Spec.Kind) {
		writeError(w, http.StatusConflict, p.kindErrMsg)
		return
	}
	start := time.Now()
	// Task.Spec.Kind is immutable; the single pre-loop check above is sufficient.
	// The in-loop and post-loop re-checks are removed (Finding 14: TOCTOU scaffolding
	// on an immutable field adds dead branches without protecting against a real race).
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if gerr := s.c.Get(r.Context(), key, &t); gerr != nil {
			return gerr
		}
		p.mutate(&t)
		return s.c.Status().Update(r.Context(), &t)
	}); err != nil {
		if s.metrics != nil {
			s.metrics.RecordRESTRequest(p.metricName, "error", time.Since(start).Seconds())
		}
		writeClientErr(w, err)
		return
	}
	elapsed := time.Since(start)
	if s.metrics != nil {
		if p.onSuccess != nil {
			p.onSuccess(&t)
		}
		s.metrics.RecordRESTRequest(p.metricName, "ok", elapsed.Seconds())
	}
	logFields := append(reqLogFields(r), "action", p.logAction, "resource_id", key.Name)
	logFields = append(logFields, p.extraLogFields...)
	logFields = append(logFields, "duration_ms", elapsed.Milliseconds())
	s.log.InfoContext(r.Context(), p.logMsg, logFields...)
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

func (s *Server) reviewVerdict(w http.ResponseWriter, r *http.Request) {
	var req reviewVerdictReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Decision == "" {
		writeError(w, http.StatusBadRequest, "decision required")
		return
	}
	switch req.Decision {
	case "approve", "request_changes", "comment":
	default:
		writeError(w, http.StatusBadRequest, "decision must be one of approve, request_changes, comment")
		return
	}
	for _, sa := range req.Semver {
		if !validChangeSignificance[sa.Level] {
			writeError(w, http.StatusBadRequest, "semver level must be one of major|minor|patch")
			return
		}
	}
	s.mutateTaskStatus(w, r, mutateTaskStatusParams{
		metricName: "review_verdict",
		logMsg:     "restapi: reviewVerdict",
		logAction:  "review_verdict",
		kindOK:     func(kind string) bool { return kind == "review" },
		kindErrMsg: "review verdict only applies to a review task",
		mutate: func(t *tatarav1alpha1.Task) {
			t.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: req.Decision, Body: req.Body, Suggestions: req.Suggestions, Semver: req.Semver}
		},
		extraLogFields: []any{"decision", req.Decision},
	})
}

type prOutcomeReq struct {
	Action string `json:"action"`
	Reason string `json:"reason,omitempty"`
}

// prOutcomeKindOK is the kind gate for the pr_outcome endpoint.
// Only issueLifecycle tasks may receive the pr_outcome signal; selfImprove
// tasks (now retired) must never be admitted here.
func prOutcomeKindOK(kind string) bool { return kind == "issueLifecycle" }

func (s *Server) prOutcome(w http.ResponseWriter, r *http.Request) {
	var req prOutcomeReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Action == "" {
		writeError(w, http.StatusBadRequest, "action required")
		return
	}
	switch req.Action {
	case "merge", "close":
	default:
		writeError(w, http.StatusBadRequest, "action must be one of merge, close")
		return
	}
	s.mutateTaskStatus(w, r, mutateTaskStatusParams{
		metricName: "pr_outcome",
		logMsg:     "restapi: prOutcome",
		logAction:  "pr_outcome",
		kindOK:     prOutcomeKindOK,
		kindErrMsg: "pr outcome only applies to an issueLifecycle task",
		mutate: func(t *tatarav1alpha1.Task) {
			t.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: req.Action, Reason: req.Reason}
		},
		extraLogFields: []any{"pr_action", req.Action},
	})
}

type issueOutcomeReq struct {
	Action  string `json:"action"`
	Comment string `json:"comment,omitempty"`
	Plan    string `json:"plan,omitempty"` // posted as implementation-start message when action==implement
	// Locked declares, when Action==implement, that the clarify agent found
	// no open questions and every decision is settled (item Request C/d).
	Locked bool `json:"locked,omitempty"`
}

func (s *Server) issueOutcome(w http.ResponseWriter, r *http.Request) {
	var req issueOutcomeReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Action == "" {
		writeError(w, http.StatusBadRequest, "action required")
		return
	}
	switch req.Action {
	case "implement", "close", "discuss":
	default:
		writeError(w, http.StatusBadRequest, "action must be one of implement, close, discuss")
		return
	}
	if (req.Action == "close" || req.Action == "discuss") && req.Comment == "" {
		writeError(w, http.StatusBadRequest, "comment required when action is close or discuss")
		return
	}
	s.mutateTaskStatus(w, r, mutateTaskStatusParams{
		metricName: "issue_outcome",
		logMsg:     "restapi: issueOutcome",
		logAction:  "issue_outcome",
		kindOK:     func(kind string) bool { return kind == "clarify" || kind == "triageIssue" || kind == "issueLifecycle" },
		kindErrMsg: "issue outcome only applies to a clarify, triageIssue or issueLifecycle task",
		mutate: func(t *tatarav1alpha1.Task) {
			t.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: req.Action, Comment: req.Comment, Plan: req.Plan, Locked: req.Locked}
		},
		extraLogFields: []any{"issue_action", req.Action},
		onSuccess: func(t *tatarav1alpha1.Task) {
			s.metrics.IssueOutcome(req.Action)
		},
	})
}

// --- POST /tasks/{t}/implement-outcome ---

type implementOutcomeReq struct {
	Action string `json:"action"`
	Reason string `json:"reason"`
}

func (s *Server) implementOutcome(w http.ResponseWriter, r *http.Request) {
	var req implementOutcomeReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Action == "" {
		writeError(w, http.StatusBadRequest, "action required")
		return
	}
	validImplementActions := map[string]bool{"declined": true, "already_done": true}
	if !validImplementActions[req.Action] {
		writeError(w, http.StatusBadRequest, "action must be one of declined, already_done")
		return
	}
	if strings.TrimSpace(req.Reason) == "" {
		writeError(w, http.StatusBadRequest, "reason required (non-empty) for action "+req.Action)
		return
	}
	s.mutateTaskStatus(w, r, mutateTaskStatusParams{
		metricName: "implement_outcome",
		logMsg:     "restapi: implementOutcome",
		logAction:  "implement_outcome",
		kindOK:     func(kind string) bool { return kind == "implement" || kind == "issueLifecycle" },
		kindErrMsg: "implement outcome only applies to an implement or issueLifecycle task",
		mutate: func(t *tatarav1alpha1.Task) {
			t.Status.ImplementOutcome = &tatarav1alpha1.ImplementOutcome{Action: req.Action, Reason: req.Reason}
		},
		extraLogFields: []any{"impl_action", req.Action},
	})
}

// --- POST /tasks/{t}/brainstorm-outcome ---

type brainstormOutcomeReq struct {
	Action string `json:"action"`
	Reason string `json:"reason"`
}

func (s *Server) brainstormOutcome(w http.ResponseWriter, r *http.Request) {
	var req brainstormOutcomeReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Action != "none" {
		writeError(w, http.StatusBadRequest, "action must be none")
		return
	}
	if strings.TrimSpace(req.Reason) == "" {
		writeError(w, http.StatusBadRequest, "reason required")
		return
	}
	s.mutateTaskStatus(w, r, mutateTaskStatusParams{
		metricName: "brainstorm_outcome",
		logMsg:     "restapi: brainstormOutcome",
		logAction:  "brainstorm_outcome",
		kindOK:     func(kind string) bool { return kind == "brainstorm" },
		kindErrMsg: "brainstorm outcome only applies to a brainstorm task",
		mutate: func(t *tatarav1alpha1.Task) {
			t.Status.BrainstormOutcome = &tatarav1alpha1.BrainstormOutcome{Action: req.Action, Reason: req.Reason}
		},
	})
}

// --- POST /tasks/{t}/comment ---

type issueCommentReq struct {
	Body string `json:"body"`
}

func (s *Server) postComment(w http.ResponseWriter, r *http.Request) {
	var req issueCommentReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Body == "" {
		writeError(w, http.StatusBadRequest, "body required")
		return
	}
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	var t tatarav1alpha1.Task
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeCaller(w, r) {
		return
	}
	// Only issueLifecycle Tasks have the reconcile drain that posts queued
	// comments; queuing on any other kind would leak forever.
	if t.Spec.Kind != "issueLifecycle" {
		writeError(w, http.StatusConflict, "comment only applies to an issueLifecycle task")
		return
	}
	if t.Spec.Source == nil || t.Spec.Source.IssueRef == "" {
		writeError(w, http.StatusConflict, "comment requires a task linked to an issue")
		return
	}
	start := time.Now()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh tatarav1alpha1.Task
		if gerr := s.c.Get(r.Context(), key, &fresh); gerr != nil {
			return gerr
		}
		// Cap the comment queue to prevent unbounded Task status growth (Finding 13).
		// handover has a 16KB byte-size cap; comments are capped by count.
		if len(fresh.Status.PendingComments) >= pendingCommentsMaxLen {
			return errPendingCommentsCap
		}
		fresh.Status.PendingComments = append(fresh.Status.PendingComments, req.Body)
		if uerr := s.c.Status().Update(r.Context(), &fresh); uerr != nil {
			return uerr
		}
		t = fresh
		return nil
	}); err != nil {
		if errors.Is(err, errPendingCommentsCap) {
			writeError(w, http.StatusConflict, "too many pending comments")
			return
		}
		if s.metrics != nil {
			s.metrics.RecordRESTRequest("post_comment", "error", time.Since(start).Seconds())
		}
		writeClientErr(w, err)
		return
	}
	elapsed := time.Since(start)
	if s.metrics != nil {
		s.metrics.RecordRESTRequest("post_comment", "ok", elapsed.Seconds())
	}
	s.log.InfoContext(r.Context(), "restapi: postComment",
		append(reqLogFields(r),
			"action", "post_comment",
			"resource_id", key.Name,
			"duration_ms", elapsed.Milliseconds())...)
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

// errPendingCommentsCap is returned inside RetryOnConflict when the cap is hit
// so the outer error handler can distinguish it from a k8s API error.
var errPendingCommentsCap = errors.New("pending comments cap reached")

// --- M4 Task 2: POST /tasks/{t}/change-summary ---

type changeSummaryReq struct {
	PRTitle         string `json:"prTitle,omitempty"`
	PRBody          string `json:"prBody,omitempty"`
	DeliveredScope  string `json:"deliveredScope,omitempty"`
	RemainingScope  string `json:"remainingScope,omitempty"`
	MostProblematic string `json:"mostProblematic,omitempty"` // from cli most_problematic field
	// ChangeSignificance is the push-CD semver lever (major|minor|patch). REQUIRED
	// (D2): the cli marks change_significance required and this REST layer rejects
	// a missing/invalid value, so a PR cannot be opened without a declared
	// significance. The wire key MUST stay exactly changeSignificance: decodeJSON
	// uses DisallowUnknownFields, so it is the frozen seam the cli posts.
	ChangeSignificance string `json:"changeSignificance,omitempty"`
}

// validChangeSignificance is the closed set of push-CD significance levels.
var validChangeSignificance = map[string]bool{"major": true, "minor": true, "patch": true}

func (s *Server) changeSummary(w http.ResponseWriter, r *http.Request) {
	var req changeSummaryReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.PRTitle != "" {
		if weak, guidance := titlecheck.Weak(req.PRTitle); weak {
			writeError(w, http.StatusBadRequest, "weak pr title: "+guidance)
			return
		}
	}
	if !validChangeSignificance[req.ChangeSignificance] {
		writeError(w, http.StatusBadRequest, "change significance must be one of major|minor|patch")
		return
	}
	// M1: kind gate (unlike issue_outcome/implement_outcome, this handler had
	// none - any kind, including project-scoped kinds that never open a PR,
	// could carry a RemainingScope value that a writeback path might later
	// trust). Project-scoped kinds (incident/healthCheck/brainstorm) are
	// rejected; every other kind can open a change and legitimately posts a
	// change summary.
	s.mutateTaskStatus(w, r, mutateTaskStatusParams{
		metricName: "change_summary",
		logMsg:     "restapi: changeSummary",
		logAction:  "change_summary",
		kindOK:     func(kind string) bool { return !tatarav1alpha1.IsProjectScopedKind(kind) },
		kindErrMsg: "change summary does not apply to a project-scoped task",
		mutate: func(t *tatarav1alpha1.Task) {
			t.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
				PRTitle:         req.PRTitle,
				PRBody:          req.PRBody,
				DeliveredScope:  req.DeliveredScope,
				RemainingScope:  req.RemainingScope,
				MostProblematic: req.MostProblematic,
				Significance:    req.ChangeSignificance,
			}
		},
	})
}

// --- M3: POST /tasks/{t}/handover ---

const handoverMaxBytes = 16 * 1024 // 16 KB cap

type handoverReq struct {
	Handover string `json:"handover"`
}

// truncateValidUTF8 cuts s to at most maxBytes bytes on a rune boundary.
func truncateValidUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	cap := maxBytes
	for cap > 0 && !utf8.RuneStart(s[cap]) {
		cap--
	}
	return s[:cap]
}

func (s *Server) handover(w http.ResponseWriter, r *http.Request) {
	var req handoverReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	// Cap at 16KB on a rune boundary to avoid splitting multi-byte UTF-8 sequences.
	if len(req.Handover) > handoverMaxBytes {
		req.Handover = truncateValidUTF8(req.Handover, handoverMaxBytes)
	}
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	var t tatarav1alpha1.Task
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeCaller(w, r) {
		return
	}
	start := time.Now()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if gerr := s.c.Get(r.Context(), key, &t); gerr != nil {
			return gerr
		}
		t.Status.Handover = req.Handover
		return s.c.Status().Update(r.Context(), &t)
	}); err != nil {
		if s.metrics != nil {
			s.metrics.RecordRESTRequest("handover", "error", time.Since(start).Seconds())
		}
		writeClientErr(w, err)
		return
	}
	elapsed := time.Since(start)
	if s.metrics != nil {
		s.metrics.RecordRESTRequest("handover", "ok", elapsed.Seconds())
	}
	s.log.InfoContext(r.Context(), "restapi: handover",
		append(reqLogFields(r),
			"action", "handover",
			"resource_id", key.Name,
			"duration_ms", elapsed.Milliseconds())...)
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

// --- Task 7: PATCH /subtasks/{s} ---

type subtaskPatchReq struct {
	Phase  *string `json:"phase,omitempty"`
	Result *string `json:"result,omitempty"`
	TurnID *string `json:"turnId,omitempty"`
}

// validSubtaskPhases enumerates the canonical SubtaskStatus.Phase values, matching
// the +kubebuilder:validation:Enum on api/v1alpha1.SubtaskStatus. Writing any other
// value is rejected by the CRD status schema with an opaque admission error, so we
// normalize and validate at the REST boundary instead.
var validSubtaskPhases = []string{"Pending", "Running", "Done", "Failed"}

// canonicalSubtaskPhase maps a caller-supplied phase to its canonical enum value
// case-insensitively (so "done" becomes "Done"), returning ok=false for any value
// that is not a recognized phase.
func canonicalSubtaskPhase(phase string) (string, bool) {
	for _, p := range validSubtaskPhases {
		if strings.EqualFold(phase, p) {
			return p, true
		}
	}
	return "", false
}

func (s *Server) patchSubtask(w http.ResponseWriter, r *http.Request) {
	var req subtaskPatchReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if !authorizeCaller(w, r) {
		return
	}
	// Normalize the phase to its canonical enum value case-insensitively so a
	// lowercase "done" no longer reaches the CRD status schema as an invalid
	// value (which surfaces as an opaque admission rejection). Reject genuinely
	// unknown phases here with a clear 400 instead.
	if req.Phase != nil {
		canonical, ok := canonicalSubtaskPhase(*req.Phase)
		if !ok {
			writeError(w, http.StatusBadRequest, "phase must be one of Pending, Running, Done, Failed")
			return
		}
		req.Phase = &canonical
	}
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "s")}
	var st tatarav1alpha1.Subtask
	start := time.Now()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if gerr := s.c.Get(r.Context(), key, &st); gerr != nil {
			return gerr
		}
		if req.Phase != nil {
			st.Status.Phase = *req.Phase
		}
		if req.Result != nil {
			st.Status.Result = *req.Result
		}
		if req.TurnID != nil {
			st.Status.TurnID = *req.TurnID
		}
		return s.c.Status().Update(r.Context(), &st)
	}); err != nil {
		if s.metrics != nil {
			s.metrics.RecordRESTRequest("patch_subtask", "error", time.Since(start).Seconds())
		}
		writeClientErr(w, err)
		return
	}
	elapsed := time.Since(start)
	if s.metrics != nil {
		s.metrics.RecordRESTRequest("patch_subtask", "ok", elapsed.Seconds())
	}
	s.log.InfoContext(r.Context(), "restapi: patchSubtask",
		append(reqLogFields(r),
			"action", "patch_subtask",
			"resource_id", key.Name,
			"duration_ms", elapsed.Milliseconds())...)
	writeJSON(w, http.StatusOK, toSubtaskDTO(st))
}

// --- Refine agent: issue + commit aggregation and mutation endpoints ---

// issueDTO is the wire type for GET /projects/{p}/issues.
type issueDTO struct {
	Repo     string    `json:"repo"`
	Number   int       `json:"number"`
	Title    string    `json:"title"`
	Body     string    `json:"body,omitempty"`
	Author   string    `json:"author,omitempty"`
	Labels   []string  `json:"labels,omitempty"`
	State    string    `json:"state,omitempty"`
	ClosedAt time.Time `json:"closedAt,omitempty"`
	IsPR     bool      `json:"isPr,omitempty"`
}

// commitDTO is the wire type for GET /projects/{p}/commits.
type commitDTO struct {
	Repo    string    `json:"repo"`
	SHA     string    `json:"sha"`
	Message string    `json:"message"`
	Author  string    `json:"author,omitempty"`
	Date    time.Time `json:"date"`
}

// resolveProjectSCMProviderToken resolves the project's SCM provider name and
// raw bot token from its ScmSecretRef. It does not check for an empty token -
// callers that must reject an empty token do so themselves, since Reader and
// Writer disagree on whether that is an error (Finding: do not add Reader's
// empty-token check to Writer's caller as a byproduct of this shared helper).
func (s *Server) resolveProjectSCMProviderToken(w http.ResponseWriter, r *http.Request, proj *tatarav1alpha1.Project) (provider, token string, ok bool) {
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	if provider == "" {
		writeError(w, http.StatusConflict, "project has no scm provider configured")
		return "", "", false
	}
	var sec corev1.Secret
	if err := s.c.Get(r.Context(), types.NamespacedName{Namespace: s.ns, Name: proj.Spec.ScmSecretRef}, &sec); err != nil {
		writeClientErr(w, err)
		return "", "", false
	}
	token = string(sec.Data["token"])
	return provider, token, true
}

// projectSCMWriterAndToken resolves the SCMWriter and bot token for project p.
// Returns (nil, "", error-written-to-w) on any failure so callers can return immediately.
func (s *Server) projectSCMWriterAndToken(w http.ResponseWriter, r *http.Request, proj *tatarav1alpha1.Project) (scm.SCMWriter, string, bool) {
	if s.scmFor == nil {
		writeError(w, http.StatusNotImplemented, "scm writer not configured")
		return nil, "", false
	}
	provider, token, ok := s.resolveProjectSCMProviderToken(w, r, proj)
	if !ok {
		return nil, "", false
	}
	writer, err := s.scmFor(provider)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return nil, "", false
	}
	if token == "" {
		writeError(w, http.StatusInternalServerError, "internal error")
		return nil, "", false
	}
	return writer, token, true
}

// projectSCMReader resolves a token-bound SCMReader for project p.
func (s *Server) projectSCMReader(w http.ResponseWriter, r *http.Request, proj *tatarav1alpha1.Project) (scm.SCMReader, string, bool) {
	if s.readerFor == nil {
		writeError(w, http.StatusNotImplemented, "scm reader not configured")
		return nil, "", false
	}
	provider, token, ok := s.resolveProjectSCMProviderToken(w, r, proj)
	if !ok {
		return nil, "", false
	}
	reader, err := s.readerFor(provider, token)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return nil, "", false
	}
	return reader, token, true
}

// projectRepos returns all repositories belonging to project projName.
func (s *Server) projectRepos(ctx context.Context, projName string) ([]tatarav1alpha1.Repository, error) {
	var list tatarav1alpha1.RepositoryList
	if err := s.c.List(ctx, &list, client.InNamespace(s.ns)); err != nil {
		return nil, err
	}
	var out []tatarav1alpha1.Repository
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == projName {
			out = append(out, list.Items[i])
		}
	}
	return out, nil
}

// repoSlugInProject checks whether the given slug ("owner/repo") matches a repo
// in the project and returns the repo's clone URL.  Returns ("", false) if not found.
func repoSlugInProject(repos []tatarav1alpha1.Repository, slug string) (string, bool) {
	for i := range repos {
		o, n, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		if o+"/"+n == slug {
			return repos[i].Spec.URL, true
		}
	}
	return "", false
}

// listProjectIssues handles GET /projects/{p}/issues.
// Query params: closedSinceDays (int, default 30).
func (s *Server) listProjectIssues(w http.ResponseWriter, r *http.Request) {
	projName := chi.URLParam(r, "p")
	var proj tatarav1alpha1.Project
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: projName}, &proj); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeCaller(w, r) {
		return
	}
	reader, _, ok := s.projectSCMReader(w, r, &proj)
	if !ok {
		return
	}
	repos, err := s.projectRepos(r.Context(), projName)
	if err != nil {
		writeClientErr(w, err)
		return
	}

	closedSinceDays := 30
	if v := r.URL.Query().Get("closedSinceDays"); v != "" {
		if n, err2 := parseInt(v); err2 == nil && n > 0 {
			closedSinceDays = n
		}
	}
	since := time.Now().Add(-time.Duration(closedSinceDays) * 24 * time.Hour)

	var issues []issueDTO
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		open, err := reader.ListOpenIssues(r.Context(), owner, name)
		if err != nil {
			s.log.ErrorContext(r.Context(), "restapi: listProjectIssues ListOpenIssues failed",
				append(reqLogFields(r), "repo", repos[i].Name, "err", err)...)
			continue
		}
		for _, iss := range open {
			if iss.IsPR {
				continue
			}
			issues = append(issues, issueDTO{
				Repo: iss.Repo, Number: iss.Number, Title: iss.Title,
				Author: iss.Author, Labels: iss.Labels, State: iss.State, IsPR: false,
			})
		}
		closed, err := reader.ListClosedIssues(r.Context(), owner, name, since)
		if err != nil {
			s.log.ErrorContext(r.Context(), "restapi: listProjectIssues ListClosedIssues failed",
				append(reqLogFields(r), "repo", repos[i].Name, "err", err)...)
			continue
		}
		for _, iss := range closed {
			if iss.IsPR {
				continue
			}
			issues = append(issues, issueDTO{
				Repo: iss.Repo, Number: iss.Number, Title: iss.Title,
				Author: iss.Author, Labels: iss.Labels, State: iss.State,
				ClosedAt: iss.ClosedAt, IsPR: false,
			})
		}
	}
	if issues == nil {
		issues = []issueDTO{}
	}
	s.log.InfoContext(r.Context(), "restapi: listProjectIssues",
		append(reqLogFields(r), "action", "list_project_issues", "resource_id", projName, "count", len(issues))...)
	if s.metrics != nil {
		s.metrics.RecordRESTRequest("list_project_issues", "ok", 0)
	}
	writeJSON(w, http.StatusOK, map[string]any{"issues": issues})
}

type closeIssueReq struct {
	Comment string `json:"comment"`
}

// closeProjectIssue handles POST /projects/{p}/issues/{owner}/{repo}/{number}/close.
func (s *Server) closeProjectIssue(w http.ResponseWriter, r *http.Request) {
	projName := chi.URLParam(r, "p")
	repoSlug := chi.URLParam(r, "owner") + "/" + chi.URLParam(r, "repo")
	number, err := parseInt(chi.URLParam(r, "number"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid issue number")
		return
	}
	var req closeIssueReq
	if err2 := decodeJSON(r, w, &req); err2 != nil {
		writeDecodeError(w, r, err2)
		return
	}
	if req.Comment == "" {
		writeError(w, http.StatusBadRequest, "comment required")
		return
	}
	var proj tatarav1alpha1.Project
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: projName}, &proj); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeCaller(w, r) {
		return
	}
	repos, err := s.projectRepos(r.Context(), projName)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	_, ok := repoSlugInProject(repos, repoSlug)
	if !ok {
		writeError(w, http.StatusBadRequest, "repo not found in project")
		return
	}
	// Hard refusal: refine must not close an issue an implement/review/clarify pod
	// is actively working (a non-terminal Task for this issue). The legit close of a
	// delivered/duplicate issue with no live task still passes.
	if s.hasLiveTaskForIssue(r.Context(), repoSlug, number) {
		writeError(w, http.StatusConflict, "issue has an active task; cannot close while work is in flight")
		return
	}
	writer, token, ok := s.projectSCMWriterAndToken(w, r, &proj)
	if !ok {
		return
	}
	start := time.Now()
	if err := writer.CloseIssue(r.Context(), token, repoSlug, number, req.Comment); err != nil {
		if s.metrics != nil {
			s.metrics.RecordRESTRequest("close_project_issue", "error", time.Since(start).Seconds())
		}
		writeClientErr(w, err)
		return
	}
	// Best-effort: propagate the close to any Task work-item ledger entries so
	// issueScan dedup sees the updated state without waiting for the next cycle.
	if serr := markWorkItemsClosedViaClient(r.Context(), s.c, s.ns, repoSlug, number); serr != nil {
		s.log.WarnContext(r.Context(), "restapi: closeProjectIssue: ledger propagation failed (best-effort)",
			"action", "ledger_close_error", "resource_id", repoSlug+"#"+chi.URLParam(r, "number"), "error", serr)
	}
	elapsed := time.Since(start)
	if s.metrics != nil {
		s.metrics.RecordRESTRequest("close_project_issue", "ok", elapsed.Seconds())
	}
	s.log.InfoContext(r.Context(), "restapi: closeProjectIssue",
		append(reqLogFields(r), "action", "refine_close", "resource_id", repoSlug+"#"+chi.URLParam(r, "number"), "duration_ms", elapsed.Milliseconds())...)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type editIssueReq struct {
	Title *string `json:"title,omitempty"`
	Body  *string `json:"body,omitempty"`
	// No Labels field: edit_issue (the refine agent's only mutation tool besides
	// close) must never set labels. Labels drive the lifecycle (incl. the trigger
	// label), so allowing a label delta here would let the refiner self-escalate.
	// Label/phase state stays operator/maintainer controlled.
}

// editProjectIssue handles PATCH /projects/{p}/issues/{owner}/{repo}/{number}.
func (s *Server) editProjectIssue(w http.ResponseWriter, r *http.Request) {
	projName := chi.URLParam(r, "p")
	repoSlug := chi.URLParam(r, "owner") + "/" + chi.URLParam(r, "repo")
	number, err := parseInt(chi.URLParam(r, "number"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid issue number")
		return
	}
	var req editIssueReq
	if err2 := decodeJSON(r, w, &req); err2 != nil {
		writeDecodeError(w, r, err2)
		return
	}
	var proj tatarav1alpha1.Project
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: projName}, &proj); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeCaller(w, r) {
		return
	}
	repos, err := s.projectRepos(r.Context(), projName)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	_, ok := repoSlugInProject(repos, repoSlug)
	if !ok {
		writeError(w, http.StatusBadRequest, "repo not found in project")
		return
	}
	// Hard refusal: refine must not rewrite an issue an implement/review/clarify pod
	// is actively working (a non-terminal Task for this issue), which would change
	// the goal out from under a live pod.
	if s.hasLiveTaskForIssue(r.Context(), repoSlug, number) {
		writeError(w, http.StatusConflict, "issue has an active task; cannot edit while work is in flight")
		return
	}
	writer, token, ok := s.projectSCMWriterAndToken(w, r, &proj)
	if !ok {
		return
	}
	editReq := scm.EditIssueReq{Title: req.Title, Body: req.Body}
	start := time.Now()
	if err := writer.EditIssue(r.Context(), token, repoSlug, number, editReq); err != nil {
		if s.metrics != nil {
			s.metrics.RecordRESTRequest("edit_project_issue", "error", time.Since(start).Seconds())
		}
		writeClientErr(w, err)
		return
	}
	elapsed := time.Since(start)
	if s.metrics != nil {
		s.metrics.RecordRESTRequest("edit_project_issue", "ok", elapsed.Seconds())
	}
	s.log.InfoContext(r.Context(), "restapi: editProjectIssue",
		append(reqLogFields(r), "action", "refine_edit", "resource_id", repoSlug+"#"+chi.URLParam(r, "number"), "duration_ms", elapsed.Milliseconds())...)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type createProjectIssueReq struct {
	Title  string   `json:"title"`
	Body   string   `json:"body"`
	Labels []string `json:"labels,omitempty"`
}

// createProjectIssue handles POST /projects/{p}/issues/{owner}/{repo}.
func (s *Server) createProjectIssue(w http.ResponseWriter, r *http.Request) {
	projName := chi.URLParam(r, "p")
	repoSlug := chi.URLParam(r, "owner") + "/" + chi.URLParam(r, "repo")
	var req createProjectIssueReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Title == "" || req.Body == "" {
		writeError(w, http.StatusBadRequest, "title and body required")
		return
	}
	var proj tatarav1alpha1.Project
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: projName}, &proj); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeCaller(w, r) {
		return
	}
	repos, err := s.projectRepos(r.Context(), projName)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	repoURL, ok := repoSlugInProject(repos, repoSlug)
	if !ok {
		writeError(w, http.StatusBadRequest, "repo not found in project")
		return
	}
	writer, token, ok := s.projectSCMWriterAndToken(w, r, &proj)
	if !ok {
		return
	}
	issueReq := scm.IssueReq{Title: req.Title, Body: req.Body, Labels: req.Labels}
	start := time.Now()
	created, err := writer.CreateIssue(r.Context(), repoURL, token, issueReq)
	if err != nil {
		if s.metrics != nil {
			s.metrics.RecordRESTRequest("create_project_issue", "error", time.Since(start).Seconds())
		}
		writeClientErr(w, err)
		return
	}
	elapsed := time.Since(start)
	if s.metrics != nil {
		s.metrics.RecordRESTRequest("create_project_issue", "ok", elapsed.Seconds())
	}
	s.log.InfoContext(r.Context(), "restapi: createProjectIssue",
		append(reqLogFields(r), "action", "refine_create", "resource_id", created.Ref, "duration_ms", elapsed.Milliseconds())...)
	writeJSON(w, http.StatusCreated, created)
}

// listProjectCommits handles GET /projects/{p}/commits.
// Query params: sinceDays (int, default 30).
func (s *Server) listProjectCommits(w http.ResponseWriter, r *http.Request) {
	projName := chi.URLParam(r, "p")
	var proj tatarav1alpha1.Project
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: projName}, &proj); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeCaller(w, r) {
		return
	}
	reader, _, ok := s.projectSCMReader(w, r, &proj)
	if !ok {
		return
	}
	repos, err := s.projectRepos(r.Context(), projName)
	if err != nil {
		writeClientErr(w, err)
		return
	}

	sinceDays := 30
	if v := r.URL.Query().Get("sinceDays"); v != "" {
		if n, err2 := parseInt(v); err2 == nil && n > 0 {
			sinceDays = n
		}
	}
	since := time.Now().Add(-time.Duration(sinceDays) * 24 * time.Hour)

	var commits []commitDTO
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		repoCommits, err := reader.ListCommits(r.Context(), owner, name, since)
		if err != nil {
			s.log.ErrorContext(r.Context(), "restapi: listProjectCommits ListCommits failed",
				append(reqLogFields(r), "repo", repos[i].Name, "err", err)...)
			continue
		}
		for _, c := range repoCommits {
			commits = append(commits, commitDTO{
				Repo: owner + "/" + name, SHA: c.SHA, Message: c.Message,
				Author: c.Author, Date: c.Date,
			})
		}
	}
	if commits == nil {
		commits = []commitDTO{}
	}
	s.log.InfoContext(r.Context(), "restapi: listProjectCommits",
		append(reqLogFields(r), "action", "list_project_commits", "resource_id", projName, "count", len(commits))...)
	if s.metrics != nil {
		s.metrics.RecordRESTRequest("list_project_commits", "ok", 0)
	}
	writeJSON(w, http.StatusOK, map[string]any{"commits": commits})
}

// parseInt parses a decimal integer from s.
func parseInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

// markWorkItemsClosedViaClient marks all WorkItem entries matching (repo, number)
// as closed in every Task in ns. Best-effort: conflict retries are not applied
// because this is a secondary propagation path (issueScan self-heals on the next
// cycle if this fails).
func markWorkItemsClosedViaClient(ctx context.Context, c client.Client, ns, repo string, number int) error {
	var list tatarav1alpha1.TaskList
	if err := c.List(ctx, &list, client.InNamespace(ns)); err != nil {
		return fmt.Errorf("markWorkItemsClosedViaClient: list tasks: %w", err)
	}
	for i := range list.Items {
		task := &list.Items[i]
		updated := false
		for j := range task.Status.WorkItems {
			wi := &task.Status.WorkItems[j]
			if wi.Repo == repo && wi.Number == number && wi.State != "closed" {
				wi.State = "closed"
				updated = true
			}
		}
		if !updated {
			continue
		}
		// Read-modify-write once; ignore conflict (next cycle self-heals).
		fresh := &tatarav1alpha1.Task{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			continue
		}
		for j := range fresh.Status.WorkItems {
			wi := &fresh.Status.WorkItems[j]
			if wi.Repo == repo && wi.Number == number {
				wi.State = "closed"
			}
		}
		_ = c.Status().Update(ctx, fresh)
	}
	return nil
}

// --- Harness-state endpoints: GET/POST /projects/{p}/harness-state/{key} ---

func (s *Server) getHarnessState(w http.ResponseWriter, r *http.Request) {
	projName := chi.URLParam(r, "p")
	var proj tatarav1alpha1.Project
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: projName}, &proj); err != nil {
		writeClientErr(w, err)
		return
	}
	key := chi.URLParam(r, "key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key required")
		return
	}
	store := harness.Store{Client: s.c, Namespace: s.ns}
	entry, err := store.Get(r.Context(), projName, key)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, entry)
}

type harnessCASReq struct {
	Value   string `json:"value"`
	Version string `json:"version"`
}

func (s *Server) casHarnessState(w http.ResponseWriter, r *http.Request) {
	projName := chi.URLParam(r, "p")
	var proj tatarav1alpha1.Project
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: projName}, &proj); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeCaller(w, r) {
		return
	}
	key := chi.URLParam(r, "key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key required")
		return
	}
	var req harnessCASReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	start := time.Now()
	store := harness.Store{Client: s.c, Namespace: s.ns}
	entry, err := store.CAS(r.Context(), projName, key, req.Value, req.Version)
	if errors.Is(err, harness.ErrConflict) {
		if s.metrics != nil {
			s.metrics.RecordRESTRequest("harness_state_cas", "conflict", time.Since(start).Seconds())
		}
		writeError(w, http.StatusConflict, "harness-state version conflict; re-read and retry")
		return
	}
	if err != nil {
		if s.metrics != nil {
			s.metrics.RecordRESTRequest("harness_state_cas", "error", time.Since(start).Seconds())
		}
		writeClientErr(w, err)
		return
	}
	elapsed := time.Since(start)
	if s.metrics != nil {
		s.metrics.RecordRESTRequest("harness_state_cas", "ok", elapsed.Seconds())
	}
	logFields := append(reqLogFields(r), "action", "harness_state_cas",
		"resource_id", projName, "key", key, "duration_ms", elapsed.Milliseconds())
	s.log.InfoContext(r.Context(), "restapi: casHarnessState", logFields...)
	writeJSON(w, http.StatusOK, entry)
}
