package restapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
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
	"github.com/szymonrychu/tatara-operator/internal/scm"
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

// authorizeForTask checks that the caller's OIDC identity (Claims.Subject or
// PreferredUsername) matches the expected agent pod for the given Task.
// When no Claims are present in the context (auth middleware absent, e.g. tests),
// the check is skipped. Returns false and writes a 403 when authorization fails.
func authorizeForTask(w http.ResponseWriter, r *http.Request, t *tatarav1alpha1.Task) bool {
	claims, ok := auth.ClaimsFromContext(r.Context())
	if !ok {
		// No auth middleware in this path; skip enforcement.
		return true
	}
	podName := agent.PodName(t)
	if claims.Subject == podName || claims.PreferredUsername == podName {
		return true
	}
	writeError(w, http.StatusForbidden, "caller is not the agent for this task")
	return false
}

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
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	var t tatarav1alpha1.Task
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeForTask(w, r, &t) {
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
		writeClientErr(w, err)
		return
	}
	s.log.InfoContext(r.Context(), "restapi: patchTask",
		"action", "patch_task",
		"resource_id", key.Name,
		"duration_ms", time.Since(start).Milliseconds())
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
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
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
			Name:      fmt.Sprintf("%s-st-%d", taskName, time.Now().UnixNano()),
			Namespace: s.ns,
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
		writeClientErr(w, err)
		return
	}
	s.log.InfoContext(r.Context(), "restapi: createSubtask",
		"action", "create_subtask",
		"resource_id", st.Name,
		"task", taskName,
		"duration_ms", time.Since(start).Milliseconds())
	writeJSON(w, http.StatusCreated, toSubtaskDTO(*st))
}

// --- Task 11: POST /projects/{p}/issues, /tasks/{t}/review, /tasks/{t}/pr-outcome ---

type proposeIssueReq struct {
	RepositoryRef string `json:"repositoryRef"`
	Title         string `json:"title"`
	Body          string `json:"body"`
	Kind          string `json:"kind"`
}

func (s *Server) proposeIssue(w http.ResponseWriter, r *http.Request) {
	var req proposeIssueReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
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
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "task-",
			Namespace:    s.ns,
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
	s.log.InfoContext(r.Context(), "restapi: proposeIssue",
		"action", "propose_issue",
		"resource_id", task.Name,
		"project", projName,
		"repository", req.RepositoryRef,
		"duration_ms", time.Since(start).Milliseconds())
	if s.metrics != nil {
		s.metrics.ScanTaskCreated("propose", "implement")
	}
	// The proposal Task starts Pending; the controller opens the idea-labelled
	// issue and completes the Task (Succeeded). No AwaitingApproval parking.
	writeJSON(w, http.StatusCreated, toTaskDTO(*task))
}

// --- POST /projects/{p}/issue-comment ---

type issueCommentOnProjectReq struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
	Body   string `json:"body"`
}

// commentOnIssue posts a comment on a specific issue via the Project's SCM
// provider and bot token. This is the egress-mediation path for the
// comment_on_issue tool call from a brainstorm agent.
//
// Request body: { repo: "owner/repo", number: N, body: "..." }
// Response:     200 { status: "ok" }
// Errors:       400 missing/zero fields; 404 project/repo not found; 500 SCM error.
func (s *Server) commentOnIssue(w http.ResponseWriter, r *http.Request) {
	var req issueCommentOnProjectReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
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

	// Find the matching Repository by slug (owner/repo from URL).
	var repoList tatarav1alpha1.RepositoryList
	if err := s.c.List(r.Context(), &repoList, client.InNamespace(s.ns)); err != nil {
		writeClientErr(w, err)
		return
	}
	var matchedRepoURL string
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

	start := time.Now()
	issueRef := fmt.Sprintf("%s#%d", req.Repo, req.Number)
	commentErr := writer.Comment(r.Context(), token, issueRef, req.Body)
	result := "ok"
	if commentErr != nil {
		result = "error"
	}
	if s.metrics != nil {
		s.metrics.SCMWrite(provider, "comment", result)
	}
	if commentErr != nil {
		s.log.ErrorContext(r.Context(), "restapi: commentOnIssue scm write failed",
			"err", commentErr, "project", projName, "repo", req.Repo)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	s.log.InfoContext(r.Context(), "restapi: commentOnIssue",
		"action", "scm_issue_comment",
		"project", projName,
		"repo", req.Repo,
		"number", req.Number,
		"duration_ms", time.Since(start).Milliseconds())

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type reviewVerdictReq struct {
	Decision    string                      `json:"decision"`
	Body        string                      `json:"body,omitempty"`
	Suggestions []tatarav1alpha1.Suggestion `json:"suggestions,omitempty"`
}

func (s *Server) reviewVerdict(w http.ResponseWriter, r *http.Request) {
	var req reviewVerdictReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
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
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	var t tatarav1alpha1.Task
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeForTask(w, r, &t) {
		return
	}
	if t.Spec.Kind != "review" {
		writeError(w, http.StatusConflict, "review verdict only applies to a review task")
		return
	}
	start := time.Now()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if gerr := s.c.Get(r.Context(), key, &t); gerr != nil {
			return gerr
		}
		if t.Spec.Kind != "review" {
			return nil
		}
		t.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: req.Decision, Body: req.Body, Suggestions: req.Suggestions}
		return s.c.Status().Update(r.Context(), &t)
	}); err != nil {
		writeClientErr(w, err)
		return
	}
	if t.Spec.Kind != "review" {
		writeError(w, http.StatusConflict, "review verdict only applies to a review task")
		return
	}
	s.log.InfoContext(r.Context(), "restapi: reviewVerdict",
		"action", "review_verdict",
		"resource_id", key.Name,
		"decision", req.Decision,
		"duration_ms", time.Since(start).Milliseconds())
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

type prOutcomeReq struct {
	Action string `json:"action"`
	Reason string `json:"reason,omitempty"`
}

func (s *Server) prOutcome(w http.ResponseWriter, r *http.Request) {
	var req prOutcomeReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
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
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	var t tatarav1alpha1.Task
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeForTask(w, r, &t) {
		return
	}
	if t.Spec.Kind != "selfImprove" {
		writeError(w, http.StatusConflict, "pr outcome only applies to a selfImprove task")
		return
	}
	start := time.Now()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if gerr := s.c.Get(r.Context(), key, &t); gerr != nil {
			return gerr
		}
		if t.Spec.Kind != "selfImprove" {
			return nil
		}
		t.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: req.Action, Reason: req.Reason}
		return s.c.Status().Update(r.Context(), &t)
	}); err != nil {
		writeClientErr(w, err)
		return
	}
	if t.Spec.Kind != "selfImprove" {
		writeError(w, http.StatusConflict, "pr outcome only applies to a selfImprove task")
		return
	}
	s.log.InfoContext(r.Context(), "restapi: prOutcome",
		"action", "pr_outcome",
		"resource_id", key.Name,
		"pr_action", req.Action,
		"duration_ms", time.Since(start).Milliseconds())
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

type issueOutcomeReq struct {
	Action  string `json:"action"`
	Comment string `json:"comment,omitempty"`
	Plan    string `json:"plan,omitempty"` // posted as implementation-start message when action==implement
}

func (s *Server) issueOutcome(w http.ResponseWriter, r *http.Request) {
	var req issueOutcomeReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
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
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	var t tatarav1alpha1.Task
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeForTask(w, r, &t) {
		return
	}
	if t.Spec.Kind != "triageIssue" && t.Spec.Kind != "issueLifecycle" {
		writeError(w, http.StatusConflict, "issue outcome only applies to a triageIssue or issueLifecycle task")
		return
	}
	start := time.Now()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if gerr := s.c.Get(r.Context(), key, &t); gerr != nil {
			return gerr
		}
		if t.Spec.Kind != "triageIssue" && t.Spec.Kind != "issueLifecycle" {
			return nil
		}
		t.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: req.Action, Comment: req.Comment, Plan: req.Plan}
		return s.c.Status().Update(r.Context(), &t)
	}); err != nil {
		writeClientErr(w, err)
		return
	}
	if t.Spec.Kind != "triageIssue" && t.Spec.Kind != "issueLifecycle" {
		writeError(w, http.StatusConflict, "issue outcome only applies to a triageIssue or issueLifecycle task")
		return
	}
	s.log.InfoContext(r.Context(), "restapi: issueOutcome",
		"action", "issue_outcome",
		"resource_id", key.Name,
		"issue_action", req.Action,
		"duration_ms", time.Since(start).Milliseconds())
	if s.metrics != nil {
		s.metrics.IssueOutcome(req.Action)
	}
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

// --- POST /tasks/{t}/implement-outcome ---

type implementOutcomeReq struct {
	Action string `json:"action"`
	Reason string `json:"reason"`
}

func (s *Server) implementOutcome(w http.ResponseWriter, r *http.Request) {
	var req implementOutcomeReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if req.Action == "" {
		writeError(w, http.StatusBadRequest, "action required")
		return
	}
	if req.Action != "declined" {
		writeError(w, http.StatusBadRequest, "action must be declined")
		return
	}
	if strings.TrimSpace(req.Reason) == "" {
		writeError(w, http.StatusBadRequest, "reason required when action is declined")
		return
	}
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	var t tatarav1alpha1.Task
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeForTask(w, r, &t) {
		return
	}
	if t.Spec.Kind != "issueLifecycle" {
		writeError(w, http.StatusConflict, "implement outcome only applies to an issueLifecycle task")
		return
	}
	start := time.Now()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if gerr := s.c.Get(r.Context(), key, &t); gerr != nil {
			return gerr
		}
		if t.Spec.Kind != "issueLifecycle" {
			return nil
		}
		t.Status.ImplementOutcome = &tatarav1alpha1.ImplementOutcome{Action: req.Action, Reason: req.Reason}
		return s.c.Status().Update(r.Context(), &t)
	}); err != nil {
		writeClientErr(w, err)
		return
	}
	if t.Spec.Kind != "issueLifecycle" {
		writeError(w, http.StatusConflict, "implement outcome only applies to an issueLifecycle task")
		return
	}
	s.log.InfoContext(r.Context(), "restapi: implementOutcome",
		"action", "implement_outcome",
		"resource_id", key.Name,
		"impl_action", req.Action,
		"duration_ms", time.Since(start).Milliseconds())
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

// --- POST /tasks/{t}/comment ---

type issueCommentReq struct {
	Body string `json:"body"`
}

func (s *Server) postComment(w http.ResponseWriter, r *http.Request) {
	var req issueCommentReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
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
	if !authorizeForTask(w, r, &t) {
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
		fresh.Status.PendingComments = append(fresh.Status.PendingComments, req.Body)
		if uerr := s.c.Status().Update(r.Context(), &fresh); uerr != nil {
			return uerr
		}
		t = fresh
		return nil
	}); err != nil {
		writeClientErr(w, err)
		return
	}
	s.log.InfoContext(r.Context(), "restapi: postComment",
		"action", "post_comment",
		"resource_id", key.Name,
		"duration_ms", time.Since(start).Milliseconds())
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

// --- M4 Task 2: POST /tasks/{t}/change-summary ---

type changeSummaryReq struct {
	PRTitle         string `json:"prTitle,omitempty"`
	PRBody          string `json:"prBody,omitempty"`
	DeliveredScope  string `json:"deliveredScope,omitempty"`
	RemainingScope  string `json:"remainingScope,omitempty"`
	MostProblematic string `json:"mostProblematic,omitempty"` // from cli most_problematic field
}

func (s *Server) changeSummary(w http.ResponseWriter, r *http.Request) {
	var req changeSummaryReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	var t tatarav1alpha1.Task
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	if !authorizeForTask(w, r, &t) {
		return
	}
	start := time.Now()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if gerr := s.c.Get(r.Context(), key, &t); gerr != nil {
			return gerr
		}
		t.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
			PRTitle:         req.PRTitle,
			PRBody:          req.PRBody,
			DeliveredScope:  req.DeliveredScope,
			RemainingScope:  req.RemainingScope,
			MostProblematic: req.MostProblematic,
		}
		return s.c.Status().Update(r.Context(), &t)
	}); err != nil {
		writeClientErr(w, err)
		return
	}
	s.log.InfoContext(r.Context(), "restapi: changeSummary",
		"action", "change_summary",
		"resource_id", key.Name,
		"duration_ms", time.Since(start).Milliseconds())
	writeJSON(w, http.StatusOK, toTaskDTO(t))
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
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
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
	if !authorizeForTask(w, r, &t) {
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
		writeClientErr(w, err)
		return
	}
	s.log.InfoContext(r.Context(), "restapi: handover",
		"action", "handover",
		"resource_id", key.Name,
		"duration_ms", time.Since(start).Milliseconds())
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

// --- Task 7: PATCH /subtasks/{s} ---

type subtaskPatchReq struct {
	Phase  *string `json:"phase,omitempty"`
	Result *string `json:"result,omitempty"`
	TurnID *string `json:"turnId,omitempty"`
}

func (s *Server) patchSubtask(w http.ResponseWriter, r *http.Request) {
	var req subtaskPatchReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
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
		writeClientErr(w, err)
		return
	}
	s.log.InfoContext(r.Context(), "restapi: patchSubtask",
		"action", "patch_subtask",
		"resource_id", key.Name,
		"duration_ms", time.Since(start).Milliseconds())
	writeJSON(w, http.StatusOK, toSubtaskDTO(st))
}
