package restapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/go-chi/chi/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeClientErr(w http.ResponseWriter, err error) {
	if apierrors.IsNotFound(err) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeError(w, http.StatusInternalServerError, err.Error())
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
	proj := chi.URLParam(r, "p")
	var list tatarav1alpha1.RepositoryList
	if err := s.c.List(r.Context(), &list, client.InNamespace(s.ns)); err != nil {
		writeClientErr(w, err)
		return
	}
	out := make([]RepositoryDTO, 0)
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == proj {
			out = append(out, toRepositoryDTO(list.Items[i]))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	proj := chi.URLParam(r, "p")
	var list tatarav1alpha1.TaskList
	if err := s.c.List(r.Context(), &list, client.InNamespace(s.ns)); err != nil {
		writeClientErr(w, err)
		return
	}
	out := make([]TaskDTO, 0)
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == proj {
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

func decodeJSON(r *http.Request, dst any) error {
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
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	var t tatarav1alpha1.Task
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
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
	if err := s.c.Status().Update(r.Context(), &t); err != nil {
		writeClientErr(w, err)
		return
	}
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
	if err := decodeJSON(r, &req); err != nil {
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
	if err := s.c.Create(r.Context(), st); err != nil {
		writeClientErr(w, err)
		return
	}
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
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if req.Title == "" || req.Body == "" || req.Kind == "" || req.RepositoryRef == "" {
		writeError(w, http.StatusBadRequest, "repositoryRef, title, body, kind required")
		return
	}
	projName := chi.URLParam(r, "p")
	var proj tatarav1alpha1.Project
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: projName}, &proj); err != nil {
		writeClientErr(w, err)
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
			ApprovalRequired: true,
			ProposedIssue: &tatarav1alpha1.ProposedIssueSpec{
				RepositoryRef: req.RepositoryRef, Title: req.Title, Body: req.Body, Kind: req.Kind,
			},
		},
	}
	if err := s.c.Create(r.Context(), task); err != nil {
		writeClientErr(w, err)
		return
	}
	task.Status.Phase = "AwaitingApproval"
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type: tatarav1alpha1.ConditionApprovalApproved, Status: metav1.ConditionFalse,
		Reason: "Proposed", Message: "issue proposed via REST; awaiting human approval",
		LastTransitionTime: metav1.NewTime(time.Now()),
	})
	if err := s.c.Status().Update(r.Context(), task); err != nil {
		writeClientErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toTaskDTO(*task))
}

type reviewVerdictReq struct {
	Decision    string                      `json:"decision"`
	Body        string                      `json:"body,omitempty"`
	Suggestions []tatarav1alpha1.Suggestion `json:"suggestions,omitempty"`
}

func (s *Server) reviewVerdict(w http.ResponseWriter, r *http.Request) {
	var req reviewVerdictReq
	if err := decodeJSON(r, &req); err != nil {
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
	var t tatarav1alpha1.Task
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	if t.Spec.Kind != "review" {
		writeError(w, http.StatusConflict, "review verdict only applies to a review task")
		return
	}
	t.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: req.Decision, Body: req.Body, Suggestions: req.Suggestions}
	if err := s.c.Status().Update(r.Context(), &t); err != nil {
		writeClientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

type prOutcomeReq struct {
	Action string `json:"action"`
	Reason string `json:"reason,omitempty"`
}

func (s *Server) prOutcome(w http.ResponseWriter, r *http.Request) {
	var req prOutcomeReq
	if err := decodeJSON(r, &req); err != nil {
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
	var t tatarav1alpha1.Task
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	if t.Spec.Kind != "selfImprove" {
		writeError(w, http.StatusConflict, "pr outcome only applies to a selfImprove task")
		return
	}
	t.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: req.Action, Reason: req.Reason}
	if err := s.c.Status().Update(r.Context(), &t); err != nil {
		writeClientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

type issueOutcomeReq struct {
	Action  string `json:"action"`
	Comment string `json:"comment,omitempty"`
}

func (s *Server) issueOutcome(w http.ResponseWriter, r *http.Request) {
	var req issueOutcomeReq
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	if req.Action == "" {
		writeError(w, http.StatusBadRequest, "action required")
		return
	}
	switch req.Action {
	case "implement", "close":
	default:
		writeError(w, http.StatusBadRequest, "action must be one of implement, close")
		return
	}
	if req.Action == "close" && req.Comment == "" {
		writeError(w, http.StatusBadRequest, "comment required when action is close")
		return
	}
	var t tatarav1alpha1.Task
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	if t.Spec.Kind != "triageIssue" {
		writeError(w, http.StatusConflict, "issue outcome only applies to a triageIssue task")
		return
	}
	t.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: req.Action, Comment: req.Comment}
	if err := s.c.Status().Update(r.Context(), &t); err != nil {
		writeClientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

// --- M4 Task 2: POST /tasks/{t}/change-summary ---

type changeSummaryReq struct {
	PRTitle        string `json:"prTitle,omitempty"`
	PRBody         string `json:"prBody,omitempty"`
	DeliveredScope string `json:"deliveredScope,omitempty"`
	RemainingScope string `json:"remainingScope,omitempty"`
}

func (s *Server) changeSummary(w http.ResponseWriter, r *http.Request) {
	var req changeSummaryReq
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	var t tatarav1alpha1.Task
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	t.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		PRTitle:        req.PRTitle,
		PRBody:         req.PRBody,
		DeliveredScope: req.DeliveredScope,
		RemainingScope: req.RemainingScope,
	}
	if err := s.c.Status().Update(r.Context(), &t); err != nil {
		writeClientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

// --- M3: POST /tasks/{t}/handover ---

const handoverMaxBytes = 16 * 1024 // 16 KB cap

type handoverReq struct {
	Handover string `json:"handover"`
}

func (s *Server) handover(w http.ResponseWriter, r *http.Request) {
	var req handoverReq
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	// Cap at 16KB.
	if len(req.Handover) > handoverMaxBytes {
		req.Handover = req.Handover[:handoverMaxBytes]
	}
	var t tatarav1alpha1.Task
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	t.Status.Handover = req.Handover
	if err := s.c.Status().Update(r.Context(), &t); err != nil {
		writeClientErr(w, err)
		return
	}
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
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	var st tatarav1alpha1.Subtask
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "s")}
	if err := s.c.Get(r.Context(), key, &st); err != nil {
		writeClientErr(w, err)
		return
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
	if err := s.c.Status().Update(r.Context(), &st); err != nil {
		writeClientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toSubtaskDTO(st))
}
