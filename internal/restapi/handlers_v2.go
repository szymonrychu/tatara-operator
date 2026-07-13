package restapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/prompt"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// The contract-C limits that are not CRD constants.
const (
	// noteBodyMaxBytes is C.2.6's note-body cap, applied on a rune boundary.
	noteBodyMaxBytes = 4096
	// maxNotes is the Go-side note cap (C.2.6). At the cap the OLDEST note is
	// spilled to tatara-memory and dropped. There is NO 409-on-cap: an agent
	// must ALWAYS be able to write its handoff.
	maxNotes = 50
	// logTailMaxBytes caps the CI log tail served for a FAILING check (C.2.10).
	logTailMaxBytes = 4000
	// ciPacerEntries bounds the in-process CI pacer LRU.
	ciPacerEntries = 512
	// defaultCommitDays / defaultCommitLimit are C.2.9's defaults.
	defaultCommitDays  = 30
	defaultCommitLimit = 200
	// defaultListLimit / maxListLimit are C.2.8's.
	defaultListLimit = 100
	maxListLimit     = 500
	// pendingCommentsCap bounds Issue/MergeRequest status.pendingComments.
	pendingCommentsCap = 20
)

// NoteFetcher rehydrates a spilled note batch from tatara-memory by its
// track_id. *memclient.Client satisfies it (C.2.5, fix H10).
type NoteFetcher interface {
	Fetch(ctx context.Context, trackID string) (json.RawMessage, error)
}

// ApprovalVerifier is the C.6 approval grammar. The /outcome clarify path calls
// it for EVERY owned Issue (fix H9); it is NOT in any agent-facing schema. The
// implementation lives in internal/controller (Task 10); this is the seam the
// REST layer owns so the two can land independently.
//
// A nil verifier FAILS CLOSED: decision=implement then parks the Task at
// identity-unverified rather than granting an unverified mandate.
type ApprovalVerifier interface {
	// VerifyApproval reports whether iss carries a valid human approval, and
	// the single-use evidence that proves it.
	VerifyApproval(ctx context.Context, proj *tatarav1alpha1.Project, iss *tatarav1alpha1.Issue) (*tatarav1alpha1.ApprovalEvidence, bool)
}

// --- shared lookups -------------------------------------------------------

func (s *Server) now() time.Time {
	if s.nowFn != nil {
		return s.nowFn()
	}
	return time.Now()
}

func (s *Server) getTaskCR(ctx context.Context, name string) (*tatarav1alpha1.Task, error) {
	var t tatarav1alpha1.Task
	if err := s.c.Get(ctx, types.NamespacedName{Namespace: s.ns, Name: name}, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

func (s *Server) getProjectCR(ctx context.Context, name string) (*tatarav1alpha1.Project, error) {
	var p tatarav1alpha1.Project
	if err := s.c.Get(ctx, types.NamespacedName{Namespace: s.ns, Name: name}, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// repoCR resolves a Repository CR by NAME (never an owner/repo slug: a slug
// lets an agent name a repo outside the project) and checks it belongs to proj.
func (s *Server) repoCR(ctx context.Context, projName, repoName string) (*tatarav1alpha1.Repository, error) {
	var repo tatarav1alpha1.Repository
	if err := s.c.Get(ctx, types.NamespacedName{Namespace: s.ns, Name: repoName}, &repo); err != nil {
		return nil, err
	}
	if repo.Spec.ProjectRef != projName {
		return nil, fmt.Errorf("repository %q is not in project %q", repoName, projName)
	}
	return &repo, nil
}

// ownedIssues returns every Issue CR this Task controller-owns.
func (s *Server) ownedIssues(ctx context.Context, task *tatarav1alpha1.Task) ([]tatarav1alpha1.Issue, error) {
	var list tatarav1alpha1.IssueList
	if err := s.c.List(ctx, &list, client.InNamespace(s.ns)); err != nil {
		return nil, err
	}
	out := make([]tatarav1alpha1.Issue, 0, len(list.Items))
	for i := range list.Items {
		if ctrl, ok := own.ControllerOwner(&list.Items[i]); ok && ctrl == task.Name {
			out = append(out, list.Items[i])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ownedMRs returns every MergeRequest CR this Task controller-owns.
func (s *Server) ownedMRs(ctx context.Context, task *tatarav1alpha1.Task) ([]tatarav1alpha1.MergeRequest, error) {
	var list tatarav1alpha1.MergeRequestList
	if err := s.c.List(ctx, &list, client.InNamespace(s.ns)); err != nil {
		return nil, err
	}
	out := make([]tatarav1alpha1.MergeRequest, 0, len(list.Items))
	for i := range list.Items {
		if ctrl, ok := own.ControllerOwner(&list.Items[i]); ok && ctrl == task.Name {
			out = append(out, list.Items[i])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// openMRs filters to the MRs that are still open: the merge-order and review
// coverage rules are about the MRs still in flight.
func openMRs(mrs []tatarav1alpha1.MergeRequest) []tatarav1alpha1.MergeRequest {
	out := make([]tatarav1alpha1.MergeRequest, 0, len(mrs))
	for i := range mrs {
		if mrs[i].Status.State == "" || mrs[i].Status.State == "open" {
			out = append(out, mrs[i])
		}
	}
	return out
}

// --- 6. GET /tasks/{t}/context --------------------------------------------

type contextResp struct {
	Task   string `json:"task"`
	Bundle string `json:"bundle"`
}

func (s *Server) taskContext(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := chi.URLParam(r, "t")
	task, err := s.getTaskCR(ctx, name)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	proj, err := s.getProjectCR(ctx, task.Spec.ProjectRef)
	if err != nil {
		writeClientErr(w, err)
		return
	}

	if r.URL.Query().Get("index") == "true" {
		var list tatarav1alpha1.TaskList
		if err := s.c.List(ctx, &list, client.InNamespace(s.ns)); err != nil {
			writeClientErr(w, err)
			return
		}
		tasks := make([]*tatarav1alpha1.Task, 0, len(list.Items))
		for i := range list.Items {
			if list.Items[i].Spec.ProjectRef == task.Spec.ProjectRef {
				tasks = append(tasks, &list.Items[i])
			}
		}
		bundle, err := prompt.RenderIndex(prompt.IndexInput{
			Project: task.Spec.ProjectRef, Scope: "all", Tasks: tasks,
			Now: s.now(), MaxBundleBytes: proj.Spec.MaxBundleBytes,
		})
		if err != nil {
			writeClientErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, contextResp{Task: task.Name, Bundle: bundle})
		return
	}

	issues, err := s.ownedIssues(ctx, task)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	mrs, err := s.ownedMRs(ctx, task)
	if err != nil {
		writeClientErr(w, err)
		return
	}

	notes := task.Status.Notes
	notesTotal := 0
	if r.URL.Query().Get("notes") == "all" {
		rehydrated, err := s.rehydrateNotes(ctx, task)
		if err != nil {
			s.log.ErrorContext(ctx, "restapi: rehydrating spilled notes failed",
				append(reqLogFields(r), "task", task.Name, "error", err)...)
			writeError(w, http.StatusBadGateway, "spilled notes are unavailable")
			return
		}
		notes = append(rehydrated, notes...)
		notesTotal = len(notes)
	}

	bundle, err := prompt.Render(prompt.Input{
		Task: task, Issues: issues, MergeRequests: mrs,
		Events: task.Status.PendingEvents, Notes: notes, NotesTotal: notesTotal,
		MaxBundleBytes: proj.Spec.MaxBundleBytes, Logger: s.log,
	})
	if err != nil {
		writeClientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, contextResp{Task: task.Name, Bundle: bundle})
}

// rehydrateNotes pulls every spilled note batch back out of tatara-memory, in
// spill order (stats.notesSpilledRefs ACCUMULATES, oldest batch first), so the
// notes=all bundle renders the FULL history. This is the read path the
// <notes ... fetch=...> marker names.
func (s *Server) rehydrateNotes(ctx context.Context, task *tatarav1alpha1.Task) ([]tatarav1alpha1.Note, error) {
	refs := task.Status.Stats.NotesSpilledRefs
	if len(refs) == 0 {
		return nil, nil
	}
	if s.memory == nil {
		return nil, errors.New("restapi: no tatara-memory client configured")
	}
	var out []tatarav1alpha1.Note
	for _, ref := range refs {
		raw, err := s.memory.Fetch(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("fetch spilled notes %q: %w", ref, err)
		}
		var batch []tatarav1alpha1.Note
		if err := json.Unmarshal(raw, &batch); err != nil {
			return nil, fmt.Errorf("decode spilled notes %q: %w", ref, err)
		}
		out = append(out, batch...)
	}
	return out, nil
}

// --- 7. POST /tasks/{t}/notes ---------------------------------------------

// noteReq is C.2.6. `agent` is NOT a body key: the operator stamps it from
// status.agentKind, so agent="operator" is UNREACHABLE from this endpoint.
type noteReq struct {
	Kind string `json:"kind"`
	Body string `json:"body"`
}

func (s *Server) postNote(w http.ResponseWriter, r *http.Request) {
	if !authorizeCaller(w, r) {
		return
	}
	ctx := r.Context()
	name := chi.URLParam(r, "t")

	var req noteReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	switch req.Kind {
	case "note", "plan", "handoff":
	default:
		writeError(w, http.StatusBadRequest, "kind must be one of note, plan, handoff")
		return
	}
	if strings.TrimSpace(req.Body) == "" {
		writeError(w, http.StatusBadRequest, "body required")
		return
	}

	task, err := s.getTaskCR(ctx, name)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	if tatarav1alpha1.StageTerminal(task) {
		writeError(w, http.StatusConflict, "task is in a terminal stage")
		return
	}
	agent := task.Status.AgentKind
	if agent == "" {
		// Never defaulted (fix 19). agent="operator" is the operator's own,
		// in-process, and is not reachable from this endpoint.
		writeError(w, http.StatusConflict, "task has no agent kind")
		return
	}

	note := tatarav1alpha1.Note{
		At:    metav1.NewTime(s.now()),
		Agent: agent,
		Kind:  req.Kind,
		Body:  truncateValidUTF8(req.Body, noteBodyMaxBytes),
	}

	// The 50-note cap: spill the OLDEST batch to tatara-memory ONCE, outside
	// the retry loop (a spill is a non-idempotent network call), then let the
	// pure trim re-run on every conflict.
	spillN := len(task.Status.Notes) + 1 - maxNotes
	trackID := ""
	if spillN > 0 {
		if s.spiller == nil {
			writeError(w, http.StatusInternalServerError, "internal error")
			s.log.ErrorContext(ctx, "restapi: note cap reached with no spiller configured",
				append(reqLogFields(r), "task", task.Name)...)
			return
		}
		trackID, err = s.spiller.Spill(ctx, "Task", task.Name, task.Status.Notes[:spillN])
		if err != nil {
			s.log.ErrorContext(ctx, "restapi: spilling oldest notes failed",
				append(reqLogFields(r), "task", task.Name, "error", err)...)
			writeError(w, http.StatusBadGateway, "note spill failed")
			return
		}
	}

	key := types.NamespacedName{Namespace: s.ns, Name: task.Name}
	err = objbudget.FitTask(ctx, s.c, s.spiller, key, func(t *tatarav1alpha1.Task) {
		t.Status.Notes = append(t.Status.Notes, note)
		if spillN > 0 && len(t.Status.Notes) > maxNotes {
			drop := len(t.Status.Notes) - maxNotes
			t.Status.Notes = t.Status.Notes[drop:]
			t.Status.Stats.NotesSpilled += drop
			t.Status.Stats.NotesSpilledRefs = append(t.Status.Stats.NotesSpilledRefs, trackID)
		}
	})
	if errors.Is(err, objbudget.ErrObjectTooLarge) {
		// The ONE way this write can still fail (fix L32). It does not 409 the
		// agent: it fails the TASK, loudly, via a minimal patch that carries
		// none of the oversized lists and therefore cannot itself 413.
		obs.RestOutcomeRejectedTotal.WithLabelValues("note", stage.ReasonObjectTooLarge).Inc()
		if perr := objbudget.MinimalFailPatch(ctx, s.c, task, stage.ReasonObjectTooLarge); perr != nil {
			s.log.ErrorContext(ctx, "restapi: minimal fail patch failed",
				append(reqLogFields(r), "task", task.Name, "error", perr)...)
		}
		s.log.ErrorContext(ctx, "restapi: task exceeds the byte budget with nothing left to evict; failed at object-too-large",
			append(reqLogFields(r), "task", task.Name)...)
		writeError(w, http.StatusInsufficientStorage, "task exceeds the byte budget")
		return
	}
	if err != nil {
		writeClientErr(w, err)
		return
	}

	fresh, err := s.getTaskCR(ctx, task.Name)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	s.log.InfoContext(ctx, "restapi: note appended",
		append(reqLogFields(r), "action", "task_note", "task", task.Name,
			"agent_kind", agent, "note_kind", req.Kind, "spilled", spillN)...)
	writeJSON(w, http.StatusCreated, toTaskDTO(*fresh))
}

// --- 9/10/11. the MIRROR reads. ZERO forge requests. ----------------------

type issueMirrorDTO struct {
	Repo         string   `json:"repo"`
	Number       int      `json:"number"`
	Title        string   `json:"title"`
	Body         string   `json:"body,omitempty"`
	Author       string   `json:"author,omitempty"`
	Labels       []string `json:"labels,omitempty"`
	State        string   `json:"state,omitempty"`
	Status       string   `json:"status,omitempty"`
	CreatedAt    string   `json:"createdAt,omitempty"`
	UpdatedAt    string   `json:"updatedAt,omitempty"`
	URL          string   `json:"url,omitempty"`
	TaskRef      string   `json:"taskRef"`
	LastSyncedAt string   `json:"lastSyncedAt,omitempty"`
}

type mrMirrorDTO struct {
	Repo         string `json:"repo"`
	Number       int    `json:"number"`
	Title        string `json:"title"`
	Body         string `json:"body,omitempty"`
	Author       string `json:"author,omitempty"`
	State        string `json:"state,omitempty"`
	Status       string `json:"status,omitempty"`
	HeadBranch   string `json:"headBranch,omitempty"`
	HeadSHA      string `json:"headSHA,omitempty"`
	CIStatus     string `json:"ciStatus,omitempty"`
	Mergeable    bool   `json:"mergeable"`
	Significance string `json:"significance,omitempty"`
	ReviewedSHA  string `json:"reviewedSHA"`
	ReviewRounds int    `json:"reviewRounds"`
	URL          string `json:"url,omitempty"`
	TaskRef      string `json:"taskRef"`
	LastSyncedAt string `json:"lastSyncedAt,omitempty"`
}

type commentMirrorDTO struct {
	ExternalID string `json:"externalId"`
	Author     string `json:"author,omitempty"`
	Body       string `json:"body"`
	CreatedAt  string `json:"createdAt,omitempty"`
	IsBot      bool   `json:"isBot"`
	Truncated  bool   `json:"truncated"`
	Path       string `json:"path,omitempty"`
	Line       int    `json:"line,omitempty"`
	InReplyTo  string `json:"inReplyTo,omitempty"`
}

// listLimit parses ?limit (default 100, max 500).
func listLimit(r *http.Request, def, max int) int {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

func controllerTaskRef(obj client.Object) string {
	name, _ := own.ControllerOwner(obj)
	return name
}

// scmIssues is GET /projects/{p}/scm/issues: SERVED FROM THE MIRROR. It makes
// ZERO forge requests (fix C1).
func (s *Server) scmIssues(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	projName := chi.URLParam(r, "p")
	if _, err := s.getProjectCR(ctx, projName); err != nil {
		writeClientErr(w, err)
		return
	}
	repoName := r.URL.Query().Get("repo")
	if repoName == "" {
		writeError(w, http.StatusBadRequest, "repo required")
		return
	}
	if _, err := s.repoCR(ctx, projName, repoName); err != nil {
		writeClientErr(w, err)
		return
	}
	state := r.URL.Query().Get("state")
	if state == "" {
		state = "open"
	}
	switch state {
	case "open", "closed", "all":
	default:
		writeError(w, http.StatusBadRequest, "state must be one of open, closed, all")
		return
	}
	var since time.Time
	if v := r.URL.Query().Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "since must be RFC3339")
			return
		}
		since = t
	}
	var labels []string
	if v := r.URL.Query().Get("labels"); v != "" {
		labels = strings.Split(v, ",")
	}
	limit := listLimit(r, defaultListLimit, maxListLimit)

	var list tatarav1alpha1.IssueList
	if err := s.c.List(ctx, &list, client.InNamespace(s.ns)); err != nil {
		writeClientErr(w, err)
		return
	}
	out := make([]issueMirrorDTO, 0, len(list.Items))
	for i := range list.Items {
		iss := &list.Items[i]
		if iss.Spec.ProjectRef != projName || iss.Spec.RepositoryRef != repoName {
			continue
		}
		if state != "all" && iss.Status.State != state {
			continue
		}
		if !since.IsZero() && (iss.Status.UpdatedAt == nil || iss.Status.UpdatedAt.Time.Before(since)) {
			continue
		}
		if len(labels) > 0 && !hasAnyLabel(iss.Status.Labels, labels) {
			continue
		}
		out = append(out, issueMirrorDTO{
			Repo: iss.Spec.RepositoryRef, Number: iss.Spec.Number,
			Title: iss.Status.Title, Body: iss.Status.Body, Author: iss.Status.Author,
			Labels: iss.Status.Labels, State: iss.Status.State, Status: iss.Status.Status,
			CreatedAt: rfc3339(iss.Status.CreatedAt), UpdatedAt: rfc3339(iss.Status.UpdatedAt),
			URL: iss.Spec.URL, TaskRef: controllerTaskRef(iss),
			LastSyncedAt: rfc3339(iss.Status.LastSyncedAt),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	if len(out) > limit {
		out = out[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{"issues": out})
}

func hasAnyLabel(have, want []string) bool {
	for _, w := range want {
		w = strings.TrimSpace(w)
		for _, h := range have {
			if h == w {
				return true
			}
		}
	}
	return false
}

// scmMRs is GET /projects/{p}/scm/mrs: SERVED FROM THE MIRROR. Zero forge
// requests.
func (s *Server) scmMRs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	projName := chi.URLParam(r, "p")
	if _, err := s.getProjectCR(ctx, projName); err != nil {
		writeClientErr(w, err)
		return
	}
	repoName := r.URL.Query().Get("repo")
	if repoName == "" {
		writeError(w, http.StatusBadRequest, "repo required")
		return
	}
	if _, err := s.repoCR(ctx, projName, repoName); err != nil {
		writeClientErr(w, err)
		return
	}
	state := r.URL.Query().Get("state")
	if state == "" {
		state = "open"
	}
	switch state {
	case "open", "merged", "closed", "all":
	default:
		writeError(w, http.StatusBadRequest, "state must be one of open, merged, closed, all")
		return
	}
	limit := listLimit(r, defaultListLimit, maxListLimit)

	var list tatarav1alpha1.MergeRequestList
	if err := s.c.List(ctx, &list, client.InNamespace(s.ns)); err != nil {
		writeClientErr(w, err)
		return
	}
	out := make([]mrMirrorDTO, 0, len(list.Items))
	for i := range list.Items {
		mr := &list.Items[i]
		if mr.Spec.ProjectRef != projName || mr.Spec.RepositoryRef != repoName {
			continue
		}
		if state != "all" && mr.Status.State != state {
			continue
		}
		out = append(out, mrMirrorDTO{
			Repo: mr.Spec.RepositoryRef, Number: mr.Spec.Number,
			Title: mr.Status.Title, Body: mr.Status.Body, Author: mr.Status.Author,
			State: mr.Status.State, Status: mr.Status.Status,
			HeadBranch: mr.Status.HeadBranch, HeadSHA: mr.Status.HeadSHA,
			CIStatus: mr.Status.CIStatus, Mergeable: mr.Status.Mergeable,
			Significance: mr.Status.Significance, ReviewedSHA: mr.Status.ReviewedSHA,
			ReviewRounds: mr.Status.ReviewRounds, URL: mr.Spec.URL,
			TaskRef: controllerTaskRef(mr), LastSyncedAt: rfc3339(mr.Status.LastSyncedAt),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	if len(out) > limit {
		out = out[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{"mrs": out})
}

// scmComments is GET /projects/{p}/scm/comments: SERVED FROM THE MIRROR. Zero
// forge requests. isPR=false reads the Issue CR's thread, true the MR's.
func (s *Server) scmComments(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	projName := chi.URLParam(r, "p")
	if _, err := s.getProjectCR(ctx, projName); err != nil {
		writeClientErr(w, err)
		return
	}
	repoName := r.URL.Query().Get("repo")
	if repoName == "" {
		writeError(w, http.StatusBadRequest, "repo required")
		return
	}
	if _, err := s.repoCR(ctx, projName, repoName); err != nil {
		writeClientErr(w, err)
		return
	}
	number, err := strconv.Atoi(r.URL.Query().Get("number"))
	if err != nil || number <= 0 {
		writeError(w, http.StatusBadRequest, "number required")
		return
	}
	isPR := r.URL.Query().Get("isPR") == "true"

	var (
		comments  []tatarav1alpha1.Comment
		total     int
		spilled   int
		lastSync  *metav1.Time
		objectKey = types.NamespacedName{Namespace: s.ns}
	)
	if isPR {
		objectKey.Name = tatarav1alpha1.MergeRequestName(repoName, number)
		var mr tatarav1alpha1.MergeRequest
		if err := s.c.Get(ctx, objectKey, &mr); err != nil {
			writeClientErr(w, err)
			return
		}
		comments, total, spilled, lastSync = mr.Status.Comments, mr.Status.CommentCount, mr.Status.SpilledComments, mr.Status.LastSyncedAt
	} else {
		objectKey.Name = tatarav1alpha1.IssueName(repoName, number)
		var iss tatarav1alpha1.Issue
		if err := s.c.Get(ctx, objectKey, &iss); err != nil {
			writeClientErr(w, err)
			return
		}
		comments, total, spilled, lastSync = iss.Status.Comments, iss.Status.CommentCount, iss.Status.SpilledComments, iss.Status.LastSyncedAt
	}

	out := make([]commentMirrorDTO, 0, len(comments))
	for i := range comments {
		c := comments[i]
		out = append(out, commentMirrorDTO{
			ExternalID: c.ExternalID, Author: c.Author, Body: c.Body,
			CreatedAt: rfc3339(&c.CreatedAt), IsBot: c.IsBot, Truncated: c.Truncated,
			Path: c.Path, Line: c.Line, InReplyTo: c.InReplyTo,
		})
	}
	if total == 0 {
		total = len(out) + spilled
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"comments": out, "total": total, "spilled": spilled,
		"lastSyncedAt": rfc3339(lastSync),
	})
}

// --- 12. GET /projects/{p}/scm/commits (live, cheap, one repo) -------------

func (s *Server) scmCommits(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	projName := chi.URLParam(r, "p")
	proj, err := s.getProjectCR(ctx, projName)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	repoName := r.URL.Query().Get("repo")
	if repoName == "" {
		writeError(w, http.StatusBadRequest, "repo required")
		return
	}
	repo, err := s.repoCR(ctx, projName, repoName)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	days := defaultCommitDays
	if v := r.URL.Query().Get("sinceDays"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "sinceDays must be a positive integer")
			return
		}
		days = n
	}
	limit := listLimit(r, defaultCommitLimit, maxListLimit)

	reader, _, ok := s.projectSCMReader(w, r, proj)
	if !ok {
		return
	}
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		writeError(w, http.StatusConflict, "repository has no parseable url")
		return
	}
	since := s.now().AddDate(0, 0, -days)
	commits, err := reader.ListCommits(ctx, owner, name, since)
	if err != nil {
		s.log.ErrorContext(ctx, "restapi: listing commits failed",
			append(reqLogFields(r), "repo", repoName, "error", err)...)
		writeError(w, http.StatusBadGateway, "scm read failed")
		return
	}
	out := make([]commitDTO, 0, len(commits))
	for _, c := range commits {
		if len(out) >= limit {
			break
		}
		out = append(out, commitDTO{Repo: repoName, SHA: c.SHA, Message: c.Message, Author: c.Author, Date: c.Date})
	}
	writeJSON(w, http.StatusOK, map[string]any{"commits": out})
}

// --- 13. GET /projects/{p}/scm/ci: THE ONLY LIVE FORGE READ ON THE HOT PATH -

type ciCheckDTO struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion,omitempty"`
	URL        string `json:"url,omitempty"`
	LogTail    string `json:"logTail,omitempty"`
}

type ciDTO struct {
	Repo      string       `json:"repo"`
	Number    int          `json:"number"`
	HeadSHA   string       `json:"headSHA"`
	Status    string       `json:"status"`
	Mergeable bool         `json:"mergeable"`
	Cached    bool         `json:"cached"`
	FetchedAt string       `json:"fetchedAt"`
	Checks    []ciCheckDTO `json:"checks"`
}

// ciPacer is the C.2.10 server-side pacer: a minimum interval of 20s per
// (repo, number). A call inside the window is served from the last result with
// "cached":true and does NOT hit the forge.
//
// It is an in-process LRU, and that is DELIBERATE (addendum 6): the operator
// runs 3 replicas and the REST listener is not leader-elected, so the true
// worst case is 3 fetches per 20s per PR rather than 1. That is a 3x on a
// number chosen with an order of magnitude of headroom, and it is ACCEPTED. Do
// not "fix" it with distributed state before the metric says so.
type ciPacer struct {
	mu      sync.Mutex
	entries map[string]ciDTO
}

func newCIPacer() *ciPacer { return &ciPacer{entries: make(map[string]ciDTO)} }

func (p *ciPacer) get(key string, now time.Time, minInterval time.Duration) (ciDTO, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.entries[key]
	if !ok {
		return ciDTO{}, false
	}
	at, err := time.Parse(time.RFC3339, e.FetchedAt)
	if err != nil || now.Sub(at) >= minInterval {
		return ciDTO{}, false
	}
	e.Cached = true
	return e, true
}

func (p *ciPacer) put(key string, v ciDTO) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.entries) >= ciPacerEntries {
		oldestKey, oldest := "", ""
		for k, e := range p.entries {
			if oldest == "" || e.FetchedAt < oldest {
				oldestKey, oldest = k, e.FetchedAt
			}
		}
		delete(p.entries, oldestKey)
	}
	p.entries[key] = v
}

func (s *Server) scmCI(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	projName := chi.URLParam(r, "p")
	proj, err := s.getProjectCR(ctx, projName)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	repoName := r.URL.Query().Get("repo")
	if repoName == "" {
		writeError(w, http.StatusBadRequest, "repo required")
		return
	}
	repo, err := s.repoCR(ctx, projName, repoName)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	number, err := strconv.Atoi(r.URL.Query().Get("number"))
	if err != nil || number <= 0 {
		writeError(w, http.StatusBadRequest, "number required")
		return
	}

	key := repoName + "!" + strconv.Itoa(number)
	now := s.now()
	if cached, ok := s.ciPacer.get(key, now, tatarav1alpha1.CIPollMinInterval); ok {
		obs.RestCIReadTotal.WithLabelValues("cached").Inc()
		writeJSON(w, http.StatusOK, map[string]any{"ci": cached})
		return
	}

	if s.ciFor == nil {
		writeError(w, http.StatusNotImplemented, "scm ci reader not configured")
		return
	}
	provider, token, ok := s.resolveProjectSCMProviderToken(w, r, proj)
	if !ok {
		return
	}
	reader, err := s.ciFor(provider, token)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	res, err := reader.PRChecks(ctx, repo.Spec.URL, token, number)
	if err != nil {
		s.log.ErrorContext(ctx, "restapi: ci read failed",
			append(reqLogFields(r), "repo", repoName, "number", number, "error", err)...)
		writeError(w, http.StatusBadGateway, "scm read failed")
		return
	}
	obs.RestCIReadTotal.WithLabelValues("live").Inc()

	out := ciDTO{
		Repo: repoName, Number: number, HeadSHA: res.HeadSHA, Status: res.Status,
		Mergeable: res.Mergeable, Cached: false, FetchedAt: now.UTC().Format(time.RFC3339),
		Checks: make([]ciCheckDTO, 0, len(res.Checks)),
	}
	for _, c := range res.Checks {
		dto := ciCheckDTO{Name: c.Name, Status: c.Status, Conclusion: c.Conclusion, URL: c.URL}
		// logTail is served ONLY for a check whose conclusion is
		// failure|timed_out|cancelled. A green run's logs are never fetched.
		if failingConclusion(c.Conclusion) && c.JobID != "" {
			tail, err := reader.JobLogTail(ctx, repo.Spec.URL, token, c.JobID, logTailMaxBytes)
			if err != nil {
				s.log.WarnContext(ctx, "restapi: ci log tail unavailable",
					append(reqLogFields(r), "repo", repoName, "number", number, "check", c.Name, "error", err)...)
			} else {
				dto.LogTail = tail
			}
		}
		out.Checks = append(out.Checks, dto)
	}
	s.ciPacer.put(key, out)

	s.log.InfoContext(ctx, "restapi: ci read",
		append(reqLogFields(r), "action", "scm_read_ci", "repo", repoName, "number", number,
			"status", out.Status, "checks", len(out.Checks))...)
	writeJSON(w, http.StatusOK, map[string]any{"ci": out})
}

func failingConclusion(c string) bool {
	switch c {
	case "failure", "timed_out", "cancelled":
		return true
	default:
		return false
	}
}

// --- the callerTask: EVERY write endpoint names its Task ------------------

// callerTask resolves the ?task= / X-Tatara-Task Task the write is made on
// behalf of. The pod's identity is not per-Task (one shared OIDC client, see
// authorizeCaller), so the Task is named explicitly and every ownership gate is
// evaluated against the CRs, never against a claim in the token.
func (s *Server) callerTask(w http.ResponseWriter, r *http.Request, name string) (*tatarav1alpha1.Task, bool) {
	if name == "" {
		writeError(w, http.StatusBadRequest, "task required")
		return nil, false
	}
	task, err := s.getTaskCR(r.Context(), name)
	if err != nil {
		writeClientErr(w, err)
		return nil, false
	}
	return task, true
}

// --- 14. POST /projects/{p}/scm/issue-write -------------------------------

// issueWriteReq is C.2.12. There is NO status param and NO labels param:
// approval and every lifecycle label are operator-owned, and a labels key would
// let an agent stamp the trigger label and self-escalate.
type issueWriteReq struct {
	Task    string `json:"task,omitempty"`
	Action  string `json:"action"`
	Repo    string `json:"repo"`
	Number  int    `json:"number,omitempty"`
	Title   string `json:"title,omitempty"`
	Body    string `json:"body,omitempty"`
	Comment string `json:"comment,omitempty"`
}

func (s *Server) issueWrite(w http.ResponseWriter, r *http.Request) {
	if !authorizeCaller(w, r) {
		return
	}
	ctx := r.Context()
	projName := chi.URLParam(r, "p")

	var req issueWriteReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Repo == "" {
		writeError(w, http.StatusBadRequest, "repo required")
		return
	}
	proj, err := s.getProjectCR(ctx, projName)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	repo, err := s.repoCR(ctx, projName, req.Repo)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	task, ok := s.callerTask(w, r, taskParam(r, req.Task))
	if !ok {
		return
	}

	switch req.Action {
	case "create":
		if req.Number != 0 {
			writeError(w, http.StatusBadRequest, "action=create forbids number")
			return
		}
		if strings.TrimSpace(req.Title) == "" || strings.TrimSpace(req.Body) == "" {
			writeError(w, http.StatusBadRequest, "action=create requires title and body")
			return
		}
		s.issueCreate(w, r, proj, repo, task, req)
	case "edit":
		if req.Number == 0 {
			writeError(w, http.StatusBadRequest, "action=edit requires number")
			return
		}
		if strings.TrimSpace(req.Title) == "" && strings.TrimSpace(req.Body) == "" {
			writeError(w, http.StatusBadRequest, "action=edit requires at least one of title, body")
			return
		}
		s.issueDeferred(w, r, proj, repo, task, req)
	case "close":
		if req.Number == 0 {
			writeError(w, http.StatusBadRequest, "action=close requires number")
			return
		}
		if strings.TrimSpace(req.Comment) == "" {
			writeError(w, http.StatusBadRequest, "action=close requires comment")
			return
		}
		s.issueDeferred(w, r, proj, repo, task, req)
	case "comment":
		if req.Number == 0 {
			writeError(w, http.StatusBadRequest, "action=comment requires number")
			return
		}
		if strings.TrimSpace(req.Body) == "" {
			writeError(w, http.StatusBadRequest, "action=comment requires body")
			return
		}
		s.issueDeferred(w, r, proj, repo, task, req)
	default:
		writeError(w, http.StatusBadRequest, "action must be one of create, edit, close, comment")
	}
}

// taskParam prefers the body's task, then ?task=, then the pod's TATARA_TASK
// header. Empty is a 400 at callerTask.
func taskParam(r *http.Request, body string) string {
	if body != "" {
		return body
	}
	if v := r.URL.Query().Get("task"); v != "" {
		return v
	}
	return r.Header.Get("X-Tatara-Task")
}

// issueCreate is SYNCHRONOUS: the agent NEEDS the number back, and there is
// nothing to return from a reconciler (C.2.12, fix M7).
func (s *Server) issueCreate(w http.ResponseWriter, r *http.Request, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task, req issueWriteReq) {
	ctx := r.Context()
	writer, token, ok := s.projectSCMWriterAndToken(w, r, proj)
	if !ok {
		return
	}
	created, err := writer.CreateIssue(ctx, repo.Spec.URL, token, scm.IssueReq{Title: req.Title, Body: req.Body})
	controller.RecordSCM(s.metrics, providerOf(proj), "create_issue", err)
	if err != nil {
		s.log.ErrorContext(ctx, "restapi: creating issue failed",
			append(reqLogFields(r), "repo", repo.Name, "error", err)...)
		writeError(w, http.StatusBadGateway, "scm write failed")
		return
	}
	number := issueRefNumber(created.Ref)
	if number == 0 {
		writeError(w, http.StatusBadGateway, "scm returned no issue number")
		return
	}
	if err := s.mintIssueCR(ctx, proj, repo, task, number, created.URL, req.Title, req.Body); err != nil {
		writeClientErr(w, err)
		return
	}
	s.log.InfoContext(ctx, "restapi: issue created",
		append(reqLogFields(r), "action", "issue_write_create", "task", task.Name,
			"repo", repo.Name, "number", number, "url", created.URL)...)
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "repo": repo.Name, "number": number, "url": created.URL,
	})
}

// mintIssueCR creates the Issue CR controller-owned by task, seeds its mirror
// status from what we just posted, and appends the ref to the Task.
func (s *Server) mintIssueCR(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task, number int, url, title, body string) error {
	name := tatarav1alpha1.IssueName(repo.Name, number)
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.ns},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo.Name, Number: number, URL: url, ProjectRef: proj.Name,
		},
	}
	own.AddPlainOwner(iss, task)
	if err := own.HandOverController(iss, nil, task); err != nil {
		return err
	}
	if err := s.c.Create(ctx, iss); err != nil {
		return fmt.Errorf("create issue CR %s: %w", name, err)
	}
	now := metav1.NewTime(s.now())
	iss.Status = tatarav1alpha1.IssueStatus{
		Title: title, Body: body, Author: botLogin(proj), State: "open", Status: "new",
		CreatedAt: &now, UpdatedAt: &now, LastSyncedAt: &now,
	}
	if err := s.c.Status().Update(ctx, iss); err != nil {
		return fmt.Errorf("seed issue CR status %s: %w", name, err)
	}
	return s.appendTaskRef(ctx, task.Name, name, "")
}

// appendTaskRef appends an issue / MR CR name to the Task's ref list.
func (s *Server) appendTaskRef(ctx context.Context, taskName, issueRef, mrRef string) error {
	key := types.NamespacedName{Namespace: s.ns, Name: taskName}
	return objbudget.FitTask(ctx, s.c, s.spiller, key, func(t *tatarav1alpha1.Task) {
		if issueRef != "" && !contains(t.Status.IssueRefs, issueRef) {
			t.Status.IssueRefs = append(t.Status.IssueRefs, issueRef)
			t.Status.Stats.IssueCount = len(t.Status.IssueRefs)
		}
		if mrRef != "" && !contains(t.Status.MRRefs, mrRef) {
			t.Status.MRRefs = append(t.Status.MRRefs, mrRef)
			t.Status.Stats.MRCount = len(t.Status.MRRefs)
		}
	})
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func botLogin(proj *tatarav1alpha1.Project) string {
	if proj.Spec.Scm == nil {
		return ""
	}
	return proj.Spec.Scm.BotLogin
}

// issueRefNumber pulls the number out of an owner/repo#N ref.
func issueRefNumber(ref string) int {
	i := strings.LastIndexByte(ref, '#')
	if i < 0 {
		return 0
	}
	n, err := strconv.Atoi(ref[i+1:])
	if err != nil {
		return 0
	}
	return n
}

// issueDeferred is the DEFERRED half of issue_write (edit|close|comment): it
// writes an Issue.status.pendingComments[] entry with a client-supplied
// requestId and returns 200. The forge write and the mirror write cannot be
// made atomic in an HTTP handler, and the retry re-posts (C.2.12, fix M7).
func (s *Server) issueDeferred(w http.ResponseWriter, r *http.Request, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task, req issueWriteReq) {
	ctx := r.Context()
	name := tatarav1alpha1.IssueName(repo.Name, req.Number)
	var iss tatarav1alpha1.Issue
	if err := s.c.Get(ctx, types.NamespacedName{Namespace: s.ns, Name: name}, &iss); err != nil {
		writeClientErr(w, err)
		return
	}
	// Controller-ownership gate on EVERY action that names a number (fix 7).
	// Without it, two Tasks that both own an issue could each spawn a pod and
	// converse with EACH OTHER on a human's thread.
	if ctrl, ok := own.ControllerOwner(&iss); !ok || ctrl != task.Name {
		obs.RestOwnershipRefusedTotal.WithLabelValues("issue").Inc()
		writeError(w, http.StatusConflict, "task does not own this issue")
		return
	}

	body := req.Body
	action := req.Action
	if action == "close" {
		body = req.Comment
	}
	// The self-comment guard is preserved for action=comment.
	if action == "comment" {
		if iss.Status.State == "closed" {
			writeJSON(w, http.StatusConflict, commentRefusedResp{
				Error: "comment refused", Refused: true, Reason: "target_closed",
			})
			return
		}
		if ok, reason := controller.PermitComment(task.Spec.Kind, mirrorToSCMComments(iss.Status.Comments),
			botLogin(proj), nil); !ok {
			writeJSON(w, http.StatusConflict, commentRefusedResp{
				Error: "comment refused", Refused: true, Reason: reason,
			})
			return
		}
	}

	requestID := newRequestID(task.Name, action, name, body)
	pc := tatarav1alpha1.PendingComment{RequestID: requestID, Action: pendingAction(action), Body: body}
	if action == "edit" {
		pc.Body = editIntentBody(req.Title, req.Body)
	}
	key := types.NamespacedName{Namespace: s.ns, Name: name}
	if err := objbudget.FitIssue(ctx, s.c, s.spiller, key, func(i *tatarav1alpha1.Issue) {
		for _, e := range i.Status.PendingComments {
			if e.RequestID == requestID {
				return
			}
		}
		if len(i.Status.PendingComments) >= pendingCommentsCap {
			return
		}
		i.Status.PendingComments = append(i.Status.PendingComments, pc)
	}); err != nil {
		writeClientErr(w, err)
		return
	}

	s.log.InfoContext(ctx, "restapi: issue write queued",
		append(reqLogFields(r), "action", "issue_write_"+action, "task", task.Name,
			"repo", repo.Name, "number", req.Number, "request_id_key", requestID)...)
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "repo": repo.Name, "number": req.Number,
		"requestId": requestID, "deferred": true,
	})
}

// pendingAction maps an issue_write action onto the PendingComment enum
// (comment|reply). edit and close are carried as comment intents; the Issue
// reconciler reads the action back out of the intent body prefix.
func pendingAction(action string) string {
	if action == "reply" {
		return "reply"
	}
	return "comment"
}

// editIntentBody encodes an edit intent's title/body pair.
func editIntentBody(title, body string) string {
	var b strings.Builder
	b.WriteString("<!-- tatara-edit -->\n")
	if title != "" {
		b.WriteString("title: " + title + "\n")
	}
	if body != "" {
		b.WriteString(body)
	}
	return b.String()
}

// newRequestID is the client-supplied idempotency key of a deferred write. It
// is derived, not random, so a retried HTTP call produces the SAME key and the
// reconciler's dedup holds.
func newRequestID(parts ...string) string {
	return fmt.Sprintf("%x", sha256Sum(strings.Join(parts, "|")))
}

func mirrorToSCMComments(in []tatarav1alpha1.Comment) []scm.IssueComment {
	out := make([]scm.IssueComment, 0, len(in))
	for _, c := range in {
		out = append(out, scm.IssueComment{
			ExternalID: c.ExternalID, Author: c.Author, Body: c.Body, CreatedAt: c.CreatedAt.Time,
		})
	}
	return out
}

// --- 15. POST /projects/{p}/scm/mr-write ----------------------------------

// mrWriteReq is C.2.13. Actions are exactly three: open, comment, reply. There
// is no merge, no approve, no request_changes: merge is operator-only, and a
// hallucinated merge call has nowhere to land.
type mrWriteReq struct {
	Task      string `json:"task,omitempty"`
	Action    string `json:"action"`
	Repo      string `json:"repo"`
	Number    int    `json:"number,omitempty"`
	Title     string `json:"title,omitempty"`
	Body      string `json:"body,omitempty"`
	InReplyTo string `json:"inReplyTo,omitempty"`
}

func (s *Server) mrWrite(w http.ResponseWriter, r *http.Request) {
	if !authorizeCaller(w, r) {
		return
	}
	ctx := r.Context()
	projName := chi.URLParam(r, "p")

	var req mrWriteReq
	if err := decodeJSON(r, w, &req); err != nil {
		writeDecodeError(w, r, err)
		return
	}
	if req.Repo == "" {
		writeError(w, http.StatusBadRequest, "repo required")
		return
	}
	proj, err := s.getProjectCR(ctx, projName)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	repo, err := s.repoCR(ctx, projName, req.Repo)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	task, ok := s.callerTask(w, r, taskParam(r, req.Task))
	if !ok {
		return
	}

	switch req.Action {
	case "open":
		if req.Number != 0 {
			writeError(w, http.StatusBadRequest, "action=open forbids number")
			return
		}
		if strings.TrimSpace(req.Title) == "" || strings.TrimSpace(req.Body) == "" {
			writeError(w, http.StatusBadRequest, "action=open requires title and body")
			return
		}
		s.mrOpen(w, r, proj, repo, task, req)
	case "comment":
		if req.Number == 0 || strings.TrimSpace(req.Body) == "" {
			writeError(w, http.StatusBadRequest, "action=comment requires number and body")
			return
		}
		s.mrDeferred(w, r, repo, task, req)
	case "reply":
		if req.Number == 0 || strings.TrimSpace(req.Body) == "" || req.InReplyTo == "" {
			writeError(w, http.StatusBadRequest, "action=reply requires number, inReplyTo and body")
			return
		}
		s.mrDeferred(w, r, repo, task, req)
	default:
		writeError(w, http.StatusBadRequest, "action must be one of open, comment, reply")
	}
}

// mrOpen is SYNCHRONOUS, IDEMPOTENT, and REFUSED after a merge (C.2.13).
func (s *Server) mrOpen(w http.ResponseWriter, r *http.Request, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task, req mrWriteReq) {
	ctx := r.Context()
	head := "task/" + task.Name

	mrs, err := s.ownedMRs(ctx, task)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	for i := range mrs {
		mr := &mrs[i]
		if mr.Spec.RepositoryRef != repo.Name {
			continue
		}
		// REFUSED when the Task already merged an MR for this repo (fix 2).
		// This is the structural stop on the duplicate-PR path after a partial
		// merge.
		if mr.Status.State == "merged" {
			obs.RestOwnershipRefusedTotal.WithLabelValues("mr-merged").Inc()
			writeError(w, http.StatusConflict, "task already merged an MR for this repo")
			return
		}
		// IDEMPOTENT (fix 13): a second open 422s on GitHub, and a TTL-stopped
		// implement pod that already opened its MR must have a way forward.
		if (mr.Status.State == "" || mr.Status.State == "open") &&
			(mr.Status.HeadBranch == "" || mr.Status.HeadBranch == head) {
			writeJSON(w, http.StatusOK, map[string]any{
				"status": "ok", "repo": repo.Name, "number": mr.Spec.Number,
				"url": mr.Spec.URL, "existing": true,
			})
			return
		}
	}

	writer, token, ok := s.projectSCMWriterAndToken(w, r, proj)
	if !ok {
		return
	}
	// The body passes through the C.7 close-directive ALLOWLIST: only the
	// issues this Task controller-owns may be auto-closed by the MR.
	issues, err := s.ownedIssues(ctx, task)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	body := controller.FilterCloseDirectives(req.Body, repoSlug(repo), allowedCloses(issues, repo))

	target := repo.Spec.DefaultBranch
	if target == "" {
		target = "main"
	}
	url, err := writer.OpenChange(ctx, repo.Spec.URL, token, head, target, req.Title, body)
	controller.RecordSCM(s.metrics, providerOf(proj), "open_change", err)
	if err != nil {
		s.log.ErrorContext(ctx, "restapi: opening MR failed",
			append(reqLogFields(r), "repo", repo.Name, "head", head, "error", err)...)
		writeError(w, http.StatusBadGateway, "scm write failed")
		return
	}
	number := prNumberFromURL(url)
	if number == 0 {
		writeError(w, http.StatusBadGateway, "scm returned no MR number")
		return
	}
	if err := s.mintMRCR(ctx, proj, repo, task, number, url, req.Title, body, head); err != nil {
		writeClientErr(w, err)
		return
	}
	s.log.InfoContext(ctx, "restapi: MR opened",
		append(reqLogFields(r), "action", "mr_write_open", "task", task.Name,
			"repo", repo.Name, "number", number, "url", url)...)
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "repo": repo.Name, "number": number, "url": url, "existing": false,
	})
}

func repoSlug(repo *tatarav1alpha1.Repository) string {
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return repo.Name
	}
	return owner + "/" + name
}

// allowedCloses is the C.7 allowlist: every Issue this Task controller-owns, in
// BOTH the reference forms a directive can carry (the repo CR name and the
// owner/repo slug).
func allowedCloses(issues []tatarav1alpha1.Issue, repo *tatarav1alpha1.Repository) map[controller.RepoNum]bool {
	out := make(map[controller.RepoNum]bool, len(issues)*2)
	slug := repoSlug(repo)
	for i := range issues {
		iss := &issues[i]
		out[controller.RepoNum{Repo: iss.Spec.RepositoryRef, Number: iss.Spec.Number}] = true
		if iss.Spec.RepositoryRef == repo.Name {
			out[controller.RepoNum{Repo: slug, Number: iss.Spec.Number}] = true
		}
	}
	return out
}

// prNumberFromURL pulls the PR/MR number off the URL the forge returned.
func prNumberFromURL(url string) int {
	i := strings.LastIndexByte(url, '/')
	if i < 0 {
		return 0
	}
	n, err := strconv.Atoi(url[i+1:])
	if err != nil {
		return 0
	}
	return n
}

func (s *Server) mintMRCR(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task, number int, url, title, body, head string) error {
	name := tatarav1alpha1.MergeRequestName(repo.Name, number)
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.ns},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo.Name, Number: number, URL: url, ProjectRef: proj.Name,
		},
	}
	own.AddPlainOwner(mr, task)
	if err := own.HandOverController(mr, nil, task); err != nil {
		return err
	}
	if err := s.c.Create(ctx, mr); err != nil {
		return fmt.Errorf("create merge request CR %s: %w", name, err)
	}
	now := metav1.NewTime(s.now())
	mr.Status = tatarav1alpha1.MergeRequestStatus{
		Title: title, Body: body, Author: botLogin(proj), State: "open", Status: "new",
		HeadBranch: head, CreatedAt: &now, UpdatedAt: &now, LastSyncedAt: &now,
	}
	if err := s.c.Status().Update(ctx, mr); err != nil {
		return fmt.Errorf("seed merge request CR status %s: %w", name, err)
	}
	return s.appendTaskRef(ctx, task.Name, "", name)
}

// mrDeferred is the DEFERRED half of mr_write (comment|reply). mr_write(comment)
// has no DuplicateRecentBotComment guard today; the requestId marker is what
// replaces it.
func (s *Server) mrDeferred(w http.ResponseWriter, r *http.Request,
	repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task, req mrWriteReq) {
	ctx := r.Context()
	name := tatarav1alpha1.MergeRequestName(repo.Name, req.Number)
	var mr tatarav1alpha1.MergeRequest
	if err := s.c.Get(ctx, types.NamespacedName{Namespace: s.ns, Name: name}, &mr); err != nil {
		writeClientErr(w, err)
		return
	}
	if ctrl, ok := own.ControllerOwner(&mr); !ok || ctrl != task.Name {
		obs.RestOwnershipRefusedTotal.WithLabelValues("mr").Inc()
		writeError(w, http.StatusConflict, "task does not own this merge request")
		return
	}

	requestID := newRequestID(task.Name, req.Action, name, req.Body, req.InReplyTo)
	pc := tatarav1alpha1.PendingComment{
		RequestID: requestID, Action: req.Action, Body: req.Body, InReplyTo: req.InReplyTo,
	}
	key := types.NamespacedName{Namespace: s.ns, Name: name}
	if err := objbudget.FitMergeRequest(ctx, s.c, s.spiller, key, func(m *tatarav1alpha1.MergeRequest) {
		for _, e := range m.Status.PendingComments {
			if e.RequestID == requestID {
				return
			}
		}
		if len(m.Status.PendingComments) >= pendingCommentsCap {
			return
		}
		m.Status.PendingComments = append(m.Status.PendingComments, pc)
	}); err != nil {
		writeClientErr(w, err)
		return
	}

	s.log.InfoContext(ctx, "restapi: mr write queued",
		append(reqLogFields(r), "action", "mr_write_"+req.Action, "task", task.Name,
			"repo", repo.Name, "number", req.Number, "request_id_key", requestID)...)
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "repo": repo.Name, "number": req.Number,
		"requestId": requestID, "deferred": true,
	})
}

// updateTaskSpec applies a spec mutation under RetryOnConflict. Spec is
// operator-writable and agent-unwritable; this is the operator writing it.
func (s *Server) updateTaskSpec(ctx context.Context, name string, mutate func(*tatarav1alpha1.Task)) error {
	key := types.NamespacedName{Namespace: s.ns, Name: name}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var t tatarav1alpha1.Task
		if err := s.c.Get(ctx, key, &t); err != nil {
			return err
		}
		mutate(&t)
		return s.c.Update(ctx, &t)
	})
}
