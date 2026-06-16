package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// Config holds webhook server dependencies.
type Config struct {
	Client    client.Client
	Namespace string
	Metrics   *obs.OperatorMetrics
	Logger    *slog.Logger
}

// Server serves the SCM webhook endpoint.
type Server struct {
	cfg Config
	log *slog.Logger
}

// NewServer constructs a webhook Server.
func NewServer(cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Metrics == nil {
		panic("webhook.NewServer: cfg.Metrics must not be nil")
	}
	return &Server{cfg: cfg, log: cfg.Logger}
}

// Mount registers the webhook route onto an existing chi router. Use this
// when composing with other route groups on a shared listener.
func (s *Server) Mount(r chi.Router) {
	r.Post("/operator/webhooks/{project}", s.handle)
}

// Handler returns a standalone http.Handler with the webhook route. Kept for
// backward-compatible use by NewRunnable in tests.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	s.Mount(r)
	return r
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	projectName := chi.URLParam(r, "project")

	body, err := readBody(r)
	if err != nil {
		s.count("unknown", "other", "other", "bad_request")
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	provider, err := scm.Select(r.Header)
	if err != nil {
		s.count("unknown", "other", "other", "bad_request")
		http.Error(w, "unrecognized provider", http.StatusBadRequest)
		return
	}
	providerName := provider.Provider()

	var proj tatarav1.Project
	if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, projectName), &proj); err != nil {
		if apierrors.IsNotFound(err) {
			s.count(providerName, "other", "other", "unknown_project")
			http.Error(w, "unknown project", http.StatusNotFound)
			return
		}
		s.count(providerName, "other", "other", "error")
		http.Error(w, "lookup project", http.StatusInternalServerError)
		return
	}

	webhookSecret, err := s.webhookSecret(ctx, proj.Spec.ScmSecretRef)
	if err != nil {
		s.count(providerName, "other", "other", "error")
		http.Error(w, "secret", http.StatusInternalServerError)
		return
	}

	ev, err := provider.DetectAndVerify(r.Header, body, webhookSecret)
	if err != nil {
		s.count(providerName, "other", "other", "bad_signature")
		http.Error(w, "verification failed", http.StatusUnauthorized)
		return
	}

	switch ev.Kind {
	case "push":
		s.handlePush(ctx, w, providerName, projectName, ev)
	case "issue", "mr":
		s.handleWorkItem(ctx, w, providerName, proj, ev)
	default:
		s.count(providerName, "other", ev.Action, "ignored")
		w.WriteHeader(http.StatusAccepted)
	}
}

func (s *Server) handlePush(ctx context.Context, w http.ResponseWriter, provider, projectName string, ev scm.WebhookEvent) {
	var repos tatarav1.RepositoryList
	if err := s.cfg.Client.List(ctx, &repos, client.InNamespace(s.cfg.Namespace)); err != nil {
		s.count(provider, "push", ev.Action, "error")
		http.Error(w, "list repositories", http.StatusInternalServerError)
		return
	}
	for i := range repos.Items {
		repo := &repos.Items[i]
		if repo.Spec.ProjectRef != projectName {
			continue
		}
		if !scm.SameRemote(repo.Spec.URL, ev.Repo) || ev.Branch != repo.Spec.DefaultBranch {
			continue
		}
		if repo.Annotations == nil {
			repo.Annotations = map[string]string{}
		}
		repo.Annotations[tatarav1.ReingestRequestedAnnotation] = time.Now().UTC().Format(time.RFC3339)
		if err := s.cfg.Client.Update(ctx, repo); err != nil {
			s.count(provider, "push", ev.Action, "error")
			http.Error(w, "annotate repository", http.StatusInternalServerError)
			return
		}
		s.log.InfoContext(ctx, "webhook push re-ingest requested",
			"provider", provider, "project", projectName, "repository", repo.Name, "branch", ev.Branch)
		s.count(provider, "push", ev.Action, "accepted")
		w.WriteHeader(http.StatusAccepted)
		return
	}
	s.count(provider, "push", ev.Action, "ignored")
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleWorkItem(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	// issue_comment created (on an issue OR an MR): find a live lifecycle Task
	// for the work item and react. Intercepted before the trigger-label gate so
	// bot comments still pass through correctly (they are ignored by the
	// bot-author gate in handleIssueComment). "created" is unique to
	// issue_comment events, so this routes both issue and MR comments.
	if ev.Action == "created" {
		s.handleIssueComment(ctx, w, provider, proj, ev)
		return
	}

	// triggerLabel on a Conversation task -> jump to Implement without spawning
	// a new task. Checked before the dedup loop so we never create a duplicate.
	// Also clears DeadlineAt so MRCI gets a fresh babysit deadline (not the stale
	// conversation idle deadline). Wrapped in RetryOnConflict to avoid clobbering
	// a concurrent lifecycle reconcile.
	if ev.Action == "labeled" && !ev.IsPR && ev.ChangedLabel == proj.Spec.TriggerLabel {
		if task, found := s.findLifecycleTask(ctx, proj.Name, ev.IssueRef); found &&
			task.Status.LifecycleState == "Conversation" {
			updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				fresh := &tatarav1.Task{}
				if err := s.cfg.Client.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
					return err
				}
				fresh.Status.LifecycleState = "Implement"
				fresh.Status.DeadlineAt = nil // clear stale conversation deadline
				return s.cfg.Client.Status().Update(ctx, fresh)
			})
			if updateErr != nil {
				s.count(provider, ev.Kind, ev.Action, "error")
				http.Error(w, "update task", http.StatusInternalServerError)
				return
			}
			s.log.InfoContext(ctx, "triggerLabel on Conversation task: set Implement",
				"project", proj.Name, "task", task.Name, "issue_ref", ev.IssueRef)
			s.count(provider, ev.Kind, ev.Action, "accepted")
			w.WriteHeader(http.StatusAccepted)
			return
		}
	}

	// Determine Task Kind and gating based on PR/issue and author.
	bot := ""
	scope := "labeledOrMentioned"
	if proj.Spec.Scm != nil {
		bot = proj.Spec.Scm.BotLogin
		if proj.Spec.Scm.PRReactionScope != "" {
			scope = proj.Spec.Scm.PRReactionScope
		}
	}

	// kind switch: issue with triggerLabel -> issueLifecycle (was "implement");
	// bot PR -> issueLifecycle (was "selfImprove"); migration note: in-flight
	// "implement"/"selfImprove" tasks created before this deploy still complete
	// via the old writeback arms.
	//
	// For GitLab, AuthorLogin == ActorLogin (the webhook carries only the event
	// actor, not the MR author). Trusting AuthorLogin==bot for kind selection
	// would misclassify any event the bot triggers on a human's MR. Restrict the
	// bot-authorship branch to GitHub, where AuthorLogin is the real PR author.
	// GitLab MRs default to "review" and the authoritative authorship gate runs
	// in the controller via GetPRState (see scm-author-vs-actor-egress-gate memory).
	kind := "issueLifecycle"
	if ev.IsPR {
		if ev.AuthorLogin == bot && bot != "" && provider == "github" {
			kind = "issueLifecycle"
		} else {
			kind = "review"
		}
		if scope == "labeledOrMentioned" && !slices.Contains(ev.Labels, proj.Spec.TriggerLabel) && !mentionsBot(ev.Body, bot) {
			s.count(provider, ev.Kind, ev.Action, "ignored")
			w.WriteHeader(http.StatusAccepted)
			return
		}
	} else {
		if !slices.Contains(ev.Labels, proj.Spec.TriggerLabel) {
			s.count(provider, ev.Kind, ev.Action, "ignored")
			w.WriteHeader(http.StatusAccepted)
			return
		}
	}

	var repos tatarav1.RepositoryList
	if err := s.cfg.Client.List(ctx, &repos, client.InNamespace(s.cfg.Namespace)); err != nil {
		s.count(provider, ev.Kind, ev.Action, "error")
		http.Error(w, "list repositories", http.StatusInternalServerError)
		return
	}
	var repo *tatarav1.Repository
	for i := range repos.Items {
		r := &repos.Items[i]
		if r.Spec.ProjectRef == proj.Name && scm.SameRemote(r.Spec.URL, ev.Repo) {
			repo = r
			break
		}
	}
	if repo == nil {
		s.log.InfoContext(ctx, "work item labeled but no matching repository",
			"project", proj.Name, "remote", ev.Repo, "issue_ref", ev.IssueRef)
		s.count(provider, ev.Kind, ev.Action, "no_repo")
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// Dedupe: creating an issue with the label fires both issues.opened and
	// issues.labeled for the same issue. Skip if a non-terminal Task already
	// exists for this issue ref (re-labeling after completion still re-triggers).
	// Use MatchingLabels so the cache filters Tasks to the dedup key before the
	// in-Go scan, avoiding an O(all-tasks) walk per webhook.
	dedupSlug, _ := issueRefRepoSlug(ev.IssueRef)
	var existing tatarav1.TaskList
	listOpts := []client.ListOption{
		client.InNamespace(s.cfg.Namespace),
		client.MatchingLabels{
			tatarav1.LabelSourceRepo:   dedupSlug,
			tatarav1.LabelSourceNumber: strconv.Itoa(ev.Number),
		},
	}
	if err := s.cfg.Client.List(ctx, &existing, listOpts...); err != nil {
		s.count(provider, ev.Kind, ev.Action, "error")
		http.Error(w, "list tasks", http.StatusInternalServerError)
		return
	}
	for i := range existing.Items {
		t := &existing.Items[i]
		if t.Spec.Source != nil && t.Spec.Source.IssueRef == ev.IssueRef &&
			t.Status.Phase != "Succeeded" && t.Status.Phase != "Failed" {
			s.log.InfoContext(ctx, "work item already has an active task; skipping duplicate",
				"project", proj.Name, "issue_ref", ev.IssueRef, "task", t.Name)
			s.count(provider, ev.Kind, ev.Action, "duplicate")
			w.WriteHeader(http.StatusAccepted)
			return
		}
	}

	// Determine the lifecycle entry state for issueLifecycle tasks. This is set
	// as a create-time annotation so reconcileLifecycle can initialize
	// LifecycleState atomically from it, without a separate post-create
	// Status().Update that may be lost.
	ann := map[string]string{}
	// Dedup labels: set for issueLifecycle tasks so the webhook-born Task uses
	// the same dedup key as a cron mrScan/issueScan Task for the same work item,
	// preventing duplicate lifecycle Tasks.
	var labels map[string]string
	if kind == "issueLifecycle" {
		if ev.IsPR {
			ann[tatarav1.LifecycleEntryAnnotation] = "MRCI"
			// Bot-PR dedup key: linked issue number from "Closes #N" when present,
			// else the PR number - mirroring mrScan's key exactly.
			dedupNumber := ev.Number
			if issueNum, linked := scm.LinkedIssueNumber(ev.Body); linked {
				dedupNumber = issueNum
			}
			repoSlug, _ := issueRefRepoSlug(ev.IssueRef)
			labels = map[string]string{
				tatarav1.LabelSourceRepo:   repoSlug,
				tatarav1.LabelSourceNumber: strconv.Itoa(dedupNumber),
				tatarav1.LabelSourceKind:   kind,
				tatarav1.LabelActivity:     "webhook",
			}
		} else {
			ann[tatarav1.LifecycleEntryAnnotation] = "Implement"
			repoSlug, _ := issueRefRepoSlug(ev.IssueRef)
			labels = map[string]string{
				tatarav1.LabelSourceRepo:   repoSlug,
				tatarav1.LabelSourceNumber: strconv.Itoa(ev.Number),
				tatarav1.LabelSourceKind:   kind,
				tatarav1.LabelActivity:     "webhook",
			}
		}
	}

	// For issueLifecycle tasks, use a deterministic name derived from the dedup
	// key so concurrent webhook deliveries for the same work item race to a
	// single winner: the second Create returns AlreadyExists instead of silently
	// creating a duplicate (GenerateName makes both succeeds). review tasks use
	// GenerateName because multiple review Tasks per PR are intentional.
	taskName := ""
	taskGenerateName := "task-"
	if kind == "issueLifecycle" {
		taskName = issueLifecycleTaskName(proj.Name, ev.IssueRef)
		taskGenerateName = ""
	}

	task := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:            taskName,
			GenerateName:    taskGenerateName,
			Namespace:       s.cfg.Namespace,
			Annotations:     ann,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(&proj, tatarav1.GroupVersion.WithKind("Project"))},
		},
		Spec: tatarav1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Goal:          ev.Body,
			Kind:          kind,
			Source: &tatarav1.TaskSource{
				Provider:    provider,
				IssueRef:    ev.IssueRef,
				URL:         ev.URL,
				AuthorLogin: ev.AuthorLogin,
				IsPR:        ev.IsPR,
				Number:      ev.Number,
			},
		},
	}
	agent.StampPodName(task, proj.Name, provider, repo.Name)
	if err := s.cfg.Client.Create(ctx, task); err != nil {
		if apierrors.IsAlreadyExists(err) && kind == "issueLifecycle" {
			s.log.InfoContext(ctx, "work item task already exists (concurrent create); treating as duplicate",
				"project", proj.Name, "issue_ref", ev.IssueRef)
			s.count(provider, ev.Kind, ev.Action, "duplicate")
			w.WriteHeader(http.StatusAccepted)
			return
		}
		s.count(provider, ev.Kind, ev.Action, "error")
		http.Error(w, "create task", http.StatusInternalServerError)
		return
	}
	// NOTE: No post-create Status().Update for lifecycle state. The entry state is
	// carried by the LifecycleEntryAnnotation and consumed by reconcileLifecycle on
	// the first reconcile.
	s.log.InfoContext(ctx, "work item created task",
		"project", proj.Name, "repository", repo.Name,
		"task", task.Name, "issue_ref", ev.IssueRef, "kind", kind)
	s.count(provider, ev.Kind, ev.Action, "task_created")
	w.WriteHeader(http.StatusAccepted)
}

// handleIssueComment reacts to an issue_comment (action=created) webhook on an
// issue OR an MR. Bot comments are ignored to prevent self-trigger loops.
// Otherwise:
//   - a live task with a turn in flight -> the comment is queued as a pending
//     interjection so the reconciler injects it into the running session;
//   - a live but idle task -> LastActivityAt/DeadlineAt are bumped and a
//     Conversation/Stopped task is re-activated to Triage (a fresh triage
//     re-reads the full thread);
//   - no live task -> a Parked owning task is reactivated, else a fresh task is
//     created at Triage.
func (s *Server) handleIssueComment(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}

	// ActorLogin is the sender of the event (comment author for issue_comment).
	if botLogin != "" && ev.ActorLogin == botLogin {
		s.log.InfoContext(ctx, "issue_comment: bot-authored comment ignored",
			"project", proj.Name, "issue_ref", ev.IssueRef)
		s.count(provider, ev.Kind, ev.Action, "ignored")
		w.WriteHeader(http.StatusAccepted)
		return
	}

	task, found := s.findLifecycleTask(ctx, proj.Name, ev.IssueRef)
	if !found {
		// No agent nursing this issue/MR: reactivate a Parked owning task, else
		// create a fresh lifecycle task at Triage. Applies to issues AND MRs.
		if parked, ok := s.findReactivatableTask(ctx, proj.Name, ev.IssueRef); ok {
			s.reactivateTask(ctx, w, provider, proj, ev, parked)
			return
		}
		s.createLifecycleTaskAtTriage(ctx, w, provider, proj, ev)
		return
	}

	// A live task is nursing this work item with a turn in flight: the comment
	// must interrupt that run. Queue it as a pending interjection for the
	// reconciler to inject into the live session (like a user adding context
	// mid-session). Do not change lifecycle state or shorten the deadline.
	if taskHasInflightTurn(task) {
		if updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tatarav1.Task{}
			if err := s.cfg.Client.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
				return err
			}
			now := metav1.Now()
			fresh.Status.LastActivityAt = &now
			if ev.CommentBody != "" {
				fresh.Status.PendingInterjections = appendCapped(
					fresh.Status.PendingInterjections, ev.CommentBody, maxPendingInterjections)
			}
			return s.cfg.Client.Status().Update(ctx, fresh)
		}); updateErr != nil {
			s.log.ErrorContext(ctx, "issue_comment: queue interjection", "error", updateErr, "task", task.Name)
			s.count(provider, ev.Kind, ev.Action, "error")
			http.Error(w, "update task", http.StatusInternalServerError)
			return
		}
		s.log.InfoContext(ctx, "issue_comment: queued interjection for in-flight turn",
			"project", proj.Name, "task", task.Name, "issue_ref", ev.IssueRef)
		s.count(provider, ev.Kind, ev.Action, "accepted")
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// Idle live task (between turns, Conversation, or Stopped): bump activity and
	// deadline; re-activate Conversation/Stopped to Triage so the reconciler
	// re-spawns. A fresh triage re-reads the full thread, so the new comment is
	// already in context.
	idleMinutes := 60
	if proj.Spec.Scm != nil && proj.Spec.Scm.ConversationIdleMinutes > 0 {
		idleMinutes = proj.Spec.Scm.ConversationIdleMinutes
	}

	// Wrap in RetryOnConflict to avoid clobbering a concurrent lifecycle reconcile
	// that may be advancing LifecycleState at the same time (FIX 3).
	if updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1.Task{}
		if err := s.cfg.Client.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		now := metav1.Now()
		deadline := metav1.NewTime(now.Add(time.Duration(idleMinutes) * time.Minute))
		fresh.Status.LastActivityAt = &now
		fresh.Status.DeadlineAt = &deadline
		// Conversation or Stopped -> re-activate to Triage so the reconciler re-spawns.
		if fresh.Status.LifecycleState == "Conversation" || fresh.Status.LifecycleState == "Stopped" {
			fresh.Status.LifecycleState = "Triage"
		}
		return s.cfg.Client.Status().Update(ctx, fresh)
	}); updateErr != nil {
		s.log.ErrorContext(ctx, "issue_comment: update task status", "error", updateErr, "task", task.Name)
		s.count(provider, ev.Kind, ev.Action, "error")
		http.Error(w, "update task", http.StatusInternalServerError)
		return
	}
	s.log.InfoContext(ctx, "issue_comment: lifecycle task updated",
		"project", proj.Name, "task", task.Name,
		"issue_ref", ev.IssueRef)
	s.count(provider, ev.Kind, ev.Action, "accepted")
	w.WriteHeader(http.StatusAccepted)
}

// createLifecycleTaskAtTriage creates a new issueLifecycle Task at Triage for an
// issue_comment on an issue with no existing live task. Uses the same dedup labels
// and lifecycle-entry annotation as issueScan so the two paths share the same key.
func (s *Server) createLifecycleTaskAtTriage(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	// Find the matching Repository for the event's repo URL.
	var repos tatarav1.RepositoryList
	if err := s.cfg.Client.List(ctx, &repos, client.InNamespace(s.cfg.Namespace)); err != nil {
		s.count(provider, ev.Kind, ev.Action, "error")
		http.Error(w, "list repositories", http.StatusInternalServerError)
		return
	}
	var repo *tatarav1.Repository
	for i := range repos.Items {
		r := &repos.Items[i]
		if r.Spec.ProjectRef == proj.Name && scm.SameRemote(r.Spec.URL, ev.Repo) {
			repo = r
			break
		}
	}
	if repo == nil {
		s.log.InfoContext(ctx, "issue_comment: no matching repository for untracked issue",
			"project", proj.Name, "remote", ev.Repo, "issue_ref", ev.IssueRef)
		s.count(provider, ev.Kind, ev.Action, "ignored")
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// Dedup labels matching the issueScan convention.
	repoSlug, _ := issueRefRepoSlug(ev.IssueRef)
	labels := map[string]string{
		tatarav1.LabelSourceRepo:   repoSlug,
		tatarav1.LabelSourceNumber: strconv.Itoa(ev.Number),
		tatarav1.LabelSourceKind:   "issueLifecycle",
		tatarav1.LabelActivity:     "webhook",
	}
	ann := map[string]string{tatarav1.LifecycleEntryAnnotation: "Triage"}

	task := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:            issueLifecycleTaskName(proj.Name, ev.IssueRef),
			Namespace:       s.cfg.Namespace,
			Annotations:     ann,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(&proj, tatarav1.GroupVersion.WithKind("Project"))},
		},
		Spec: tatarav1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Goal:          ev.Body,
			Kind:          "issueLifecycle",
			Source: &tatarav1.TaskSource{
				Provider:    provider,
				IssueRef:    ev.IssueRef,
				URL:         ev.URL,
				AuthorLogin: ev.AuthorLogin,
				IsPR:        ev.IsPR,
				Number:      ev.Number,
			},
		},
	}
	agent.StampPodName(task, proj.Name, provider, repo.Name)
	if err := s.cfg.Client.Create(ctx, task); err != nil {
		if apierrors.IsAlreadyExists(err) {
			s.log.InfoContext(ctx, "issue_comment: lifecycle task already exists (concurrent create); treating as duplicate",
				"project", proj.Name, "issue_ref", ev.IssueRef)
			s.count(provider, ev.Kind, ev.Action, "duplicate")
			w.WriteHeader(http.StatusAccepted)
			return
		}
		s.count(provider, ev.Kind, ev.Action, "error")
		http.Error(w, "create task", http.StatusInternalServerError)
		return
	}
	s.log.InfoContext(ctx, "issue_comment: created lifecycle task at Triage for untracked issue",
		"project", proj.Name, "repository", repo.Name, "task", task.Name, "issue_ref", ev.IssueRef)
	s.count(provider, ev.Kind, ev.Action, "task_created")
	w.WriteHeader(http.StatusAccepted)
}

// findLifecycleTask finds a non-terminal issueLifecycle Task for the given
// issue ref within the project. Returns (task, true) when found.
func (s *Server) findLifecycleTask(ctx context.Context, projectName, issueRef string) (*tatarav1.Task, bool) {
	var tasks tatarav1.TaskList
	opts := s.taskListOpts(issueRef)
	if err := s.cfg.Client.List(ctx, &tasks, opts...); err != nil {
		return nil, false
	}
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.Kind != "issueLifecycle" || t.Spec.ProjectRef != projectName {
			continue
		}
		if t.Spec.Source == nil || t.Spec.Source.IssueRef != issueRef {
			continue
		}
		// Skip terminal lifecycle states (Done/Stopped/Parked) and terminal phases
		// (Succeeded/Failed). Stopped is resumable so we include it.
		switch t.Status.LifecycleState {
		case "Done", "Parked":
			continue
		}
		if t.Status.Phase == "Succeeded" || t.Status.Phase == "Failed" {
			continue
		}
		return t, true
	}
	return nil, false
}

// findReactivatableTask returns an owning issueLifecycle Task for issueRef that
// went terminal but is resumable (LifecycleState == "Parked"). Done tasks are
// NOT reactivated (their work is complete). Returns (task, true) when found.
func (s *Server) findReactivatableTask(ctx context.Context, projectName, issueRef string) (*tatarav1.Task, bool) {
	var tasks tatarav1.TaskList
	opts := s.taskListOpts(issueRef)
	if err := s.cfg.Client.List(ctx, &tasks, opts...); err != nil {
		return nil, false
	}
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.Kind != "issueLifecycle" || t.Spec.ProjectRef != projectName {
			continue
		}
		if t.Spec.Source == nil || t.Spec.Source.IssueRef != issueRef {
			continue
		}
		if t.Status.LifecycleState == "Parked" {
			return t, true
		}
	}
	return nil, false
}

// taskListOpts builds list options that pre-filter Tasks by the dedup labels
// derived from issueRef. Falls back to namespace-only if the ref is unparseable.
func (s *Server) taskListOpts(issueRef string) []client.ListOption {
	repoSlug, ok := issueRefRepoSlug(issueRef)
	if !ok {
		return []client.ListOption{client.InNamespace(s.cfg.Namespace)}
	}
	return []client.ListOption{
		client.InNamespace(s.cfg.Namespace),
		client.MatchingLabels{
			tatarav1.LabelSourceRepo: repoSlug,
			tatarav1.LabelSourceKind: "issueLifecycle",
		},
	}
}

// reactivateTask resumes a Parked owning Task: it clears the agent-run state
// (Phase, turn annotations) and the wrapper pod/service, sets LifecycleState
// back to Triage, and stamps LastActivityAt/DeadlineAt so the reconciler
// re-triages the issue with the new comment.
func (s *Server) reactivateTask(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent, task *tatarav1.Task) {
	// Best-effort delete the wrapper pod + service (shared helper; same teardown
	// the controller runs on terminal lifecycle transitions).
	if err := agent.DeleteWrapper(ctx, s.cfg.Client, s.cfg.Namespace, task); err != nil {
		s.log.ErrorContext(ctx, "reactivate: delete wrapper (non-fatal)", "error", err, "task", task.Name)
	}

	idleMinutes := 60
	if proj.Spec.Scm != nil && proj.Spec.Scm.ConversationIdleMinutes > 0 {
		idleMinutes = proj.Spec.Scm.ConversationIdleMinutes
	}
	if updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1.Task{}
		if err := s.cfg.Client.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		now := metav1.Now()
		deadline := metav1.NewTime(now.Add(time.Duration(idleMinutes) * time.Minute))
		fresh.Status.LifecycleState = "Triage"
		fresh.Status.Phase = ""
		fresh.Status.LastActivityAt = &now
		fresh.Status.DeadlineAt = &deadline
		if err := s.cfg.Client.Status().Update(ctx, fresh); err != nil {
			return err
		}
		// Clear turn annotations (metadata update, separate from status).
		fresh2 := &tatarav1.Task{}
		if err := s.cfg.Client.Get(ctx, client.ObjectKeyFromObject(task), fresh2); err != nil {
			return err
		}
		if fresh2.Annotations != nil {
			delete(fresh2.Annotations, tatarav1.AnnCurrentTurn)
			delete(fresh2.Annotations, tatarav1.AnnCurrentSubtask)
			delete(fresh2.Annotations, tatarav1.AnnTurnComplete)
			delete(fresh2.Annotations, tatarav1.AnnTurnStartedAt)
			delete(fresh2.Annotations, tatarav1.AnnPodRecreations)
		}
		return s.cfg.Client.Update(ctx, fresh2)
	}); updateErr != nil {
		s.log.ErrorContext(ctx, "reactivate: update task", "error", updateErr, "task", task.Name)
		s.count(provider, ev.Kind, ev.Action, "error")
		http.Error(w, "reactivate task", http.StatusInternalServerError)
		return
	}
	s.log.InfoContext(ctx, "issue_comment: reactivated parked lifecycle task",
		"project", proj.Name, "task", task.Name, "issue_ref", ev.IssueRef)
	s.count(provider, ev.Kind, ev.Action, "accepted")
	w.WriteHeader(http.StatusAccepted)
}

func mentionsBot(body, bot string) bool {
	return bot != "" && strings.Contains(body, "@"+bot)
}

// issueRefRepoSlug strips the trailing separator ('#' or '!') and numeric id
// from an IssueRef of the form "owner/repo#N" (GitHub) or "group/proj!N"
// (GitLab MR), returning the repo path with '/' replaced by '.'.
// Returns ("", false) when neither separator is present or the ref is malformed.
func issueRefRepoSlug(issueRef string) (string, bool) {
	idx := strings.LastIndexAny(issueRef, "#!")
	if idx < 0 {
		return "", false
	}
	return strings.ReplaceAll(issueRef[:idx], "/", "."), true
}

// issueLifecycleTaskName returns a deterministic Kubernetes-safe Task name for
// an issueLifecycle task scoped to (projectName, issueRef). Using a deterministic
// name rather than GenerateName makes concurrent webhook deliveries for the same
// work item idempotent: the second Create returns AlreadyExists which is treated
// as a duplicate, preventing two live lifecycle Tasks for one work item.
func issueLifecycleTaskName(projectName, issueRef string) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s", projectName, issueRef)))
	return "lc-" + hex.EncodeToString(h[:])[:16]
}

// maxPendingInterjections caps the queue of comments waiting to be injected into
// a live turn, so a comment storm cannot grow Task status without bound. The
// oldest entries are dropped first.
const maxPendingInterjections = 20

// taskHasInflightTurn reports whether the Task currently has an agent turn in
// flight: a current-turn id is set and its completion callback has not arrived.
func taskHasInflightTurn(t *tatarav1.Task) bool {
	return t.Annotations[tatarav1.AnnCurrentTurn] != "" && t.Annotations[tatarav1.AnnTurnComplete] == ""
}

// appendCapped appends v to s, keeping at most max entries by dropping the
// oldest. max <= 0 means unbounded.
func appendCapped(s []string, v string, max int) []string {
	s = append(s, v)
	if max > 0 && len(s) > max {
		s = s[len(s)-max:]
	}
	return s
}

func (s *Server) webhookSecret(ctx context.Context, ref string) (string, error) {
	var sec corev1.Secret
	if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, ref), &sec); err != nil {
		return "", err
	}
	v, ok := sec.Data["webhookSecret"]
	if !ok {
		return "", errors.New("secret missing webhookSecret key")
	}
	if len(v) == 0 {
		return "", errors.New("secret webhookSecret is empty")
	}
	return string(v), nil
}

func (s *Server) count(provider, kind, action, result string) {
	s.cfg.Metrics.WebhookEvent(provider, kind, action, result)
}

func objKey(ns, name string) client.ObjectKey {
	return client.ObjectKey{Namespace: ns, Name: name}
}

func readBody(r *http.Request) ([]byte, error) {
	defer func() { _ = r.Body.Close() }()
	return io.ReadAll(io.LimitReader(r.Body, 5<<20))
}

// Runnable adapts the webhook Server to controller-runtime's manager.Runnable.
type Runnable struct {
	srv  *Server
	addr string
}

// NewRunnable wraps a Server so it can be registered with mgr.Add.
func NewRunnable(srv *Server, addr string) *Runnable {
	return &Runnable{srv: srv, addr: addr}
}

// Start serves HTTP until ctx is cancelled, then gracefully shuts down.
func (run *Runnable) Start(ctx context.Context) error {
	return serveHTTP(ctx, run.addr, run.srv.Handler())
}

// HandlerRunnable serves an arbitrary http.Handler as a manager Runnable.
// Use this when composing multiple route groups (e.g. webhook + REST API) on
// a single shared listener.
type HandlerRunnable struct {
	handler http.Handler
	addr    string
}

// NewHandlerRunnable wraps any http.Handler so it can be registered with mgr.Add.
func NewHandlerRunnable(handler http.Handler, addr string) *HandlerRunnable {
	return &HandlerRunnable{handler: handler, addr: addr}
}

// Start serves HTTP until ctx is cancelled, then gracefully shuts down.
func (run *HandlerRunnable) Start(ctx context.Context) error {
	return serveHTTP(ctx, run.addr, run.handler)
}

// NeedLeaderElection implements manager.LeaderElectionRunnable. The webhook
// and REST API servers are stateless and must start on every replica
// immediately, before the leader lease is acquired.
func (run *HandlerRunnable) NeedLeaderElection() bool { return false }

func serveHTTP(ctx context.Context, addr string, handler http.Handler) error {
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return httpSrv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
