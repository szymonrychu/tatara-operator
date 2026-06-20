package webhook

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sort"
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
	"github.com/szymonrychu/tatara-operator/internal/incident"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// Config holds webhook server dependencies.
type Config struct {
	Client    client.Client
	Namespace string
	Metrics   *obs.OperatorMetrics
	Logger    *slog.Logger
	Seq       *queue.SeqAllocator
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
	if cfg.Seq == nil {
		a := queue.NewSeqAllocator()
		a.MarkReady()
		cfg.Seq = a
	}
	return &Server{cfg: cfg, log: cfg.Logger}
}

// Mount registers the webhook route onto an existing chi router. Use this
// when composing with other route groups on a shared listener.
func (s *Server) Mount(r chi.Router) {
	r.Post("/operator/webhooks/{project}", s.handle)
	r.Post("/operator/webhooks/{project}/grafana", s.handleGrafanaAlert)
}

// Handler returns a standalone http.Handler with the webhook route. Kept for
// backward-compatible use by NewRunnable in tests.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	s.Mount(r)
	return r
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()
	providerName := "unknown"
	durResult := "error"
	defer func() {
		s.cfg.Metrics.ObserveWebhookDuration(providerName, durResult, time.Since(t0).Seconds())
	}()

	ctx := r.Context()
	projectName := chi.URLParam(r, "project")

	body, err := readBody(r)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			s.count("unknown", "other", "other", "too_large")
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
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
	providerName = provider.Provider()

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

	// Guard: reject misrouted webhooks before any signature work. A GitHub delivery
	// to a GitLab-configured project (or vice versa) would otherwise fail with a
	// confusing bad_signature 401 rather than a clear provider_mismatch 400.
	if proj.Spec.Scm != nil && proj.Spec.Scm.Provider != "" && proj.Spec.Scm.Provider != providerName {
		s.count(providerName, "other", "other", "provider_mismatch")
		http.Error(w, "provider mismatch", http.StatusBadRequest)
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

	durResult = "ok"
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
	// issue_comment / Note Hook: find a live lifecycle Task for the work item
	// and react. Intercepted before the trigger-label gate so bot comments still
	// pass through correctly (they are ignored by the bot-author gate in
	// handleIssueComment). Route on ev.IsComment (set only by the SCM parsers
	// for actual comment events) rather than ev.Action=="created" to prevent
	// misrouting if a future GitHub event type reuses the "created" action name.
	if ev.IsComment {
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

	// Compute the dedup key once, before any scan or label/name derivation, so all
	// three consumers (pre-create scan, created label, deterministic task name) agree.
	//
	// For a bot PR "Closes #N": dedupNumber = N (the linked issue), dedupRef = "o/r#N".
	// This matches the dedup key an issueScan/mrScan task for issue #N carries, so
	// the pre-create scan correctly detects the existing task and the task name hash
	// equals the issueScan task name (AlreadyExists fires on concurrent creates).
	// For a plain issue: dedupNumber = ev.Number, dedupRef = ev.IssueRef (unchanged).
	dedupSlug, _ := issueRefRepoSlug(ev.IssueRef)
	dedupNumber := ev.Number
	dedupRef := ev.IssueRef
	// dedupIsPR tracks whether the dedup slot is for a PR (true) or an issue
	// (false). For a bot PR "Closes #N" the slot is the linked issue (false),
	// matching the hash an issueScan task for that issue would produce.
	// For a standalone PR with no linked issue the slot is the PR itself (true),
	// ensuring PR #5 and issue #5 in the same repo get distinct task names.
	dedupIsPR := ev.IsPR
	if kind == "issueLifecycle" && ev.IsPR {
		if issueNum, linked := scm.LinkedIssueNumber(ev.Body); linked {
			dedupNumber = issueNum
			// Reconstruct the linked issue's IssueRef so the task name hash matches
			// the issueScan task for that issue. ev.IssueRef is "owner/repo#<prNum>";
			// replace the trailing number with the linked issue number.
			if idx := strings.LastIndexByte(ev.IssueRef, '#'); idx >= 0 {
				dedupRef = ev.IssueRef[:idx+1] + strconv.Itoa(dedupNumber)
			}
			dedupIsPR = false // acts as the linked issue slot, not the PR slot
		}
	}

	// Dedupe: creating an issue with the label fires both issues.opened and
	// issues.labeled for the same issue. Skip if a non-terminal Task already
	// exists for the dedup key (re-labeling after completion still re-triggers).
	// Use MatchingLabels so the cache filters Tasks to the dedup key before the
	// in-Go scan, avoiding an O(all-tasks) walk per webhook. The IssueRef equality
	// check is omitted for the bot-PR arm: the existing task carries the linked
	// issue's IssueRef ("o/r#7") not the PR ref ("o/r#21").
	var existing tatarav1.TaskList
	isPRStr := "false"
	if dedupIsPR {
		isPRStr = "true"
	}
	listOpts := []client.ListOption{
		client.InNamespace(s.cfg.Namespace),
		client.MatchingLabels{
			tatarav1.LabelSourceRepo:   dedupSlug,
			tatarav1.LabelSourceNumber: strconv.Itoa(dedupNumber),
		},
	}
	if err := s.cfg.Client.List(ctx, &existing, listOpts...); err != nil {
		s.count(provider, ev.Kind, ev.Action, "error")
		http.Error(w, "list tasks", http.StatusInternalServerError)
		return
	}
	for i := range existing.Items {
		t := &existing.Items[i]
		if t.Spec.Source != nil && !tatarav1.TaskTerminal(t) {
			// LabelIsPR disambiguates issue #N from PR #N in the same repo. Tasks
			// without the label (scan-created or pre-label) default to "false"
			// (issue slot). A PR-slot event must not be blocked by an issue-slot
			// task and vice versa.
			existingIsPR := t.Labels[tatarav1.LabelIsPR]
			if existingIsPR == "" {
				existingIsPR = "false" // backward-compatible default
			}
			if existingIsPR != isPRStr {
				continue // different slot; not a duplicate
			}
			s.log.InfoContext(ctx, "work item already has an active task; skipping duplicate",
				"project", proj.Name, "issue_ref", ev.IssueRef, "dedup_ref", dedupRef, "task", t.Name)
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
		} else {
			ann[tatarav1.LifecycleEntryAnnotation] = "Implement"
		}
		labels = map[string]string{
			tatarav1.LabelSourceRepo:   dedupSlug,
			tatarav1.LabelSourceNumber: strconv.Itoa(dedupNumber),
			tatarav1.LabelSourceKind:   kind,
			tatarav1.LabelActivity:     "webhook",
			tatarav1.LabelIsPR:         isPRStr,
		}
	}

	// For issueLifecycle tasks, use a deterministic name derived from the dedup
	// key so concurrent webhook deliveries for the same work item race to a
	// single winner: the second Create returns AlreadyExists instead of silently
	// creating a duplicate (GenerateName makes both succeed). review tasks use
	// GenerateName because multiple review Tasks per PR are intentional.
	// The name is derived from dedupRef (not ev.IssueRef) so a bot PR "Closes #7"
	// hashes identically to an issueScan task for issue #7.
	taskName := ""
	taskGenerateName := "task-"
	if kind == "issueLifecycle" {
		taskName = issueLifecycleTaskName(proj.Name, dedupRef, dedupIsPR)
		taskGenerateName = ""
	}

	payload := tatarav1.QueuedEventPayload{
		Kind:          kind,
		Goal:          ev.Body,
		Labels:        labels,
		Annotations:   ann,
		RepositoryRef: repo.Name,
		Source: &tatarav1.TaskSource{
			Provider:    provider,
			IssueRef:    ev.IssueRef,
			URL:         ev.URL,
			AuthorLogin: ev.AuthorLogin,
			IsPR:        ev.IsPR,
			Number:      ev.Number,
		},
		Provider: provider,
		PodRepo:  repo.Name,
	}
	if kind == "issueLifecycle" {
		payload.Name = taskName // deterministic name for idempotent dedup
	} else {
		payload.GenerateName = taskGenerateName // "task-" for review tasks
	}
	_, created, err := queue.EnqueueEvent(ctx, s.cfg.Client, s.cfg.Seq, &proj, tatarav1.QueueClassNormal, false, taskName, payload)
	if err != nil {
		if errors.Is(err, queue.ErrSeqNotReady) {
			http.Error(w, "queue not ready", http.StatusServiceUnavailable)
			return
		}
		s.count(provider, ev.Kind, ev.Action, "error")
		http.Error(w, "create task", http.StatusInternalServerError)
		return
	}
	if !created {
		s.log.InfoContext(ctx, "work item already queued (concurrent delivery); treating as duplicate",
			"project", proj.Name, "issue_ref", ev.IssueRef)
		s.count(provider, ev.Kind, ev.Action, "duplicate")
		w.WriteHeader(http.StatusAccepted)
		return
	}
	s.log.InfoContext(ctx, "work item enqueued",
		"project", proj.Name, "repository", repo.Name,
		"task", taskName, "issue_ref", ev.IssueRef, "kind", kind)
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
			if ev.CommentBody != "" && !interjectionQueued(fresh.Status.PendingInterjections, ev.CommentID, ev.CommentBody) {
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
	commentIsPRStr := "false"
	if ev.IsPR {
		commentIsPRStr = "true"
	}
	labels := map[string]string{
		tatarav1.LabelSourceRepo:   repoSlug,
		tatarav1.LabelSourceNumber: strconv.Itoa(ev.Number),
		tatarav1.LabelSourceKind:   "issueLifecycle",
		tatarav1.LabelActivity:     "webhook",
		tatarav1.LabelIsPR:         commentIsPRStr,
	}
	ann := map[string]string{tatarav1.LifecycleEntryAnnotation: "Triage"}

	goal := ev.Body
	if ev.CommentBody != "" {
		goal += "\n\nTriggering comment:\n" + ev.CommentBody
	}
	lifecycleName := issueLifecycleTaskName(proj.Name, ev.IssueRef, ev.IsPR)
	payload := tatarav1.QueuedEventPayload{
		Kind:          "issueLifecycle",
		Goal:          goal,
		Name:          lifecycleName,
		Labels:        labels,
		Annotations:   ann,
		RepositoryRef: repo.Name,
		Source: &tatarav1.TaskSource{
			Provider:    provider,
			IssueRef:    ev.IssueRef,
			URL:         ev.URL,
			AuthorLogin: ev.AuthorLogin,
			IsPR:        ev.IsPR,
			Number:      ev.Number,
		},
		Provider: provider,
		PodRepo:  repo.Name,
	}
	_, created, err := queue.EnqueueEvent(ctx, s.cfg.Client, s.cfg.Seq, &proj, tatarav1.QueueClassNormal, false, lifecycleName, payload)
	if err != nil {
		if errors.Is(err, queue.ErrSeqNotReady) {
			http.Error(w, "queue not ready", http.StatusServiceUnavailable)
			return
		}
		s.count(provider, ev.Kind, ev.Action, "error")
		http.Error(w, "create task", http.StatusInternalServerError)
		return
	}
	if !created {
		s.log.InfoContext(ctx, "issue_comment: lifecycle task already queued (concurrent create); treating as duplicate",
			"project", proj.Name, "issue_ref", ev.IssueRef)
		s.count(provider, ev.Kind, ev.Action, "duplicate")
		w.WriteHeader(http.StatusAccepted)
		return
	}
	// CommentBody is folded into Goal above so the triage agent sees the triggering comment.
	s.log.InfoContext(ctx, "issue_comment: created lifecycle task at Triage for untracked issue",
		"project", proj.Name, "repository", repo.Name, "task", lifecycleName, "issue_ref", ev.IssueRef)
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
// is resumable: LifecycleState=="Parked" OR LifecycleState=="Stopped" (regardless
// of Phase). Done tasks are NOT reactivated (their work is complete). A Stopped
// task that has also reached a terminal Phase (Succeeded/Failed) is still treated
// as resumable - a new comment signals the user wants another attempt, and
// reactivating the existing Task is idempotent (same deterministic name, same owner
// chain) whereas createLifecycleTaskAtTriage would produce a duplicate.
// Returns (task, true) when found.
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
		if t.Status.LifecycleState == "Parked" || t.Status.LifecycleState == "Stopped" {
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
	// Step 1: status update (LifecycleState, Phase, timers). This is the critical
	// reactivation: once it commits, the reconciler can re-triage the task.
	if statusErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
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
		return s.cfg.Client.Status().Update(ctx, fresh)
	}); statusErr != nil {
		s.log.ErrorContext(ctx, "reactivate: update task status", "error", statusErr, "task", task.Name)
		s.count(provider, ev.Kind, ev.Action, "error")
		http.Error(w, "reactivate task", http.StatusInternalServerError)
		return
	}

	// Step 2: clear turn annotations (metadata update, separate from status subresource).
	// Non-fatal: stale annotations are cosmetic. A conflict on this step means a
	// concurrent reconcile already advanced state; the reactivation is still
	// committed from step 1. On error we log and return 202 - GitHub will not
	// redeliver and re-stamp the already-committed Triage status.
	if annErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh2 := &tatarav1.Task{}
		if err := s.cfg.Client.Get(ctx, client.ObjectKeyFromObject(task), fresh2); err != nil {
			return err
		}
		if fresh2.Annotations != nil {
			delete(fresh2.Annotations, tatarav1.AnnCurrentTurn)
			delete(fresh2.Annotations, tatarav1.AnnCurrentSubtask)
			delete(fresh2.Annotations, tatarav1.AnnTurnComplete)
			delete(fresh2.Annotations, tatarav1.AnnTurnStartedAt)
			delete(fresh2.Annotations, tatarav1.AnnTurnLastActivity)
			delete(fresh2.Annotations, tatarav1.AnnPodRecreations)
		}
		return s.cfg.Client.Update(ctx, fresh2)
	}); annErr != nil {
		// Best-effort: reactivation already committed; stale annotations are non-fatal.
		s.log.ErrorContext(ctx, "reactivate: clear annotations (non-fatal, reactivation committed)", "error", annErr, "task", task.Name)
	}
	s.log.InfoContext(ctx, "issue_comment: reactivated parked lifecycle task",
		"project", proj.Name, "task", task.Name, "issue_ref", ev.IssueRef)
	s.count(provider, ev.Kind, ev.Action, "accepted")
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleGrafanaAlert(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	projectName := chi.URLParam(r, "project")
	body, err := readBody(r)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	var proj tatarav1.Project
	if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, projectName), &proj); err != nil {
		http.Error(w, "unknown project", http.StatusNotFound)
		return
	}
	if proj.Spec.Grafana == nil || !proj.Spec.Grafana.Enabled {
		http.Error(w, "grafana not enabled", http.StatusNotFound)
		return
	}
	secret, err := s.webhookSecret(ctx, proj.Spec.Grafana.SecretRef)
	if err != nil {
		http.Error(w, "secret", http.StatusInternalServerError)
		return
	}
	bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if subtle.ConstantTimeCompare([]byte(bearer), []byte(secret)) != 1 {
		s.count("grafana", "alert", "other", "bad_signature")
		http.Error(w, "verification failed", http.StatusUnauthorized)
		return
	}
	alert, err := parseGrafanaAlert(body)
	if err != nil {
		http.Error(w, "parse alert", http.StatusBadRequest)
		return
	}
	if alert.Status != "firing" {
		s.count("grafana", "alert", alert.Status, "ignored")
		w.WriteHeader(http.StatusAccepted)
		return
	}
	groupHash := alertGroupHash(alert)
	created, err := s.createIncidentTask(ctx, &proj, alert, groupHash)
	if err != nil {
		if errors.Is(err, queue.ErrSeqNotReady) {
			http.Error(w, "queue not ready", http.StatusServiceUnavailable)
			return
		}
		s.count("grafana", "alert", "firing", "error")
		http.Error(w, "create task", http.StatusInternalServerError)
		return
	}
	if !created {
		s.count("grafana", "alert", "firing", "duplicate")
	} else {
		s.count("grafana", "alert", "firing", "created")
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) createIncidentTask(ctx context.Context, proj *tatarav1.Project, alert GrafanaAlert, groupHash string) (bool, error) {
	slugs := projectRepoSlugs(ctx, s.cfg.Client, s.cfg.Namespace, proj.Name)
	alertCtx := renderAlertContext(alert)
	goal := incident.GoalProject(alertCtx, slugs)
	payload := tatarav1.QueuedEventPayload{
		Kind:         "incident",
		Goal:         goal,
		GenerateName: "incident-",
		Labels:       map[string]string{tatarav1.LabelActivity: "incident", tatarav1.LabelAlertGroup: groupHash},
		Annotations:  map[string]string{tatarav1.AnnGrafanaAlert: alertCtx},
	}
	_, created, err := queue.EnqueueEvent(ctx, s.cfg.Client, s.cfg.Seq, proj, tatarav1.QueueClassAlert, false, groupHash, payload)
	return created, err
}

// projectRepoSlugs returns the owner/repo slugs of a project's Repositories,
// name-sorted, for the incident goal's repo list.
func projectRepoSlugs(ctx context.Context, c client.Client, ns, project string) []string {
	var rl tatarav1.RepositoryList
	if err := c.List(ctx, &rl, client.InNamespace(ns)); err != nil {
		return nil
	}
	var slugs []string
	for i := range rl.Items {
		if rl.Items[i].Spec.ProjectRef != project {
			continue
		}
		if o, n, err := scm.OwnerRepo(rl.Items[i].Spec.URL); err == nil {
			slugs = append(slugs, o+"/"+n)
		}
	}
	sort.Strings(slugs)
	return slugs
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
// an issueLifecycle task scoped to (projectName, issueRef, isPR). isPR
// disambiguates a GitHub issue #N from a GitHub PR #N in the same repo: both
// produce an IssueRef of the form "owner/repo#N" so without the flag the two
// would hash to the same name and collide. For a bot PR "Closes #N", isPR is
// false (the task represents the linked issue slot, not the PR slot). Using a
// deterministic name rather than GenerateName makes concurrent webhook deliveries
// for the same work item idempotent: the second Create returns AlreadyExists
// which is treated as a duplicate, preventing two live lifecycle Tasks for one
// work item. GitLab is unaffected (it uses '!' for MRs vs '#' for issues).
func issueLifecycleTaskName(projectName, issueRef string, isPR bool) string {
	prMark := "0"
	if isPR {
		prMark = "1"
	}
	h := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s", projectName, issueRef, prMark)))
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

// interjectionQueued reports whether a comment has already been queued to
// avoid appending a redelivered comment twice. When commentID > 0 the check
// is body equality keyed on commentID (same id => same delivery), with body
// equality as the fallback when commentID is 0 (provider did not supply an
// id). Body equality alone is sufficient for redelivery dedup: the same
// webhook redelivery carries the identical body.
func interjectionQueued(existing []string, commentID int, body string) bool {
	_ = commentID // currently unused; body equality is the dedup key (see note above)
	return slices.Contains(existing, body)
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

// count increments operator_webhook_events_total and records the webhook
// request duration in operator_webhook_duration_seconds (finding 14). t0 is
// the request-start time; it is non-zero only when called from handle().
func (s *Server) count(provider, kind, action, result string) {
	s.cfg.Metrics.WebhookEvent(provider, kind, action, result)
}

func objKey(ns, name string) client.ObjectKey {
	return client.ObjectKey{Namespace: ns, Name: name}
}

const maxBodyBytes = 5 << 20 // 5 MiB

func readBody(r *http.Request) ([]byte, error) {
	defer func() { _ = r.Body.Close() }()
	// Read up to maxBodyBytes+1 so we can detect overflow without consuming the full
	// stream. If the read returns more than maxBodyBytes the payload is too large.
	b, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxBodyBytes {
		return nil, errBodyTooLarge
	}
	return b, nil
}

// errBodyTooLarge is a sentinel error returned by readBody when the payload
// exceeds the per-request size limit. The handler converts it to a 413.
var errBodyTooLarge = errors.New("request body too large")

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
