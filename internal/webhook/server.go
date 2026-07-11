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
	Seq       *queue.SeqSource
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
		cfg.Seq = &queue.SeqSource{Client: cfg.Client, Namespace: cfg.Namespace}
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
			s.reject(w, http.StatusRequestEntityTooLarge, "request body too large", "unknown", "other", "other", "too_large")
			return
		}
		s.reject(w, http.StatusBadRequest, "read body", "unknown", "other", "other", "bad_request")
		return
	}

	provider, err := scm.Select(r.Header)
	if err != nil {
		s.reject(w, http.StatusBadRequest, "unrecognized provider", "unknown", "other", "other", "bad_request")
		return
	}
	providerName = provider.Provider()

	var proj tatarav1.Project
	if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, projectName), &proj); err != nil {
		if apierrors.IsNotFound(err) {
			s.reject(w, http.StatusNotFound, "unknown project", providerName, "other", "other", "unknown_project")
			return
		}
		s.reject(w, http.StatusInternalServerError, "lookup project", providerName, "other", "other", "error")
		return
	}

	// Guard: reject misrouted webhooks before any signature work. A GitHub delivery
	// to a GitLab-configured project (or vice versa) would otherwise fail with a
	// confusing bad_signature 401 rather than a clear provider_mismatch 400.
	if proj.Spec.Scm != nil && proj.Spec.Scm.Provider != "" && proj.Spec.Scm.Provider != providerName {
		s.reject(w, http.StatusBadRequest, "provider mismatch", providerName, "other", "other", "provider_mismatch")
		return
	}

	webhookSecret, err := s.webhookSecret(ctx, proj.Spec.ScmSecretRef)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "secret", providerName, "other", "other", "error")
		return
	}

	ev, err := provider.DetectAndVerify(r.Header, body, webhookSecret)
	if err != nil {
		s.reject(w, http.StatusUnauthorized, "verification failed", providerName, "other", "other", "bad_signature")
		return
	}

	durResult = "ok"
	switch ev.Kind {
	case "push":
		s.handlePush(ctx, w, providerName, &proj, ev)
	case "issue", "mr":
		s.handleWorkItem(ctx, w, providerName, proj, ev)
	default:
		s.accept(w, providerName, "other", ev.Action, "ignored")
	}
}

func (s *Server) handlePush(ctx context.Context, w http.ResponseWriter, provider string, proj *tatarav1.Project, ev scm.WebhookEvent) {
	var repos tatarav1.RepositoryList
	if err := s.cfg.Client.List(ctx, &repos, client.InNamespace(s.cfg.Namespace)); err != nil {
		s.reject(w, http.StatusInternalServerError, "list repositories", provider, "push", ev.Action, "error")
		return
	}
	for i := range repos.Items {
		repo := &repos.Items[i]
		if repo.Spec.ProjectRef != proj.Name {
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
			s.reject(w, http.StatusInternalServerError, "annotate repository", provider, "push", ev.Action, "error")
			return
		}
		s.log.InfoContext(ctx, "webhook push re-ingest requested",
			"provider", provider, "project", proj.Name, "repository", repo.Name, "branch", ev.Branch)

		// The documentation push trigger is retired: documentation is a scheduled
		// (cron) kind in the redesign, not a per-merge webhook. handlePush now only
		// marks the repo for re-ingest.

		s.accept(w, provider, "push", ev.Action, "accepted")
		return
	}
	s.accept(w, provider, "push", ev.Action, "ignored")
}

// matchRepo returns the Project's Repository whose URL maps to the given remote,
// or (nil, nil) when none matches. A non-nil error is a transient list failure
// the caller should surface as 500 so the SCM retries. Shared by the work-item
// router, the comment intake gate, and lifecycle-task creation.
func (s *Server) matchRepo(ctx context.Context, projName, remote string) (*tatarav1.Repository, error) {
	var repos tatarav1.RepositoryList
	if err := s.cfg.Client.List(ctx, &repos, client.InNamespace(s.cfg.Namespace)); err != nil {
		return nil, err
	}
	for i := range repos.Items {
		r := &repos.Items[i]
		if r.Spec.ProjectRef == projName && scm.SameRemote(r.Spec.URL, remote) {
			return r, nil
		}
	}
	return nil, nil
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
			task.Spec.Kind == "issueLifecycle" && task.Status.DeployState == "Conversation" {
			updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				fresh := &tatarav1.Task{}
				if err := s.cfg.Client.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
					return err
				}
				fresh.Status.DeployState = "Implement"
				fresh.Status.DeadlineAt = nil // clear stale conversation deadline
				return s.cfg.Client.Status().Update(ctx, fresh)
			})
			if updateErr != nil {
				s.reject(w, http.StatusInternalServerError, "update task", provider, ev.Kind, ev.Action, "error")
				return
			}
			s.log.InfoContext(ctx, "triggerLabel on Conversation task: set Implement",
				"project", proj.Name, "task", task.Name, "issue_ref", ev.IssueRef)
			s.accept(w, provider, ev.Kind, ev.Action, "accepted")
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

	// kind switch (task-kind redesign): a new issue (labeled or trusted-authored)
	// becomes a `clarify` umbrella front-half; any PR/MR - bot- OR human-authored -
	// becomes a `review`. The old bot-PR->issueLifecycle special case is folded
	// away (all PRs review; review re-invokes implement on an unmergeable member).
	// Retained issueLifecycle bridge tasks are drained by reconcileLifecycle but
	// no new webhook path creates them.
	kind := "clarify"

	// AuthorLogin is the real PR/issue author only on GitHub; on GitLab it is the
	// event actor (scm/gitlab.go), so a trusted maintainer acting on a third
	// party's item must NOT bypass the gate there. GitLab authorship is enforced
	// downstream by the controller's GetPRState egress gate.
	trusted := provider == "github" && tatarav1.IsTrustedAuthor(&proj, nil, ev.AuthorLogin)

	if ev.IsPR {
		kind = "review"
		if scope == "labeledOrMentioned" && !slices.Contains(ev.Labels, proj.Spec.TriggerLabel) && !mentionsBot(ev.Body, bot) && !trusted {
			s.accept(w, provider, ev.Kind, ev.Action, "ignored")
			return
		}
	} else {
		if !slices.Contains(ev.Labels, proj.Spec.TriggerLabel) && (ev.AuthorLogin == bot || !trusted) {
			s.accept(w, provider, ev.Kind, ev.Action, "ignored")
			return
		}
	}

	repo, err := s.matchRepo(ctx, proj.Name, ev.Repo)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "list repositories", provider, ev.Kind, ev.Action, "error")
		return
	}
	if repo == nil {
		s.log.InfoContext(ctx, "work item labeled but no matching repository",
			"project", proj.Name, "remote", ev.Repo, "issue_ref", ev.IssueRef)
		s.accept(w, provider, ev.Kind, ev.Action, "no_repo")
		return
	}

	// U-C (cross-repo umbrella review): a PR-create for a repo that belongs to an
	// existing tatara stream routes review INTO that stream's umbrella instead of
	// spawning a fresh per-PR review Task. The stream is identified by the PR's shared
	// head branch alone (the implement umbrella opens ONE branch across all repos); a
	// linked-issue-only "Closes #N" reference is deliberately not an auto-join trigger
	// (see streamPRMatches). An existing stream review Task is JOINED (the PR is
	// added to its ledger as role:openedPR - no second review Task per stream); a
	// stream with only an implement/clarify umbrella spawns ONE deterministic-named
	// stream review Task. External / human PRs match no umbrella and fall through to
	// the per-PR review path below (unchanged).
	if ev.IsPR && isPRCreateAction(ev.Action) {
		prRepoSlug := tatarav1.RepoFromIssueRef(ev.IssueRef)
		reviewT, umbrellaT := s.findStreamUmbrellas(ctx, proj.Name, ev.HeadBranch)
		if reviewT != nil {
			if jerr := s.joinStreamReview(ctx, reviewT, provider, prRepoSlug, ev); jerr != nil {
				s.reject(w, http.StatusInternalServerError, "join review", provider, ev.Kind, ev.Action, "error")
				return
			}
			s.log.InfoContext(ctx, "PR-create joined existing stream review umbrella",
				"project", proj.Name, "task", reviewT.Name, "issue_ref", ev.IssueRef, "branch", ev.HeadBranch)
			s.accept(w, provider, ev.Kind, ev.Action, "joined_umbrella")
			return
		}
		if umbrellaT != nil {
			created, cerr := s.createStreamReview(ctx, &proj, repo, umbrellaT, provider, ev)
			if cerr != nil {
				s.reject(w, http.StatusInternalServerError, "create review", provider, ev.Kind, ev.Action, "error")
				return
			}
			result := "task_created"
			if !created {
				result = "duplicate"
			}
			s.log.InfoContext(ctx, "PR-create spawned stream review umbrella",
				"project", proj.Name, "umbrella", umbrellaT.Name, "issue_ref", ev.IssueRef, "branch", ev.HeadBranch, "created", created)
			s.accept(w, provider, ev.Kind, ev.Action, result)
			return
		}
		// No umbrella: external / human PR - fall through to the per-PR review path.
	}

	// Reporter intake gate (issue #102): for plain issues, only act on issues
	// authored by an allowed reporter so an unknown third party cannot drive the
	// lifecycle via a labelled issue. PR review items are governed by
	// prReactionScope above. An empty reporter allowlist preserves the open default.
	if kind == "clarify" && !ev.IsPR &&
		!tatarav1.IsAllowedReporter(&proj, repo, ev.AuthorLogin) {
		s.log.InfoContext(ctx, "issue intake: author not an allowed reporter; ignoring",
			"project", proj.Name, "repository", repo.Name, "issue_ref", ev.IssueRef, "author", ev.AuthorLogin)
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}

	// Compute the dedup key once, before any scan or label/name derivation, so all
	// consumers (pre-create scan, deterministic task name) agree. Post-redesign a
	// PR routes to a per-PR `review` and an issue to a `clarify` umbrella; there is
	// no bot-PR->issueLifecycle linked-issue folding anymore, so the dedup slot is
	// the work item itself: dedupRef = ev.IssueRef, dedupNumber = ev.Number,
	// dedupIsPR distinguishes issue #N from PR #N in the same repo.
	dedupNumber := ev.Number
	dedupRef := ev.IssueRef
	dedupIsPR := ev.IsPR

	// Dedupe: creating an issue with the label fires both issues.opened and
	// issues.labeled for the same issue. Skip if a non-terminal Task already
	// exists for the dedup key (re-labeling after completion still re-triggers).
	//
	// Phase 1 stopped writing the source-repo/source-number labels, so the dedup
	// identity is matched in-Go against dedupRef (the reconstructed issue ref) and
	// Spec.Source.DedupNumber/Number, NOT a server-side label selector that would
	// drop every post-deploy Task. The dedupRef equality handles the bot-PR arm
	// too: dedupRef is rewritten to the linked issue's ref ("o/r#7"), so it matches
	// the issueScan/mrScan task that already owns that issue slot.
	// Phase 2: dedup is matched by spec/ledger identity via TaskMatchesItem.
	// List ALL Tasks in the namespace (no LabelSourceKind pre-filter): a bot-PR
	// "Closes #N" delivery must dedup against an existing issueScan/mrScan Task for
	// issue #N, and those scan tasks carry LabelSourceKind=mrScan/issueScan (not
	// "issueLifecycle"), so narrowing to issueLifecycle would hide them and let a
	// duplicate slip through. in-Go matching then applies full identity + slot
	// checks. O(tasks) per the Full-removal decision (label-selector dedup is gone;
	// deterministic-name idempotency handles the truly-concurrent hot path).
	dedupRepo := tatarav1.RepoFromIssueRef(dedupRef)
	var existing tatarav1.TaskList
	listOpts := []client.ListOption{
		client.InNamespace(s.cfg.Namespace),
	}
	if err := s.cfg.Client.List(ctx, &existing, listOpts...); err != nil {
		s.reject(w, http.StatusInternalServerError, "list tasks", provider, ev.Kind, ev.Action, "error")
		return
	}
	for i := range existing.Items {
		t := &existing.Items[i]
		if tatarav1.TaskTerminal(t) {
			continue
		}
		// Identity match via spec/ledger (covers DedupNumber, ledger entries, and
		// the legacy label fallback for pre-Phase-1 Tasks).
		if !tatarav1.TaskMatchesItem(t, dedupRepo, dedupNumber) {
			continue
		}
		// Slot check: distinguish issue #N from PR #N in the same repo.
		// Prefer Spec.Source.IsPR when set; fall back to LabelIsPR for pre-Phase-1
		// Tasks that have no Spec.Source.
		existingIsPR := false
		if t.Spec.Source != nil {
			existingIsPR = t.Spec.Source.IsPR
		} else if t.Labels[tatarav1.LabelIsPR] == "true" {
			existingIsPR = true
		}
		if existingIsPR != dedupIsPR {
			continue // different slot; not a duplicate
		}
		s.log.InfoContext(ctx, "work item already has an active task; skipping duplicate",
			"project", proj.Name, "issue_ref", ev.IssueRef, "dedup_ref", dedupRef, "task", t.Name)
		s.accept(w, provider, ev.Kind, ev.Action, "duplicate")
		return
	}

	// Observability labels. A clarify umbrella carries a deterministic name (below)
	// so concurrent deliveries race to one winner; it always starts at Triage
	// (reconcileClarify ignores any lifecycle-entry annotation), so none is set.
	// The three source dedup labels (source-repo, source-number, head-sha) are no
	// longer written: dedup is driven by Spec.Source and Status.WorkItems.
	var labels map[string]string
	if kind == "clarify" {
		isPRLabel := "false"
		if dedupIsPR {
			isPRLabel = "true"
		}
		labels = map[string]string{
			tatarav1.LabelSourceKind: kind,
			tatarav1.LabelActivity:   "webhook",
			tatarav1.LabelIsPR:       isPRLabel,
		}
	}

	// A clarify umbrella uses a deterministic name derived from the dedup key so
	// concurrent webhook deliveries for the same issue race to a single winner:
	// the second Create returns AlreadyExists instead of creating a duplicate.
	// review tasks use GenerateName because multiple review Tasks per PR are
	// intentional.
	taskName := ""
	taskGenerateName := "task-"
	if kind == "clarify" {
		taskName = issueLifecycleTaskName(proj.Name, dedupRef, dedupIsPR)
		taskGenerateName = ""
	}

	payload := tatarav1.QueuedEventPayload{
		Kind:          kind,
		Goal:          ev.Body,
		Labels:        labels,
		RepositoryRef: repo.Name,
		Source: &tatarav1.TaskSource{
			Provider:    provider,
			IssueRef:    ev.IssueRef,
			URL:         ev.URL,
			AuthorLogin: ev.AuthorLogin,
			IsPR:        ev.IsPR,
			Number:      ev.Number,
			Title:       ev.Title,
		},
		Provider: provider,
		PodRepo:  repo.Name,
	}
	if kind == "clarify" {
		payload.Name = taskName // deterministic name for idempotent dedup
	} else {
		payload.GenerateName = taskGenerateName // "task-" for review tasks
	}
	_, created, err := queue.EnqueueEvent(ctx, s.cfg.Client, s.cfg.Seq, &proj, tatarav1.QueueClassNormal, false, taskName, payload)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "create task", provider, ev.Kind, ev.Action, "error")
		return
	}
	if !created {
		s.log.InfoContext(ctx, "work item already queued (concurrent delivery); treating as duplicate",
			"project", proj.Name, "issue_ref", ev.IssueRef)
		s.accept(w, provider, ev.Kind, ev.Action, "duplicate")
		return
	}
	s.log.InfoContext(ctx, "work item enqueued",
		"project", proj.Name, "repository", repo.Name,
		"task", taskName, "issue_ref", ev.IssueRef, "kind", kind)
	s.accept(w, provider, ev.Kind, ev.Action, "task_created")
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
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}

	// Reporter intake gate (issue #102): ignore comments from accounts outside the
	// reporter allowlist so an injected comment body cannot drive the lifecycle
	// (reactivate a parked task, queue an interjection, or trigger a fresh triage).
	// An empty allowlist preserves the open default. Repo override is honored when
	// the comment maps to a known repository; on a lookup miss/error the project
	// list applies (fail-safe: the gate stays active, never bypassed).
	commentRepo, _ := s.matchRepo(ctx, proj.Name, ev.Repo)
	if !tatarav1.IsAllowedReporter(&proj, commentRepo, ev.ActorLogin) {
		s.log.InfoContext(ctx, "issue_comment: author not an allowed reporter; ignoring",
			"project", proj.Name, "issue_ref", ev.IssueRef, "author", ev.ActorLogin)
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
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
		s.createClarifyTask(ctx, w, provider, proj, ev)
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
			s.reject(w, http.StatusInternalServerError, "update task", provider, ev.Kind, ev.Action, "error")
			return
		}
		s.log.InfoContext(ctx, "issue_comment: queued interjection for in-flight turn",
			"project", proj.Name, "task", task.Name, "issue_ref", ev.IssueRef)
		s.accept(w, provider, ev.Kind, ev.Action, "accepted")
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
	// that may be advancing DeployState at the same time (FIX 3).
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
		if fresh.Status.DeployState == "Conversation" || fresh.Status.DeployState == "Stopped" {
			fresh.Status.DeployState = "Triage"
		}
		return s.cfg.Client.Status().Update(ctx, fresh)
	}); updateErr != nil {
		s.log.ErrorContext(ctx, "issue_comment: update task status", "error", updateErr, "task", task.Name)
		s.reject(w, http.StatusInternalServerError, "update task", provider, ev.Kind, ev.Action, "error")
		return
	}
	s.log.InfoContext(ctx, "issue_comment: lifecycle task updated",
		"project", proj.Name, "task", task.Name,
		"issue_ref", ev.IssueRef)
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
}

// createClarifyTask creates a new clarify umbrella Task for an issue_comment on
// an issue with no existing live task. clarify starts at Triage (reconcileClarify
// ignores any lifecycle-entry annotation) and shares the deterministic dedup name
// with the labeled-issue create path so both land on one umbrella.
func (s *Server) createClarifyTask(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	// Find the matching Repository for the event's repo URL.
	repo, err := s.matchRepo(ctx, proj.Name, ev.Repo)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "list repositories", provider, ev.Kind, ev.Action, "error")
		return
	}
	if repo == nil {
		s.log.InfoContext(ctx, "issue_comment: no matching repository for untracked issue",
			"project", proj.Name, "remote", ev.Repo, "issue_ref", ev.IssueRef)
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}

	// Observability labels for the new clarify task.
	// The three source dedup labels (source-repo, source-number, head-sha) are no
	// longer written: dedup is driven by Spec.Source and Status.WorkItems.
	commentIsPRStr := "false"
	if ev.IsPR {
		commentIsPRStr = "true"
	}
	labels := map[string]string{
		tatarav1.LabelSourceKind: "clarify",
		tatarav1.LabelActivity:   "webhook",
		tatarav1.LabelIsPR:       commentIsPRStr,
	}

	goal := ev.Body
	if ev.CommentBody != "" {
		goal += "\n\nTriggering comment:\n" + ev.CommentBody
	}
	clarifyName := issueLifecycleTaskName(proj.Name, ev.IssueRef, ev.IsPR)
	payload := tatarav1.QueuedEventPayload{
		Kind:          "clarify",
		Goal:          goal,
		Name:          clarifyName,
		Labels:        labels,
		RepositoryRef: repo.Name,
		Source: &tatarav1.TaskSource{
			Provider:    provider,
			IssueRef:    ev.IssueRef,
			URL:         ev.URL,
			AuthorLogin: ev.AuthorLogin,
			IsPR:        ev.IsPR,
			Number:      ev.Number,
			Title:       ev.Title,
		},
		Provider: provider,
		PodRepo:  repo.Name,
	}
	_, created, err := queue.EnqueueEvent(ctx, s.cfg.Client, s.cfg.Seq, &proj, tatarav1.QueueClassNormal, false, clarifyName, payload)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "create task", provider, ev.Kind, ev.Action, "error")
		return
	}
	if !created {
		s.log.InfoContext(ctx, "issue_comment: clarify task already queued (concurrent create); treating as duplicate",
			"project", proj.Name, "issue_ref", ev.IssueRef)
		s.accept(w, provider, ev.Kind, ev.Action, "duplicate")
		return
	}
	// CommentBody is folded into Goal above so the clarify agent sees the triggering comment.
	s.log.InfoContext(ctx, "issue_comment: created clarify task for untracked issue",
		"project", proj.Name, "repository", repo.Name, "task", clarifyName, "issue_ref", ev.IssueRef)
	s.accept(w, provider, ev.Kind, ev.Action, "task_created")
}

// isFrontHalfKind reports whether a Task kind is a live conversational front-half
// that a new issue comment can be delivered to / reactivate: the new `clarify`
// kind plus the retained `issueLifecycle` bridge (drained until Phase 6 retires
// it).
func isFrontHalfKind(kind string) bool {
	return kind == "clarify" || kind == "issueLifecycle"
}

// findLifecycleTask finds a non-terminal front-half Task (clarify or the
// issueLifecycle bridge) for the given issue ref within the project. Returns
// (task, true) when found.
func (s *Server) findLifecycleTask(ctx context.Context, projectName, issueRef string) (*tatarav1.Task, bool) {
	var tasks tatarav1.TaskList
	opts := s.taskListOpts()
	if err := s.cfg.Client.List(ctx, &tasks, opts...); err != nil {
		return nil, false
	}
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if !isFrontHalfKind(t.Spec.Kind) || t.Spec.ProjectRef != projectName {
			continue
		}
		if t.Spec.Source == nil || t.Spec.Source.IssueRef != issueRef {
			continue
		}
		// Skip terminal lifecycle states (Done/Stopped/Parked) and terminal phases
		// (Succeeded/Failed). Stopped is resumable so we include it.
		switch t.Status.DeployState {
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
// is resumable: DeployState=="Parked" OR DeployState=="Stopped" (regardless
// of Phase). Done tasks are NOT reactivated (their work is complete). A Stopped
// task that has also reached a terminal Phase (Succeeded/Failed) is still treated
// as resumable - a new comment signals the user wants another attempt, and
// reactivating the existing Task is idempotent (same deterministic name, same owner
// chain) whereas createLifecycleTaskAtTriage would produce a duplicate.
// Returns (task, true) when found.
func (s *Server) findReactivatableTask(ctx context.Context, projectName, issueRef string) (*tatarav1.Task, bool) {
	var tasks tatarav1.TaskList
	opts := s.taskListOpts()
	if err := s.cfg.Client.List(ctx, &tasks, opts...); err != nil {
		return nil, false
	}
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if !isFrontHalfKind(t.Spec.Kind) || t.Spec.ProjectRef != projectName {
			continue
		}
		if t.Spec.Source == nil || t.Spec.Source.IssueRef != issueRef {
			continue
		}
		if t.Status.DeployState == "Parked" || t.Status.DeployState == "Stopped" {
			return t, true
		}
	}
	return nil, false
}

// taskListOpts builds list options for the front-half-task reads. It lists all
// Tasks in the namespace and lets the callers filter kind in-Go via
// isFrontHalfKind (clarify + the issueLifecycle bridge): a single-value
// LabelSourceKind selector can no longer narrow across BOTH kinds, and the count
// is bounded, so in-Go matching is preferred over a server-side selector that
// would drop one of the two kinds.
func (s *Server) taskListOpts() []client.ListOption {
	return []client.ListOption{
		client.InNamespace(s.cfg.Namespace),
	}
}

// reactivateTask resumes a Parked owning Task: it clears the agent-run state
// (Phase, turn annotations) and the wrapper pod/service, sets DeployState
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
	// Step 1: status update (DeployState, Phase, timers). This is the critical
	// reactivation: once it commits, the reconciler can re-triage the task.
	if statusErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1.Task{}
		if err := s.cfg.Client.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		now := metav1.Now()
		deadline := metav1.NewTime(now.Add(time.Duration(idleMinutes) * time.Minute))
		fresh.Status.DeployState = "Triage"
		fresh.Status.Phase = ""
		fresh.Status.LastActivityAt = &now
		fresh.Status.DeadlineAt = &deadline
		return s.cfg.Client.Status().Update(ctx, fresh)
	}); statusErr != nil {
		s.log.ErrorContext(ctx, "reactivate: update task status", "error", statusErr, "task", task.Name)
		s.reject(w, http.StatusInternalServerError, "reactivate task", provider, ev.Kind, ev.Action, "error")
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
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
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
		s.reject(w, http.StatusUnauthorized, "verification failed", "grafana", "alert", "other", "bad_signature")
		return
	}
	alert, err := parseGrafanaAlert(body)
	if err != nil {
		http.Error(w, "parse alert", http.StatusBadRequest)
		return
	}
	if alert.Status != "firing" {
		s.accept(w, "grafana", "alert", alert.Status, "ignored")
		return
	}
	groupHash := alertGroupHash(alert)
	created, err := s.createIncidentTask(ctx, &proj, alert, groupHash)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "create task", "grafana", "alert", "firing", "error")
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
	var goal string
	if alert.CommonLabels["tatara_tier_quality"] == "true" {
		goal = incident.GoalTierRevert(proj.Name, alert.CommonLabels["kind"], alert.CommonLabels["model"])
	} else {
		goal = incident.GoalProject(alertCtx, slugs)
	}
	payload := tatarav1.QueuedEventPayload{
		Kind:         "incident",
		Goal:         goal,
		GenerateName: "incident-",
		AlertRule:    alertRuleName(alert),
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

// isPRCreateAction reports whether a PR/MR webhook action opens a PR for review
// (the U-C stream-review routing only fires on a genuine PR-create, not on
// label/synchronize/close events which the per-PR review path already handles).
func isPRCreateAction(action string) bool {
	switch action {
	case "opened", "reopened", "ready_for_review":
		return true
	}
	return false
}

// streamPRMatches reports whether task is the umbrella of the stream a newly
// created PR belongs to. Membership is the STRONG signal ONLY: the shared head
// branch (the implement umbrella opens one branch across every repo, tracked as
// role:openedPR HeadBranch, and a stream review Task carries it as
// AnnReviewHeadBranch). A linked-issue-only match ("Closes #N" citing the umbrella's
// source issue) is deliberately NOT an auto-join trigger: a human PR that merely
// references the source issue on its own branch would otherwise be swept into the
// collective approve/withhold. Such a PR falls through to the normal per-PR review
// path instead.
func streamPRMatches(task *tatarav1.Task, headBranch string) bool {
	if headBranch == "" {
		return false
	}
	if task.Annotations[tatarav1.AnnReviewHeadBranch] == headBranch {
		return true
	}
	for _, wi := range task.Status.WorkItems {
		if wi.Role == tatarav1.RoleOpenedPR && wi.Kind == tatarav1.WorkItemPR && wi.HeadBranch == headBranch {
			return true
		}
	}
	return false
}

// findStreamUmbrellas scans the project's Tasks for the umbrella a PR-create should
// route into: a non-terminal review-kind Task to JOIN (review), and/or an
// implement/clarify umbrella that established the stream (umbrella). The implement
// umbrella may already be terminal (Succeeded, awaiting the review/merge/deploy
// half) - it still identifies the stream, so its terminal-ness is not filtered.
func (s *Server) findStreamUmbrellas(ctx context.Context, projName, headBranch string) (review, umbrella *tatarav1.Task) {
	var tasks tatarav1.TaskList
	if err := s.cfg.Client.List(ctx, &tasks, client.InNamespace(s.cfg.Namespace)); err != nil {
		return nil, nil
	}
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.ProjectRef != projName {
			continue
		}
		if !streamPRMatches(t, headBranch) {
			continue
		}
		switch t.Spec.Kind {
		case "review":
			if review == nil && !tatarav1.TaskTerminal(t) {
				review = t
			}
		case "implement", "clarify":
			if umbrella == nil {
				umbrella = t
			}
		}
	}
	return review, umbrella
}

// joinStreamReview upserts a newly created PR as a role:openedPR ledger member of an
// existing stream review Task, so the umbrella review's approve/withhold decision
// spans it (U-D) without a second review Task per stream. Idempotent: a redelivered
// PR-create refreshes the existing entry's role/state/branch rather than duplicating.
func (s *Server) joinStreamReview(ctx context.Context, task *tatarav1.Task, provider, prRepoSlug string, ev scm.WebhookEvent) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1.Task{}
		if err := s.cfg.Client.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		for i := range fresh.Status.WorkItems {
			wi := &fresh.Status.WorkItems[i]
			if wi.Repo == prRepoSlug && wi.Number == ev.Number && wi.Kind == tatarav1.WorkItemPR {
				wi.Role = tatarav1.RoleOpenedPR
				wi.State = tatarav1.WIOpen
				if ev.HeadBranch != "" {
					wi.HeadBranch = ev.HeadBranch
				}
				return s.cfg.Client.Status().Update(ctx, fresh)
			}
		}
		fresh.Status.WorkItems = append(fresh.Status.WorkItems, tatarav1.WorkItemRef{
			Provider:   provider,
			Repo:       prRepoSlug,
			Number:     ev.Number,
			Kind:       tatarav1.WorkItemPR,
			Role:       tatarav1.RoleOpenedPR,
			State:      tatarav1.WIOpen,
			Title:      ev.Title,
			HeadBranch: ev.HeadBranch,
		})
		return s.cfg.Client.Status().Update(ctx, fresh)
	})
}

// createStreamReview spawns the single stream review umbrella Task for a stream that
// has an implement/clarify umbrella but no review Task yet. It carries the umbrella's
// originating issue as Spec.Source (so a withheld approval re-adds tatara-implementation
// on the issue and drives implement) and the shared head branch as AnnReviewHeadBranch
// (the stream key: subsequent PR-creates match and JOIN this Task, and the controller
// seeds its cross-repo openedPR span from the sibling umbrella). The deterministic name
// makes concurrent PR-create deliveries for one branch race to a single winner.
func (s *Server) createStreamReview(ctx context.Context, proj *tatarav1.Project, repo *tatarav1.Repository, umbrella *tatarav1.Task, provider string, ev scm.WebhookEvent) (bool, error) {
	repoRef := umbrella.Spec.RepositoryRef
	if repoRef == "" {
		repoRef = repo.Name
	}
	var source *tatarav1.TaskSource
	if src := umbrella.Spec.Source; src != nil && !src.IsPR && src.IssueRef != "" {
		source = &tatarav1.TaskSource{
			Provider: src.Provider, IssueRef: src.IssueRef, URL: src.URL,
			AuthorLogin: src.AuthorLogin, IsPR: false, Number: src.Number, Title: src.Title,
		}
	} else {
		source = &tatarav1.TaskSource{
			Provider: provider, IssueRef: ev.IssueRef, URL: ev.URL,
			AuthorLogin: ev.AuthorLogin, IsPR: true, Number: ev.Number, Title: ev.Title,
		}
	}
	key := ev.HeadBranch
	if key == "" {
		key = umbrella.Name
	}
	name := streamReviewTaskName(proj.Name, key)
	payload := tatarav1.QueuedEventPayload{
		Kind: "review",
		Goal: fmt.Sprintf("Review the cross-repo change stream on branch %q: verify every opened PR across all repos, "+
			"approve (native Approve + tatara-approved) only when ALL are green and mergeable, otherwise re-add "+
			"tatara-implementation to route the stream back to implement.", ev.HeadBranch),
		Name:          name,
		RepositoryRef: repoRef,
		Source:        source,
		Provider:      provider,
		PodRepo:       repoRef,
		Annotations:   map[string]string{tatarav1.AnnReviewHeadBranch: ev.HeadBranch},
	}
	_, created, err := queue.EnqueueEvent(ctx, s.cfg.Client, s.cfg.Seq, proj, tatarav1.QueueClassNormal, false, name, payload)
	return created, err
}

// streamReviewTaskName is the deterministic K8s-safe name of a stream review
// umbrella Task, keyed on (project, stream key = shared head branch). Concurrent
// PR-create deliveries for the same branch collide on this name so exactly one
// stream review Task is created.
func streamReviewTaskName(project, key string) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s", project, key)))
	return "rev-" + hex.EncodeToString(h[:])[:16]
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

// accept counts the event and writes a 202 Accepted response. Used at the ~20
// call sites that count a result and always respond StatusAccepted.
func (s *Server) accept(w http.ResponseWriter, provider, kind, action, result string) {
	s.count(provider, kind, action, result)
	w.WriteHeader(http.StatusAccepted)
}

// reject counts the event and writes an http.Error response. Used at the ~21
// call sites that count a result and always respond with a non-2xx status.
func (s *Server) reject(w http.ResponseWriter, status int, msg, provider, kind, action, result string) {
	s.count(provider, kind, action, result)
	http.Error(w, msg, status)
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
