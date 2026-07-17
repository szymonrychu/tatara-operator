package webhook

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/incident"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// Config holds webhook server dependencies.
type Config struct {
	Client client.Client
	// APIReader is the manager's UNCACHED reader. driveCommentUnpark passes it
	// through to controller.ApplyUnpark, whose F.6 re-entry Get must never be
	// served from a cache that lags AppendTaskEvent's write microseconds
	// earlier in the same request (same #347/#348 idiom as
	// TaskReconciler.APIReader). Nil (unit tests) falls back to Client.
	APIReader client.Reader
	Namespace string
	Metrics   *obs.OperatorMetrics
	Logger    *slog.Logger
	Seq       *queue.SeqSource
	// Spiller is the A.7 byte-budget eviction sink (internal/memclient in
	// production). Required for the task-centric pendingEvents path (E.3): the
	// webhook mirrors comments onto Issue/MergeRequest CRs and re-syncs a
	// parked(identity-unverified) Task's thread on demand, both of which go
	// through the objbudget.Fit* guard. A nil Spiller degrades gracefully: the
	// mirror/re-verify side effects are skipped (logged at ERROR) and the
	// A nil Spiller degrades gracefully: the mirror side effects are skipped and
	// logged at ERROR.
	Spiller objbudget.Spiller
	// SpillerFor resolves the A.7 spill client PER PROJECT (the tatara-memory
	// endpoint is per-project). Production wires it (fix W1); it takes precedence
	// over the single Spiller. When nil, NewServer defaults it to a closure
	// returning the single Spiller, so existing single-Spiller callers/tests keep
	// working unchanged.
	SpillerFor func(*tatarav1.Project) objbudget.Spiller
	// ReaderFor builds a token-bound scm.SCMReader for the C3-3 on-demand
	// re-sync (internal/webhook/pending_events.go's scmReader). Same idiom as
	// internal/controller/issue_controller.go's field of the same name. Nil
	// defaults to scm.ReaderByProvider (production); tests inject a fake
	// reader so the identity-unverified re-verify path never needs a live
	// forge call.
	ReaderFor func(provider, token string) (scm.SCMReader, error)
	// IncidentDedupVolatileLabels overrides the per-series label denylist stripped
	// from the incident dedup key. Nil => defaultVolatileDenylist.
	IncidentDedupVolatileLabels []string
	// IncidentRefireCommentCooldown rate-limits the coalesced refire comment (A4).
	IncidentRefireCommentCooldown time.Duration
	// Now, when set, overrides time.Now for the coalesced refire-comment cooldown
	// (A4), so a test can drive the cooldown window deterministically. Nil (the
	// production default) uses the real clock.
	Now func() time.Time
}

// Server serves the SCM webhook endpoint.
type Server struct {
	cfg           Config
	log           *slog.Logger
	dedupDenylist map[string]bool
	now           func() time.Time
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
	if cfg.ReaderFor == nil {
		cfg.ReaderFor = scm.ReaderByProvider
	}
	if cfg.SpillerFor == nil {
		// Back-compat: single-Spiller callers (and tests) keep working - resolve
		// every project to the one Spiller they supplied.
		single := cfg.Spiller
		cfg.SpillerFor = func(*tatarav1.Project) objbudget.Spiller { return single }
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Server{cfg: cfg, log: cfg.Logger, dedupDenylist: denylistSet(cfg.IncidentDedupVolatileLabels), now: now}
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
		s.handleForgeItem(ctx, w, providerName, proj, ev)
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

// handleForgeItem routes an issue/MR webhook delivery.
//
// The webhook is now the PRIMARY minter (Task 3): handleIssueOpened and
// handleMROpened call the shared controller.Minter funnel IMMEDIATELY, within
// the HTTP handler, so a new human issue/PR mints its Task on delivery rather
// than at the next B.4 sweep tick. The sweep remains the BACKSTOP - the
// funnel's deterministic natural-key mint makes a sweep pass over the same
// item a no-op, so the two never race for a second owner.
//
// What the webhook additionally owns is the LOW-LATENCY side channel (contract
// E.3): a comment is mirrored onto its Issue/MergeRequest CR immediately and
// queued as a pendingEvent on the owning Task, so a maintainer's "go ahead"
// lands within seconds. An orphan comment (no owning Task yet) also mints
// through the same funnel before that delivery, so a maintainer's first
// "@bot go" spawns work immediately too.
//
// Everything else is accepted and ignored; the sweep converges it.
func (s *Server) handleForgeItem(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	if ev.IsComment {
		s.handleIssueComment(ctx, w, provider, proj, ev)
		return
	}
	if ev.Kind == "issue" && !ev.IsPR && (ev.Action == "opened" || ev.Action == "reopened") {
		s.handleIssueOpened(ctx, w, provider, proj, ev)
		return
	}
	// ev.IsReview does not exist yet (Task 4 adds it); gate on the fields that
	// do exist today. ev.IsComment is already false here (handled above).
	if ev.Kind == "mr" && ev.IsPR && (ev.Action == "opened" || ev.Action == "reopened") {
		s.handleMROpened(ctx, w, provider, proj, ev)
		return
	}
	s.accept(w, provider, ev.Kind, ev.Action, "ignored")
}

// minter builds the ONE shared intake funnel (internal/controller.Minter) from
// the webhook's own dependencies. Metrics is nil: the webhook mint does not
// double-count controller.OrphanAdopted, which the sweep's own Minter already
// increments on its backstop pass.
func (s *Server) minter() *controller.Minter {
	return &controller.Minter{
		Client:    s.cfg.Client,
		APIReader: s.cfg.APIReader, // nil-safe: Minter falls back to Client
		Scheme:    s.cfg.Client.Scheme(),
		Metrics:   nil,
	}
}

// repoSlug returns "owner/name" for a Repository URL, or "" on error. Local
// twin of internal/controller's unexported helper of the same name - kept
// package-local rather than exported, matching that package's KISS precedent.
func repoSlug(repo *tatarav1.Repository) string {
	if repo == nil {
		return ""
	}
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return ""
	}
	return owner + "/" + name
}

// handleIssueOpened marks a freshly opened (or reopened) issue as LIVE.
//
// It applies the SAME two intake gates handleIssueComment does, and for the same
// reasons: a BOT-authored issue event must never mark (the operator's own
// issue_write(create) would hand itself an ACTIVE Task - a self-trigger loop with
// no human in it), and an author outside the reporter allowlist must never mark
// (issue #102: an INJECTED issue never becomes a Task).
//
// GitHub sends opened/reopened separately; GitLab collapses open and reopen into
// "opened" (scm.glActionAndLabel). Both are the same signal: a human just put
// this issue in front of us.
//
// A failure to mark is a 500, matching handlePush's annotate failure: the
// delivery is the ONLY liveness signal this issue will ever get, and swallowing
// it silently leaves a human's brand-new issue parked in the backlog.
func (s *Server) handleIssueOpened(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	if isBotActor(&proj, ev.ActorLogin) {
		s.log.InfoContext(ctx, "issues: bot-authored issue event ignored",
			"project", proj.Name, "issue_ref", ev.IssueRef, "action", ev.Action)
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	repo, err := s.matchRepo(ctx, proj.Name, ev.Repo)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "list repositories", provider, ev.Kind, ev.Action, "error")
		return
	}
	if repo == nil || ev.Number <= 0 {
		// Not an enrolled repository: there is no mirror to mark, and the sweep will
		// never look at it either.
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	if !tatarav1.IsAllowedReporter(&proj, repo, ev.ActorLogin) {
		s.log.InfoContext(ctx, "issues: author not an allowed reporter; ignoring",
			"project", proj.Name, "issue_ref", ev.IssueRef, "author", ev.ActorLogin)
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}

	marked, err := controller.MarkWebhookOriginated(ctx, s.cfg.Client, &proj, repo, ev.Number, ev.URL, time.Now())
	if err != nil {
		s.log.ErrorContext(ctx, "issues: mark webhook-originated failed", "error", err,
			"project", proj.Name, "issue_ref", ev.IssueRef)
		s.reject(w, http.StatusInternalServerError, "mark issue", provider, ev.Kind, ev.Action, "error")
		return
	}
	s.log.InfoContext(ctx, "issues: webhook-originated issue marked live",
		"action", "issue_webhook_originated", "resource_id", tatarav1.IssueName(repo.Name, ev.Number),
		"project", proj.Name, "repository", repo.Name, "number", ev.Number,
		"issue_action", ev.Action, "author", ev.ActorLogin, "marked", marked)

	item := controller.ForgeItem{Issue: scm.Issue{
		Number: ev.Number, State: "open", Author: ev.ActorLogin,
		Title: ev.Title, Body: ev.Body, Labels: ev.Labels, URL: ev.URL,
	}}
	if _, created, merr := s.minter().MintForItem(ctx, &proj, repo, item, true, s.cfg.SpillerFor(&proj)); merr != nil {
		s.log.ErrorContext(ctx, "issues: primary mint failed", "error", merr,
			"project", proj.Name, "issue_ref", ev.IssueRef)
		s.reject(w, http.StatusInternalServerError, "mint issue", provider, ev.Kind, ev.Action, "error")
		return
	} else if created {
		s.log.InfoContext(ctx, "issues: webhook minted clarify task",
			"action", "issue_webhook_mint", "project", proj.Name, "repository", repo.Name, "number", ev.Number)
	}
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
}

// handleMROpened mints a review Task immediately for a human-authored PR/MR
// open (or reopen) delivery, mirroring handleIssueOpened's gates: the bot
// self-loop guard first (an agent's own PR must never mint a review Task -
// controller.ClassifyPR inside MintForItem already ignores a bot-authored
// non-adoptable PR, but the explicit gate here keeps the webhook's self-loop
// guard parallel across both handlers), then the reporter allowlist, then the
// shared controller.Minter funnel.
func (s *Server) handleMROpened(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	if isBotActor(&proj, ev.ActorLogin) {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	repo, err := s.matchRepo(ctx, proj.Name, ev.Repo)
	if err != nil {
		s.reject(w, http.StatusInternalServerError, "list repositories", provider, ev.Kind, ev.Action, "error")
		return
	}
	if repo == nil || ev.Number <= 0 {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	if !tatarav1.IsAllowedReporter(&proj, repo, ev.ActorLogin) {
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}
	item := controller.ForgeItem{IsPR: true, PR: scm.PRRef{
		Number: ev.Number, Author: ev.ActorLogin, HeadSHA: ev.HeadSHA,
		HeadBranch: ev.HeadBranch, Repo: repoSlug(repo), Body: ev.Body, Labels: ev.Labels,
	}}
	if _, created, merr := s.minter().MintForItem(ctx, &proj, repo, item, false, s.cfg.SpillerFor(&proj)); merr != nil {
		s.log.ErrorContext(ctx, "mr: primary mint failed", "error", merr,
			"project", proj.Name, "issue_ref", ev.IssueRef)
		s.reject(w, http.StatusInternalServerError, "mint mr", provider, ev.Kind, ev.Action, "error")
		return
	} else if created {
		s.log.InfoContext(ctx, "mr: webhook minted review task",
			"action", "mr_webhook_mint", "project", proj.Name, "repository", repo.Name, "number", ev.Number)
	}
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
}

// handleIssueComment reacts to an issue_comment (action=created) webhook on an
// issue OR an MR. Bot comments are ignored to prevent self-trigger loops, and a
// comment from outside the reporter allowlist is dropped at intake so an
// injected body cannot drive the machine.
//
// The surviving comment is handed to deliverPendingEvent (contract E.3), which
// mirrors it onto the Issue/MergeRequest CR, queues a TaskEvent on the owning
// Task's pendingEvents, and - for a Task parked(identity-unverified) - re-runs
// the C.6 approval grammar right now.
func (s *Server) handleIssueComment(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	// ActorLogin is the sender of the event (comment author for issue_comment).
	if isBotActor(&proj, ev.ActorLogin) {
		s.log.InfoContext(ctx, "issue_comment: bot-authored comment ignored",
			"project", proj.Name, "issue_ref", ev.IssueRef)
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}

	// Reporter intake gate (issue #102): ignore comments from accounts outside the
	// reporter allowlist. An empty allowlist preserves the open default. The repo
	// override is honored when the comment maps to a known repository; on a lookup
	// miss/error the project list applies (fail-safe: the gate stays active).
	commentRepo, _ := s.matchRepo(ctx, proj.Name, ev.Repo)
	if !tatarav1.IsAllowedReporter(&proj, commentRepo, ev.ActorLogin) {
		s.log.InfoContext(ctx, "issue_comment: author not an allowed reporter; ignoring",
			"project", proj.Name, "issue_ref", ev.IssueRef, "author", ev.ActorLogin)
		s.accept(w, provider, ev.Kind, ev.Action, "ignored")
		return
	}

	if s.commentIsOrphan(ctx, commentRepo, ev) {
		var item controller.ForgeItem
		if ev.IsPR {
			item = controller.ForgeItem{IsPR: true, PR: scm.PRRef{
				Number: ev.Number, Author: ev.ActorLogin, HeadBranch: ev.HeadBranch, Repo: repoSlug(commentRepo)}}
		} else {
			item = controller.ForgeItem{Issue: scm.Issue{
				Number: ev.Number, State: "open", Author: ev.ActorLogin,
				Title: ev.Title, Body: ev.Body, Labels: ev.Labels, URL: ev.URL}}
		}
		if _, _, merr := s.minter().MintForItem(ctx, &proj, commentRepo, item, false, s.cfg.SpillerFor(&proj)); merr != nil {
			s.log.ErrorContext(ctx, "issue_comment: orphan mint failed", "error", merr, "issue_ref", ev.IssueRef)
		}
	}
	s.deliverPendingEvent(ctx, proj, commentRepo, ev)
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
}

// commentIsOrphan reports whether the mirror CR a comment targets has no
// owning Task yet - absent, or present but un-owned. It reads UNCACHED
// (s.cfg.APIReader when set, else s.cfg.Client) so the orphan check never
// races the cache behind a concurrent mint. On an error other than NotFound
// it returns false (fail-closed on minting: do not mint on an inconclusive
// read).
func (s *Server) commentIsOrphan(ctx context.Context, repo *tatarav1.Repository, ev scm.WebhookEvent) bool {
	if repo == nil || ev.Number <= 0 {
		return false
	}
	name := tatarav1.IssueName(repo.Name, ev.Number)
	var obj client.Object = &tatarav1.Issue{}
	if ev.IsPR {
		name = tatarav1.MergeRequestName(repo.Name, ev.Number)
		obj = &tatarav1.MergeRequest{}
	}
	var rdr client.Reader = s.cfg.Client
	if s.cfg.APIReader != nil {
		rdr = s.cfg.APIReader
	}
	if err := rdr.Get(ctx, objKey(s.cfg.Namespace, name), obj); err != nil {
		return apierrors.IsNotFound(err) // no mirror yet -> orphan; on other error, do not mint
	}
	_, owned := own.ControllerOwner(obj)
	return !owned
}

// isBotActor reports whether login is the project's configured bot identity.
// Every inbound path that could turn a comment into a Task must check this
// before doing so - an incident agent's own evidence comment on an issue
// (work-stream B) must never spawn a competing clarify/issue Task. Fail-open
// (false) when login is empty or the project has no bot login configured,
// matching the rest of the bot-actor guard family.
func isBotActor(proj *tatarav1.Project, login string) bool {
	if login == "" || proj.Spec.Scm == nil || proj.Spec.Scm.BotLogin == "" {
		return false
	}
	return login == proj.Spec.Scm.BotLogin
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
	dedupKey := incidentDedupKey(alert, proj.Name, s.dedupDenylist)
	created, err := s.createIncidentTask(ctx, &proj, alert, dedupKey)
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

// incidentStaleAge bounds how long an open (non-terminal) incident Task may sit
// before a persistent re-fire of the same alert group re-triggers it. Generous so a
// live investigation is never disrupted, yet finite so a wedged incident cannot
// suppress escalation forever (liveness finding #5). Anchor: LastActivityAt
// (fallback CreationTimestamp).
func (s *Server) createIncidentTask(ctx context.Context, proj *tatarav1.Project, alert GrafanaAlert, dedupKey string) (bool, error) {
	slugs := projectRepoSlugs(ctx, s.cfg.Client, s.cfg.Namespace, proj.Name)
	alertCtx := renderAlertContext(alert)
	tierRevert := alert.CommonLabels["tatara_tier_quality"] == "true"
	// In-flight-work dedup (finding #6): a firing alert that implicates a repo which
	// already has a non-terminal Task must not spin a competing clarify->implement
	// cycle (e.g. a component mid-deploy throwing a symptomatic alert). The rule-
	// key only catches a re-fire of the SAME rule; this catches a DIFFERENT alert
	// on a repo that is already being worked. The tier-revert self-heal is
	// exempt: it targets tatara-helmfile and must always proceed.
	if !tierRevert {
		implicated := s.alertImplicatedRepos(ctx, proj.Name, alert)
		if len(implicated) > 0 && s.repoHasNonTerminalTask(ctx, proj.Name, implicated) {
			s.log.InfoContext(ctx, "incident skipped: implicated repo has in-flight work",
				"action", "incident_skip_repo_inflight", "project", proj.Name,
				"alert_group", dedupKey, "repos", strings.Join(implicated, ","))
			return false, nil
		}
	}
	var goal string
	if tierRevert {
		goal = incident.GoalTierRevert(proj.Name, alert.CommonLabels["kind"], alert.CommonLabels["model"])
	} else {
		goal = incident.GoalProject(alertCtx, slugs)
	}
	payload := tatarav1.QueuedEventPayload{
		Kind:         "incident",
		Goal:         goal,
		GenerateName: "incident-",
		AlertRule:    alertRuleName(alert),
		DedupKey:     dedupKey,
		Labels:       map[string]string{tatarav1.LabelActivity: "incident"},
		Annotations:  map[string]string{tatarav1.AnnGrafanaAlert: alertCtx},
	}
	_, created, err := queue.EnqueueEvent(ctx, s.cfg.Client, s.cfg.Seq, proj, tatarav1.QueueClassAlert, false, dedupKey, payload)
	if err != nil || created {
		return created, err
	}
	// Suppressed (finding #5.5, O5): classify WHY for the business metric. An
	// open tracker Issue is a distinct suppression reason from a live
	// QueuedEvent/Task - it is what keeps suppression alive after the incident
	// Task itself has terminated (A2).
	reason := "live_task"
	if iss, ok, ferr := queue.FindOpenIncidentIssue(ctx, s.cfg.Client, s.cfg.Namespace, proj.Name, dedupKey); ferr == nil && ok {
		reason = "open_issue"
		s.enqueueRefireComment(ctx, proj, iss, alert)
	}
	s.cfg.Metrics.IncidentDedupSuppressed(reason)
	s.log.InfoContext(ctx, "incident dedup suppressed",
		"action", "incident_dedup_suppressed", "project", proj.Name,
		"alertname", alertRuleName(alert), "rule_key", dedupKey, "reason", reason)
	return false, nil
}

// enqueueRefireComment records a suppressed refire on the open tracker Issue:
// it ALWAYS increments RefireCount, and enqueues ONE PendingComment (setting
// LastRefireCommentAt) only when the cooldown has elapsed. No agent is
// spawned (A4) - this is an operator comment through the existing
// Issue.status.PendingComments drain.
func (s *Server) enqueueRefireComment(ctx context.Context, proj *tatarav1.Project, iss *tatarav1.Issue, alert GrafanaAlert) {
	now := s.now()
	cooldown := s.cfg.IncidentRefireCommentCooldown
	posted := false
	key := types.NamespacedName{Namespace: iss.Namespace, Name: iss.Name}
	spiller := s.cfg.SpillerFor(proj)
	err := objbudget.FitIssue(ctx, s.cfg.Client, spiller, key, func(i *tatarav1.Issue) {
		i.Status.RefireCount++
		if i.Status.LastRefireCommentAt != nil && now.Sub(i.Status.LastRefireCommentAt.Time) < cooldown {
			return // coalesced: within cooldown, counter already bumped
		}
		body := fmt.Sprintf("Alert re-fired %s; labels {%s}; %d recurrence.",
			now.UTC().Format(time.RFC3339), stableLabelSummary(alert, s.dedupDenylist), i.Status.RefireCount)
		reqID := fmt.Sprintf("refire-%s-%d", iss.Name, i.Status.RefireCount)
		if len(i.Status.PendingComments) < 20 {
			i.Status.PendingComments = append(i.Status.PendingComments, tatarav1.PendingComment{
				RequestID: reqID, Action: "comment", Body: body,
			})
			t := metav1.NewTime(now)
			i.Status.LastRefireCommentAt = &t
			posted = true
		}
	})
	if err != nil {
		s.log.ErrorContext(ctx, "incident refire comment enqueue failed",
			"action", "incident_refire_comment", "issue", iss.Name, "error", err)
		return
	}
	result := "coalesced"
	if posted {
		result = "posted"
	}
	s.cfg.Metrics.IncidentRefireComment(result)
	s.log.InfoContext(ctx, "incident refire comment",
		"action", "incident_refire_comment", "project", proj.Name,
		"issue", iss.Name, "rule_key", iss.Labels[queue.LabelAlertRuleKey], "result", result)
}

// stableLabelSummary renders the alert's stable (non-volatile) common labels
// for the refire comment body, matching what the dedup key hashes over.
func stableLabelSummary(a GrafanaAlert, denylist map[string]bool) string {
	stable := make(map[string]string, len(a.CommonLabels))
	for k, v := range a.CommonLabels {
		if k == "alertname" || denylist[k] {
			continue
		}
		stable[k] = v
	}
	return sortedKV(stable)
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

// alertImplicatedRepos returns the Repository CR NAMES an alert implicates: any
// project repo whose component name (or full owner/repo slug) appears as a LABEL
// VALUE on the alert (commonLabels or per-alert labels, e.g. service=tatara-operator).
// This is the deterministic alert->repo mapping the in-flight-work dedup keys on.
func (s *Server) alertImplicatedRepos(ctx context.Context, projName string, alert GrafanaAlert) []string {
	values := map[string]struct{}{}
	for _, v := range alert.CommonLabels {
		values[v] = struct{}{}
	}
	for _, a := range alert.Alerts {
		for _, v := range a.Labels {
			values[v] = struct{}{}
		}
	}
	var rl tatarav1.RepositoryList
	if err := s.cfg.Client.List(ctx, &rl, client.InNamespace(s.cfg.Namespace)); err != nil {
		return nil
	}
	var out []string
	for i := range rl.Items {
		repo := &rl.Items[i]
		if repo.Spec.ProjectRef != projName {
			continue
		}
		o, n, err := scm.OwnerRepo(repo.Spec.URL)
		if err != nil {
			continue
		}
		if _, ok := values[n]; ok {
			out = append(out, repo.Name)
			continue
		}
		if _, ok := values[o+"/"+n]; ok {
			out = append(out, repo.Name)
		}
	}
	sort.Strings(out)
	return out
}

// mirrorRefRepo extracts the Repository CR name from an Issue/MergeRequest CR
// name ("iss-<repo>-<n>" / "mr-<repo>-<n>"), the form Task.status.issueRefs and
// .mrRefs carry.
func mirrorRefRepo(ref string) string {
	s := strings.TrimPrefix(strings.TrimPrefix(ref, "iss-"), "mr-")
	i := strings.LastIndexByte(s, '-')
	if i <= 0 {
		return ""
	}
	return s[:i]
}

// repoHasNonTerminalTask reports whether any LIVE Task in the project already
// spans one of the implicated Repository CRs - via its primary repositoryRef, its
// mergeOrder, or the Issue/MergeRequest mirrors it owns.
func (s *Server) repoHasNonTerminalTask(ctx context.Context, projName string, implicated []string) bool {
	want := map[string]struct{}{}
	for _, name := range implicated {
		want[name] = struct{}{}
	}
	var tasks tatarav1.TaskList
	if err := s.cfg.Client.List(ctx, &tasks, client.InNamespace(s.cfg.Namespace)); err != nil {
		return false
	}
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.ProjectRef != projName || tatarav1.TaskDone(t) {
			continue
		}
		if _, ok := want[t.Spec.RepositoryRef]; ok && t.Spec.RepositoryRef != "" {
			return true
		}
		for _, name := range t.Spec.MergeOrder {
			if _, ok := want[name]; ok {
				return true
			}
		}
		for _, ref := range append(append([]string{}, t.Status.IssueRefs...), t.Status.MRRefs...) {
			if _, ok := want[mirrorRefRepo(ref)]; ok {
				return true
			}
		}
	}
	return false
}

// maxPendingEvents caps Task.Status.PendingEvents (contract E.3), applied
// Go-side, drop-oldest, BEFORE every write. The CRD's MaxItems=25 is a
// backstop only: an API-server 422 is not retried by retry.RetryOnConflict and
// would hot-loop webhook redelivery, so the cap here must stay strictly below
// it.
const maxPendingEvents = 20

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
