package webhook

import (
	"context"
	"crypto/subtle"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/incident"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
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
	if cfg.ReaderFor == nil {
		cfg.ReaderFor = scm.ReaderByProvider
	}
	if cfg.SpillerFor == nil {
		// Back-compat: single-Spiller callers (and tests) keep working - resolve
		// every project to the one Spiller they supplied.
		single := cfg.Spiller
		cfg.SpillerFor = func(*tatarav1.Project) objbudget.Spiller { return single }
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
// It MINTS NOTHING. The B.4 SWEEP is the only intake: it reads the forge on its
// cadence, mirrors issues/MRs onto Issue/MergeRequest CRs, and mints the Tasks.
// A webhook that minted its own Task would race the sweep for the same (repo,
// number) natural key and produce a second owner.
//
// What the webhook DOES own is the two SIGNALS the sweep cannot derive from a
// forge listing:
//
//   - the LOW-LATENCY side channel (contract E.3): a comment is mirrored onto its
//     Issue/MergeRequest CR immediately and queued as a pendingEvent on the owning
//     Task, so a maintainer's "go ahead" lands within seconds, not at the next
//     sweep;
//   - the LIVENESS marker (contract F.3's Create edge): an issues.opened /
//     issues.reopened stamps tatara.dev/webhook-originated on the mirror Issue CR,
//     and the next sweep mints an ACTIVE Task for it. Without this a brand-new
//     human issue is byte-for-byte a cold backlog issue (open, human-authored,
//     zero comments) and the sweep parks it - the platform's front door, shut.
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
	s.accept(w, provider, ev.Kind, ev.Action, "ignored")
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

	s.deliverPendingEvent(ctx, proj, commentRepo, ev)
	s.accept(w, provider, ev.Kind, ev.Action, "accepted")
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

// incidentStaleAge bounds how long an open (non-terminal) incident Task may sit
// before a persistent re-fire of the same alert group re-triggers it. Generous so a
// live investigation is never disrupted, yet finite so a wedged incident cannot
// suppress escalation forever (liveness finding #5). Anchor: LastActivityAt
// (fallback CreationTimestamp).
func (s *Server) createIncidentTask(ctx context.Context, proj *tatarav1.Project, alert GrafanaAlert, groupHash string) (bool, error) {
	slugs := projectRepoSlugs(ctx, s.cfg.Client, s.cfg.Namespace, proj.Name)
	alertCtx := renderAlertContext(alert)
	tierRevert := alert.CommonLabels["tatara_tier_quality"] == "true"
	// In-flight-work dedup (finding #6): a firing alert that implicates a repo which
	// already has a non-terminal Task must not spin a competing clarify->implement
	// cycle (e.g. a component mid-deploy throwing a symptomatic alert). The alert-
	// group hash only catches a re-fire of the SAME alert; this catches a DIFFERENT
	// alert on a repo that is already being worked. The tier-revert self-heal is
	// exempt: it targets tatara-helmfile and must always proceed.
	if !tierRevert {
		implicated := s.alertImplicatedRepos(ctx, proj.Name, alert)
		if len(implicated) > 0 && s.repoHasNonTerminalTask(ctx, proj.Name, implicated) {
			s.log.InfoContext(ctx, "incident skipped: implicated repo has in-flight work",
				"action", "incident_skip_repo_inflight", "project", proj.Name,
				"alert_group", groupHash, "repos", strings.Join(implicated, ","))
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
		DedupKey:     groupHash,
		Labels:       map[string]string{tatarav1.LabelActivity: "incident"},
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
