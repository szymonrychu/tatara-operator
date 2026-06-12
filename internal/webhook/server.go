package webhook

import (
	"context"
	"errors"
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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
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
	// Approval flip: a human (not the bot) removing the approval label from a
	// bot-authored proposal issue signals human approval. The actual proposal
	// match (ProposedIssue set, source author is the bot, approval required) is
	// re-checked per Task in flipApproval; here we only gate the entry on the
	// event shape. On GitHub the issue author (AuthorLogin) is reliable, so we
	// also require it to be the bot; GitLab cannot distinguish author from actor
	// in the payload, so the Task-side check is authoritative there.
	if ev.Action == "unlabeled" && ev.ChangedLabel == approvalLabel(proj) && proj.Spec.Scm != nil &&
		ev.ActorLogin != proj.Spec.Scm.BotLogin &&
		(provider != "github" || ev.AuthorLogin == proj.Spec.Scm.BotLogin) {
		s.flipApproval(ctx, w, provider, proj, ev)
		return
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
	kind := "issueLifecycle"
	if ev.IsPR {
		if ev.AuthorLogin == bot && bot != "" {
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
	var existing tatarav1.TaskList
	if err := s.cfg.Client.List(ctx, &existing, client.InNamespace(s.cfg.Namespace)); err != nil {
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
			repoSlug := strings.ReplaceAll(ev.IssueRef[:strings.LastIndex(ev.IssueRef, "#")], "/", ".")
			labels = map[string]string{
				tatarav1.LabelSourceRepo:   repoSlug,
				tatarav1.LabelSourceNumber: strconv.Itoa(dedupNumber),
				tatarav1.LabelSourceKind:   kind,
				tatarav1.LabelActivity:     "webhook",
			}
		} else {
			ann[tatarav1.LifecycleEntryAnnotation] = "Implement"
			repoSlug := strings.ReplaceAll(ev.IssueRef[:strings.LastIndex(ev.IssueRef, "#")], "/", ".")
			labels = map[string]string{
				tatarav1.LabelSourceRepo:   repoSlug,
				tatarav1.LabelSourceNumber: strconv.Itoa(ev.Number),
				tatarav1.LabelSourceKind:   kind,
				tatarav1.LabelActivity:     "webhook",
			}
		}
	}

	task := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName:    "task-",
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
	if err := s.cfg.Client.Create(ctx, task); err != nil {
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

// flipApproval finds the Task whose Source.IssueRef matches ev.IssueRef and
// sets the ApprovalApproved condition to True.
func (s *Server) flipApproval(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	var tasks tatarav1.TaskList
	if err := s.cfg.Client.List(ctx, &tasks, client.InNamespace(s.cfg.Namespace)); err != nil {
		s.count(provider, ev.Kind, ev.Action, "error")
		http.Error(w, "list tasks", http.StatusInternalServerError)
		return
	}
	bot := proj.Spec.Scm.BotLogin
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.Source == nil || t.Spec.Source.IssueRef != ev.IssueRef {
			continue
		}
		// Only a genuine tatara proposal may be flipped: it must carry a
		// ProposedIssue, be bot-authored, and require approval.
		if t.Spec.ProposedIssue == nil || !t.Spec.ApprovalRequired || t.Spec.Source.AuthorLogin != bot {
			continue
		}
		apimeta.SetStatusCondition(&t.Status.Conditions, metav1.Condition{
			Type:    tatarav1.ConditionApprovalApproved,
			Status:  metav1.ConditionTrue,
			Reason:  "HumanApproved",
			Message: "approval label removed by human",
		})
		if err := s.cfg.Client.Status().Update(ctx, t); err != nil {
			s.count(provider, ev.Kind, ev.Action, "error")
			http.Error(w, "flip approval", http.StatusInternalServerError)
			return
		}
		s.log.InfoContext(ctx, "approval label removed; task approval flipped",
			"project", proj.Name, "task", t.Name, "issue_ref", ev.IssueRef)
		s.count(provider, ev.Kind, ev.Action, "approval_flipped")
		w.WriteHeader(http.StatusAccepted)
		return
	}
	// No matching task found; ignore.
	s.count(provider, ev.Kind, ev.Action, "ignored")
	w.WriteHeader(http.StatusAccepted)
}

func approvalLabel(p tatarav1.Project) string {
	if p.Spec.Scm != nil && p.Spec.Scm.ApprovalLabel != "" {
		return p.Spec.Scm.ApprovalLabel
	}
	return "tatara/awaiting-approval"
}

func mentionsBot(body, bot string) bool {
	return bot != "" && strings.Contains(body, "@"+bot)
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
