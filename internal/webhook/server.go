package webhook

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/go-chi/chi/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
		s.count("unknown", "other", "bad_request")
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	provider, err := scm.Select(r.Header)
	if err != nil {
		s.count("unknown", "other", "bad_request")
		http.Error(w, "unrecognized provider", http.StatusBadRequest)
		return
	}
	providerName := provider.Provider()

	var proj tatarav1.Project
	if err := s.cfg.Client.Get(ctx, objKey(s.cfg.Namespace, projectName), &proj); err != nil {
		if apierrors.IsNotFound(err) {
			s.count(providerName, "other", "unknown_project")
			http.Error(w, "unknown project", http.StatusNotFound)
			return
		}
		s.count(providerName, "other", "error")
		http.Error(w, "lookup project", http.StatusInternalServerError)
		return
	}

	webhookSecret, err := s.webhookSecret(ctx, proj.Spec.ScmSecretRef)
	if err != nil {
		s.count(providerName, "other", "error")
		http.Error(w, "secret", http.StatusInternalServerError)
		return
	}

	ev, err := provider.DetectAndVerify(r.Header, body, webhookSecret)
	if err != nil {
		s.count(providerName, "other", "bad_signature")
		http.Error(w, "verification failed", http.StatusUnauthorized)
		return
	}

	switch ev.Kind {
	case "push":
		s.handlePush(ctx, w, providerName, projectName, ev)
	case "issue", "mr":
		s.handleWorkItem(ctx, w, providerName, proj, ev)
	default:
		s.count(providerName, "other", "ignored")
		w.WriteHeader(http.StatusAccepted)
	}
}

func (s *Server) handlePush(ctx context.Context, w http.ResponseWriter, provider, projectName string, ev scm.WebhookEvent) {
	var repos tatarav1.RepositoryList
	if err := s.cfg.Client.List(ctx, &repos, client.InNamespace(s.cfg.Namespace)); err != nil {
		s.count(provider, "push", "error")
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
			s.count(provider, "push", "error")
			http.Error(w, "annotate repository", http.StatusInternalServerError)
			return
		}
		s.log.InfoContext(ctx, "webhook push re-ingest requested",
			"provider", provider, "project", projectName, "repository", repo.Name, "branch", ev.Branch)
		s.count(provider, "push", "accepted")
		w.WriteHeader(http.StatusAccepted)
		return
	}
	s.count(provider, "push", "ignored")
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleWorkItem(ctx context.Context, w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	if !slices.Contains(ev.Labels, proj.Spec.TriggerLabel) {
		s.count(provider, ev.Kind, "ignored")
		w.WriteHeader(http.StatusAccepted)
		return
	}

	var repos tatarav1.RepositoryList
	if err := s.cfg.Client.List(ctx, &repos, client.InNamespace(s.cfg.Namespace)); err != nil {
		s.count(provider, ev.Kind, "error")
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
		s.count(provider, ev.Kind, "no_repo")
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// Dedupe: creating an issue with the label fires both issues.opened and
	// issues.labeled for the same issue. Skip if a non-terminal Task already
	// exists for this issue ref (re-labeling after completion still re-triggers).
	var existing tatarav1.TaskList
	if err := s.cfg.Client.List(ctx, &existing, client.InNamespace(s.cfg.Namespace)); err != nil {
		s.count(provider, ev.Kind, "error")
		http.Error(w, "list tasks", http.StatusInternalServerError)
		return
	}
	for i := range existing.Items {
		t := &existing.Items[i]
		if t.Spec.Source != nil && t.Spec.Source.IssueRef == ev.IssueRef &&
			t.Status.Phase != "Succeeded" && t.Status.Phase != "Failed" {
			s.log.InfoContext(ctx, "work item already has an active task; skipping duplicate",
				"project", proj.Name, "issue_ref", ev.IssueRef, "task", t.Name)
			s.count(provider, ev.Kind, "duplicate")
			w.WriteHeader(http.StatusAccepted)
			return
		}
	}

	task := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName:    "task-",
			Namespace:       s.cfg.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(&proj, tatarav1.GroupVersion.WithKind("Project"))},
		},
		Spec: tatarav1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Goal:          ev.Body,
			Source: &tatarav1.TaskSource{
				Provider: provider,
				IssueRef: ev.IssueRef,
				URL:      ev.URL,
			},
		},
	}
	if err := s.cfg.Client.Create(ctx, task); err != nil {
		s.count(provider, ev.Kind, "error")
		http.Error(w, "create task", http.StatusInternalServerError)
		return
	}
	s.log.InfoContext(ctx, "work item created task",
		"project", proj.Name, "repository", repo.Name,
		"task", task.Name, "issue_ref", ev.IssueRef)
	s.count(provider, ev.Kind, "task_created")
	w.WriteHeader(http.StatusAccepted)
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

func (s *Server) count(provider, kind, result string) {
	s.cfg.Metrics.WebhookEvent(provider, kind, result)
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
