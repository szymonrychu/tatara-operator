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

// Handler returns the chi router mounting the webhook endpoint.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Post("/operator/webhooks/{project}", s.handle)
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
		s.handleWorkItem(w, providerName, proj, ev)
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

func (s *Server) handleWorkItem(w http.ResponseWriter, provider string, proj tatarav1.Project, ev scm.WebhookEvent) {
	if !slices.Contains(ev.Labels, proj.Spec.TriggerLabel) {
		s.count(provider, ev.Kind, "ignored")
		w.WriteHeader(http.StatusAccepted)
		return
	}
	// M2 stub: a labeled work item is accepted and logged, but Task creation
	// is deliberately NOT performed here. M5 (work-item -> Task) completes this
	// branch by creating a Task with goal=ev.Body and source from ev.
	s.log.Info("webhook work item with trigger label (M2 stub, Task creation in M5)",
		"provider", provider, "project", proj.Name, "kind", ev.Kind, "issue_ref", ev.IssueRef)
	s.count(provider, ev.Kind, "accepted")
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
	httpSrv := &http.Server{
		Addr:              run.addr,
		Handler:           run.srv.Handler(),
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
