package restapi

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// Config holds the REST server dependencies.
type Config struct {
	Client    client.Client
	Namespace string
	// SCMFor returns an SCMWriter for the given provider name ("github"|"gitlab").
	// When nil, the /projects/{p}/issue-comment endpoint returns 501.
	SCMFor func(provider string) (scm.SCMWriter, error)
	// ReaderFor returns a token-bound SCMReader for the given provider name.
	// Used by the issue-comment gate to detect an existing bot comment.
	// When nil, the gate is skipped (post proceeds).
	ReaderFor func(provider, token string) (scm.SCMReader, error)
	Logger    *slog.Logger
	Metrics   *obs.OperatorMetrics
}

// Server exposes OIDC-gated CRUD over the tatara CRDs, backed by the
// controller-runtime client. It shares the HTTP_ADDR listener with the
// webhook server; callers mount it onto a shared chi router.
type Server struct {
	c         client.Client
	ns        string
	scmFor    func(provider string) (scm.SCMWriter, error)
	readerFor func(provider, token string) (scm.SCMReader, error)
	log       *slog.Logger
	metrics   *obs.OperatorMetrics
}

// NewServer constructs a Server from cfg.
func NewServer(cfg Config) *Server {
	l := cfg.Logger
	if l == nil {
		l = slog.Default()
	}
	return &Server{c: cfg.Client, ns: cfg.Namespace, scmFor: cfg.SCMFor, readerFor: cfg.ReaderFor, log: l, metrics: cfg.Metrics}
}

// Mount registers the REST routes on r. verify is the OIDC middleware;
// when nil, routes are mounted without auth (tests only). The routes use
// the bare paths from the pin set so they do not collide with the
// webhook server's /operator/webhooks/... prefix on the same listener.
func (s *Server) Mount(r chi.Router, verify func(http.Handler) http.Handler) {
	r.Group(func(r chi.Router) {
		if verify != nil {
			r.Use(verify)
		}
		s.routes(r)
	})
}

// routes wires every REST endpoint. Handlers are filled in by later tasks.
func (s *Server) routes(r chi.Router) {
	r.Get("/projects", s.listProjects)
	r.Get("/projects/{p}", s.getProject)
	r.Get("/projects/{p}/repositories", s.listRepositories)
	r.Get("/projects/{p}/tasks", s.listTasks)
	r.Get("/tasks/{t}", s.getTask)
	r.Patch("/tasks/{t}", s.patchTask)
	r.Get("/tasks/{t}/subtasks", s.listSubtasks)
	r.Post("/tasks/{t}/subtasks", s.createSubtask)
	r.Patch("/subtasks/{s}", s.patchSubtask)
	r.Get("/projects/{p}/issues", s.listProjectIssues)
	r.Get("/projects/{p}/commits", s.listProjectCommits)
	r.Post("/projects/{p}/issues/{owner}/{repo}/{number}/close", s.closeProjectIssue)
	r.Patch("/projects/{p}/issues/{owner}/{repo}/{number}", s.editProjectIssue)
	r.Post("/projects/{p}/issues/{owner}/{repo}", s.createProjectIssue)
	r.Post("/projects/{p}/issues", s.proposeIssue)
	r.Post("/projects/{p}/issue-comment", s.commentOnIssue)
	r.Post("/tasks/{t}/review", s.reviewVerdict)
	r.Post("/tasks/{t}/pr-outcome", s.prOutcome)
	r.Post("/tasks/{t}/issue-outcome", s.issueOutcome)
	r.Post("/tasks/{t}/implement-outcome", s.implementOutcome)
	r.Post("/tasks/{t}/brainstorm-outcome", s.brainstormOutcome)
	r.Post("/tasks/{t}/comment", s.postComment)
	r.Post("/tasks/{t}/change-summary", s.changeSummary)
	r.Post("/tasks/{t}/handover", s.handover)
}
