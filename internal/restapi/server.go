package restapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Config holds the REST server dependencies.
type Config struct {
	Client    client.Client
	Namespace string
}

// Server exposes OIDC-gated CRUD over the tatara CRDs, backed by the
// controller-runtime client. It shares the HTTP_ADDR listener with the
// webhook server; callers mount it onto a shared chi router.
type Server struct {
	c  client.Client
	ns string
}

// NewServer constructs a Server from cfg.
func NewServer(cfg Config) *Server {
	return &Server{c: cfg.Client, ns: cfg.Namespace}
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
	r.Post("/projects/{p}/issues", s.proposeIssue)
	r.Post("/tasks/{t}/review", s.reviewVerdict)
	r.Post("/tasks/{t}/pr-outcome", s.prOutcome)
	r.Post("/tasks/{t}/issue-outcome", s.issueOutcome)
	r.Post("/tasks/{t}/comment", s.postComment)
	r.Post("/tasks/{t}/change-summary", s.changeSummary)
	r.Post("/tasks/{t}/handover", s.handover)
}
