package restapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/szymonrychu/tatara-operator/internal/objbudget"
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
	// CIFor returns the live-CI capability behind GET /projects/{p}/scm/ci
	// (contract C.2.10). When nil, that endpoint returns 501.
	CIFor func(provider, token string) (scm.CIReader, error)
	// Spiller sends an eviction batch (comments, notes) to tatara-memory. It is
	// the A.7 byte guard's escape valve and the 50-note cap's drop target.
	Spiller objbudget.Spiller
	// Memory rehydrates spilled notes for task_context(notes=all).
	Memory NoteFetcher
	// Approval is the C.6 grammar. A nil verifier FAILS CLOSED: a clarify
	// decision=implement then parks at identity-unverified.
	Approval ApprovalVerifier
	// Now is injectable for tests; nil means time.Now.
	Now     func() time.Time
	Logger  *slog.Logger
	Metrics *obs.OperatorMetrics
}

// Server exposes OIDC-gated CRUD over the tatara CRDs, backed by the
// controller-runtime client. It shares the HTTP_ADDR listener with the
// webhook server; callers mount it onto a shared chi router.
type Server struct {
	c         client.Client
	ns        string
	scmFor    func(provider string) (scm.SCMWriter, error)
	readerFor func(provider, token string) (scm.SCMReader, error)
	ciFor     func(provider, token string) (scm.CIReader, error)
	spiller   objbudget.Spiller
	memory    NoteFetcher
	approval  ApprovalVerifier
	nowFn     func() time.Time
	ciPacer   *ciPacer
	log       *slog.Logger
	metrics   *obs.OperatorMetrics
}

// NewServer constructs a Server from cfg.
func NewServer(cfg Config) *Server {
	l := cfg.Logger
	if l == nil {
		l = slog.Default()
	}
	return &Server{
		c: cfg.Client, ns: cfg.Namespace, scmFor: cfg.SCMFor, readerFor: cfg.ReaderFor,
		ciFor: cfg.CIFor, spiller: cfg.Spiller, memory: cfg.Memory, approval: cfg.Approval,
		nowFn: cfg.Now, ciPacer: newCIPacer(), log: l, metrics: cfg.Metrics,
	}
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

// routes wires the complete contract-C.1 endpoint table (15 routes). The 19
// pre-redesign routes are DELETED and now 404: the whole old surface collapsed
// into task_context / task_note / submit_outcome / scm_read / issue_write /
// mr_write.
func (s *Server) routes(r chi.Router) {
	r.Get("/projects", s.listProjects)                      // 1
	r.Get("/projects/{p}", s.getProject)                    // 2  project_get
	r.Get("/projects/{p}/repositories", s.listRepositories) // 3  repo_list
	r.Get("/projects/{p}/tasks", s.listTasks)               // 4  task_list
	r.Get("/tasks/{t}", s.getTask)                          // 5  task_get
	r.Get("/tasks/{t}/context", s.taskContext)              // 6  task_context
	r.Post("/tasks/{t}/notes", s.postNote)                  // 7  task_note
	r.Post("/tasks/{t}/outcome", s.postOutcome)             // 8  submit_outcome
	r.Get("/projects/{p}/scm/issues", s.scmIssues)          // 9  scm_read(issues)
	r.Get("/projects/{p}/scm/mrs", s.scmMRs)                // 10 scm_read(mr)
	r.Get("/projects/{p}/scm/comments", s.scmComments)      // 11 scm_read(comments)
	r.Get("/projects/{p}/scm/commits", s.scmCommits)        // 12 scm_read(commits)
	r.Get("/projects/{p}/scm/ci", s.scmCI)                  // 13 scm_read(ci)
	r.Post("/projects/{p}/scm/issue-write", s.issueWrite)   // 14 issue_write
	r.Post("/projects/{p}/scm/mr-write", s.mrWrite)         // 15 mr_write
}
