package restapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/auth"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// maxBodyBytes caps the request body at 1 MB, matching the webhook server's
// approach and preventing unbounded memory reads on any POST/PATCH endpoint.
const maxBodyBytes = 1 << 20 // 1 MB

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// Headers already sent; log so the failure is visible server-side.
		log.Log.Error(err, "restapi: writeJSON encode failed")
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// writeClientErr returns a generic 500 for non-404 errors, avoiding leaking
// internal k8s error details to API callers.
func writeClientErr(w http.ResponseWriter, err error) {
	if apierrors.IsNotFound(err) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	// Log real error server-side; return generic message to caller.
	log.Log.Error(err, "restapi: client error")
	writeError(w, http.StatusInternalServerError, "internal error")
}

// authorizeCaller gates a mutating handler on the caller carrying a valid
// OIDC bearer token (a non-empty, verifier-validated Subject) for the operator
// audience. The auth middleware has already verified the issuer, audience and
// signature before this runs; this is the in-handler assertion that a verified
// identity is present.
//
// NOTE: per-task (object-level) authorization keyed on the agent Pod name is NOT
// enforceable under the current identity model. Every agent Pod mints its bearer
// token via a SINGLE shared OIDC client (CLI_OIDC_CLIENT_ID/SECRET, client-
// credentials grant), so the token's sub is the Keycloak service-account UUID
// and preferred_username is "service-account-<client-id>" - identical for every
// Pod and never equal to agent.PodName(t). Comparing claims to the Pod name
// would 403 every legitimate agent write. Tightening to per-task scope requires
// per-Pod identity (e.g. a projected ServiceAccount token whose sub is the Pod's
// ServiceAccount, or a token-exchange that stamps the Pod/Task into the sub),
// tracked in MEMORY/ROADMAP. When no Claims are present (middleware absent, e.g.
// tests) the check is skipped. Returns false and writes a 403 on failure.
func authorizeCaller(w http.ResponseWriter, r *http.Request) bool {
	claims, ok := auth.ClaimsFromContext(r.Context())
	if !ok {
		// No auth middleware in this path; skip enforcement.
		return true
	}
	if claims.Subject != "" {
		return true
	}
	writeError(w, http.StatusForbidden, "caller has no verified identity")
	return false
}

// reqLogFields returns the common structured log fields for an INFO business
// action: request_id (from chi middleware) and user (from OIDC claims).
// Hard rule 12 requires these on every InfoContext call.
func reqLogFields(r *http.Request) []any {
	rid := chiMiddleware.GetReqID(r.Context())
	user := ""
	if claims, ok := auth.ClaimsFromContext(r.Context()); ok {
		user = claims.Subject
		if user == "" {
			user = claims.PreferredUsername
		}
	}
	return []any{"request_id", rid, "user", user}
}

func (s *Server) listProjects(w http.ResponseWriter, r *http.Request) {
	var list tatarav1alpha1.ProjectList
	if err := s.c.List(r.Context(), &list, client.InNamespace(s.ns)); err != nil {
		writeClientErr(w, err)
		return
	}
	out := make([]ProjectDTO, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, toProjectDTO(list.Items[i]))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) getProject(w http.ResponseWriter, r *http.Request) {
	var p tatarav1alpha1.Project
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "p")}
	if err := s.c.Get(r.Context(), key, &p); err != nil {
		writeClientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toProjectDTO(p))
}

func (s *Server) listRepositories(w http.ResponseWriter, r *http.Request) {
	projName := chi.URLParam(r, "p")
	var proj tatarav1alpha1.Project
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: projName}, &proj); err != nil {
		writeClientErr(w, err)
		return
	}
	var list tatarav1alpha1.RepositoryList
	if err := s.c.List(r.Context(), &list, client.InNamespace(s.ns)); err != nil {
		writeClientErr(w, err)
		return
	}
	out := make([]RepositoryDTO, 0)
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == projName {
			out = append(out, toRepositoryDTO(list.Items[i]))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	projName := chi.URLParam(r, "p")
	var proj tatarav1alpha1.Project
	if err := s.c.Get(r.Context(), client.ObjectKey{Namespace: s.ns, Name: projName}, &proj); err != nil {
		writeClientErr(w, err)
		return
	}
	var list tatarav1alpha1.TaskList
	if err := s.c.List(r.Context(), &list, client.InNamespace(s.ns)); err != nil {
		writeClientErr(w, err)
		return
	}
	out := make([]TaskDTO, 0)
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == projName {
			out = append(out, toTaskDTO(list.Items[i]))
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func decodeJSON(r *http.Request, w http.ResponseWriter, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

// writeDecodeError writes the appropriate HTTP error for a decodeJSON failure.
// Oversized bodies become 413; all other decode errors become 400 with a generic
// message so internal json-decoder detail is not echoed to callers.
func writeDecodeError(w http.ResponseWriter, r *http.Request, err error) {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	// Log real error server-side; return generic message to caller.
	log.Log.Error(err, "restapi: decode body failed", "path", r.URL.Path)
	writeError(w, http.StatusBadRequest, "invalid JSON body")
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	var t tatarav1alpha1.Task
	key := client.ObjectKey{Namespace: s.ns, Name: chi.URLParam(r, "t")}
	if err := s.c.Get(r.Context(), key, &t); err != nil {
		writeClientErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTaskDTO(t))
}

// truncateValidUTF8 cuts s to at most maxBytes bytes on a rune boundary.
func truncateValidUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// validChangeSignificance is the closed set of semver levels an agent may
// declare on submit_outcome. CI cuts the release tag from the resulting
// semver:<level> PR label (contract H.4).
var validChangeSignificance = map[string]bool{"major": true, "minor": true, "patch": true}

// commitDTO is the wire type for GET /projects/{p}/scm/commits.
type commitDTO struct {
	Repo    string    `json:"repo"`
	SHA     string    `json:"sha"`
	Message string    `json:"message"`
	Author  string    `json:"author,omitempty"`
	Date    time.Time `json:"date"`
}

// resolveProjectSCMProviderToken resolves the project's SCM provider name and
// raw bot token from its ScmSecretRef. It does not check for an empty token -
// callers that must reject an empty token do so themselves, since Reader and
// Writer disagree on whether that is an error (Finding: do not add Reader's
// empty-token check to Writer's caller as a byproduct of this shared helper).
func (s *Server) resolveProjectSCMProviderToken(w http.ResponseWriter, r *http.Request, proj *tatarav1alpha1.Project) (provider, token string, ok bool) {
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	if provider == "" {
		writeError(w, http.StatusConflict, "project has no scm provider configured")
		return "", "", false
	}
	var sec corev1.Secret
	if err := s.c.Get(r.Context(), types.NamespacedName{Namespace: s.ns, Name: proj.Spec.ScmSecretRef}, &sec); err != nil {
		writeClientErr(w, err)
		return "", "", false
	}
	token = string(sec.Data["token"])
	return provider, token, true
}

// projectSCMWriterAndToken resolves the SCMWriter and bot token for project p.
// Returns (nil, "", error-written-to-w) on any failure so callers can return immediately.
func (s *Server) projectSCMWriterAndToken(w http.ResponseWriter, r *http.Request, proj *tatarav1alpha1.Project) (scm.SCMWriter, string, bool) {
	if s.scmFor == nil {
		writeError(w, http.StatusNotImplemented, "scm writer not configured")
		return nil, "", false
	}
	provider, token, ok := s.resolveProjectSCMProviderToken(w, r, proj)
	if !ok {
		return nil, "", false
	}
	writer, err := s.scmFor(provider)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return nil, "", false
	}
	if token == "" {
		writeError(w, http.StatusInternalServerError, "internal error")
		return nil, "", false
	}
	return writer, token, true
}

// projectSCMReader resolves a token-bound SCMReader for project p.
func (s *Server) projectSCMReader(w http.ResponseWriter, r *http.Request, proj *tatarav1alpha1.Project) (scm.SCMReader, string, bool) {
	if s.readerFor == nil {
		writeError(w, http.StatusNotImplemented, "scm reader not configured")
		return nil, "", false
	}
	provider, token, ok := s.resolveProjectSCMProviderToken(w, r, proj)
	if !ok {
		return nil, "", false
	}
	reader, err := s.readerFor(provider, token)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return nil, "", false
	}
	return reader, token, true
}

// commentRefusedResp is the 409 body the comment gate returns when a write is
// refused (a closed target, or the C.7 self-comment guard).
type commentRefusedResp struct {
	Error   string `json:"error"`
	Refused bool   `json:"refused"`
	Reason  string `json:"reason"`
}
