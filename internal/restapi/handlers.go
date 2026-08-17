package restapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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
	"github.com/szymonrychu/tatara-operator/internal/obs"
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

// retryAfterSeconds is the hint on a 503 caused by a tatara-memory outage.
// Long enough that an agent retrying in a tight loop cannot amplify the outage,
// short enough that a blip costs one turn's pause and not the turn.
const retryAfterSeconds = "10"

// writeRetryAfter answers 503 Service Unavailable with a Retry-After header.
// It is for a write the operator REFUSES on purpose while a dependency is down
// - not an operator bug (500) and not a bad gateway response (502), both of
// which read to a caller as "this will not work, give up".
func writeRetryAfter(w http.ResponseWriter, msg string) {
	w.Header().Set("Retry-After", retryAfterSeconds)
	writeError(w, http.StatusServiceUnavailable, msg)
}

// writeClientErr maps k8s apiserver errors onto the right HTTP status:
// NotFound -> 404, Invalid (a CRD/validation rejection, e.g. #398's line=0
// failing a CRD Minimum marker) -> 422 with the validation detail surfaced
// to the caller so it can fix and retry, anything else -> a generic 500 that
// withholds internal k8s error details.
func writeClientErr(w http.ResponseWriter, err error) {
	if apierrors.IsNotFound(err) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if apierrors.IsInvalid(err) {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
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

// decodeFieldChars caps the field name echoed back to the caller. The name is
// a key out of the caller's own JSON, so it is caller-controlled and otherwise
// only bounded by maxBodyBytes; a 1 MB key must not become a 1 MB error body.
const decodeFieldChars = 64

// unknownFieldPrefix is what encoding/json puts in front of the rejected key
// when DisallowUnknownFields trips. The error is a bare errors.New, so the name
// can only be recovered by parsing - there is no typed error to match on.
const unknownFieldPrefix = `json: unknown field `

// decodeErrorMessage turns a decoder failure into a message that names the
// field the caller got wrong. Anything it does not recognise degrades to the
// generic text: only names derived from the caller's own keys are echoed, never
// the decoder's raw output, so internal type detail cannot leak.
func decodeErrorMessage(err error) string {
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) && typeErr.Field != "" {
		return fmt.Sprintf("invalid JSON body: field %q has the wrong type",
			truncateValidUTF8(typeErr.Field, decodeFieldChars))
	}
	if msg := err.Error(); strings.HasPrefix(msg, unknownFieldPrefix) {
		if name, uerr := strconv.Unquote(strings.TrimPrefix(msg, unknownFieldPrefix)); uerr == nil && name != "" {
			return fmt.Sprintf("invalid JSON body: unknown field %q",
				truncateValidUTF8(name, decodeFieldChars))
		}
	}
	return "invalid JSON body"
}

// writeDecodeError writes the appropriate HTTP error for a decodeJSON failure.
// Oversized bodies become 413; every other decode failure becomes a 400 naming
// the offending field.
//
// It is a METHOD so the line goes through s.log at WARN. The only thing that
// can fail here is bytes the CALLER supplied, so the answer is always a 4xx and
// the operator did nothing wrong - but this site logged it at log.Log.Error,
// which is what "Tatara operator error recurring" counts (#558). One agent
// guessing a payload shape 5 times read as 5 operator errors.
//
// Naming the field is the other half. The old bare "invalid JSON body" is why
// the agent had to guess: it re-submitted 5 times over 48s against a 400 that
// never said which key was rejected.
func (s *Server) writeDecodeError(w http.ResponseWriter, r *http.Request, err error) {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	msg := decodeErrorMessage(err)
	// The full decoder error stays server-side; only msg goes to the caller.
	s.log.Warn("restapi: decode body failed", "path", r.URL.Path, "error", err.Error(), "detail", msg)
	writeError(w, http.StatusBadRequest, msg)
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
	return tatarav1alpha1.TruncateUTF8(s, maxBytes)
}

// titleLogPrefixChars is how much of a rejected title goes on the log line:
// enough to recognise which outcome it was, short enough not to wrap the line.
const titleLogPrefixChars = 80

// titleLogFields describes a failed issue-title forge write. BOTH lengths are
// on the line on purpose. Now that titles are clamped, a length-caused 4xx is
// impossible, so a line carrying only the raw agent-supplied count reports a
// cause that cannot be the one firing - "title_chars=900" beside a 400 that the
// 255-rune value actually took - while the value that did reach the forge
// appears nowhere. raw says what the agent wrote, sent says what was rejected,
// and the two differing is itself the signal that the clamp engaged.
//
// The prefix is taken from the SENT title so every field on the line describes
// the same request.
func titleLogFields(raw, sent string) []any {
	return []any{
		"title_chars", utf8.RuneCountInString(raw),
		"sent_title_chars", utf8.RuneCountInString(sent),
		"title_prefix", tatarav1alpha1.TruncateRunes(sent, titleLogPrefixChars),
	}
}

// clampTitleForForge clamps an agent-supplied title on its way to a forge write,
// and when the clamp actually changed something it says so - counter plus one
// INFO line - instead of rewriting what the agent wrote in silence.
//
// It is how every title write in this package clamps: the three CreateIssue
// sites, the deferred issue edit intent, and submit_outcome's merge request
// title (editSubmittedMRs). A forge caps a merge request title exactly as it
// caps an issue title, so there is one cap and one clamp - see ClampIssueTitle.
//
// The clamp exists to make an over-long title a NON-event, so it is expected to
// engage on ordinary traffic. That is the argument for the signal, not against
// it: titleLogFields only ever fires on the forge-error path, and the whole
// point of the clamp is that that path stops firing. Without this the platform
// edits an agent's title with no trace anywhere.
//
// One dedicated line rather than a field spliced into each site's success log:
// the four sites' success lines are four different messages (one of them emitted
// by shared outcome-commit code that has no title in scope), and a reader who
// wants to know whether the clamp is engaging should not have to know which four.
func (s *Server) clampTitleForForge(ctx context.Context, r *http.Request, site, task, raw string) string {
	sent := tatarav1alpha1.ClampIssueTitle(raw)
	if sent == raw {
		return sent
	}
	obs.RestTitleClampedTotal.WithLabelValues(site).Inc()
	fields := append(reqLogFields(r), "action", "issue_title_clamped",
		"site", site, "task", task)
	s.log.InfoContext(ctx, "restapi: issue title clamped",
		append(fields, titleLogFields(raw, sent)...)...)
	return sent
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
