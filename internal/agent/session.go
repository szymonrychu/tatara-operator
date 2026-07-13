// Package agent talks to the tatara-claude-code-wrapper REST API and builds
// the wrapper Pod + Service that runs a single Claude session per Task.
package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// TurnResult is the outcome of one wrapper turn, as reported by the wrapper's
// GET /v1/messages/{turnId} response and the turn-complete callback.
type TurnResult struct {
	State, FinalText, StopReason, Err string
	// LastActivityAt is the wrapper's most recent transcript activity timestamp
	// for the turn. Zero when the wrapper does not report it (older wrapper or
	// unparseable value); callers fall back to turn-started-at.
	LastActivityAt time.Time
}

// SessionInfo is the wrapper's GET /v1/session response: ALL SIX existing
// fields (the wrapper's session.Snapshot - state, turnsCompleted, turnsFinished,
// model, repo, lastActivityAt) PLUS contractVersion (contract G.5/G.10).
//
// ContractVersion is a POINTER on purpose: an OLD wrapper omits the field
// entirely, and "absent" is a contract mismatch that must be distinguished from
// a wrapper that genuinely reported 0. Decoding into a plain int would collapse
// the two.
type SessionInfo struct {
	State          string    `json:"state"`
	TurnsCompleted int       `json:"turnsCompleted"`
	TurnsFinished  int       `json:"turnsFinished"`
	Model          string    `json:"model"`
	Repo           string    `json:"repo"`
	LastActivityAt time.Time `json:"lastActivityAt"`
	// +optional
	ContractVersion *int `json:"contractVersion,omitempty"`
}

// The wrapper's session states (session.State in tatara-claude-code-wrapper).
const (
	SessionStateBooting = "booting"
	SessionStateReady   = "ready"
	SessionStateBusy    = "busy"
	SessionStateDead    = "dead"
)

// TurnInFlight reports whether the wrapper currently has a turn running. The
// wrapper exposes no turnInFlight boolean; state=busy IS that boolean.
func (s SessionInfo) TurnInFlight() bool { return s.State == SessionStateBusy }

// ContractVersion is the operator/wrapper wire contract version (G.10). It is
// injected into every agent pod as TATARA_CONTRACT_VERSION and asserted against
// the wrapper's reported contractVersion at pod-ready, BEFORE turn-0.
const ContractVersion = 2

// Session is the operator's view of one wrapper session. baseURL is the
// per-pod wrapper address (http://<svc>.<ns>.svc:8080).
type Session interface {
	SubmitTurn(ctx context.Context, baseURL, text, callbackURL string) (turnID string, err error)
	// SubmitHandoffTurn submits the ONE turn a wrapper still admits past its TTL
	// deadline t0: the G.7 step-3 handoff-note turn. It sets "handoff": true on
	// POST /v1/messages. The allowance is scoped to PAST t0 - before t0 a
	// handoff turn is an ordinary turn and does not consume it - so the operator
	// sets the flag ONLY here, never on a normal turn.
	SubmitHandoffTurn(ctx context.Context, baseURL, text, callbackURL string) (turnID string, err error)
	GetTurn(ctx context.Context, baseURL, turnID string) (TurnResult, error)
	// GetSession reads GET /v1/session. It carries the wrapper's contractVersion
	// (G.10) alongside the six pre-existing fields.
	GetSession(ctx context.Context, baseURL string) (SessionInfo, error)
	DeleteSession(ctx context.Context, baseURL string) error
}

// ContractMismatchError is returned by AssertContractVersion when the wrapper
// speaks a contract version the operator does not. Got is 0 when the wrapper
// reported no contractVersion field at all (an old wrapper).
type ContractMismatchError struct {
	Expected int
	Got      int
}

func (e *ContractMismatchError) Error() string {
	return fmt.Sprintf("agent contract mismatch: operator speaks v%d, wrapper reported v%d", e.Expected, e.Got)
}

// IsContractMismatch reports whether err is a *ContractMismatchError.
func IsContractMismatch(err error) bool {
	var cme *ContractMismatchError
	return errors.As(err, &cme)
}

// AssertContractVersion is the G.10 handshake. At pod-ready, BEFORE a single
// turn is submitted, the operator reads GET /v1/session and compares the
// wrapper's contractVersion against its own. A mismatch - or a response with no
// contractVersion field at all - fails the Task instantly with
// stageReason=agent-contract-mismatch and ZERO tokens burned.
//
// The wrapper image is pinned in a DIFFERENT helm release than the operator and
// helmfile applies releases concurrently, so new-operator + old-agent is a
// reachable state. Without this assertion such a pod burns its whole turn budget
// producing nothing, because a tool 404 is just a tool error the model tries to
// work around.
func AssertContractVersion(ctx context.Context, s Session, baseURL string) error {
	info, err := s.GetSession(ctx, baseURL)
	if err != nil {
		return err
	}
	if info.ContractVersion == nil {
		return &ContractMismatchError{Expected: ContractVersion, Got: 0}
	}
	if *info.ContractVersion != ContractVersion {
		return &ContractMismatchError{Expected: ContractVersion, Got: *info.ContractVersion}
	}
	return nil
}

// IsTTLGone reports whether err is the wrapper's 410 Gone: this pod is past its
// TTL deadline t0 and will never take another turn. It is deliberately NOT 409
// (already taken by "a turn is in flight", which G.7 step 2 branches on) and
// deliberately NOT 503 (which means "retry later", the opposite of the truth).
func IsTTLGone(err error) bool {
	var he *HTTPError
	if errors.As(err, &he) {
		return he.Status == http.StatusGone
	}
	return false
}

// HTTPError is returned when the wrapper responds with a non-2xx status.
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("wrapper http %d: %s", e.Status, e.Body)
}

// UnreachableError wraps a transport-level failure reaching the wrapper: the
// HTTP request never produced a response. It typically means the wrapper pod
// is still booting its turn server even though the pod is Ready and the
// Service has endpoints. Callers treat it as a transient retry, not a hard
// failure that should trigger reconcile backoff.
type UnreachableError struct {
	Err error
}

func (e *UnreachableError) Error() string {
	if e.Err == nil {
		return "agent unreachable"
	}
	return "agent unreachable: " + e.Err.Error()
}

func (e *UnreachableError) Unwrap() error { return e.Err }

// isUnreachable reports whether err from http.Client.Do is a transport-level
// failure to reach the wrapper (dial refused/reset, no route, DNS not found):
// the pod is still booting its turn server. Timeouts and context cancellation
// are deliberately NOT boot-races - a hung or shutting-down server must keep
// erroring (and backing off) rather than requeue on a fixed short interval
// forever.
func isUnreachable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return false
	}
	return true
}

// isTransientWrapperStatus reports whether a wrapper HTTP status means "not
// ready yet" rather than a hard failure: the same transient condition as a
// connection-refused boot-race, only the Service happened to still route to a
// pod whose session just went Booting/Dead. 503 = session not ready / dead
// (crash-recovery mid-task); 425 = too early (server up, session still
// initialising). Callers requeue these under agentBootDeadline instead of
// tripping reconcile backoff. 409 ("session busy") is deliberately NOT here: it
// is handled separately as transient backpressure (see IsSessionBusy) on a short
// bounded requeue, and unlike a boot-race it is NOT bounded by agentBootDeadline
// because the session is actively running a prior turn, not failing to boot.
func isTransientWrapperStatus(status int) bool {
	return status == http.StatusServiceUnavailable || status == http.StatusTooEarly
}

// IsSessionBusy reports whether err from a SubmitTurn call is the wrapper's HTTP
// 409 "session busy" response: the session already has a turn in flight, so the
// submit was refused. This is expected, transient backpressure - the operator's
// view of the in-flight turn raced the wrapper's session release - not a dispatch
// failure. Callers requeue on a short bounded interval and count it as
// result="transient", rather than returning a hard reconcile error that would
// tight-loop on controller-runtime backoff (issue #168). Unlike
// IsTransientWrapper, a busy session is running a prior turn (not failing to
// boot), so it is NOT subject to agentBootDeadline termination; a session stuck
// busy forever is caught by the turn-timeout / planning-stall watchdogs instead.
func IsSessionBusy(err error) bool {
	var he *HTTPError
	if errors.As(err, &he) {
		return he.Status == http.StatusConflict
	}
	return false
}

// IsTransientWrapper reports whether err from a SubmitTurn call is the transient
// "wrapper not ready yet" condition - either a transport-level UnreachableError
// (the turn server is still booting) or an HTTPError with a transient status
// (503/425). Both mean the same thing and should be requeued under
// agentBootDeadline, not surfaced as hard reconcile errors. Which one occurs is
// decided only by endpoint-readiness propagation timing, so they are handled
// identically.
func IsTransientWrapper(err error) bool {
	var unreachable *UnreachableError
	if errors.As(err, &unreachable) {
		return true
	}
	var he *HTTPError
	if errors.As(err, &he) {
		return isTransientWrapperStatus(he.Status)
	}
	return false
}

// SubmitOutcome maps a SubmitTurn error to a low-cardinality outcome label for
// the operator_turn_submit_total metric and failure logs. It returns "ok" for a
// nil error, then one of: unreachable, http_503, http_409, http_425,
// http_error (other non-2xx), timeout (context deadline/cancel), or error (any
// other transport/decode failure). The fine-grained transport breakdown lives
// in operator_agent_http_total{method,outcome}; this label exists so the
// turn-submit failure ratio can separate transient wrapper-not-ready outcomes
// from genuine hard failures.
func SubmitOutcome(err error) string {
	if err == nil {
		return "ok"
	}
	var unreachable *UnreachableError
	if errors.As(err, &unreachable) {
		return "unreachable"
	}
	var he *HTTPError
	if errors.As(err, &he) {
		switch he.Status {
		case http.StatusServiceUnavailable:
			return "http_503"
		case http.StatusConflict:
			return "http_409"
		case http.StatusTooEarly:
			return "http_425"
		default:
			return "http_error"
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	return "error"
}
