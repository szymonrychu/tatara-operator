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

// Session is the operator's view of one wrapper session. baseURL is the
// per-pod wrapper address (http://<svc>.<ns>.svc:8080).
type Session interface {
	SubmitTurn(ctx context.Context, baseURL, text, callbackURL string) (turnID string, err error)
	// Interject injects new user input into the turn currently in flight, as a
	// user typing into the live session mid-turn would. Returns an HTTPError
	// with status 409 when no turn is in flight.
	Interject(ctx context.Context, baseURL, text string) error
	GetTurn(ctx context.Context, baseURL, turnID string) (TurnResult, error)
	DeleteSession(ctx context.Context, baseURL string) error
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
