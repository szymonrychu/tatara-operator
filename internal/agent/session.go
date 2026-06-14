// Package agent talks to the tatara-claude-code-wrapper REST API and builds
// the wrapper Pod + Service that runs a single Claude session per Task.
package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
)

// TurnResult is the outcome of one wrapper turn, as reported by the wrapper's
// GET /v1/messages/{turnId} response and the turn-complete callback.
type TurnResult struct {
	State, FinalText, StopReason, Err string
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
