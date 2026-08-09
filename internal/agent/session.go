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
	// LastSubagentActivityAt is the most recent line the wrapper saw in ANY
	// <sessionDir>/subagents/agent-*.jsonl. The wrapper's own lastActivityAt tails
	// only the PARENT transcript, and a parent waiting on a Task tool call writes
	// nothing at all while its subagent works: one measured run went silent in the
	// parent file for 2095 seconds (35 minutes) with a perfectly healthy agent
	// underneath it. That is a stall by every signal the operator had.
	//
	// A POINTER, and this is the whole point of the field. An old wrapper omits it
	// entirely; a plain time.Time would decode as the zero value, which reads as
	// "no subagent activity, ever" - indistinguishable from a wrapper that reported
	// genuine silence, and exactly the bug this exists to fix. nil means NOT
	// REPORTED, and every consumer falls back to lastActivityAt alone.
	// +optional
	LastSubagentActivityAt *time.Time `json:"lastSubagentActivityAt,omitempty"`
	// OutstandingSubagentCalls is how many Task tool calls the wrapper has seen
	// started and not yet finished. Non-zero means the parent is legitimately
	// blocked on a child, which is the state that produced the 2095s blind spot.
	//
	// A POINTER for the same reason as LastSubagentActivityAt: from an old wrapper
	// zero would assert "no subagents running", which is a claim it never made.
	// +optional
	OutstandingSubagentCalls *int `json:"outstandingSubagentCalls,omitempty"`
}

// Probe lifecycle states, as reported by GET /v1/probe/{probeId}.
//
// The states are about DELIVERY, not about liveness directly: the wrapper writes
// a probe into the PTY, and Claude answers it at its next TOOL-CALL BOUNDARY.
const (
	// ProbeStatePending: the probe was written and its queue-operation `enqueue`
	// line was seen, with no matching `remove`. The agent has NOT picked it up.
	// Blocked inside one long tool call looks exactly like this - the probe is
	// buffered, not lost - so pending is "no answer yet", never "dead".
	ProbeStatePending = "pending"
	// ProbeStateDelivered: the enqueue was removed, so the agent consumed the
	// probe, and has not yet produced the answer line.
	ProbeStateDelivered = "delivered"
	// ProbeStateAnswered: an assistant message starting with the TATARA-ALIVE
	// marker landed after delivery. The agent is alive and the turn continues.
	ProbeStateAnswered = "answered"
)

// ProbeResult is the wrapper's GET /v1/probe/{probeId} response.
type ProbeResult struct {
	// ProbeID echoes the probe this status is for.
	ProbeID string
	// State is one of ProbeStatePending / ProbeStateDelivered / ProbeStateAnswered.
	State string
	// Answer is the agent's one-sentence reply, present only once answered.
	Answer string
	// SentAt / DeliveredAt / AnsweredAt are zero until the corresponding
	// transition happened.
	SentAt, DeliveredAt, AnsweredAt time.Time
}

// Answered reports whether the agent replied to this probe.
func (p ProbeResult) Answered() bool { return p.State == ProbeStateAnswered }

// ErrProbeUnsupported is returned by Probe / ProbeStatus / Interrupt when the
// wrapper does not serve the endpoint at all: HTTP 404 (route not mounted) or 405
// (path exists, method does not).
//
// It is a SENTINEL rather than a plain HTTPError because it is the ONLY thing
// keeping an old wrapper working byte-identically. The operator and the wrapper
// image are pinned in different helm releases and roll independently, so
// new-operator + old-wrapper is a guaranteed window on every train (#544 was 56
// minutes of exactly that). A caller that sees this must fall back to the
// pre-probe behaviour verbatim - never fail, never park, never escalate.
//
// One deliberate imprecision: a NEW wrapper also 404s an UNKNOWN probeId, so a
// probe whose id the wrapper has forgotten reads as unsupported. The consequence
// is identical either way (fall back to the pre-probe path), so the ambiguity
// costs nothing and collapsing it keeps the fallback single-branched.
var ErrProbeUnsupported = errors.New("agent: wrapper does not serve the stall probe endpoints")

// IsProbeUnsupported reports whether err means the wrapper has no probe support.
func IsProbeUnsupported(err error) bool { return errors.Is(err, ErrProbeUnsupported) }

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
//
// 2 -> 3: the agent-judged approval gate. submit_outcome(decision=implement)
// gained approvalCitations, the agent's {id, quote} citation of the maintainer
// comment it judged to be an approval. The same constant lives at 3 in
// tatara-cli (internal/mcp/contract.go) and tatara-claude-code-wrapper
// (internal/version/version.go); all three move in ONE shot. An old cli refuses
// to start its MCP server and an old wrapper fails AssertContractVersion at
// pod-ready, so a straggler is a loud pre-work failure rather than a silent
// mid-turn 400.
//
// 3 -> 4: the #521 lifecycle and agent merge. The `clarify` profile is DELETED
// and its three decisions became action values on the implement outcome
// (approved / discuss / rejected alongside submitted / declined), carrying
// approvingMaintainer, planNoteId and approvalCitations; `documentation` got its
// own schema so it does not inherit them. Three independent breaking changes to
// the agent-facing surface, so the constant moves once for all of them.
//
// 4 STAYS 4 ACROSS THE STALL PROBE, deliberately. /v1/probe, /v1/probe/{id} and
// /v1/interrupt are purely ADDITIVE HTTP, and the operator maps their absence to
// ErrProbeUnsupported and falls back verbatim - so an old wrapper is not merely
// tolerated, it behaves byte-identically. Bumping would drag in tatara-cli
// (internal/mcp/contract.go moves in the same shot) and burn the +-1 skew window
// on a change that breaks nothing, leaving the next genuine break with no skew
// room - the #544 shape with the pins swapped.
const ContractVersion = 4

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
	// Probe asks the agent, MID-TURN, whether it is alive. It is not a turn: the
	// wrapper writes the text straight into the PTY and never 409s, because the
	// only moment worth asking is precisely when a turn IS in flight. Returns the
	// probeId to poll with ProbeStatus.
	//
	// ErrProbeUnsupported from a wrapper that has no such endpoint.
	Probe(ctx context.Context, baseURL, text string) (probeID string, err error)
	// ProbeStatus reads one probe's delivery/answer state.
	//
	// ErrProbeUnsupported from a wrapper that has no such endpoint (and, harmlessly,
	// from a wrapper that has forgotten this probeId - see ErrProbeUnsupported).
	ProbeStatus(ctx context.Context, baseURL, probeID string) (ProbeResult, error)
	// Interrupt sends ESC to the agent's PTY, which cancels the in-flight tool
	// call synchronously (~40ms measured) while the session, sessionId, transcript
	// and workspace all survive. It is the primitive that makes a stalled turn's
	// handoff SUBMITTABLE: without it POST /v1/messages 409s for as long as the
	// hung turn is in flight, which is why the stalled-turn stop almost always
	// lands on the synthetic note today.
	//
	// ErrProbeUnsupported from a wrapper that has no such endpoint.
	Interrupt(ctx context.Context, baseURL string) error
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

// ContractSkewWindow is how far the wrapper's reported contract version may sit
// from the operator's while still being read as a RELEASE TRAIN IN FLIGHT rather
// than a broken pin.
//
// One, in either direction, and that is not a round number picked for taste: the
// operator and the wrapper move exactly one contract version per release, so at
// any instant during a train the only reachable skew is +-1. A gap of 2 or more
// means somebody's pin is genuinely wrong and no amount of waiting fixes it.
const ContractSkewWindow = 1

// ContractSkewError is the TRANSIENT sibling of ContractMismatchError: the
// wrapper speaks an ADJACENT contract version, which is the guaranteed state in
// the middle of a non-atomic release train.
//
// The operator image and the agent image are pinned in DIFFERENT helm releases
// (helmfile.yaml.gotmpl vs values/project-*/common.yaml) and helm rolls them
// independently, so every single train has a non-empty window where the two
// disagree. On 2026-08-08 that window was 56 minutes wide and the strict-equality
// gate destroyed 5 Tasks inside it (#544). It is deliberately NOT a
// *ContractMismatchError and does NOT unwrap to one: IsContractMismatch is the
// predicate that routes to the terminal park, and a rollout window must never
// reach it.
type ContractSkewError struct {
	Expected int
	Got      int
}

func (e *ContractSkewError) Error() string {
	return fmt.Sprintf("agent contract skew: operator speaks v%d, wrapper reported v%d (release train in flight)",
		e.Expected, e.Got)
}

// IsContractSkew reports whether err is a *ContractSkewError: a version skew the
// caller should WAIT OUT rather than act on.
func IsContractSkew(err error) bool {
	var cse *ContractSkewError
	return errors.As(err, &cse)
}

// contractSkewed reports whether got is within ContractSkewWindow of the
// operator's own version. Zero is excluded: it is the sentinel for "the wrapper
// reported no contractVersion field at all", which is an OLD wrapper, not a
// neighbouring release.
func contractSkewed(got int) bool {
	if got <= 0 {
		return false
	}
	d := got - ContractVersion
	if d < 0 {
		d = -d
	}
	return d <= ContractSkewWindow
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
	got := *info.ContractVersion
	if got == ContractVersion {
		return nil
	}
	// A NEIGHBOURING version is a release train mid-flight, not a broken wrapper.
	// It is reported separately so the caller can wait it out; strict equality
	// here is what destroyed 5 Tasks on 2026-08-08 (#544).
	if contractSkewed(got) {
		return &ContractSkewError{Expected: ContractVersion, Got: got}
	}
	return &ContractMismatchError{Expected: ContractVersion, Got: got}
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
