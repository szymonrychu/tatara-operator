package restapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// forgeAlertRulePrefix is the human-visible GitHub label carrying the incident
// rule-key, a recovery index if the Issue CRs are lost. Value: <hash16>.
const forgeAlertRulePrefix = "tatara-alert-rule="

// outcomeAcceptedCondition is the DURABLE idempotency record of an accepted
// submit_outcome. Its Message is sha256(agentKind|payload), so a TTL-stopped
// pod's retry of an IDENTICAL outcome is recognised and answered 200 with the
// unchanged Task - it must not 409 the Task into failure. It rides in the SAME
// status write as the stage transition, so the record and the effect are atomic.
// The name lives in api/v1alpha1 because internal/controller reads the same
// condition and may not import this package.
const outcomeAcceptedCondition = tatarav1alpha1.ConditionOutcomeAccepted

// discardWriter satisfies http.ResponseWriter by throwing away everything
// written to it. Some helpers (projectSCMWriterAndToken) write an HTTP error
// as a side effect of a resolution failure; handing them a discardWriter
// instead of the request's real ResponseWriter is how a caller that has
// already committed to sending its OWN single response - and only wants the
// resolved value, not a competing write - can still call them.
type discardWriter struct{ h http.Header }

func (d *discardWriter) Header() http.Header {
	if d.h == nil {
		d.h = http.Header{}
	}
	return d.h
}

func (d *discardWriter) Write(b []byte) (int, error) { return len(b), nil }

func (d *discardWriter) WriteHeader(int) {}

func sha256Sum(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

// outcomeEnvelope is C.2.7's two-stage decode. DisallowUnknownFields on BOTH
// stages: an unknown key is a 400, never a silently-dropped instruction.
type outcomeEnvelope struct {
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

// implementPayload is the /outcome implement wire shape, and after #521 it
// carries the folded gate as well as the code outcome. THE KEY SET IS FROZEN
// against tatara-cli's post-outcomeArgMap output and must be diffed key for key
// against it: a mismatch fails SILENTLY at runtime, because DisallowUnknownFields
// only catches keys the operator does not know, never ones the cli stopped
// sending.
//
//	action title body changeSignificance mergeOrder
//	reason approvingMaintainer planNoteId approvalCitations
//
// THERE IS ONE `reason` KEY AND ITS LEGALITY IS DECIDED BY `action`. tatara-cli
// maps the agent's snake_case `decline_reason` onto the wire key `reason` in its
// outcomeArgMap, and implement's schema ALSO has a top-level `reason` for the
// three gate actions - both wrote payload["reason"], and buildOutcomePayload
// ranges over a Go map, so a call carrying both produced a NONDETERMINISTIC
// payload. The cli closed that by making exactly one of the two agent-facing
// arguments legal per action, which collapses on the wire to a SINGLE key.
//
// THERE IS NO `declineReason` KEY. An operator-side `declineReason` would be a
// second contract tatara-cli does not implement: every real decline arrives as
// {"action":"declined","reason":"..."} and would be refused twice over. This is
// the operator's half of the one contract - the operator is the trust boundary
// and an old or hand-rolled client can send anything, so the per-action
// legality is enforced here too rather than assumed.
//
// APPROVINGMAINTAINER AND APPROVALCITATIONS TRAVEL AS A PAIR. Both present is a
// human-cited approval; both absent is the autoApproveTataraProposals path,
// where a bot-authored, anchor-verified proposal is released with NO human
// comment at all - so there is no comment author to name and requiring the field
// would make the carve-out unreachable on the two Projects that have it enabled.
// One without the other is refused. planNoteId is required either way: the plan
// pin is orthogonal to who approved.
type implementPayload struct {
	Action             string   `json:"action"`
	Title              string   `json:"title,omitempty"`
	Body               string   `json:"body,omitempty"`
	ChangeSignificance string   `json:"changeSignificance,omitempty"`
	MergeOrder         []string `json:"mergeOrder,omitempty"`
	// Reason is REQUIRED on declined (why no code is coming) and on the three
	// gate actions (approved / discuss / rejected), and REFUSED on submitted: a
	// code outcome carries a title and a body, never a reason.
	Reason string `json:"reason,omitempty"`
	// The gate fields. Present only on action=approved; the handler refuses
	// them on every other action so a code outcome cannot smuggle an approval.
	ApprovingMaintainer string                            `json:"approvingMaintainer,omitempty"`
	PlanNoteID          string                            `json:"planNoteId,omitempty"`
	ApprovalCitations   []tatarav1alpha1.ApprovalCitation `json:"approvalCitations,omitempty"`
}

// gateGrantedResponse / gateResponse is the 200 body the folded gate returns.
//
// A REFUSAL IS A 200 AND IT DOES NOT PARK. Under the old model the clarify pod
// was dead after its one turn, so parking at identity-unverified was the only
// way to hold the work; under the merged model the agent is still alive and
// should be told no and keep talking. `declared` echoes back the
// approvingMaintainer the agent sent, and the KEY IS ALWAYS PRESENT: an empty
// string means "the agent declared no approver" (the auto-approve path, where
// it legitimately sent none), not "the field never showed up". That is why
// this field carries NO `omitempty` - the field would otherwise be absent
// from the JSON on the auto-approve refusal path, which is undefined rather
// than the defined empty string - so the refusal is self-explaining in the
// pod's transcript either way. The skills instruct agents to branch on
// `reason` ONLY; `declared` is for the human reading the log.
type gateResponse struct {
	Granted bool `json:"granted"`
	// Reason keeps its omitempty. Unlike Declared it was never DEFINED as
	// present-and-empty on any path: this type is constructed only inside
	// refuseGate, and every call site there passes a non-empty reason
	// constant, so the omitempty is inert today rather than hiding a real
	// undefined state - there is no path that needs "reason absent" to mean
	// something distinct from "reason empty".
	Reason   string `json:"reason,omitempty"`
	Declared string `json:"declared"`
}

type reviewedSHA struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
	SHA    string `json:"sha"`
}

type reviewFindingPayload struct {
	Repo     string `json:"repo"`
	Number   int    `json:"number"`
	Path     string `json:"path,omitempty"`
	Line     *int   `json:"line,omitempty"`
	Body     string `json:"body"`
	Severity string `json:"severity"`
}

type reviewPayload struct {
	Verdict            string                 `json:"verdict"`
	ChangeSignificance string                 `json:"changeSignificance,omitempty"`
	ReviewedSHAs       []reviewedSHA          `json:"reviewedSHAs"`
	Findings           []reviewFindingPayload `json:"findings,omitempty"`
}

// headMovedResponse is the STRUCTURED, self-healing 409 body the review handler
// returns when a reported head moved (cross-repo contract: tatara-cli keys on
// reason=="head-moved" to render it as a NON-error tool result the agent acts
// on, NOT a hard failure). The field names are load-bearing across repos.
type headMovedResponse struct {
	Reason          string `json:"reason"`
	Repo            string `json:"repo"`
	Number          int    `json:"number"`
	ReviewedSHA     string `json:"reviewedSHA"`
	LiveSHA         string `json:"liveSHA"`
	MirrorRefreshed bool   `json:"mirrorRefreshed"`
	Message         string `json:"message"`
}

type proposalPayload struct {
	Repo  string `json:"repo"`
	Title string `json:"title"`
	Body  string `json:"body"`
	Kind  string `json:"kind"`
}

type brainstormPayload struct {
	Action    string            `json:"action"`
	Proposals []proposalPayload `json:"proposals,omitempty"`
	Reason    string            `json:"reason,omitempty"`
}

type incidentIssue struct {
	Repo   string          `json:"repo"`
	Title  string          `json:"title"`
	Body   string          `json:"body"`
	Parent *incidentParent `json:"parent,omitempty"`
}

// incidentParent identifies the open tracker a genuinely-new-but-related
// incident issue links itself under as a GitHub sub-issue (B2/B3). Repo is a
// Repository CR name in this project, same convention as incidentIssue.Repo.
type incidentParent struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
}

// incidentComment is the target+body of an action=comment_issue outcome: fresh
// evidence appended to an EXISTING open incident tracker instead of filing a
// near-duplicate. Repo is a Repository CR name in this project. The operator
// gates it to Issue CRs carrying an incident rule/group label, so an incident
// agent can only comment on a TRACKER, never an arbitrary human issue.
type incidentComment struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
	Body   string `json:"body"`
}

type incidentPayload struct {
	Action     string           `json:"action"`
	AlertRules []string         `json:"alertRules"`
	Issue      *incidentIssue   `json:"issue,omitempty"`
	Comment    *incidentComment `json:"comment,omitempty"`
	Reason     string           `json:"reason"`
}

type foldRef struct {
	Task string `json:"task"`
}

type closeRef struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
	Reason string `json:"reason"`
}

type linkRef struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
	IsPR   bool   `json:"isPR,omitempty"`
}

type refinePayload struct {
	Folds  []foldRef  `json:"folds,omitempty"`
	Closes []closeRef `json:"closes,omitempty"`
	Links  []linkRef  `json:"links,omitempty"`
}

// decodeStrict is the second decode stage: DisallowUnknownFields over the raw
// payload bytes.
func decodeStrict(raw []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	return nil
}

// postOutcome is POST /tasks/{t}/outcome: the ONE terminal signal (C.2.7).
//
// IT MAKES NO FORGE WRITE. The single forge call it can make is a READ
// (GetPRHead, kind=review), to verify the SHA the agent says it reviewed is
// still the live head. The SCM review itself is PERSISTED AS INTENT
// (mr.status.pendingReview) and posted by the MergeRequest RECONCILER (C.5.3).
func (s *Server) postOutcome(w http.ResponseWriter, r *http.Request) {
	if !authorizeCaller(w, r) {
		return
	}
	// BOUND THE HANDLER BEFORE THE CLAIM. The claim below is a LEASE with a TTL,
	// and the lease is only sound while a handler cannot outlive its own claim:
	// past the TTL an identical retry treats a claim as an ORPHANED STUB and
	// re-runs every side effect. Nothing else bounds this handler - no
	// WriteTimeout in the request path - and the brainstorm path loops CreateIssue
	// per proposal at ~30s each. OutcomeHandlerBudget < OutcomeClaimTTL, so this
	// deadline is what makes the lease provably safe. r is re-bound so the kind
	// handlers, which pull the request's own context, cannot bypass it.
	ctx, cancel := context.WithTimeout(r.Context(), tatarav1alpha1.OutcomeHandlerBudget)
	defer cancel()
	r = r.WithContext(ctx)
	name := chi.URLParam(r, "t")

	var env outcomeEnvelope
	if err := decodeJSON(r, w, &env); err != nil {
		s.writeDecodeError(w, r, err)
		return
	}
	if env.Kind == "" {
		writeError(w, http.StatusBadRequest, "kind required")
		return
	}
	if len(env.Payload) == 0 {
		writeError(w, http.StatusBadRequest, "payload required")
		return
	}

	// IDEMPOTENCY FIRST, but as a READ-ONLY PEEK (issue #578). The replay and
	// in-flight verdicts have to be reached before anything else runs - a
	// TTL-stopped pod's retry of a COMMITTED outcome must not 409 the Task into
	// failure - but reaching them costs a Get, not a write.
	//
	// THE CLAIM WRITE IS LAZY AND LIVES IN o.claim(). It used to be taken HERE,
	// above the terminal/parked/kind gates, above the payload decode and above
	// every kind handler's read-only validation block - so EVERY 4xx this
	// endpoint can produce mutated status.conditions first and failed second.
	// `release()` undid it, which made the NET effect nil, but the write was
	// observable: an agent that task_get'd between the claim and the release saw
	// OutcomeAccepted=True for an outcome it had just been told was refused,
	// concluded the write had landed, and (with the wrapper's mandatory retry
	// directive on top) re-submitted until maxPodRecreations was spent. release()
	// is also best-effort, so a failed one leaves the bogus claim until the TTL.
	//
	// The claim-first ordering's own contract is "claimed atomically before any
	// forge/child-mint SIDE EFFECT (C7)". Validation is not a side effect, so the
	// claim does not have to precede it - only the execution phase. Concurrency
	// is unchanged: two identical POSTs both peek free, both validate (idempotent,
	// no writes), and the atomic CAS inside o.claim() still lets exactly one win;
	// the loser re-Gets a fresh bare claim inside the TTL and is told to retry.
	fp := outcomeFingerprint(env.Kind, env.Payload)
	key := types.NamespacedName{Namespace: s.ns, Name: name}
	task, state, err := peekOutcomeClaim(ctx, s.c, key, fp, s.now())
	if err != nil {
		writeClientErr(w, err)
		return
	}
	switch state {
	case claimCommitted:
		s.log.InfoContext(ctx, "restapi: outcome replay accepted as a no-op",
			append(reqLogFields(r), "action", "submit_outcome", "task", task.Name, "kind", env.Kind)...)
		writeJSON(w, http.StatusOK, toTaskDTO(*task))
		return
	case claimInFlight:
		obs.RestOutcomeRejectedTotal.WithLabelValues(env.Kind, "claim-in-flight").Inc()
		s.log.InfoContext(ctx, "restapi: an identical outcome is in flight on another replica; asking the caller to retry",
			append(reqLogFields(r), "action", "submit_outcome", "task", task.Name, "kind", env.Kind)...)
		writeError(w, http.StatusConflict, "outcome in flight, retry")
		return
	}

	// oc is built BEFORE the two gates. They run before any kind handler and
	// stamp nothing, and now they also run before the claim, so they write
	// NOTHING at all. oc.proj is nil until the lookup below; neither gate reads it.
	oc := &outcomeCtx{s: s, w: w, r: r, task: task, fp: fp, kind: env.Kind}
	if tatarav1alpha1.TaskDone(task) {
		oc.conflict("task is done", "terminal-stage")
		return
	}
	// A PARKED Task runs no pod, so an outcome submitted against one is from a
	// pod the park was supposed to have taken down. Refuse it: applying it would
	// move a Task the operator has already stalled.
	if tatarav1alpha1.Parked(task) {
		oc.conflict("task is parked", "task-parked")
		return
	}
	// The pod's claim is not trusted: kind MUST equal status.agentKind.
	if env.Kind != task.Status.AgentKind {
		oc.conflict("kind does not match the task's agent kind", "kind-mismatch")
		return
	}

	proj, err := s.getProjectCR(ctx, task.Spec.ProjectRef)
	if err != nil {
		writeClientErr(w, err)
		return
	}
	oc.proj = proj

	switch env.Kind {
	case "implement":
		var p implementPayload
		if !oc.decode(env.Payload, &p) {
			return
		}
		// THE FOLD. `clarify` was its own kind with its own payload until #521;
		// its three decisions are action values here now, and the routing is the
		// only thing that tells the gate half from the code half apart.
		switch p.Action {
		case "approved", "discuss", "rejected":
			oc.gate(p)
		default:
			oc.implement(p)
		}
	case "documentation", "upgrade":
		var p implementPayload
		if !oc.decode(env.Payload, &p) {
			return
		}
		// NEITHER KIND HAS A GATE TO DRIVE - each writes a change and opens a
		// merge request - so neither ever reaches oc.gate, and tatara-cli gives
		// both the SAME schema whose action enum is submitted|declined only
		// (documentationOutcomeSchema, reused verbatim for upgrade). This arm is
		// the operator-side half of that split.
		//
		// It is also why an upgrade Task is minted STRAIGHT into
		// under-implementation rather than triaged to refined: refined's only
		// exit into under-implementation is submit_outcome(action=approved), and
		// no such action exists on this schema.
		oc.implement(p)
	case "review":
		var p reviewPayload
		if !oc.decode(env.Payload, &p) {
			return
		}
		oc.review(p)
	case "brainstorm":
		var p brainstormPayload
		if !oc.decode(env.Payload, &p) {
			return
		}
		oc.brainstorm(p)
	case "incident":
		var p incidentPayload
		if !oc.decode(env.Payload, &p) {
			return
		}
		oc.incident(p)
	case "refine":
		var p refinePayload
		if !oc.decode(env.Payload, &p) {
			return
		}
		oc.refine(p)
	default:
		// Unreachable behind the kind-mismatch gate unless status.agentKind is
		// itself bogus, but it is still a class-B rejection holding a claim.
		oc.release()
		writeError(w, http.StatusBadRequest, "unknown outcome kind")
	}
}

func outcomeFingerprint(kind string, payload []byte) string {
	// Re-marshal through a generic value so whitespace and key order in the
	// request body cannot change the fingerprint of an identical outcome.
	var v any
	if err := json.Unmarshal(payload, &v); err != nil {
		return fmt.Sprintf("%x", sha256Sum(kind+"|"+string(payload)))
	}
	canon, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%x", sha256Sum(kind+"|"+string(payload)))
	}
	return fmt.Sprintf("%x", sha256Sum(kind+"|"+string(canon)))
}

// outcomeClaimState is claimOutcomeFingerprint's three-state verdict. The
// distinction it draws already existed in etcd and nothing read it: the replay
// site matched on the fingerprint alone, so a BARE CLAIM left behind by a
// validation 4xx or a crash was indistinguishable from a COMPLETED outcome, and
// every identical retry got 200-and-do-nothing forever.
type outcomeClaimState int

const (
	// claimWon: we stamped the fingerprint on THIS Status().Update (or re-claimed
	// an orphaned stub). Proceed to validation and commit.
	claimWon outcomeClaimState = iota
	// claimCommitted: a kind handler's commit already overwrote the claim's
	// Reason. Genuinely finished; replay 200 with the unchanged Task.
	claimCommitted
	// claimInFlight: a BARE claim younger than OutcomeClaimTTL. Another replica is
	// between its claim and its commit. 409 "retry"; admitting this through would
	// run the side effects twice.
	claimInFlight
)

// classifyOutcomeClaim decides which of the three states t's OutcomeAccepted
// condition represents for fingerprint fp. It is the ONE definition of that
// verdict, shared by the read-only peek and the atomic claim so the two can
// never drift.
//
// A condition for a DIFFERENT fingerprint, or none at all, is claimWon - as is
// an ORPHANED STUB (a bare claim older than OutcomeClaimTTL, left by a process
// that died between its claim and its commit).
func classifyOutcomeClaim(t *tatarav1alpha1.Task, fp string, now time.Time) outcomeClaimState {
	cond := tatarav1alpha1.OutcomeCondition(t)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Message != fp {
		return claimWon
	}
	if cond.Reason != tatarav1alpha1.OutcomeReasonClaimed {
		return claimCommitted
	}
	if now.Sub(cond.LastTransitionTime.Time) < tatarav1alpha1.OutcomeClaimTTL {
		return claimInFlight
	}
	return claimWon
}

// peekOutcomeClaim READS the Task and classifies the claim, WRITING NOTHING
// (issue #578). It is what runs at the top of postOutcome so that the replay
// and in-flight verdicts are still reached before anything else, without the
// endpoint mutating status.conditions for a request it is about to 4xx.
func peekOutcomeClaim(ctx context.Context, c client.Client, key types.NamespacedName,
	fp string, now time.Time) (*tatarav1alpha1.Task, outcomeClaimState, error) {
	fresh := &tatarav1alpha1.Task{}
	if err := c.Get(ctx, key, fresh); err != nil {
		return nil, claimWon, err
	}
	return fresh, classifyOutcomeClaim(fresh, fp, now), nil
}

// claimOutcomeFingerprint atomically claims fp against a FRESH re-read of the
// Task, before any forge/child-mint side effect (C7), and reports which of the
// three states it found.
//
// THE CLAIM MUST PRECEDE EVERY SIDE EFFECT AND MUST NOT PRECEDE VALIDATION
// (issue #578). The handler runs on every replica, so a stale read of a
// stamped-only-at-commit fingerprint admits two concurrent identical POSTs
// straight through to the same forge write / child-mint / ReviewRounds
// increment - hence the atomic CAS here. Optimistic concurrency lets exactly one
// of two concurrent identical POSTs win the Update; the loser reads back a fresh
// bare claim and is told to RETRY (409) rather than being answered 200 with
// nothing done.
//
// What it may NOT do is run above the validation, which is what it did until
// #578: validation performs no side effect, so nothing is protected by claiming
// ahead of it, and every 4xx below became a mutate-then-fail. Callers reach this
// through outcomeCtx.claim(), at the boundary between a handler's read-only
// validation block and its execution phase.
//
// The claim is a LEASE, not a tombstone: Reason carries claimed-vs-committed and
// LastTransitionTime carries the expiry, both on fields that already existed.
func claimOutcomeFingerprint(ctx context.Context, c client.Client, key types.NamespacedName,
	fp string, now time.Time) (*tatarav1alpha1.Task, outcomeClaimState, error) {
	fresh := &tatarav1alpha1.Task{}
	state := claimWon
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		if state = classifyOutcomeClaim(fresh, fp, now); state != claimWon {
			return nil
		}
		// Free, or an ORPHANED STUB we RE-CLAIM by refreshing LastTransitionTime.
		// Two replicas racing the re-claim is safe: one wins the Update, the other
		// conflicts, re-Gets, sees a claim younger than the TTL and answers
		// claimInFlight.
		setCondition(fresh, metav1.Condition{
			Type:               outcomeAcceptedCondition,
			Status:             metav1.ConditionTrue,
			Reason:             conditionReason(""),
			Message:            fp,
			LastTransitionTime: metav1.NewTime(now),
		})
		return c.Status().Update(ctx, fresh)
	})
	return fresh, state, err
}

// outcomeCtx carries the per-request state every payload handler needs.
type outcomeCtx struct {
	s    *Server
	w    http.ResponseWriter
	r    *http.Request
	task *tatarav1alpha1.Task
	proj *tatarav1alpha1.Project
	fp   string
	kind string
	// claimed records whether THIS request holds the fingerprint claim. It is
	// false through the whole read-only validation phase (#578), which is what
	// makes every 4xx this endpoint produces a pure no-write rejection, and what
	// makes release() a no-op on those paths - there is nothing to release.
	claimed bool
}

// claim takes the atomic fingerprint claim at the boundary between a handler's
// read-only validation block and its execution phase, and reports whether the
// caller may proceed. It is idempotent within a request.
//
// EVERY SIDE EFFECT MUST SIT BEHIND IT: forge writes, child mints, mirror
// writes, spec writes, and commit (which calls it as a backstop, so no execution
// path can commit unclaimed). A false return means the response has ALREADY been
// written - a replay 200 or an in-flight 409 that only became visible now,
// between the peek and here - and the caller must return immediately.
func (o *outcomeCtx) claim() bool {
	if o.claimed {
		return true
	}
	ctx := o.r.Context()
	key := types.NamespacedName{Namespace: o.s.ns, Name: o.task.Name}
	fresh, state, err := claimOutcomeFingerprint(ctx, o.s.c, key, o.fp, o.s.now())
	if err != nil {
		writeClientErr(o.w, err)
		return false
	}
	switch state {
	case claimCommitted:
		o.s.log.InfoContext(ctx, "restapi: outcome replay accepted as a no-op",
			append(reqLogFields(o.r), "action", "submit_outcome", "task", o.task.Name, "kind", o.kind)...)
		writeJSON(o.w, http.StatusOK, toTaskDTO(*fresh))
		return false
	case claimInFlight:
		obs.RestOutcomeRejectedTotal.WithLabelValues(o.kind, "claim-in-flight").Inc()
		o.s.log.InfoContext(ctx, "restapi: an identical outcome is in flight on another replica; asking the caller to retry",
			append(reqLogFields(o.r), "action", "submit_outcome", "task", o.task.Name, "kind", o.kind)...)
		writeError(o.w, http.StatusConflict, "outcome in flight, retry")
		return false
	}
	o.claimed = true
	return true
}

// release drops OUR claim so an identical retry RE-VALIDATES immediately
// instead of waiting out OutcomeClaimTTL.
//
// Every pre-execution rejection is CLASS B: it runs before any committed
// effect, so nothing may be cached under the fingerprint. A class-A
// (post-execution) rejection does not arise here - commit is the only thing
// that begins execution, and it stamps its own terminal reason, which release
// refuses to touch.
//
// OWNERSHIP-CHECKED under CAS: only a condition still carrying OUR fingerprint
// AND Reason "Outcome" is released. NEVER a committed condition; NEVER another
// request's claim - a slow handler can reach its rejection long after another
// replica re-claimed the orphaned slot and committed it.
//
// WHY those two fields PROVE ownership. Reason rules out a COMMITTED condition
// (commit stamps the kind's Reason, never "Outcome"). The fingerprint rules out
// a DIFFERENT request's claim. What is left - another replica's claim on the
// SAME fingerprint - is excluded by the HANDLER-BUDGET INVARIANT:
//
//	OutcomeHandlerBudget (3m) < OutcomeClaimTTL (5m)
//
// A re-claim of a live claim only happens once the claim reads as an orphaned
// stub, i.e. older than the TTL. Our handler is hard-bounded below the TTL, so
// it cannot still be running then: no handler outlives its own claim, so any
// claim we still see carrying our fingerprint IS ours. Break that invariant and
// the two-field check stops being sufficient - a slow handler could release the
// claim of the replica that re-claimed and is actively working the same
// fingerprint.
//
// A failed release is not fatal and does not change the response: the claim
// then expires as an orphaned stub after the TTL, which is the same self-heal a
// crashed process gets.
func (o *outcomeCtx) release() {
	// NOTHING TO RELEASE ON THE VALIDATION PATHS (#578). The claim is taken
	// lazily, at the validation/execution boundary, so a rejection that fires
	// before it never wrote anything - and must not go anywhere near the
	// condition, which at that point can only be some OTHER request's.
	if !o.claimed {
		return
	}
	o.claimed = false
	ctx := o.r.Context()
	key := types.NamespacedName{Namespace: o.s.ns, Name: o.task.Name}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := o.s.c.Get(ctx, key, fresh); err != nil {
			return err
		}
		cond := tatarav1alpha1.OutcomeCondition(fresh)
		if cond == nil || cond.Message != o.fp || cond.Reason != tatarav1alpha1.OutcomeReasonClaimed {
			return nil
		}
		meta.RemoveStatusCondition(&fresh.Status.Conditions, outcomeAcceptedCondition)
		return o.s.c.Status().Update(ctx, fresh)
	})
	if err != nil {
		o.s.log.ErrorContext(ctx, "restapi: releasing the outcome claim failed; the retry waits out the claim TTL",
			append(reqLogFields(o.r), "task", o.task.Name, "kind", o.kind, "error", err)...)
		return
	}
	o.s.log.InfoContext(ctx, "restapi: outcome rejected before execution; the claim is released for an immediate retry",
		append(reqLogFields(o.r), "action", "submit_outcome", "task", o.task.Name, "kind", o.kind)...)
}

func (o *outcomeCtx) bad(msg string, reason string) {
	o.release()
	obs.RestOutcomeRejectedTotal.WithLabelValues(o.kind, reason).Inc()
	writeError(o.w, http.StatusBadRequest, msg)
}

func (o *outcomeCtx) conflict(msg string, reason string) {
	o.release()
	obs.RestOutcomeRejectedTotal.WithLabelValues(o.kind, reason).Inc()
	writeError(o.w, http.StatusConflict, msg)
}

// decode parses the kind's payload, releasing the claim on a malformed one: a
// decode failure is as class B as any other validation failure, and leaving the
// claim would 409 the corrected resubmit for the whole TTL.
func (o *outcomeCtx) decode(payload []byte, v any) bool {
	if err := decodeStrict(payload, v); err != nil {
		o.release()
		o.s.writeDecodeError(o.w, o.r, err)
		return false
	}
	return true
}

// commit applies the Task status mutation (a stage transition, notes, counters)
// AND stamps the idempotency condition in ONE status write.
//
// It is the REST layer's half of the D1 emit. An /outcome is how a Task reaches
// parked(implement-declined), parked(awaiting-human), parked(identity-unverified),
// rejected(declined), rejected(false-positive) and delivered - i.e. most of the
// terminal outcomes the platform ever produces - and not one of them was counted
// before. The counter fires ONCE, AFTER the write lands: objbudget.FitTask re-runs
// the closure to size the write and again on every conflict retry, so an emit
// inside it would be inflated 2-3x.
func (o *outcomeCtx) commit(mutate func(*tatarav1alpha1.Task) error) bool {
	// THE CLAIM BACKSTOP (#578). commit is the one thing every kind handler's
	// execution phase ends in, so claiming here guarantees no outcome is ever
	// committed unclaimed even if a handler forgot the explicit claim before its
	// first forge write. Idempotent: a handler that already claimed pays nothing.
	if !o.claim() {
		return false
	}
	ctx := o.r.Context()
	s := o.s
	key := types.NamespacedName{Namespace: s.ns, Name: o.task.Name}
	var mutErr error
	from := o.task.Status.State
	var to, toReason, toPark string
	err := objbudget.FitTask(ctx, s.c, s.spillerForOrNil(o.proj), key, func(t *tatarav1alpha1.Task) {
		if mutate != nil {
			if err := mutate(t); err != nil {
				mutErr = err
				return
			}
		}
		to, toReason, toPark = t.Status.State, t.Status.StateReason, t.Status.ParkReason
		setCondition(t, metav1.Condition{
			Type:               outcomeAcceptedCondition,
			Status:             metav1.ConditionTrue,
			Reason:             conditionReason(o.kind),
			Message:            o.fp,
			LastTransitionTime: metav1.NewTime(s.now()),
		})
	})
	if mutErr != nil {
		var ill *stage.IllegalTransitionError
		if errors.As(mutErr, &ill) {
			s.log.ErrorContext(ctx, "restapi: outcome asked for an illegal stage transition",
				append(reqLogFields(o.r), "task", o.task.Name, "from", ill.From, "to", ill.To)...)
			// RELEASING IS SAFE HERE even though commit runs AFTER non-idempotent
			// forge writes (brainstorm propose CreateIssue, incident file_issue
			// CreateIssue), because this branch is unreachable for a retry that
			// could duplicate them:
			//
			// Enter sets AgentKind = AgentKindFor(to), so agentKind is a pure
			// function of stage, and each kind handler only ever runs on the unique
			// stage that maps to its kind - where the edge it requests is always in
			// the F.3 table. An illegal transition therefore means the stage MOVED
			// between the gate's read and commit's fresh Get. Every concurrent
			// operator-driven exit from a pod stage lands on parked/failed
			// (terminal) or delivered, and the retry's own terminal/delivered gate
			// refuses all of those before the handler - hence its forge writes - can
			// run again.
			o.conflict(mutErr.Error(), "illegal-transition")
			return false
		}
		writeError(o.w, http.StatusInternalServerError, "internal error")
		s.log.ErrorContext(ctx, "restapi: outcome mutation failed",
			append(reqLogFields(o.r), "task", o.task.Name, "error", mutErr)...)
		return false
	}
	if errors.Is(err, objbudget.ErrObjectTooLarge) {
		obs.RestOutcomeRejectedTotal.WithLabelValues(o.kind, stage.ReasonObjectTooLarge).Inc()
		if perr := objbudget.MinimalFailPatch(ctx, s.c, o.task, stage.ReasonObjectTooLarge); perr != nil {
			s.log.ErrorContext(ctx, "restapi: minimal fail patch failed",
				append(reqLogFields(o.r), "task", o.task.Name, "error", perr)...)
		}
		writeError(o.w, http.StatusInsufficientStorage, "task exceeds the byte budget")
		return false
	}
	if err != nil {
		writeClientErr(o.w, err)
		return false
	}
	s.metrics.TaskTerminalEntry(o.task.Spec.Kind, from, to, toReason)
	// The same choke-point gap D1 closed for operator_task_terminal_total:
	// this handler's stage.Park calls (awaiting-human, identity-unverified,
	// implement-declined, ...) never route through controller.ParkTask, the
	// only other place TaskParked() is called - so an outcome-driven first
	// park undercounted operator_task_parked_total (metric-wiring audit, #370).
	if toPark != "" && o.task.Status.ParkReason == "" {
		s.metrics.TaskParked(from, toPark)
	}
	if to != from {
		s.log.InfoContext(ctx, "state transition",
			append(reqLogFields(o.r), "action", "stage_transition", "task", o.task.Name,
				"from", from, "to", to, "state_reason", toReason)...)
	}
	// THE PARK TAKES THE POD DOWN. controller.ParkTask does this for every
	// operator-side park; an outcome-driven one has to do it here, because this
	// handler never routes through that choke point. A parked Task holding a live
	// pod burns an admission slot with no clock armed.
	if toPark != "" && o.task.Status.ParkReason == "" {
		if derr := agent.DeleteWrapper(ctx, s.c, s.ns, o.task); derr != nil {
			s.log.ErrorContext(ctx, "restapi: park landed but the agent pod delete failed; the task reconciler repairs it",
				append(reqLogFields(o.r), "task", o.task.Name, "error", derr)...)
		}
	}
	o.task.Status.State, o.task.Status.StateReason, o.task.Status.ParkReason = to, toReason, toPark
	return true
}

// conditionReason is a CamelCase k8s condition reason. The empty kind is the
// bare CLAIM, and that Reason is what tells a committed outcome apart from a
// claimed one everywhere else in the operator - hence the shared definition.
func conditionReason(kind string) string {
	return tatarav1alpha1.OutcomeReasonFor(kind)
}

// setCondition upserts c by Type as a WHOLE-STRUCT OVERWRITE.
//
// DO NOT "TIDY" THIS INTO meta.SetStatusCondition. The overwrite is LOAD-BEARING
// for the outcome claim's LEASE: LastTransitionTime carries the lease expiry, and
// claimOutcomeFingerprint's re-claim of an ORPHANED STUB refreshes it by writing a
// whole new condition. meta.SetStatusCondition only re-stamps LastTransitionTime
// when Status CHANGES, and a re-claim goes True -> True, so it would leave the
// orphan's expired stamp in place - minting a lease born already expired. The next
// identical retry, a second later, would then read that claim as orphaned in turn,
// re-claim it, and run every side effect AGAIN. No race and no second replica
// needed: that duplicate is reachable on a single-version, single-pod cluster.
//
// TestOutcome_ReclaimOfAnOrphanedStubRefreshesTheLeaseClock pins the refresh.
func setCondition(t *tatarav1alpha1.Task, c metav1.Condition) {
	for i := range t.Status.Conditions {
		if t.Status.Conditions[i].Type == c.Type {
			t.Status.Conditions[i] = c
			return
		}
	}
	t.Status.Conditions = append(t.Status.Conditions, c)
}

// mrTerminalStates reports whether the Task owns >= 1 MR and EVERY owned MR is
// terminal (state not in openMRs' open set), plus the per-MR states for logging.
// An empty slice is NOT terminal.
func mrTerminalStates(mrs []tatarav1alpha1.MergeRequest) (states []string, allTerminal bool) {
	if len(mrs) == 0 {
		return nil, false
	}
	for i := range mrs {
		states = append(states, mrs[i].Status.State)
	}
	return states, len(openMRs(mrs)) == 0
}

// terminalNoop answers a submit_outcome whose MRs already went terminal with an
// explicit 2xx no-op (never a silent success: the body and the log both name the
// discard). Nothing is committed and, since #578, nothing was ever claimed, so
// an identical retry re-validates and no-ops again.
//
// IT IS A 2xx BECAUSE THE PRECONDITION IS ABOUT THE FORGE, NOT THE PAYLOAD
// (#578). "Every MR you own already merged or closed" is not something the agent
// can correct and resubmit: the identical request can never succeed, so a 4xx is
// a category error - it says "you sent something wrong, fix it and retry" when
// the truthful answer is "there is nothing left to attach this to, stop". The
// wrapper's outcome re-prompt reads an is_error tool result and re-issues a
// MANDATORY retry directive, which is how the doomed 400 became a
// pod-recreation loop rather than one wasted call.
//
// THE ENDPOINT DOES NOT FINALIZE - the CONVERGENT RECONCILER-SIDE finalize does
// (terminalMREdge / ownMRsShippedEdge in internal/controller/reviewpost.go,
// wired at reconcileClocks and the pre-dispatch guard). This only stops the pod
// re-submitting; the Task retires on its own next pass.
func (o *outcomeCtx) terminalNoop(states []string) {
	o.release()
	ctx := o.r.Context()
	obs.RestOutcomeAcceptedTotal.WithLabelValues(o.kind, "mr-terminal-noop").Inc()
	o.s.log.InfoContext(ctx, "restapi: submit_outcome no-op: kind=review task owns only terminal MRs; the review target already merged/closed",
		append(reqLogFields(o.r), "action", "submit_outcome_noop", "task", o.task.Name,
			"resource_id", o.task.Name, "kind", o.kind, "mr_states", strings.Join(states, ","))...)
	writeJSON(o.w, http.StatusOK, map[string]any{"noop": true, "reason": "mr-terminal"})
}

// takenOverNoop answers a review submit_outcome whose review target a maintainer
// took over (the parent review Task controller-owns zero MRs, its refs now owned
// by a takeover Task) with an explicit 2xx no-op, mirroring terminalNoop: the
// in-flight agent turn ends cleanly instead of hitting the doomed 400 that
// respawn-looped the pod. The convergent reconciler-side finalize
// (rejected(mr-taken-over)) is what actually retires the Task; this only stops
// the pod re-submitting. The claim is released like any pre-execution class-B
// path so an identical retry re-validates and no-ops again.
func (o *outcomeCtx) takenOverNoop() {
	o.release()
	ctx := o.r.Context()
	obs.RestOutcomeAcceptedTotal.WithLabelValues(o.kind, "mr-taken-over-noop").Inc()
	o.s.log.InfoContext(ctx, "restapi: submit_outcome no-op: kind=review task owns zero MRs; its review target was taken over by a maintainer",
		append(reqLogFields(o.r), "action", "submit_outcome_noop", "task", o.task.Name,
			"resource_id", o.task.Name, "kind", o.kind)...)
	writeJSON(o.w, http.StatusOK, map[string]any{"noop": true, "reason": "mr-taken-over"})
}

// ok writes the accepted 200 with the fresh Task.
func (o *outcomeCtx) ok(action string, fields ...any) {
	ctx := o.r.Context()
	fresh, err := o.s.getTaskCR(ctx, o.task.Name)
	if err != nil {
		writeClientErr(o.w, err)
		return
	}
	obs.RestOutcomeAcceptedTotal.WithLabelValues(o.kind, action).Inc()
	o.s.log.InfoContext(ctx, "restapi: outcome accepted",
		append(append(reqLogFields(o.r), "action", "submit_outcome", "task", o.task.Name,
			"kind", o.kind, "outcome", action, "state", fresh.Status.State), fields...)...)
	writeJSON(o.w, http.StatusOK, toTaskDTO(*fresh))
}

// note records an agent-authored note in the same status write as the
// transition. The writer is ALWAYS status.agentKind: an agent can never produce
// agent="operator".
func agentNote(t *tatarav1alpha1.Task, agent, kind, body string, now time.Time) {
	t.Status.Notes = append(t.Status.Notes, tatarav1alpha1.Note{
		At: metav1.NewTime(now), Agent: agent, Kind: kind,
		Body: truncateValidUTF8(body, noteBodyMaxBytes),
	})
}

// --- implement / documentation --------------------------------------------

func (o *outcomeCtx) implement(p implementPayload) {
	ctx := o.r.Context()
	s := o.s

	// THE SINGLE `reason` FIELD, WITH PER-ACTION LEGALITY. The frozen wire
	// contract has ONE reason key: tatara-cli maps the agent's snake_case
	// `decline_reason` onto it and the gate's own `reason` argument onto the same
	// key, and made exactly one of the two legal per action. So the key's meaning
	// is decided by `action`, and this is where the operator decides it too -
	// tatara-cli is not the trust boundary and an old or hand-rolled client can
	// send anything.
	//
	// `submitted` REFUSES it: a code outcome is a title, a body and a
	// significance, and a reason riding one is a client that thinks it is
	// declining. `declined` REQUIRES it: a decline with no reason terminates the
	// Task with nothing on the thread explaining why. The three gate actions
	// require it in o.gate.
	switch p.Action {
	case "submitted":
		if strings.TrimSpace(p.Title) == "" || strings.TrimSpace(p.Body) == "" ||
			strings.TrimSpace(p.ChangeSignificance) == "" {
			o.bad("action=submitted requires title, body and changeSignificance", "missing-field")
			return
		}
		if !validChangeSignificance[p.ChangeSignificance] {
			o.bad("changeSignificance must be one of major, minor, patch", "bad-significance")
			return
		}
		if p.Reason != "" {
			o.bad("reason is only for action=declined, approved, discuss or rejected", "unexpected-field")
			return
		}
	case "declined":
		if strings.TrimSpace(p.Reason) == "" {
			o.bad("action=declined requires a non-empty reason", "missing-field")
			return
		}
	default:
		o.bad("action must be one of submitted, declined", "bad-action")
		return
	}
	// NO GATE FIELD MAY RIDE A CODE OUTCOME. Without this a submitted payload
	// could carry an approval nobody evaluated.
	if p.ApprovingMaintainer != "" || p.PlanNoteID != "" || len(p.ApprovalCitations) > 0 {
		o.bad("approvingMaintainer, planNoteId and approvalCitations are only valid when action=approved", "unexpected-field")
		return
	}

	mrs, err := s.ownedMRs(ctx, o.task)
	if err != nil {
		writeClientErr(o.w, err)
		return
	}

	// THE PLAN PIN, RE-CHECKED BEFORE CODE SHIPS. The gate hashed the plan note
	// at grant; if the agent rewrote it afterwards, the change it is submitting
	// is not the change that was approved.
	//
	// THE CHEAP PATH OUT IS `refined`, NEVER A PARK. An agent that finds the plan
	// gate expensive will simply stop updating its plan note, which destroys the
	// note's value as continuation state - so a mismatch sends it back to the
	// gate to ask again, with its pod alive and its work intact.
	if p.Action == "submitted" && o.kind == "implement" {
		if refused := s.planPinRefusal(ctx, o.task); refused {
			if !o.commit(func(t *tatarav1alpha1.Task) error {
				return stage.Enter(t, mrs, tatarav1alpha1.StateRefined, "", s.now())
			}) {
				return
			}
			o.refuseGate(controller.ApprovalRefusedPlanHashMismatch, "")
			return
		}
	}

	if p.Action == "declined" {
		docDecline := o.kind == "documentation"
		if !o.commit(func(t *tatarav1alpha1.Task) error {
			// The mutation FIRST: objbudget.FitTask persists whatever this
			// closure mutated even when it returns an error, so a note appended
			// before a REFUSED transition would land - and, now that a class-B
			// rejection releases the claim, land AGAIN on every retry.
			var err error
			if docDecline {
				// A declined documentation batch is DONE, not parked: there was
				// nothing to document.
				err = stage.Enter(t, mrs, tatarav1alpha1.StateDone, stage.ReasonDocTimeout, s.now())
			} else {
				err = stage.Park(t, stage.ReasonImplementDeclined, s.now())
			}
			if err != nil {
				return err
			}
			agentNote(t, o.kind, "note", "declined: "+p.Reason, s.now())
			return nil
		}) {
			return
		}
		if o.kind == "documentation" {
			if err := s.stampDocumentedBy(ctx, o.proj, o.task); err != nil {
				writeClientErr(o.w, err)
				return
			}
		}
		if !docDecline {
			s.stampDeclineCIEvidence(ctx, o.r, o.task, mrs)
		}
		o.ok("declined")
		return
	}

	// MERGE ORDER RESOLUTION (fix C2). This is what made the COMMON case -
	// one issue, one repo, one MR - unmergeable in v3: mergeOrder was nil, the
	// C.5.2 loop ran ZERO times, and delivered was unreachable.
	open := openMRs(mrs)
	repos := ownedMRRepos(open)
	switch {
	case len(repos) == 0:
		// THE CARVE-OUTS ARE KIND-AGNOSTIC (#578). They were gated on
		// spec.kind=review until a kind=issue Task whose own MR had merged out of
		// band fell straight through to the 400 below and respawn-looped on it.
		// Nothing about "every MR this task owns already reached a terminal forge
		// state" is specific to a review Task - see terminalNoop.
		if states, term := mrTerminalStates(mrs); term {
			o.terminalNoop(states)
			return
		}
		over, err := controller.TaskTakenOver(ctx, s.c, o.task)
		if err != nil {
			writeClientErr(o.w, err)
			return
		}
		if over {
			o.takenOverNoop()
			return
		}
		// STILL A 400, and legitimately so: the Task owns NO MR at all (an empty
		// set is not terminal). That IS an agent error - it said it submitted code
		// and opened nothing - and unlike the terminal case, a retry AFTER opening
		// an MR genuinely succeeds.
		o.bad("action=submitted but this task owns no open MR", "no-open-mr")
		return
	case len(repos) == 1:
		// mergeOrder is OPTIONAL. With one repo there is exactly one order and
		// nothing to get wrong. This is NOT a lexical default.
		if len(p.MergeOrder) == 0 {
			p.MergeOrder = repos
		}
	default:
		// mergeOrder is REQUIRED. There is NO LEXICAL DEFAULT: lexical order is
		// agent-skills < cli < claude-code-wrapper < operator, which merges cli
		// BEFORE operator - precisely the fleet outage this redesign prevents.
		if len(p.MergeOrder) == 0 {
			o.bad("mergeOrder required for a multi-repo change", "merge-order-missing")
			return
		}
	}
	for _, repo := range repos {
		if !contains(p.MergeOrder, repo) {
			o.bad("mergeOrder does not cover repo "+repo, "merge-order-coverage")
			return
		}
	}

	// B1: NO AGENT LEAVES A DIRTY PR. This is the LAST validation step and it is
	// deliberately the last: it is the only one that talks to the forge, so every
	// cheap local refusal above has already had its say. See readiness.go for the
	// three axes, why a read failure fails OPEN, and why a refusal here costs the
	// agent nothing.
	rd, readOK := o.evaluateReadiness(ctx, open, submitScope)
	if readOK && o.refuseNotReady(ctx, rd, "submission") {
		return
	}
	// PENDING IS NOT A VERDICT, so it is not a refusal - but it is not an advance
	// either. Handing a change whose pipeline has not answered to a review pod is
	// how a reviewer spends a full turn reading code that the next check_suite
	// delivery is about to invalidate. The outcome is ACCEPTED (the agent's work
	// is done and must not be re-run) and the transition is HELD on
	// status.ciWaitSince, which controller.reconcileCIWait resolves three ways -
	// green, red, or the CIWaitDeadline fail-open.
	ciHold := readOK && anyCIPending(rd)

	// VALIDATION ENDS HERE, EXECUTION BEGINS (#578): every 400 above wrote
	// nothing, and everything below mutates.
	if !o.claim() {
		return
	}

	// changeSignificance is written to EVERY owned MR's status.significance. It
	// is IMPLEMENT-OWNED (fix 12).
	for i := range open {
		mr := &open[i]
		key := types.NamespacedName{Namespace: s.ns, Name: mr.Name}
		if err := objbudget.FitMergeRequest(ctx, s.c, s.spillerForOrNil(o.proj), key, func(m *tatarav1alpha1.MergeRequest) {
			m.Status.Significance = p.ChangeSignificance
		}); err != nil {
			writeClientErr(o.w, err)
			return
		}
	}
	if err := s.updateTaskSpec(ctx, o.task.Name, func(t *tatarav1alpha1.Task) {
		t.Spec.MergeOrder = p.MergeOrder
	}); err != nil {
		writeClientErr(o.w, err)
		return
	}

	if !o.commit(func(t *tatarav1alpha1.Task) error {
		if ciHold {
			// NO stage.Enter, and the Task therefore stays exactly where it is,
			// UN-PARKED. Un-parked is load-bearing twice: TaskReconciler returns
			// early on a parked Task so nothing would ever resolve the hold, and
			// CIRefreshCadence drops a parked MR to the 24h mirror cadence so the
			// missed-webhook backstop would run once a day. See the CIWaitSince
			// field comment.
			stamp := metav1.NewTime(s.now())
			t.Status.CIWaitSince = &stamp
		} else if err := stage.Enter(t, mrs, tatarav1alpha1.StateAwaitingReview, "", s.now()); err != nil {
			return err
		}
		agentNote(t, o.kind, "note", "submitted: "+p.Title+"\n\n"+p.Body, s.now())
		return nil
	}) {
		return
	}

	// Record the LIVE MR head as the last bot-pushed SHA (never trust an
	// agent-reported SHA). This is the machine signal ReconcileOwnership uses to
	// detect a later external push. Best-effort: the stage transition already
	// committed above, so nothing here may touch o.w - projectSCMWriterAndToken
	// writes an HTTP error to whatever ResponseWriter it is given on failure
	// (a live k8s Get for the scm secret, which can transiently fail), and o.w
	// must carry exactly the ONE response o.ok() sends below. A discardWriter
	// throws that side-effect response away; every failure here just skips the
	// stamp - the tiny race with a same-instant human push, or a mirror left
	// stale, settles via the sweep - and is logged at WARN so a stuck cursor is
	// debuggable instead of silently stale.
	if writer, token, ok := s.projectSCMWriterAndToken(&discardWriter{}, o.r, o.proj); ok {
		for i := range open {
			mr := &open[i]
			repo, err := s.repoCR(ctx, o.proj.Name, mr.Spec.RepositoryRef)
			if err != nil {
				s.log.WarnContext(ctx, "restapi: record_bot_head skipped: repository lookup failed",
					"action", "record_bot_head_skip", "reason", "repo_lookup", "task", o.task.Name, "mr", mr.Name, "error", err)
				continue
			}
			live, err := writer.GetPRHead(ctx, repo.Spec.URL, token, mr.Spec.Number)
			if err != nil {
				s.log.WarnContext(ctx, "restapi: record_bot_head skipped: live head read failed",
					"action", "record_bot_head_skip", "reason", "get_pr_head", "task", o.task.Name, "mr", mr.Name, "error", err)
				continue
			}
			if live == "" {
				s.log.WarnContext(ctx, "restapi: record_bot_head skipped: scm returned an empty head",
					"action", "record_bot_head_skip", "reason", "empty_head", "task", o.task.Name, "mr", mr.Name)
				continue
			}
			key := types.NamespacedName{Namespace: s.ns, Name: mr.Name}
			if err := objbudget.FitMergeRequest(ctx, s.c, s.spillerForOrNil(o.proj), key, func(m *tatarav1alpha1.MergeRequest) {
				m.Status.LastBotHeadSHA = live
			}); err != nil {
				obs.MirrorWriteDroppedTotal.WithLabelValues(o.proj.Name, "MergeRequest", "record_bot_head").Inc()
				s.log.WarnContext(ctx, "restapi: record_bot_head skipped: mirror write failed",
					"action", "record_bot_head_skip", "reason", "fit_conflict", "task", o.task.Name, "mr", mr.Name, "error", err)
				continue
			}
			s.log.InfoContext(ctx, "restapi: recorded live bot head at implement accept",
				"action", "record_bot_head", "task", o.task.Name, "mr", mr.Name, "sha", live)
		}
	} else {
		s.log.WarnContext(ctx, "restapi: record_bot_head skipped: scm writer/token resolution failed",
			"action", "record_bot_head_skip", "reason", "scm_resolution", "task", o.task.Name)
	}

	s.editSubmittedMRs(ctx, o, open, p.Title, p.Body)

	o.ok("submitted", "merge_order", strings.Join(p.MergeOrder, ","),
		"change_significance", p.ChangeSignificance, "ci_hold", ciHold)
}

// editSubmittedMRs writes the submitted title and body onto the Task's own merge
// requests on the forge, and refreshes the mirror from what was actually sent.
//
// Until this existed those two fields reached nothing but an internal Task note,
// while tatara-cli's tool schema documented them as "MR title" / "MR body". That
// gap made a whole class of review finding unfixable: `mr_write` exposes
// open/comment/reply and no edit, so an agent asked to correct a stale version
// in a merge request title had no carrier for it at all and could only decline.
//
// EXTERNAL MERGE REQUESTS ARE SKIPPED. An external MR is somebody else's - the
// platform reviews it but never pushes to it - and its title is theirs to write.
// Empty ownership is NOT external: it means "not yet classified", which at
// implement-submit is the ordinary shape of an MR the agent opened this same
// turn (mint leaves the field empty until ReconcileOwnership backfills it), and
// the two paths that hand the platform somebody else's MR - takeover and upgrade
// adoption - both stamp `tatara` explicitly. So `external` is the only value
// that ever means "not ours".
//
// BEST-EFFORT, ON THE SAME CONTRACT AS THE record_bot_head STAMP ABOVE: the
// stage transition has ALREADY committed, so nothing here may touch o.w (hence
// the discardWriter) and no failure here may turn an accepted outcome into a
// 500. A refused edit leaves the forge and the mirror as they were, logged at
// WARN, and the next turn's submit sends the same title again.
func (s *Server) editSubmittedMRs(ctx context.Context, o *outcomeCtx, open []tatarav1alpha1.MergeRequest, rawTitle, body string) {
	// Clamped for the same reason every other forge title write is: a forge caps
	// a merge request title (GitLab at 255 chars, the same Issuable validation an
	// issue gets) and answers 400 on an over-long one, which here would discard
	// the edit silently.
	title := s.clampTitleForForge(ctx, o.r, obs.TitleSiteMREdit, o.task.Name, rawTitle)
	writer, token, ok := s.projectSCMWriterAndToken(&discardWriter{}, o.r, o.proj)
	if !ok {
		s.log.WarnContext(ctx, "restapi: mr_edit skipped: scm writer/token resolution failed",
			"action", "mr_edit_skip", "reason", "scm_resolution", "task", o.task.Name)
		return
	}
	for i := range open {
		mr := &open[i]
		if mr.Status.Ownership == tatarav1alpha1.OwnershipExternal {
			s.log.InfoContext(ctx, "restapi: mr_edit skipped: the merge request is externally owned",
				"action", "mr_edit_skip", "reason", "external", "task", o.task.Name, "mr", mr.Name)
			continue
		}
		repo, err := s.repoCR(ctx, o.proj.Name, mr.Spec.RepositoryRef)
		if err != nil {
			s.log.WarnContext(ctx, "restapi: mr_edit skipped: repository lookup failed",
				"action", "mr_edit_skip", "reason", "repo_lookup", "task", o.task.Name, "mr", mr.Name, "error", err)
			continue
		}
		// Both fields are always sent: `submitted` REJECTS a payload missing
		// either, so there is no reachable title-only or body-only submit. The
		// pointer shape is EditPRReq's contract for callers that do have one.
		editErr := writer.EditPR(ctx, repo.Spec.URL, token, mr.Spec.Number,
			scm.EditPRReq{Title: &title, Body: &body})
		controller.RecordSCM(s.metrics, providerOf(o.proj), "edit_pr", editErr)
		if editErr != nil {
			s.log.WarnContext(ctx, "restapi: mr_edit skipped: forge edit failed",
				"action", "mr_edit_skip", "reason", "edit_pr", "task", o.task.Name, "mr", mr.Name, "error", editErr)
			continue
		}
		// The mirror follows the forge write, and only it. Leaving it stale would
		// keep the OLD title in front of every reader that uses the mirror rather
		// than the forge - the prompt bundle the next turn is built from, and the
		// review pod - which is the same wrong title the reviewer asked to have
		// corrected, still on screen after the fix landed.
		key := types.NamespacedName{Namespace: s.ns, Name: mr.Name}
		if err := objbudget.FitMergeRequest(ctx, s.c, s.spillerForOrNil(o.proj), key, func(m *tatarav1alpha1.MergeRequest) {
			m.Status.Title = title
			m.Status.Body = truncateValidUTF8(body, tatarav1alpha1.MergeRequestBodyMaxBytes)
		}); err != nil {
			obs.MirrorWriteDroppedTotal.WithLabelValues(o.proj.Name, "MergeRequest", "mr_edit").Inc()
			s.log.WarnContext(ctx, "restapi: mr_edit mirror refresh failed",
				"action", "mr_edit_skip", "reason", "fit_conflict", "task", o.task.Name, "mr", mr.Name, "error", err)
			continue
		}
		s.log.InfoContext(ctx, "restapi: merge request title and body written to the forge",
			"action", "mr_edit", "task", o.task.Name, "mr", mr.Name, "number", mr.Spec.Number,
			"title_chars", utf8.RuneCountInString(title), "body_bytes", len(body))
	}
}

// ownedMRRepos is the DEDUPED repo list of the MRs still open, in stable order.
func ownedMRRepos(mrs []tatarav1alpha1.MergeRequest) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(mrs))
	for i := range mrs {
		repo := mrs[i].Spec.RepositoryRef
		if !seen[repo] {
			seen[repo] = true
			out = append(out, repo)
		}
	}
	return out
}

// stampDocumentedBy stamps status.documentedBy on every Task the batch covered
// (F.3: either way, documented or declined). proj is the BATCH's own project -
// docbatch.go's MintDocBatch only ever collects covered tasks with
// Spec.ProjectRef == proj.Name, so every covered task shares it; passed down
// from the caller instead of re-resolved per iteration (that Get would also
// abort the whole loop on failure, unlike the covered-task Get above which
// tolerates NotFound).
func (s *Server) stampDocumentedBy(ctx context.Context, proj *tatarav1alpha1.Project, batch *tatarav1alpha1.Task) error {
	spiller := s.spillerForOrNil(proj)
	for _, name := range batch.Spec.DocumentsTasks {
		key := types.NamespacedName{Namespace: s.ns, Name: name}
		var covered tatarav1alpha1.Task
		if err := s.c.Get(ctx, key, &covered); err != nil {
			if client.IgnoreNotFound(err) == nil {
				continue
			}
			return err
		}
		if err := objbudget.FitTask(ctx, s.c, spiller, key, func(t *tatarav1alpha1.Task) {
			t.Status.DocumentedBy = batch.Name
		}); err != nil {
			return err
		}
	}
	return nil
}

// stampDeclineCIEvidence records WHAT CI SAID at the instant the agent gave up,
// and WHICH CODE it said it about.
//
// A decline is one of two incompatible things: a verdict on the CHANGE ("this
// bump is wrong, superseded, unwanted"), which must stay permanent, or a verdict
// on the INFRASTRUCTURE ("I could not submit, this endpoint answered 409 ci-red
// and will on every further attempt"), which must be re-driven when the blocker
// clears. Once the Task is parked(implement-declined) nothing separates them:
// the park reason is identical and the decline reason is free text nothing may
// parse. So the discriminator has to be captured HERE, at decline time, and
// controller.driveCIRecoveryUnparks is what later reads it.
//
// AFTER commit, NEVER BEFORE. Stamping first would attach a description of a
// decline to a Task whose park did not land (an illegal-transition conflict is
// reachable: the stage can move between the gate's read and commit's Get), and
// evidence about a decline that never happened is exactly the false red that
// re-opens a settled decision. Stamping second fails the other way - no
// evidence, so the Task is never re-driven and behaves precisely as it did
// before this existed - which is the direction to fail in.
//
// BEST-EFFORT, and it never touches o.w: the park has already committed and the
// response must carry exactly the one answer o.ok is about to write. A failure
// costs the recovery, not the outcome.
//
// The write REMOVES both keys when no merge request qualifies. A Task can
// decline twice, and the previous decline's red must not survive into a world
// where it is no longer true - a stale red left behind is a licence the driver
// would honour.
//
// It rides updateTaskSpec because that helper is a plain (non-status) Update
// with conflict retry, which is what a metadata write needs; nothing about the
// spec is touched.
func (s *Server) stampDeclineCIEvidence(ctx context.Context, r *http.Request,
	task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest) {

	ci, heads := tatarav1alpha1.CIDeclineEvidence(mrs)
	err := s.updateTaskSpec(ctx, task.Name, func(t *tatarav1alpha1.Task) {
		if ci == "" {
			delete(t.Annotations, tatarav1alpha1.AnnDeclineCI)
			delete(t.Annotations, tatarav1alpha1.AnnDeclineHeads)
			return
		}
		if t.Annotations == nil {
			t.Annotations = map[string]string{}
		}
		t.Annotations[tatarav1alpha1.AnnDeclineCI] = ci
		t.Annotations[tatarav1alpha1.AnnDeclineHeads] = heads
	})
	if err != nil {
		s.log.WarnContext(ctx, "restapi: decline ci evidence not recorded; this decline can never be re-driven",
			append(reqLogFields(r), "action", "decline_ci_evidence_skip", "task", task.Name, "error", err)...)
		return
	}
	s.log.InfoContext(ctx, "restapi: recorded what ci said at decline time",
		append(reqLogFields(r), "action", "decline_ci_evidence", "task", task.Name,
			"decline_ci", ci, "decline_heads", heads)...)
}

// --- review ---------------------------------------------------------------

var significanceRank = map[string]int{"patch": 1, "minor": 2, "major": 3}

func (o *outcomeCtx) review(p reviewPayload) {
	ctx := o.r.Context()
	s := o.s

	switch p.Verdict {
	case "approve":
	case "request_changes":
		if len(p.Findings) == 0 {
			o.bad("verdict=request_changes requires at least one finding", "missing-findings")
			return
		}
	default:
		o.bad("verdict must be one of approve, request_changes", "bad-verdict")
		return
	}
	if p.ChangeSignificance != "" && !validChangeSignificance[p.ChangeSignificance] {
		o.bad("changeSignificance must be one of major, minor, patch", "bad-significance")
		return
	}
	if len(p.ReviewedSHAs) == 0 {
		o.bad("reviewedSHAs is required: report the head SHA you actually checked out and read, for every MR this task owns", "missing-reviewed-shas")
		return
	}

	all, err := s.ownedMRs(ctx, o.task)
	if err != nil {
		writeClientErr(o.w, err)
		return
	}
	open := openMRs(all)
	if len(open) == 0 {
		// THE CARVE-OUTS ARE KIND-AGNOSTIC (#578). THIS IS THE SITE THAT BURNED
		// mt-i-mtg-decks-22: a kind=ISSUE Task runs an agentKind=REVIEW pod at
		// awaiting-review (stage.AgentKindFor keys awaiting-review on the STATE,
		// not spec.kind), so when its own MR merged out of band and a human
		// comment woke it, spec.kind was "issue", the review-only gate did not
		// apply, and it hit the hard 400 - three times in one turn, across seven
		// pod runs and three pod recreations, each one re-taking the claim it had
		// just been told failed.
		if states, term := mrTerminalStates(all); term {
			o.terminalNoop(states)
			return
		}
		over, err := controller.TaskTakenOver(ctx, s.c, o.task)
		if err != nil {
			writeClientErr(o.w, err)
			return
		}
		if over {
			o.takenOverNoop()
			return
		}
		// Still a 400 when the Task owns NO MR at all: an empty set is not
		// terminal, and that shape is a mint/binding fault the operator repairs
		// (intake.go repairMRBinding), after which the identical retry succeeds.
		o.bad("this task owns no open MR", "no-open-mr")
		return
	}

	// COVERAGE IS TOTAL. A reviewedSHAs that omits an owned MR is a 400, NOT
	// "unreviewed but fine": a multi-repo Task is exactly where a review agent
	// is most likely to read three MRs and report two.
	reported := map[string]string{}
	for _, rs := range p.ReviewedSHAs {
		if rs.Repo == "" || rs.Number == 0 || rs.SHA == "" {
			o.bad("every reviewedSHAs entry requires repo, number and sha", "bad-reviewed-sha")
			return
		}
		reported[mrKey(rs.Repo, rs.Number)] = rs.SHA
	}
	for i := range open {
		mr := &open[i]
		k := mrKey(mr.Spec.RepositoryRef, mr.Spec.Number)
		if _, ok := reported[k]; !ok {
			o.bad(fmt.Sprintf("reviewed_shas does not cover %s - review every MR in this task, or request_changes", k),
				"review-coverage")
			return
		}
	}
	for k := range reported {
		if !mrKeyOwned(open, k) {
			o.bad("task does not own "+k, "reviewed-sha-unowned")
			return
		}
	}

	// THE LIVE HEAD READ - the ONE forge call this handler makes, and it is a
	// READ. v3 stamped reviewedSHA from the live head at /outcome, which
	// certifies whatever was pushed BETWEEN the agent's checkout and its
	// outcome: the merge pin then guarantees that unreviewed code is what ships.
	writer, token, ok := s.projectSCMWriterAndToken(o.w, o.r, o.proj)
	if !ok {
		return
	}
	for i := range open {
		mr := &open[i]
		repo, err := s.repoCR(ctx, o.proj.Name, mr.Spec.RepositoryRef)
		if err != nil {
			writeClientErr(o.w, err)
			return
		}
		live, err := writer.GetPRHead(ctx, repo.Spec.URL, token, mr.Spec.Number)
		if err != nil {
			s.log.ErrorContext(ctx, "restapi: live head read failed",
				append(reqLogFields(o.r), "task", o.task.Name, "repo", repo.Name,
					"number", mr.Spec.Number, "error", err)...)
			writeError(o.w, http.StatusBadGateway, "scm read failed")
			return
		}
		k := mrKey(mr.Spec.RepositoryRef, mr.Spec.Number)
		if live != reported[k] {
			// HEAD MOVED - SELF-HEAL. The agent reviewed the mirror's head, which
			// lags (hourly sweep); for a fast-moving MR it stays stale, so a bare
			// 409 loops - the agent re-reviews the SAME stale sha and 409s forever.
			// Instead: PULL THE NEW COMMITS UNDERNEATH - resync THIS MR's mirror to
			// the live head (and its thread) on demand - then return a STRUCTURED,
			// non-fatal head-moved body the cli renders as guidance, so the agent
			// re-syncs its workspace, re-reviews the fresh diff, and resubmits with
			// the new sha. NOTHING is stamped (reviewedSHA/pendingReview): the
			// review was of stale code and is NOT accepted.
			reader, _, rok := s.projectSCMReader(o.w, o.r, o.proj)
			if !rok {
				return
			}
			if err := controller.SyncMergeRequestOnDemand(ctx, s.c, s.spillerForOrNil(o.proj), reader, o.proj, repo, mr, live); err != nil {
				s.log.WarnContext(ctx, "restapi: on-demand mirror resync after head-moved hit an error; the live head was stamped, the thread may lag a sweep",
					append(reqLogFields(o.r), "task", o.task.Name, "repo", mr.Spec.RepositoryRef,
						"number", mr.Spec.Number, "error", err)...)
			}
			obs.RestOutcomeRejectedTotal.WithLabelValues(o.kind, "head-moved").Inc()
			s.metrics.RecordReviewHeadMoved(mr.Spec.RepositoryRef)
			// Head-moved writes its structured 409 body directly rather than
			// through o.conflict, so it must release explicitly. It stamps
			// nothing, and the agent's honest resubmit-with-the-new-sha is a
			// DIFFERENT fingerprint anyway - but an identical retry must
			// re-validate against the live head, not sit out the TTL.
			o.release()
			s.log.InfoContext(ctx, "review head moved since checkout; mirror refreshed to the live head",
				append(reqLogFields(o.r), "action", "review_head_moved", "task", o.task.Name,
					"repo", mr.Spec.RepositoryRef, "number", mr.Spec.Number,
					"reviewedSHA", reported[k], "liveSHA", live)...)
			writeJSON(o.w, http.StatusConflict, headMovedResponse{
				Reason:          "head-moved",
				Repo:            mr.Spec.RepositoryRef,
				Number:          mr.Spec.Number,
				ReviewedSHA:     reported[k],
				LiveSHA:         live,
				MirrorRefreshed: true,
				Message: fmt.Sprintf("The head of %s#%d moved from %s to %s since you checked out. "+
					"Your review was of stale code and was NOT submitted; the mirror is refreshed to the new head. "+
					"Re-sync your workspace (git fetch && git checkout %s), re-review the new diff, and submit again.",
					mr.Spec.RepositoryRef, mr.Spec.Number, reported[k], live, live),
			})
			return
		}
	}

	// B1, THE REVIEW-AGENT EQUIVALENT. An APPROVE is a hand-off into the merge
	// corridor, and the corridor is POD-LESS: a change approved while its
	// pipeline is red or its branch conflicts spends the whole 4h merge budget
	// re-reading a verdict that only a new commit can change (issue #476, the
	// incident ci_gate.go was written for). Refusing it here puts the finding in
	// front of the reviewer that is still in-turn, one whole stage earlier.
	//
	// APPROVE ONLY. request_changes is already the "this is not ready" answer and
	// has nothing to be ready for; refusing it would leave the reviewer with no
	// legal outcome at all on the exact PR that most needs its findings recorded.
	//
	// PENDING IS ACCEPTED HERE, with no hold. The merge corridor already stalls
	// on ci-not-green (merge.go) and re-reads every 60s, so a CI hold at
	// awaiting-review would only duplicate a wait that already exists and is
	// already bounded.
	if p.Verdict == "approve" {
		if rd, readOK := o.evaluateReadiness(ctx, open, approveScope); readOK && o.refuseNotReady(ctx, rd, "approval") {
			return
		}
	}

	// VALIDATION ENDS HERE, EXECUTION BEGINS (#578). The head-moved branch above
	// deliberately stays UNCLAIMED: it stamps nothing on the Task, its mirror
	// resync is idempotent, and its 409 is guidance the agent acts on rather than
	// a claim anyone waits out.
	if !o.claim() {
		return
	}

	// PERSIST THE INTENT, and only the intent (C.5.3 phase 1). The MergeRequest
	// RECONCILER posts the review; this handler makes NO forge write.
	body := reviewBody(p.Verdict)
	for i := range open {
		mr := &open[i]
		k := mrKey(mr.Spec.RepositoryRef, mr.Spec.Number)
		sha := reported[k]
		findings := findingsFor(p.Findings, mr.Spec.RepositoryRef, mr.Spec.Number)
		verdict := p.Verdict
		sig := p.ChangeSignificance
		key := types.NamespacedName{Namespace: s.ns, Name: mr.Name}
		if err := objbudget.FitMergeRequest(ctx, s.c, s.spillerForOrNil(o.proj), key, func(m *tatarav1alpha1.MergeRequest) {
			round := m.Status.ReviewRounds + 1
			m.Status.ReviewedSHA = sha
			m.Status.PendingReview = &tatarav1alpha1.PendingReview{
				Body: body, Findings: findings, SHA: sha, Round: round,
			}
			if verdict == "approve" {
				m.Status.Status = "approved"
			} else {
				m.Status.Status = "needs-changes"
				m.Status.ReviewRounds = round
			}
			// changeSignificance is IMPLEMENT-OWNED: a review may only ESCALATE
			// it. A LOWER value is IGNORED and logged WARN - the in-cluster
			// reviewer is documented-flaky and must never downgrade a major
			// release to a patch.
			if sig != "" && significanceRank[sig] > significanceRank[m.Status.Significance] {
				m.Status.Significance = sig
			}
		}); err != nil {
			writeClientErr(o.w, err)
			return
		}
		if sig != "" && significanceRank[sig] <= significanceRank[mr.Status.Significance] &&
			sig != mr.Status.Significance {
			s.log.WarnContext(ctx, "restapi: review tried to LOWER changeSignificance; ignored (it is implement-owned)",
				append(reqLogFields(o.r), "task", o.task.Name, "repo", mr.Spec.RepositoryRef,
					"number", mr.Spec.Number, "implement", mr.Status.Significance, "review", sig)...)
		}
		// G4 quality-proxy signal: tatara-quality.yaml's rubber-stamp alert
		// selects operator_review_outcome_total{verdict="changes_requested"},
		// which is NOT this payload's own "request_changes" vocabulary.
		s.metrics.RecordReviewOutcome(o.proj.Name, mr.Spec.RepositoryRef, o.proj.Spec.Agent.Model,
			reviewOutcomeVerdictLabel(verdict))
	}

	// NO stage transition here. reviewing -> implementing and reviewing ->
	// merging are BOTH gated on every owned MR having pendingReview == nil
	// (stage.LegalFor, contract C.5.3): a pod spawned before the review is
	// recorded renders a bundle with no findings in it. The MergeRequest
	// reconciler posts the review, clears pendingReview, and the Task
	// reconciler then takes the F.3 edge from the MR statuses this handler just
	// wrote.
	if !o.commit(func(t *tatarav1alpha1.Task) error {
		agentNote(t, o.kind, "note", "review: "+p.Verdict, s.now())
		return nil
	}) {
		return
	}
	o.ok(p.Verdict, "mrs", len(open), "findings", len(p.Findings))
}

// reviewOutcomeVerdictLabel maps the REST payload's verdict vocabulary
// (approve/request_changes) onto operator_review_outcome_total's label
// vocabulary (approved/changes_requested, RecordReviewOutcome's own doc
// comment), which tatara-quality.yaml's rubber-stamp alert selects on
// directly.
func reviewOutcomeVerdictLabel(verdict string) string {
	if verdict == "approve" {
		return "approved"
	}
	return "changes_requested"
}

func reviewBody(verdict string) string {
	if verdict == "approve" {
		return "## Review: approved"
	}
	return "## Review: changes requested"
}

func mrKey(repo string, number int) string { return fmt.Sprintf("%s!%d", repo, number) }

func mrKeyOwned(mrs []tatarav1alpha1.MergeRequest, key string) bool {
	for i := range mrs {
		if mrKey(mrs[i].Spec.RepositoryRef, mrs[i].Spec.Number) == key {
			return true
		}
	}
	return false
}

func findingsFor(in []reviewFindingPayload, repo string, number int) []tatarav1alpha1.ReviewFinding {
	var out []tatarav1alpha1.ReviewFinding
	for _, f := range in {
		if f.Repo != repo || f.Number != number {
			continue
		}
		out = append(out, tatarav1alpha1.ReviewFinding{
			Path: f.Path, Line: f.Line, Severity: f.Severity,
			Body: truncateValidUTF8(f.Body, tatarav1alpha1.ReviewFindingBodyMaxBytes),
		})
	}
	return out
}

// --- the gate -------------------------------------------------------------

// gate is the folded clarify handler: `o.clarify` renamed, its body kept, its
// payload swapped for implementPayload's three gate actions.
//
// THE RENAME IS DELIBERATE AND THE BODY IS NOT NEW. This function IS the
// approval gate and it is verifyApprovalScope's ONLY caller; deleting it and
// re-deriving the same checks somewhere else is how a gate loses one of the
// eight refusals nobody notices for a month.
//
// Legality vs authorisation stays separated verbatim: internal/stage owns the
// transition table and park legality and does NO approval reasoning; this
// function owns authorisation. Nothing in #521 moves that line.
func (o *outcomeCtx) gate(p implementPayload) {
	ctx := o.r.Context()
	s := o.s

	if strings.TrimSpace(p.Reason) == "" {
		o.bad("reason is required on every gate action", "missing-field")
		return
	}
	if p.Title != "" || p.Body != "" || p.ChangeSignificance != "" || len(p.MergeOrder) > 0 {
		o.bad("title, body, changeSignificance and mergeOrder are only valid when action=submitted", "unexpected-field")
		return
	}
	if p.Action != "approved" &&
		(p.ApprovingMaintainer != "" || p.PlanNoteID != "" || len(p.ApprovalCitations) > 0) {
		o.bad("approvingMaintainer, planNoteId and approvalCitations are only valid when action=approved", "unexpected-field")
		return
	}

	mrs, err := s.ownedMRs(ctx, o.task)
	if err != nil {
		writeClientErr(o.w, err)
		return
	}

	switch p.Action {
	case "discuss":
		// A PARK, not a transition (#521). The Task stays exactly where it is and
		// resumes there on the next human comment.
		if !o.commit(func(t *tatarav1alpha1.Task) error {
			if err := stage.Park(t, stage.ReasonAwaitingHuman, s.now()); err != nil {
				return err
			}
			agentNote(t, o.kind, "note", "discuss: "+p.Reason, s.now())
			return nil
		}) {
			return
		}
		o.ok("discuss")
		return
	case "rejected":
		// The OPERATOR closes the issue; the agent never does it from here.
		// The close is queued as a pending comment intent on every owned Issue,
		// drained by the Issue reconciler.
		issues, err := s.ownedIssues(ctx, o.task)
		if err != nil {
			writeClientErr(o.w, err)
			return
		}
		// VALIDATION ENDS HERE, EXECUTION BEGINS (#578).
		if !o.claim() {
			return
		}
		// THE C.1 CLOSE INVARIANT. A decline still closes the issue, but only
		// when the Task has no live PR left: an agent that declines while its
		// own PR is still open would otherwise close the issue out from under
		// it. Refused closes PARK instead.
		if err := s.closeOrParkIssues(ctx, o.proj, o.task, issues, p.Reason, obs.CloseRefusedPathGate); err != nil {
			writeClientErr(o.w, err)
			return
		}
		if !o.commit(func(t *tatarav1alpha1.Task) error {
			if err := stage.Enter(t, mrs, tatarav1alpha1.StateRejected, stage.ReasonDeclined, s.now()); err != nil {
				return err
			}
			agentNote(t, o.kind, "note", "rejected: "+p.Reason, s.now())
			return nil
		}) {
			return
		}
		o.ok("rejected")
		return
	}

	// action=approved. The agent reports its decision and CITES the comment
	// it judged to approve; the operator INDEPENDENTLY re-derives the structural
	// facts - WHO wrote the cited comment, whether the quote is really in it,
	// and whether the DECLARED approver agrees with both - and the SCOPE (EVERY
	// owned Issue, not one: fix H9). It never reads intent.

	// THE PAIR RULE. approvingMaintainer and approvalCitations travel together:
	// both present is a human-cited approval, both absent is the auto-approve
	// path. One without the other is a client bug and is refused before any
	// verification runs, because a citation with no declared approver skips the
	// two new cross-checks and a declared approver with no citation has nothing
	// to be checked against.
	if (p.ApprovingMaintainer == "") != (len(p.ApprovalCitations) == 0) {
		o.bad("approvingMaintainer and approvalCitations must both be present or both be absent", "gate-pair-mismatch")
		return
	}
	// planNoteId is UNCONDITIONALLY required, including on the auto-approve
	// path: the plan pin is orthogonal to who approved, and the agent writes a
	// plan note either way.
	if strings.TrimSpace(p.PlanNoteID) == "" {
		o.bad("action=approved requires planNoteId", "missing-field")
		return
	}
	// THE OPERATOR RESOLVES THE PLAN NOTE ITSELF, through the SAME call the
	// submit-time re-check uses (planPinRefusal). Which note gets hashed IS the
	// control: `planNoteId` is client-supplied and the wire constrains neither
	// its kind nor its recency, so resolving it by id alone hashed whatever the
	// agent named and called that the approved plan.
	//
	// The declared id is kept as a DECLARATION THAT MUST AGREE - the same shape
	// as approvingMaintainer against the citation, and for the same reason: the
	// agent still has to say which note it thinks it is being approved on, and a
	// disagreement is a refusal rather than a silent substitution.
	note := planNote(o.task)
	if note == nil {
		o.refuseGate(controller.ApprovalRefusedPlanNoteMissing, p.ApprovingMaintainer)
		return
	}
	if note.ID != strings.TrimSpace(p.PlanNoteID) {
		o.refuseGate(controller.ApprovalRefusedPlanNoteNotPlan, p.ApprovingMaintainer)
		return
	}

	issues, err := s.ownedIssues(ctx, o.task)
	if err != nil {
		writeClientErr(o.w, err)
		return
	}
	granted, evidence, refusal := s.verifyApprovalScope(ctx, o.proj, issues, p.ApprovalCitations, p.ApprovingMaintainer)
	if !granted {
		// A SCOPE-LEVEL refusal has no per-Issue verifier call behind it, so this
		// is the only place its reason can be counted and logged. The per-Issue
		// path leaves refusal empty because VerifyApproval already did both.
		if refusal != "" {
			s.metrics.ApprovalRefused(refusal)
			s.log.InfoContext(ctx, "restapi: approval refused",
				append(reqLogFields(o.r), "action", "approval_refused",
					"task", o.task.Name, "reason", refusal)...)
		}
		reason := refusal
		if reason == "" {
			reason = "citation-not-verified"
		}
		s.log.WarnContext(ctx, "restapi: the implement gate reported approval but the scope check refused",
			append(reqLogFields(o.r), "task", o.task.Name, "reason", reason,
				"owned_issues", len(issues), "live_issues", liveIssueCount(issues))...)
		// IT DOES NOT PARK. The agent is still alive under the merged model and
		// should be told no and keep talking; under the old model the clarify pod
		// was dead after its one turn, so parking was the only way to hold the
		// work. That is no longer true.
		o.refuseGate(reason, p.ApprovingMaintainer)
		return
	}

	// VALIDATION ENDS HERE, EXECUTION BEGINS (#578). Every refuseGate above is a
	// 200 that writes nothing, and verifyApprovalScope is read-only.
	if !o.claim() {
		return
	}

	planHash := notePlanHash(note.Body)
	for i := range issues {
		iss := &issues[i]
		// The SAME filter verifyApprovalScope applied, and it must be the same:
		// that function deliberately puts no out-of-scope Issue in the map, so
		// without this an ordinary success - one closed Issue alongside one live
		// approved one - looked up a nil and fired the approver-less-approval
		// ERROR below once per closed Issue on every approval that WORKED.
		if !controller.ApprovalInScope(iss) {
			continue
		}
		ev := evidence[iss.Name]
		// Count the auto-approval TRANSITION (an issue not already approved): the
		// last human gate is being removed, so it must be queryable without
		// log-scraping (hard rule 13).
		if ev != nil && ev.Auto && iss.Status.Status != "approved" {
			if kind := tatarav1alpha1.ProposalKindFromBody(iss.Status.Body); kind != "" {
				s.metrics.AutoApproveTotal(kind)
			}
		}
		if ev == nil {
			// UNREACHABLE AT HEAD, AND KEPT ANYWAY AS A DRIFT DETECTOR. It is
			// unreachable because verifyApprovalScope refuses the whole request
			// on !ok || ev == nil and this loop walks exactly the in-scope keys
			// its map holds. It is reachable again the moment the scope skip five
			// lines above drifts from the identical one inside
			// verifyApprovalScope, which is a ONE-LINE regression that has already
			// happened once (fix L3-14). Writing status=approved with a nil
			// approval produces an approved Issue with NO approver.
			s.log.ErrorContext(ctx, "restapi: approval granted with nil evidence; refusing to write an approver-less approval",
				append(reqLogFields(o.r), "task", o.task.Name, "issue", iss.Name)...)
			continue
		}
		// THE PLAN PIN. sha256 of the plan note's body AS IT STOOD AT GRANT,
		// re-checked on the transition out of the gate into code. It is NEW in
		// the merged model and it exists because the model created the gap:
		// previously approval ended the clarify Task and a FRESH implement pod
		// started, so no artifact sat between approval and execution. Now the
		// same live agent brainstorms, is approved, and implements - so the plan
		// it was approved on is an artifact it can edit afterwards.
		ev.PlanHash = planHash
		key := types.NamespacedName{Namespace: s.ns, Name: iss.Name}
		if err := objbudget.FitIssue(ctx, s.c, s.spillerForOrNil(o.proj), key, func(is *tatarav1alpha1.Issue) {
			is.Status.Status = "approved"
			is.Status.Approval = ev
		}); err != nil {
			writeClientErr(o.w, err)
			return
		}
		// THE CONFIRMATION COMMENT, and it is the SELECTIVE-QUOTING MITIGATION.
		// "go ahead" really is a substring of "do not go ahead until CI is
		// green", and no amount of quote checking closes that: the mitigation is
		// DETECTION, not prevention. The operator posts one comment naming the
		// approver and quoting exactly what was cited, so a bypass is visible on
		// the thread within minutes. It rides the existing PendingComments drain
		// and is BOT-AUTHORED, so the enqueue filter drops it and the operator's
		// own confirmation can never wake the Task that produced it.
		if err := s.queueApprovalConfirmation(ctx, o.proj, iss, o.task.Name, ev, p.PlanNoteID); err != nil {
			writeClientErr(o.w, err)
			return
		}
		// THE GRANT NEEDS ITS OWN AUDIT LINE. Every REFUSAL is covered twice -
		// operator_approval_refused_total{reason} AND action=approval_refused -
		// while the grant had only action=submit_outcome. Issue.Status.Approval
		// is ONE slot, overwritten by the next approval on that Issue, so the
		// durable record of WHO released a change into push-CD is destroyed by
		// the next one; this line is the append-only trace.
		s.log.InfoContext(ctx, "restapi: approval verified",
			append(reqLogFields(o.r), "action", "approval_verified", "task", o.task.Name,
				"issue", iss.Name, "maintainer_login", ev.Login, "declared", p.ApprovingMaintainer,
				"cited_comment_id", ev.CommentID, "auto", ev.Auto, "plan_note_id", p.PlanNoteID)...)
	}
	if !o.commit(func(t *tatarav1alpha1.Task) error {
		if err := stage.Enter(t, mrs, tatarav1alpha1.StateUnderImplementation, "", s.now()); err != nil {
			return err
		}
		agentNote(t, o.kind, "note", "approved: "+p.Reason, s.now())
		return nil
	}) {
		return
	}
	// len(evidence), not len(issues): the map holds exactly the IN-SCOPE Issues
	// that produced evidence, which is what this outcome actually approved.
	o.ok("approved", "issues", len(evidence))
}

// planPinRefusal reports whether the plan note the gate pinned at grant no
// longer hashes to the same value. It is FALSE - no refusal - when there is
// nothing to compare: an auto-approved Task, an Issue whose evidence predates
// the pin, or a Task whose plan note has been spilled and can no longer be
// hashed. Those are all "the operator cannot prove drift", and refusing on a
// thing you cannot prove would break every Task minted before this shipped.
func (s *Server) planPinRefusal(ctx context.Context, task *tatarav1alpha1.Task) bool {
	issues, err := s.ownedIssues(ctx, task)
	if err != nil {
		// A read failure is not evidence of drift. Fail OPEN here on purpose:
		// the alternative bounces every submit during a transient apiserver
		// blip, and the grant itself was already gated.
		return false
	}
	for i := range issues {
		ev := issues[i].Status.Approval
		if ev == nil || ev.PlanHash == "" {
			continue
		}
		note := planNote(task)
		if note == nil {
			// Spilled, or never written. Nothing to hash; see the doc comment.
			continue
		}
		if notePlanHash(note.Body) != ev.PlanHash {
			s.log.InfoContext(ctx, "restapi: the approved plan changed after the grant",
				"action", "approval_refused", "task", task.Name, "issue", issues[i].Name,
				"reason", controller.ApprovalRefusedPlanHashMismatch)
			s.metrics.ApprovalRefused(controller.ApprovalRefusedPlanHashMismatch)
			return true
		}
	}
	return false
}

// refuseGate writes the 200 refusal body. IT DOES NOT PARK and it is NOT an
// error: the agent is alive, it is told no, and it keeps talking.
func (o *outcomeCtx) refuseGate(reason, declared string) {
	obs.RestOutcomeAcceptedTotal.WithLabelValues(o.kind, "gate-refused").Inc()
	o.s.log.InfoContext(o.r.Context(), "restapi: gate refused",
		append(reqLogFields(o.r), "action", "submit_outcome", "task", o.task.Name,
			"kind", o.kind, "outcome", "gate-refused", "reason", reason, "declared", declared)...)
	writeJSON(o.w, http.StatusOK, gateResponse{Granted: false, Reason: reason, Declared: declared})
}

// planNote resolves THE plan note the pin applies to, and it is THE ONE
// RESOLUTION: the grant calls it to decide what to hash, and planPinRefusal
// calls it to decide what to re-hash. A pin whose two halves resolve
// differently proves nothing whichever way it lands - it either compares
// against a note that will never be re-read (no drift is ever detected) or
// reports drift on a plan nobody touched - so the agreement is structural here
// rather than a rule each side is asked to remember.
//
// It is the NEWEST note of kind `plan` (pinnedPlanNoteID), never an id the
// agent chose, and it is nil when there is no plan note left to hash: never
// written, or written and since spilled.
func planNote(t *tatarav1alpha1.Task) *tatarav1alpha1.Note {
	return findPlanNote(t, pinnedPlanNoteID(t))
}

// findPlanNote is findNote plus the KIND CHECK, and the kind check is the whole
// point: findNote matches on id ALONE, and two different things then resolve to
// a note that is not a plan.
//
// A CLIENT-SUPPLIED id naming a handoff or turn note - the plan pin hashes that
// instead of the plan, and the anti-scope-drift control guards an artifact
// nobody approved. And an EMPTY id: agentNote writes the operator's own notes
// with no id at all, so findNote(t, "") matches the first of them and a Task
// with no pinnedPlanNoteId re-hashes an operator note on every submit. Kind is
// what separates the plan from everything else the journal holds.
func findPlanNote(t *tatarav1alpha1.Task, id string) *tatarav1alpha1.Note {
	if id == "" {
		return nil
	}
	n := findNote(t, id)
	if n == nil || n.Kind != planNoteKind {
		return nil
	}
	return n
}

// findNote resolves a note id against the Task's journal. A spilled note is
// GONE from status.notes, which is why the plan note is exempted from the spill
// (see handlers_v2.go's postNote).
func findNote(t *tatarav1alpha1.Task, id string) *tatarav1alpha1.Note {
	for i := range t.Status.Notes {
		if t.Status.Notes[i].ID == id {
			return &t.Status.Notes[i]
		}
	}
	return nil
}

// notePlanHash is the plan pin: sha256 of the note body, hex.
func notePlanHash(body string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(body))) }

// queueApprovalConfirmation enqueues the operator's one confirmation comment on
// an approved Issue. Idempotent through the RequestID the PendingComments drain
// already de-duplicates on: one per (task, issue, cited comment).
func (s *Server) queueApprovalConfirmation(ctx context.Context, proj *tatarav1alpha1.Project,
	iss *tatarav1alpha1.Issue, taskName string, ev *tatarav1alpha1.ApprovalEvidence, planNoteID string) error {

	if ev == nil || ev.Auto {
		// The auto-approve path has no maintainer to name and no quote to
		// re-state; a confirmation comment there would be the operator telling
		// the thread it approved itself.
		return nil
	}
	requestID := newRequestID(taskName, "approval-confirm", iss.Name, ev.CommentID)
	body := fmt.Sprintf(
		"Approval accepted for `%s`.\n\n- approver: `%s`\n- cited comment: `%s`\n- quote: %q\n- plan note: `%s`\n\n"+
			"If this is not what you meant, say so on this thread: the operator re-reads it every turn.",
		taskName, ev.Login, ev.CommentID, ev.Phrase, planNoteID)
	key := types.NamespacedName{Namespace: s.ns, Name: iss.Name}
	return objbudget.FitIssue(ctx, s.c, s.spillerForOrNil(proj), key, func(i *tatarav1alpha1.Issue) {
		for _, e := range i.Status.PendingComments {
			if e.RequestID == requestID {
				return
			}
		}
		i.Status.PendingComments = append(i.Status.PendingComments, tatarav1alpha1.PendingComment{
			RequestID: requestID,
			Action:    "comment",
			Body:      body,
		})
	})
}

// verifyApprovalScope re-derives the approval over every LIVE owned Issue,
// offering each of them the SAME citation set the agent submitted.
//
// THE EMPTY SET IS NOT A LICENCE, and it has two shapes here, not one. A clarify
// Task with NO Issue has nothing to approve and is refused; so is a Task whose
// every owned Issue is OUT OF SCOPE (closed / done / rejected). The second shape
// was a live gate hole: ownedIssues returns every owned Issue whatever its
// state, and the verifier answers (Issue.Status.Approval, true) for an
// out-of-scope one - correct in isolation, since a closed thread is not pending
// approval and must not block the others, but that stored approval is routinely
// nil. Refusing only on len(issues)==0 therefore reported granted=true over an
// all-nil map, and a Task whose ONLY Issue a HUMAN HAD CLOSED walked to approved
// with no citation, no maintainer comment and no evidence. Closing the issue is
// the strongest veto a human has; it must not be the thing that releases the
// work.
//
// The out-of-scope Issues are FILTERED rather than required to produce evidence,
// which is what keeps a human closing ONE issue of a multi-issue Task from
// stranding the rest. controller.ApprovalInScope is called rather than restated
// so the scope filter here, the one in the write loop below, and the one inside
// the verifier's own carve-out cannot drift. There is no controller-side twin of
// this loop any more: the Task-level VerifyApprovalDetailed/ApprovalPassed pair
// lost its last production caller and was deleted, leaving this the single place
// an approval can be granted.
//
// A nil verifier FAILS CLOSED.
//
// THE THIRD RETURN IS THE REFUSAL REASON, and it is empty on a grant AND on the
// per-Issue refusal path. That asymmetry is deliberate: VerifyApproval emits
// action=approval_refused and moves operator_approval_refused_total itself, so
// re-reporting its refusal here would double-count one refusal into two series.
// Only the two EMPTY-SET shapes have no per-Issue verifier call behind them, and
// those are exactly the two that used to refuse with no telemetry at all.
//
// DECLARED is the agent's approvingMaintainer, threaded through to the verifier
// as a BOUND CROSS-CHECK and never as a second authority. It is EMPTY on the
// auto-approve path and both new refusals are gated on that emptiness - ungated
// they would refuse every auto-approved proposal, because that path has no
// comment author to name and ev.Login is the <tatara:auto> sentinel.
func (s *Server) verifyApprovalScope(ctx context.Context, proj *tatarav1alpha1.Project,
	issues []tatarav1alpha1.Issue,
	citations []tatarav1alpha1.ApprovalCitation, declared string) (bool, map[string]*tatarav1alpha1.ApprovalEvidence, string) {
	// NOT production-reachable - cmd/manager/wire.go always sets Approval - so it
	// gets no reason rather than a lie about the Task's Issues. Kept because a
	// nil verifier must fail closed if the wiring ever regresses (fix W1).
	if s.approval == nil {
		return false, nil, ""
	}
	if len(issues) == 0 {
		return false, nil, controller.ApprovalRefusedNoLiveIssue
	}
	out := make(map[string]*tatarav1alpha1.ApprovalEvidence, len(issues))
	for i := range issues {
		if !controller.ApprovalInScope(&issues[i]) {
			continue
		}
		ev, ok, refusal := s.approval.VerifyApprovalDeclared(ctx, proj, &issues[i], citations, declared)
		if refusal != "" {
			return false, nil, refusal
		}
		// A LIVE Issue that granted with NO evidence is a refusal, not a pass.
		// The verifier is not supposed to answer that way for an in-scope Issue,
		// which is exactly why it is caught here rather than trusted: the
		// downstream writer's nil guard skips the Issue write and lets control
		// fall through to stage.Enter(approved), so an approver-less grant
		// advanced the Task while writing nothing that recorded it.
		if !ok || ev == nil {
			return false, nil, ""
		}
		out[issues[i].Name] = ev
	}
	// Owned Issues exist but every one of them is out of scope. This is the
	// human-veto shape: closing the only Issue of a clarify Task.
	if len(out) == 0 {
		return false, nil, controller.ApprovalRefusedNoLiveIssue
	}
	return true, out, ""
}

// liveIssueCount is the number of issues the scope check would actually have
// judged. It exists so the refusal WARN can report that rather than len(issues),
// which for the no-live-issue shape counts the very Issues the refusal was about
// excluding. controller.ApprovalInScope is called rather than restated for the
// same reason the scope loop and the write loop call it: one definition.
func liveIssueCount(issues []tatarav1alpha1.Issue) int {
	n := 0
	for i := range issues {
		if controller.ApprovalInScope(&issues[i]) {
			n++
		}
	}
	return n
}

func (s *Server) queueIssueClose(ctx context.Context, proj *tatarav1alpha1.Project, iss *tatarav1alpha1.Issue, taskName, reason string) error {
	requestID := newRequestID(taskName, "close", iss.Name, reason)
	key := types.NamespacedName{Namespace: s.ns, Name: iss.Name}
	return objbudget.FitIssue(ctx, s.c, s.spillerForOrNil(proj), key, func(i *tatarav1alpha1.Issue) {
		for _, e := range i.Status.PendingComments {
			if e.RequestID == requestID {
				return
			}
		}
		if len(i.Status.PendingComments) >= pendingCommentsCap {
			return
		}
		i.Status.PendingComments = append(i.Status.PendingComments, tatarav1alpha1.PendingComment{
			RequestID: requestID, Action: "comment",
			Body: truncateValidUTF8(closeIntentBody(reason), tatarav1alpha1.PendingCommentBodyMaxBytes),
		})
	})
}

func closeIntentBody(reason string) string {
	return "<!-- tatara-close -->\n" + reason
}

// closeOrParkIssues is THE C.1 CLOSE INVARIANT at the REST layer: an Issue is
// closed only when nothing the Task still owns would be stranded by the close,
// and otherwise it is PARKED - the terminal notice plus the tatara-parked
// label - rather than closed.
//
// It is the single funnel for both operator-side close paths in this file (the
// gate's rejected(declined) arm and refine's closes[] list), and being one
// function is the point: those two arms are the reason the invariant was
// breakable at all. CloseIssuesOnDelivery already carried a stricter version of
// this guard - every owned MR merged AND deployed - so the gap was never the
// delivery path, it was the two paths that had no guard whatsoever.
//
// path is the obs.IssueCloseRefusedTotal label; see that counter for the
// vocabulary. It is a caller-supplied constant, never derived from a payload.
func (s *Server) closeOrParkIssues(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, issues []tatarav1alpha1.Issue, reason, path string) error {

	open, err := s.openOwnedMRs(ctx, task)
	if err != nil {
		return err
	}
	for i := range issues {
		if err := s.closeOrParkIssue(ctx, proj, task, &issues[i], reason, path, open); err != nil {
			return err
		}
	}
	return nil
}

// openOwnedMRs is the invariant's read, hoisted so a caller closing N issues
// pays ONE MergeRequest List rather than N.
func (s *Server) openOwnedMRs(ctx context.Context, task *tatarav1alpha1.Task) ([]string, error) {
	mrs, err := s.ownedMRs(ctx, task)
	if err != nil {
		return nil, err
	}
	return controller.OpenOwnedMRs(mrs), nil
}

// closeOrParkIssue applies the invariant to ONE issue against an already-read
// open set. refine's closes[] uses it directly because each entry carries its
// OWN reason, which closeOrParkIssues' single-reason shape cannot express.
func (s *Server) closeOrParkIssue(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, iss *tatarav1alpha1.Issue, reason, path string, open []string) error {

	if len(open) == 0 {
		return s.queueIssueClose(ctx, proj, iss, task.Name, reason)
	}
	if err := s.queueIssuePark(ctx, proj, iss, task.Name,
		controller.ParkedForOpenMRsComment(task.Name, open)); err != nil {
		return err
	}
	obs.IssueCloseRefusedTotal.WithLabelValues(proj.Name, path).Inc()
	s.log.InfoContext(ctx, "restapi: issue close refused; the task still owns an open merge request",
		"action", "issue_close_refused", "resource_id", iss.Name,
		"task", task.Name, "path", path, "open_mrs", strings.Join(open, ","))
	return nil
}

// queueIssuePark is queueIssueClose's refusal twin: the same durable
// PendingComment intent, carrying the park marker instead of the close one, so
// the Issue reconciler's drain posts the notice AND stamps tatara-parked. The
// requestId is keyed on "park" so a later genuine close of the same issue by
// the same Task is not deduplicated against this park.
func (s *Server) queueIssuePark(ctx context.Context, proj *tatarav1alpha1.Project,
	iss *tatarav1alpha1.Issue, taskName, body string) error {

	requestID := newRequestID(taskName, "park", iss.Name)
	key := types.NamespacedName{Namespace: s.ns, Name: iss.Name}
	return objbudget.FitIssue(ctx, s.c, s.spillerForOrNil(proj), key, func(i *tatarav1alpha1.Issue) {
		for _, e := range i.Status.PendingComments {
			if e.RequestID == requestID {
				return
			}
		}
		if len(i.Status.PendingComments) >= pendingCommentsCap {
			return
		}
		i.Status.PendingComments = append(i.Status.PendingComments, tatarav1alpha1.PendingComment{
			RequestID: requestID, Action: "comment",
			Body: truncateValidUTF8(controller.ParkIntentBody(body), tatarav1alpha1.PendingCommentBodyMaxBytes),
		})
	})
}

// --- brainstorm -----------------------------------------------------------

// brainstormQuota reads the per-session proposal quota the ProjectReconciler
// stamped on the Task. It FAILS OPEN to the schema ceiling: an unannotated or
// unparseable Task predates the quota (or was hand-created), and refusing its
// outcome would lose a whole session's work. The floor is 1, because a session
// that produced a real proposal must be able to file at least one.
func brainstormQuota(task *tatarav1alpha1.Task) int {
	raw, ok := task.Annotations[tatarav1alpha1.AnnBrainstormQuota]
	if !ok {
		return tatarav1alpha1.MaxProposalsPerOutcome
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return tatarav1alpha1.MaxProposalsPerOutcome
	}
	return min(max(n, 1), tatarav1alpha1.MaxProposalsPerOutcome)
}

// stampBrainstormPause records the agent's exhausted verdict on the Project.
// The reason is stored VERBATIM and never parsed: it exists for a human reading
// `kubectl get project -o yaml`, not for any control flow. Clearing is the
// Project reconcile's job (internal/controller, AnnBrainstormResume), never
// this handler's - except for the propose path below, where the session just
// PROVED the idea space is not exhausted.
//
// sessionStart is this brainstorm session's own Task entering `refined`
// (o.task.Status.StateEnteredAt, the "clock for the POD-LESS stages" per its
// own doc comment - the closest thing a Task has to "when this session
// started"). It is compared against Project.Status.LastMovementAt inside
// setBrainstormPause's retry loop (I3 fix round): see that function's comment
// for why.
func (s *Server) stampBrainstormPause(ctx context.Context, projName, reason string, sessionStart time.Time) error {
	return s.setBrainstormPause(ctx, projName, &reason, sessionStart)
}

// clearBrainstormPause is stampBrainstormPause's inverse, used by the propose
// path. Both funnel through setBrainstormPause so the conflict-retry and the
// no-op short circuit exist once. sessionStart is irrelevant on the clear
// path (nothing to refuse), so it passes the zero value.
func (s *Server) clearBrainstormPause(ctx context.Context, projName string) error {
	return s.setBrainstormPause(ctx, projName, nil, time.Time{})
}

// setBrainstormPause is the single funnel for both stampBrainstormPause and
// clearBrainstormPause.
//
// THE FAIL DIRECTION (I3 fix round). The design spec's intended fail
// direction is "over-resumes rather than under-resumes" (see
// ResumeBrainstormOnPush's own comment, same precedent). StampBrainstormResume
// (internal/controller/brainstorm_resume.go) used to early-return on an
// UNPAUSED project, so a merge/push/maintainer trigger landing WHILE a
// brainstorm session was in flight - before this exhausted verdict ever
// landed - was silently discarded: the session's own eventual exhausted
// verdict then paused a project that had ALREADY moved, and it stayed paused
// until the next qualifying event - the OPPOSITE of the intended direction.
// StampBrainstormResume now stamps Status.LastMovementAt UNCONDITIONALLY,
// whether or not the project was paused, so that signal survives regardless.
// Read fresh inside the retry loop (never from a caller-held snapshot, which
// could itself be stale by the time this write lands): if it is newer than
// sessionStart, the project moved out from under this verdict before it was
// ever submitted, so the pause is refused entirely - not stamped and then
// immediately resumed, which would cost a real session for nothing. The
// caller's Task commit still proceeds; refusing the pause is not refusing the
// outcome.
func (s *Server) setBrainstormPause(ctx context.Context, projName string, reason *string, sessionStart time.Time) error {
	key := types.NamespacedName{Namespace: s.ns, Name: projName}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var proj tatarav1alpha1.Project
		if err := s.c.Get(ctx, key, &proj); err != nil {
			return fmt.Errorf("set brainstorm pause: get project %s: %w", projName, err)
		}
		if reason == nil {
			if proj.Status.BrainstormPausedAt == nil && proj.Status.BrainstormPauseReason == "" {
				return nil
			}
			proj.Status.BrainstormPausedAt = nil
			proj.Status.BrainstormPauseReason = ""
		} else {
			if proj.Status.LastMovementAt != nil && proj.Status.LastMovementAt.After(sessionStart) {
				return nil
			}
			now := metav1.NewTime(s.now())
			proj.Status.BrainstormPausedAt = &now
			proj.Status.BrainstormPauseReason = *reason
		}
		if err := s.c.Status().Update(ctx, &proj); err != nil {
			return fmt.Errorf("set brainstorm pause: update project %s: %w", projName, err)
		}
		return nil
	})
}

// brainstormSessionStart resolves the brainstorm Task's own session start,
// for setBrainstormPause's movement comparison (I3 fix round). StageEnteredAt
// is "the clock for the POD-LESS stages" (task_types.go's own doc comment) -
// stamped on EVERY stage transition, including the entry into
// entry into `refined` that begins this session - so it is the closest thing a
// Task has to "when this session started". Falls back to the Task's own
// CreationTimestamp on the (should-never-happen) chance StageEnteredAt is
// unset, rather than the zero time: the zero time would make EVERY movement
// ever recorded read as "after sessionStart" and refuse the pause forever.
func brainstormSessionStart(task *tatarav1alpha1.Task) time.Time {
	if task.Status.StateEnteredAt != nil {
		return task.Status.StateEnteredAt.Time
	}
	return task.CreationTimestamp.Time
}

func (o *outcomeCtx) brainstorm(p brainstormPayload) {
	ctx := o.r.Context()
	s := o.s

	switch p.Action {
	case "propose":
		if len(p.Proposals) < 1 || len(p.Proposals) > 5 {
			o.bad("proposals must carry 1 to 5 entries when action=propose", "bad-proposals")
			return
		}
		for _, pr := range p.Proposals {
			if pr.Repo == "" || strings.TrimSpace(pr.Title) == "" || strings.TrimSpace(pr.Body) == "" {
				o.bad("every proposal requires repo, title and body", "bad-proposals")
				return
			}
			if pr.Kind != "bug" && pr.Kind != "improvement" {
				o.bad("proposal kind must be bug or improvement", "bad-proposals")
				return
			}
		}
	case "skip", "exhausted":
		if strings.TrimSpace(p.Reason) == "" {
			o.bad("action="+p.Action+" requires a non-empty reason", "missing-field")
			return
		}
	default:
		o.bad("action must be one of propose, skip, exhausted", "bad-action")
		return
	}

	// VALIDATION ENDS HERE, EXECUTION BEGINS (#578).
	if !o.claim() {
		return
	}

	if p.Action == "skip" || p.Action == "exhausted" {
		// A skip stamps NOTHING: it is "nothing this cycle", it is transient, and
		// it has no scheduling consequence at all. Only exhausted - "nothing worth
		// proposing until the project moves" - pauses, and ONE is enough.
		//
		// THE PAUSE STAMP RUNS BEFORE THE COMMIT AND FAILS CLOSED (I1 fix round).
		// It used to run best-effort AFTER the Task committed to Delivered: a
		// failed write still returned 200, the Task was already terminal so
		// nothing ever retried it, and the project fell straight back into C2's
		// busy loop with the one brake that exists for it silently lost. Losing a
		// pause costs the whole braking mechanism, unlike losing a skip's (deleted)
		// counter increment, which cost nothing - so unlike a plain best-effort
		// side write, this one must hold up the whole outcome: fail here, and the
		// Task stays non-terminal so the agent's retry of this SAME idempotent
		// outcome lands both halves together.
		if p.Action == "exhausted" {
			if err := s.stampBrainstormPause(ctx, o.proj.Name, p.Reason, brainstormSessionStart(o.task)); err != nil {
				s.log.ErrorContext(ctx, "restapi: stamping the brainstorm pause failed; agent must retry",
					append(reqLogFields(o.r), "task", o.task.Name, "project", o.proj.Name, "error", err)...)
				writeError(o.w, http.StatusInternalServerError, "internal error")
				return
			}
			s.log.InfoContext(ctx, "restapi: brainstorm paused on an exhausted verdict",
				append(reqLogFields(o.r), "action", "brainstorm_paused", "task", o.task.Name,
					"project", o.proj.Name, "reason", p.Reason)...)
		}
		// documentedBy stays EMPTY (fix 25): a brainstorm that correctly says
		// "nothing novel" must not spawn a docs pod, a docs PR about nothing, a
		// review, a merge and a release.
		if !o.commit(func(t *tatarav1alpha1.Task) error {
			if err := stage.Enter(t, nil, tatarav1alpha1.StateDone, "", s.now()); err != nil {
				return err
			}
			agentNote(t, o.kind, "note", p.Action+": "+p.Reason, s.now())
			return nil
		}) {
			return
		}
		o.ok(p.Action)
		return
	}

	// OPERATOR-SIDE TRUNCATION IS THE AUTHORITY. The skill states the quota to
	// the agent, but an agent that ignores it cannot overshoot the target:
	// extra proposals are dropped here, silently to the agent and loudly in
	// the log. The [1,5] schema ceiling above still applies first - this only
	// ever narrows further, never overrides that hard rejection.
	if quota := brainstormQuota(o.task); len(p.Proposals) > quota {
		dropped := len(p.Proposals) - quota
		s.log.InfoContext(ctx, "restapi: truncating brainstorm proposals to the session quota",
			append(reqLogFields(o.r), "action", "brainstorm_quota_truncated", "task", o.task.Name,
				"submitted", len(p.Proposals), "quota", quota)...)
		s.metrics.BrainstormQuotaTruncated(o.proj.Name, dropped)
		p.Proposals = p.Proposals[:quota]
	}

	// Each proposal becomes its OWN new implement Task, owning its OWN Issue
	// (F.3). brainstorm files issues through submit_outcome, not issue_write,
	// so the proposal cap and dedup still apply.
	writer, token, ok := s.projectSCMWriterAndToken(o.w, o.r, o.proj)
	if !ok {
		return
	}
	// Resolved once: the label is INFORMATIONAL ONLY (nothing reads it for
	// control flow - counting is Spec.ProposalKind on the CR, approval has been
	// comment-only since C.6), but every filed proposal still carries it so a
	// maintainer scanning the forge sees it triaged the same way it always has.
	brainstormingLabel, _, _, _ := controller.LifecycleLabels(o.proj.Spec.Scm)
	spawned := make([]string, 0, len(p.Proposals))
	for _, pr := range p.Proposals {
		repo, err := s.repoCR(ctx, o.proj.Name, pr.Repo)
		if err != nil {
			writeClientErr(o.w, err)
			return
		}
		// Stamp the tatara-proposed-by provenance marker into the body the forge and
		// the Issue CR both carry: it is the autoApproveTataraProposals carve-out's
		// marker factor, and putting it on the SCM issue (not just the CR) keeps it
		// alive across a mirror refresh. Harmless when the flag is off.
		body := tatarav1alpha1.StampProposalMarker(pr.Body, tatarav1alpha1.ProposalKindBrainstorm)
		title := s.clampTitleForForge(ctx, o.r, obs.TitleSiteBrainstormPropose, o.task.Name, pr.Title)
		created, err := writer.CreateIssue(ctx, repo.Spec.URL, token, scm.IssueReq{
			Title: title, Body: body, Labels: []string{brainstormingLabel},
		})
		controller.RecordSCM(s.metrics, providerOf(o.proj), "create_issue", err)
		if err != nil {
			fields := append(reqLogFields(o.r), "task", o.task.Name, "repo", repo.Name)
			fields = append(fields, titleLogFields(pr.Title, title)...)
			s.log.ErrorContext(ctx, "restapi: filing a brainstorm proposal failed",
				append(fields, "error", err)...)
			writeError(o.w, http.StatusBadGateway, "scm write failed")
			return
		}
		number := issueRefNumber(created.Ref)
		if number == 0 {
			writeError(o.w, http.StatusBadGateway, "scm returned no issue number")
			return
		}
		child, err := s.mintGateTask(ctx, o.proj, repo, pr, number, created.URL)
		if err != nil {
			writeClientErr(o.w, err)
			return
		}
		if err := s.mintIssueCR(ctx, o.proj, repo, child, number, created.URL, title, body,
			tatarav1alpha1.ProposalKindBrainstorm, nil); err != nil {
			writeClientErr(o.w, err)
			return
		}
		spawned = append(spawned, child.Name)
	}

	if !o.commit(func(t *tatarav1alpha1.Task) error {
		if err := stage.Enter(t, nil, tatarav1alpha1.StateDone, "", s.now()); err != nil {
			return err
		}
		agentNote(t, o.kind, "note", "proposed: "+strings.Join(spawned, ", "), s.now())
		return nil
	}) {
		return
	}
	// A productive session PROVES the idea space is not exhausted, so it clears
	// any standing pause without waiting for one of the five external triggers
	// (best-effort, same trade as the exhausted-side stamp).
	if err := s.clearBrainstormPause(ctx, o.proj.Name); err != nil {
		s.log.ErrorContext(ctx, "restapi: clearing the brainstorm pause failed",
			append(reqLogFields(o.r), "task", o.task.Name, "project", o.proj.Name, "error", err)...)
	}
	o.ok("propose", "spawned", strings.Join(spawned, ","))
}

// mintGateTask creates the Task a brainstorm proposal becomes: kind=implement,
// which lands at `refined` where the approval gate runs. It was kind=clarify
// until #521 folded that kind away; the CRD enum no longer accepts it, so this
// mint would be REJECTED outright if the literal had been left behind.
func (s *Server) mintGateTask(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, pr proposalPayload, number int, url string) (*tatarav1alpha1.Task, error) {
	name := tatarav1alpha1.TaskName(proj.Name, controller.SweepIssueKind, s.now(), rand.String(5))
	t := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.ns},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: repo.Name, Kind: controller.SweepIssueKind,
			Goal:              truncateValidUTF8(pr.Title+"\n\n"+pr.Body, tatarav1alpha1.GoalMaxBytes),
			InitialState:      tatarav1alpha1.StateNew,
			InitialParkReason: gateTaskMintParkReason(proj),
			Source: &tatarav1alpha1.TaskSource{
				Provider: providerOf(proj), IssueRef: issueRef(repo, number),
				URL: url, Number: number,
			},
		},
	}
	// Issue #517's descriptive pod name, stamped at creation like every other
	// mint path; without it this Task falls back to the legacy wrapper-<name>.
	agent.StampPodName(t, proj.Name, repo.Name)
	if err := s.c.Create(ctx, t); err != nil {
		return nil, fmt.Errorf("create gate task %s: %w", name, err)
	}
	return t, nil
}

// gateTaskMintParkReason is the park reason a brainstorm proposal's gate Task is
// minted with; "" mints it live, exactly as before.
//
// WHY IT PARKS: a proposal NO HUMAN HAS ENGAGED WITH must not consume an agent
// pod. Every other mint site reaches that conclusion through
// controller.MintStage, whose clause 2 deliberately excludes the bot login so a
// bot-authored, comment-free issue mints PARKED. The propose path files exactly
// such an issue and then mints its Task HERE, bypassing the intake funnel: left
// live, triageTarget routes kind=implement straight to `refined`, a ticket is
// admitted, an implement agent spawns, posts a `## Plan` comment on the thread,
// submits action=approved and is refused with no-maintainer-comment. A full
// agent session and a forge comment, spent on an idea nobody has approved.
//
// WHY backlog-sweep AND NOT A NEW REASON: it already means precisely "minted
// from something nobody has spoken to yet". It is UnparkHuman
// (internal/stage/park.go), so the FIRST non-bot comment on the thread releases
// it through the webhook's driveCommentUnpark - and it is deliberately NOT in
// NeedsLiveRoom, so that release does not additionally wait on live-pod
// capacity. MintParked emits no park counter, so this mint does not pollute the
// park-rate alert.
//
// WHY THE FLAG FLIPS IT: with autoApproveTataraProposals ON, the approval
// carve-out grants that agent WITHOUT any maintainer comment, so parking it
// would hold the Task for a comment the project has declared unnecessary. The
// flag-on path is byte-for-byte today's behaviour.
func gateTaskMintParkReason(proj *tatarav1alpha1.Project) string {
	if proj.Spec.AutoApproveTataraProposals {
		return ""
	}
	return stage.ReasonBacklogSweep
}

// issueRef is the provider-shaped owner/repo#N reference.
func issueRef(repo *tatarav1alpha1.Repository, number int) string {
	return fmt.Sprintf("%s#%d", repoSlug(repo), number)
}

func providerOf(proj *tatarav1alpha1.Project) string {
	if proj.Spec.Scm == nil {
		return ""
	}
	return proj.Spec.Scm.Provider
}

// --- incident -------------------------------------------------------------

func (o *outcomeCtx) incident(p incidentPayload) {
	ctx := o.r.Context()
	s := o.s

	if len(p.AlertRules) == 0 {
		o.bad("alertRules is required (at least one) on both actions", "missing-field")
		return
	}
	if strings.TrimSpace(p.Reason) == "" {
		o.bad("reason is required on both actions", "missing-field")
		return
	}
	switch p.Action {
	case "file_issue":
		if p.Comment != nil {
			o.bad("comment is only for action=comment_issue", "unexpected-field")
			return
		}
		if p.Issue == nil || p.Issue.Repo == "" ||
			strings.TrimSpace(p.Issue.Title) == "" || strings.TrimSpace(p.Issue.Body) == "" {
			o.bad("action=file_issue requires issue.repo, issue.title and issue.body", "missing-field")
			return
		}
		if p.Issue.Parent != nil && (p.Issue.Parent.Repo == "" || p.Issue.Parent.Number == 0) {
			o.bad("issue.parent requires repo and number", "bad-parent")
			return
		}
	case "comment_issue":
		if p.Issue != nil {
			o.bad("issue is only for action=file_issue", "unexpected-field")
			return
		}
		if p.Comment == nil || p.Comment.Repo == "" || p.Comment.Number == 0 ||
			strings.TrimSpace(p.Comment.Body) == "" {
			o.bad("action=comment_issue requires comment.repo, comment.number and comment.body", "missing-field")
			return
		}
	case "false_positive":
		if p.Issue != nil {
			o.bad("issue is only for action=file_issue", "unexpected-field")
			return
		}
		if p.Comment != nil {
			o.bad("comment is only for action=comment_issue", "unexpected-field")
			return
		}
	default:
		o.bad("action must be one of file_issue, comment_issue, false_positive", "bad-action")
		return
	}

	// comment_issue's TARGET is validated up here, in the read-only block, and
	// not where it is consumed (#578). Its two rejections are 400s and the
	// alertRules spec merge below is a mutation, so resolving the target after
	// the merge made this the one incident path that could mutate and then 4xx.
	// incidentComment re-resolves; both reads are cached Gets.
	if p.Action == "comment_issue" {
		if _, _, ok := o.commentTarget(p); !ok {
			return
		}
	}

	// VALIDATION ENDS HERE, EXECUTION BEGINS (#578).
	if !o.claim() {
		return
	}

	// alertRules are merged into Task.spec.alertRules; spec is
	// operator-writable and agent-unwritable, and this is the operator writing.
	if err := s.updateTaskSpec(ctx, o.task.Name, func(t *tatarav1alpha1.Task) {
		for _, rule := range p.AlertRules {
			if !contains(t.Spec.AlertRules, rule) {
				t.Spec.AlertRules = append(t.Spec.AlertRules, rule)
			}
		}
	}); err != nil {
		writeClientErr(o.w, err)
		return
	}

	if p.Action == "false_positive" {
		if !o.commit(func(t *tatarav1alpha1.Task) error {
			if err := stage.Enter(t, nil, tatarav1alpha1.StateRejected, stage.ReasonFalsePositive, s.now()); err != nil {
				return err
			}
			agentNote(t, o.kind, "note", "false_positive: "+p.Reason, s.now())
			return nil
		}) {
			return
		}
		o.ok("false_positive")
		return
	}

	if p.Action == "comment_issue" {
		o.incidentComment(p)
		return
	}

	// The tracker Issue is created under THIS Task (F.3), and filing it FINISHES
	// the Task: it hands the incident to a tracker a human triages on its own
	// thread, and it opens no MR, so `refined -> done` is its only path out.
	//
	// The pre-#521 handler moved to `clarifying`, a SEPARATE stage where the
	// (now deleted) clarify agent ran the human gate. #521 collapsed
	// investigating and clarifying onto one `refined`, so that same move became
	// `refined -> refined` - a self-edge the table does not have and never had,
	// which made every file_issue a 409 that RELEASED the claim and re-fired
	// CreateIssue on the agent's retry.
	repo, err := s.repoCR(ctx, o.proj.Name, p.Issue.Repo)
	if err != nil {
		writeClientErr(o.w, err)
		return
	}
	writer, token, ok := s.projectSCMWriterAndToken(o.w, o.r, o.proj)
	if !ok {
		return
	}
	ruleKey := o.task.Spec.DedupKey
	// Provenance marker for the autoApproveTataraProposals carve-out (marker factor);
	// stamped on both the forge issue and the CR so it survives a mirror refresh.
	body := tatarav1alpha1.StampProposalMarker(p.Issue.Body, tatarav1alpha1.ProposalKindIncident)
	title := s.clampTitleForForge(ctx, o.r, obs.TitleSiteIncidentFileIssue, o.task.Name, p.Issue.Title)
	issueReq := scm.IssueReq{Title: title, Body: body}
	if ruleKey != "" {
		issueReq.Labels = append(issueReq.Labels, forgeAlertRulePrefix+ruleKey)
	}
	created, err := writer.CreateIssue(ctx, repo.Spec.URL, token, issueReq)
	controller.RecordSCM(s.metrics, providerOf(o.proj), "create_issue", err)
	if err != nil {
		// A forge 4xx here is otherwise undiagnosable from the log line alone:
		// the title that caused it is never persisted anywhere.
		fields := append(reqLogFields(o.r), "task", o.task.Name, "repo", repo.Name)
		fields = append(fields, titleLogFields(p.Issue.Title, title)...)
		s.log.ErrorContext(ctx, "restapi: filing the incident tracker issue failed",
			append(fields, "error", err)...)
		writeError(o.w, http.StatusBadGateway, "scm write failed")
		return
	}
	number := issueRefNumber(created.Ref)
	if number == 0 {
		writeError(o.w, http.StatusBadGateway, "scm returned no issue number")
		return
	}
	crLabels := map[string]string{}
	if ruleKey != "" {
		crLabels[queue.LabelAlertRuleKey] = ruleKey
	}
	if o.task.Spec.GroupKey != "" {
		crLabels[queue.LabelAlertGroupKey] = o.task.Spec.GroupKey
	}
	if len(crLabels) == 0 {
		crLabels = nil
	}
	// The CR mirrors what the forge actually stored, so it gets the clamped title too.
	if err := s.mintIssueCR(ctx, o.proj, repo, o.task, number, created.URL, title, body,
		tatarav1alpha1.ProposalKindIncident, crLabels); err != nil {
		writeClientErr(o.w, err)
		return
	}

	// Auto-correlate: when the agent named no parent but this incident shares a
	// GROUP key with an OLDER open tracker (a different alert rule firing for one
	// root cause), link the new tracker under that sibling so a co-firing storm
	// collapses to one tree instead of N unrelated issues. Agent-supplied parent
	// always wins.
	// ruleKey != "" is required: FindOldestOpenGroupSibling excludes THIS Task's
	// just-minted tracker by its rule-key, so an empty key could pick the new
	// issue as its own parent. Incident Tasks always carry a DedupKey, so this is
	// belt-and-braces.
	parent := p.Issue.Parent
	if parent == nil && o.task.Spec.GroupKey != "" && ruleKey != "" {
		if sib, ok, ferr := queue.FindOldestOpenGroupSibling(ctx, s.c, s.ns, o.proj.Name, o.task.Spec.GroupKey, ruleKey); ferr == nil && ok {
			parent = &incidentParent{Repo: sib.Spec.RepositoryRef, Number: sib.Spec.Number}
			s.metrics.IncidentGroupLinked("linked")
			s.log.InfoContext(ctx, "incident auto-linked under group sibling",
				append(reqLogFields(o.r), "task", o.task.Name, "group_key", o.task.Spec.GroupKey,
					"sibling", sib.Name)...)
		} else {
			if ferr != nil {
				s.log.ErrorContext(ctx, "incident group-sibling lookup failed",
					append(reqLogFields(o.r), "task", o.task.Name, "error", ferr)...)
			}
			s.metrics.IncidentGroupLinked("no_sibling")
		}
	}
	if parent != nil {
		s.linkIncidentParent(ctx, o, writer, token, created.Ref, parent)
	}

	if !o.commit(func(t *tatarav1alpha1.Task) error {
		if err := stage.Enter(t, nil, tatarav1alpha1.StateDone, "", s.now()); err != nil {
			return err
		}
		agentNote(t, o.kind, "note", "file_issue: "+p.Reason, s.now())
		return nil
	}) {
		return
	}
	o.ok("file_issue", "repo", repo.Name, "number", number)
}

// incidentComment appends fresh evidence to an EXISTING open incident tracker
// (action=comment_issue) instead of filing a near-duplicate, then terminates the
// Task at rejected(tracked-elsewhere). It is GATED: the target Issue CR must
// carry an incident rule-key or group-key label, so an incident agent (which has
// no issue_write tool) can comment ONLY on a tracker the platform created, never
// an arbitrary human thread. The operator posts under the bot identity.
// commentTarget resolves and VALIDATES action=comment_issue's target, writing
// nothing. It is read-only so it can run in the caller's validation phase,
// BEFORE the alertRules spec merge (#578): its two rejections are 400s, and a
// 400 must never follow a mutation.
func (o *outcomeCtx) commentTarget(p incidentPayload) (*tatarav1alpha1.Repository, *tatarav1alpha1.Issue, bool) {
	ctx := o.r.Context()
	s := o.s
	repo, err := s.repoCR(ctx, o.proj.Name, p.Comment.Repo)
	if err != nil {
		writeClientErr(o.w, err)
		return nil, nil, false
	}
	var iss tatarav1alpha1.Issue
	issName := tatarav1alpha1.IssueName(repo.Name, p.Comment.Number)
	// Fix (#445): a Get failure here means the Issue CR is absent from the
	// operator's mirror (e.g. legitimately GC'd via SeverDeleteCR when its
	// owning Task was reaped, internal/controller/issue_apply.go,
	// internal/controller/reaper.go), NOT that the target is some other
	// non-tracker thread - that is the label-check branch below. Collapsing
	// both into one reason/message previously made a validly GC'd tracker
	// indistinguishable from a deliberate not-a-tracker rejection, which sent
	// the same fault back through file_issue and re-filed it repeatedly.
	if err := s.c.Get(ctx, types.NamespacedName{Namespace: s.ns, Name: issName}, &iss); err != nil {
		o.bad("comment target issue is not present in the operator's mirror", "not-mirrored")
		return nil, nil, false
	}
	if iss.Labels[queue.LabelAlertRuleKey] == "" && iss.Labels[queue.LabelAlertGroupKey] == "" {
		o.bad("comment target is not a tracked incident issue", "not-a-tracker")
		return nil, nil, false
	}
	return repo, &iss, true
}

func (o *outcomeCtx) incidentComment(p incidentPayload) {
	ctx := o.r.Context()
	s := o.s
	// Re-resolved rather than threaded down from the caller's validation pass:
	// two cheap cached Gets, against which the alternative is a second parameter
	// list that can silently go stale.
	repo, issp, ok := o.commentTarget(p)
	if !ok {
		return
	}
	iss := *issp
	ref := issueRef(repo, p.Comment.Number)
	// Fix 7 (#400): rate-limit the SCM WRITE, not the outcome itself. The
	// decision reads the Issue snapshot already fetched above (iss); the
	// suppressed increment and the post-success reset each go through their
	// own objbudget.FitIssue call against a FRESH re-read, because FitIssue's
	// mutate closure runs at least twice (a sizing pass plus the
	// retry-on-conflict pass) and must stay pure - the same shape as
	// enqueueRefireComment (internal/webhook/server.go). The SCM write itself
	// must never live inside that closure.
	now := s.now()
	cooldown := s.incidentInvestigationCommentCooldown
	suppressed := iss.Status.LastInvestigationCommentAt != nil &&
		now.Sub(iss.Status.LastInvestigationCommentAt.Time) < cooldown
	priorSuppressed := iss.Status.SuppressedInvestigationCount
	key := types.NamespacedName{Namespace: s.ns, Name: iss.Name}

	if suppressed {
		if err := objbudget.FitIssue(ctx, s.c, s.spillerForOrNil(o.proj), key, func(i *tatarav1alpha1.Issue) {
			i.Status.SuppressedInvestigationCount++
		}); err != nil {
			o.release()
			writeClientErr(o.w, err)
			return
		}
		s.metrics.IncidentTrackerComment("suppressed")
	} else {
		writer, token, ok := s.projectSCMWriterAndToken(o.w, o.r, o.proj)
		if !ok {
			return
		}
		body := p.Comment.Body
		if priorSuppressed > 0 {
			body = fmt.Sprintf("(%d prior evidence comment(s) suppressed by the investigation-comment cooldown)\n\n%s",
				priorSuppressed, body)
		}
		cerr := writer.Comment(ctx, token, ref, body)
		controller.RecordSCM(s.metrics, providerOf(o.proj), "comment", cerr)
		if cerr != nil {
			s.metrics.IncidentTrackerComment("failed")
			s.log.ErrorContext(ctx, "restapi: appending incident evidence comment failed",
				append(reqLogFields(o.r), "task", o.task.Name, "tracker", ref, "error", cerr)...)
			o.release()
			writeError(o.w, http.StatusBadGateway, "scm comment failed")
			return
		}
		s.metrics.IncidentTrackerComment("posted")
		// Best-effort: the comment already landed on the forge, so a failure here
		// must not fail the whole request - that would leave the Task unterminated
		// and duplicate the comment on retry. Log loudly and fall through to commit
		// regardless; worst case the cooldown/counter reset is lost, not the Task.
		if err := objbudget.FitIssue(ctx, s.c, s.spillerForOrNil(o.proj), key, func(i *tatarav1alpha1.Issue) {
			i.Status.SuppressedInvestigationCount = 0
			t := metav1.NewTime(now)
			i.Status.LastInvestigationCommentAt = &t
		}); err != nil {
			obs.MirrorWriteDroppedTotal.WithLabelValues(o.proj.Name, "Issue", "incident_cooldown_reset").Inc()
			s.log.ErrorContext(ctx, "restapi: resetting incident comment cooldown after posted comment failed",
				append(reqLogFields(o.r), "action", "comment_issue", "issue", key.Name, "error", err)...)
		}
	}

	if !o.commit(func(t *tatarav1alpha1.Task) error {
		if err := stage.Enter(t, nil, tatarav1alpha1.StateRejected, stage.ReasonTrackedElsewhere, s.now()); err != nil {
			return err
		}
		agentNote(t, o.kind, "note", "comment_issue "+ref+": "+p.Reason, s.now())
		return nil
	}) {
		return
	}
	o.ok("comment_issue", "repo", repo.Name, "number", p.Comment.Number, "suppressed", suppressed,
		"suppressed_count", priorSuppressed)
}

// linkIncidentParent links the freshly-filed child issue under an open tracker
// as a GitHub sub-issue, cross-referencing both. BEST-EFFORT: the issue is
// already filed, so no failure here fails the incident. On any AddSubIssue error
// (unsupported provider, 100-child cap, cross-repo 403, unique-parent conflict)
// it degrades to a "Related to" comment on both issues, so the relationship is
// never silently lost (the #328 failure mode).
func (s *Server) linkIncidentParent(ctx context.Context, o *outcomeCtx, writer scm.SCMWriter, token, childRef string, parent *incidentParent) {
	parentRepo, err := s.repoCR(ctx, o.proj.Name, parent.Repo)
	if err != nil {
		// The parent repo is not in this project (or otherwise unresolvable), so
		// there is no valid forge ref to link against or comment on. Preserve the
		// relationship as plain text on the CHILD only - a comment on the parent
		// needs a repo URL/token this project cannot vouch for.
		fallbackRef := fmt.Sprintf("%s#%d", parent.Repo, parent.Number)
		commentErr := writer.Comment(ctx, token, childRef, "Related to "+fallbackRef)
		controller.RecordSCM(s.metrics, providerOf(o.proj), "comment", commentErr)
		if commentErr != nil {
			// Nothing landed anywhere: not GitHub, not the CR, not even a comment.
			// The relationship is genuinely lost, so the failed bucket must be real
			// and this must be loud (ERROR), not a WARN masquerading as success.
			s.metrics.IncidentSublink("failed")
			s.log.ErrorContext(ctx, "incident sublink: parent repo not resolvable and fallback comment failed; relationship recorded nowhere",
				"action", "incident_sublink", "task", o.task.Name, "child", childRef,
				"parent_repo", parent.Repo, "parent_number", parent.Number, "result", "failed",
				"resolve_error", err, "comment_error", commentErr)
			return
		}
		s.metrics.IncidentSublink("fallback_comment")
		s.log.WarnContext(ctx, "incident sublink: parent repo not resolvable, fallback comment on child only",
			"action", "incident_sublink", "task", o.task.Name, "child", childRef,
			"parent_repo", parent.Repo, "parent_number", parent.Number, "result", "fallback_comment", "error", err)
		return
	}
	parentRef := issueRef(parentRepo, parent.Number) // owner/repo#N
	linkErr := writer.AddSubIssue(ctx, token, parentRef, issueRefNumber(childRef))
	if linkErr == nil {
		_ = writer.Comment(ctx, token, childRef, "Related to "+parentRef)
		_ = writer.Comment(ctx, token, parentRef, "Related sub-issue: "+childRef)
		s.metrics.IncidentSublink("linked")
		s.log.InfoContext(ctx, "incident sublink established",
			"action", "incident_sublink", "task", o.task.Name,
			"child", childRef, "parent", parentRef, "result", "linked")
		return
	}
	childCommentErr := writer.Comment(ctx, token, childRef, "Related to "+parentRef)
	controller.RecordSCM(s.metrics, providerOf(o.proj), "comment", childCommentErr)
	parentCommentErr := writer.Comment(ctx, token, parentRef, "Related: "+childRef)
	controller.RecordSCM(s.metrics, providerOf(o.proj), "comment", parentCommentErr)
	if childCommentErr != nil && parentCommentErr != nil {
		// AddSubIssue failed (e.g. cross-org 403, the #328 failure mode) AND
		// both fallback comments failed too (the same token commonly lacks
		// comment perms on the cross-repo/org parent). The relationship is
		// recorded nowhere - make the failed bucket real and alertable.
		s.metrics.IncidentSublink("failed")
		s.log.ErrorContext(ctx, "incident sublink: AddSubIssue and both fallback comments failed; relationship recorded nowhere",
			"action", "incident_sublink", "task", o.task.Name,
			"child", childRef, "parent", parentRef, "result", "failed",
			"link_error", linkErr, "child_comment_error", childCommentErr, "parent_comment_error", parentCommentErr)
		return
	}
	s.metrics.IncidentSublink("fallback_comment")
	s.log.WarnContext(ctx, "incident sublink fell back to cross-reference comment",
		"action", "incident_sublink", "task", o.task.Name,
		"child", childRef, "parent", parentRef, "result", "fallback_comment", "error", linkErr)
}

// --- refine, and the B.3 fold ---------------------------------------------

// foldMembers is refine's point of no return: its STEP 4 DELETES the member
// Tasks, and nothing here can put them back. Every shape check must therefore
// run in the read-only block above it, never after - a late class-B rejection
// releases the claim, and the retry would re-enter the liveness gate looking
// for members that no longer exist. Making the fold resumable is the only
// structural cure; until then, keep the ordering.
func (o *outcomeCtx) refine(p refinePayload) {
	ctx := o.r.Context()
	s := o.s

	if len(p.Folds) == 0 && len(p.Closes) == 0 && len(p.Links) == 0 {
		o.bad("at least one of folds, closes, links must be non-empty", "empty-refine")
		return
	}

	// LIVENESS GATE on closes[] (fix 8): a closes[] target whose controller
	// owner is not this Task has an ACTIVE task working it, and closing it out
	// from under that Task is how two agents end up on one human's thread.
	for _, c := range p.Closes {
		if c.Repo == "" || c.Number == 0 || strings.TrimSpace(c.Reason) == "" {
			o.bad("every closes entry requires repo, number and reason", "bad-closes")
			return
		}
		name := tatarav1alpha1.IssueName(c.Repo, c.Number)
		var iss tatarav1alpha1.Issue
		if err := s.c.Get(ctx, types.NamespacedName{Namespace: s.ns, Name: name}, &iss); err != nil {
			writeClientErr(o.w, err)
			return
		}
		ctrl, owned := own.ControllerOwner(&iss)
		if owned && ctrl != o.task.Name {
			o.conflict("issue has an active task", "close-target-live")
			return
		}
		// THE SELF-TERMINATION GATE (issue #467). Closing an issue THIS Task owns
		// is what stops THIS Task: the close lands on the forge, the mirror flips
		// to closed, and the WS3-I3 stop edge rejects the owner at
		// rejected(issue-closed) - refining is one of the ten stages that carry it.
		// Harmless on its own (the outcome is finishing anyway), fatal next to a
		// fold: the umbrella dies mid-adoption and its members are stranded against
		// a Task that will never run again. That is how 26 Tasks were pinned.
		//
		// Refused HERE, in the read-only block, and NOT reordered around the fold:
		// the two are not atomic and cannot be made so from one HTTP handler, and
		// a rejection after foldMembers is unrecoverable (its step 4 deletes the
		// members). Rejecting first costs nothing - the retry re-validates at once
		// and the agent resubmits the closes on a later turn.
		if owned && ctrl == o.task.Name && len(p.Folds) > 0 {
			o.conflict("closing an owned issue would stop this task mid-fold", "close-target-self-mid-fold")
			return
		}
	}

	// links[] SHAPE is checked HERE, with the other pre-execution validation, and
	// NOT at the loop that consumes it: the fold below DELETES the member Tasks,
	// so a rejection after it is unrecoverable. The release the rejection performs
	// lets the identical retry re-validate at once, and that retry would find its
	// own fold target already gone and 500 forever.
	for _, l := range p.Links {
		if l.Repo == "" || l.Number == 0 {
			o.bad("every links entry requires repo and number", "bad-links")
			return
		}
	}

	// LIVENESS GATE on folds[] (fix 8): a member with a running pod or a live
	// post-approved stage has work in flight.
	members := make([]*tatarav1alpha1.Task, 0, len(p.Folds))
	for _, f := range p.Folds {
		if f.Task == "" {
			o.bad("every folds entry requires task", "bad-folds")
			return
		}
		m, err := s.getTaskCR(ctx, f.Task)
		if err != nil {
			writeClientErr(o.w, err)
			return
		}
		if m.Name == o.task.Name {
			o.bad("a task cannot fold itself", "bad-folds")
			return
		}
		if foldMemberBusy(m) {
			o.conflict("fold target has work in flight", "fold-target-live")
			return
		}
		members = append(members, m)
	}

	// Adopt, verify, THEN delete (B.3). A crash between step 2 and step 4 is
	// safe and idempotent: nothing is lost, and a re-run re-adopts what it
	// already adopted.
	if len(members) > 0 {
		if err := s.foldMembers(ctx, o.proj, o.task, members); err != nil {
			if errors.Is(err, errFoldUnverified) {
				if !o.commit(func(t *tatarav1alpha1.Task) error {
					if err := stage.Park(t, stage.ReasonFoldAdoptionUnverified, s.now()); err != nil {
						return err
					}
					// The step-3 contract says "foldInFlight cleared, members NOT
					// deleted", and until #467 it cleared nothing: the members still
					// existed, so the marker pinned every one of them off the reaper
					// for the whole of the retention window.
					t.Status.FoldInFlight = nil
					t.Status.FoldInFlightSince = nil
					return nil
				}) {
					return
				}
				obs.RestOutcomeRejectedTotal.WithLabelValues(o.kind, stage.ReasonFoldAdoptionUnverified).Inc()
				s.log.ErrorContext(ctx, "restapi: fold adoption could not be verified; the umbrella FAILED and the members were NOT deleted",
					append(reqLogFields(o.r), "task", o.task.Name)...)
				writeError(o.w, http.StatusConflict, "fold adoption could not be verified")
				return
			}
			writeClientErr(o.w, err)
			return
		}
	}

	// closes[] is LIVE-REVALIDATED against SCM immediately before each close:
	// refine may act on a view up to an hour stale.
	if len(p.Closes) > 0 {
		writer, token, ok := s.projectSCMWriterAndToken(o.w, o.r, o.proj)
		if !ok {
			return
		}
		// THE C.1 CLOSE INVARIANT, read ONCE for the whole list.
		open, err := s.openOwnedMRs(ctx, o.task)
		if err != nil {
			writeClientErr(o.w, err)
			return
		}
		for _, c := range p.Closes {
			repo, err := s.repoCR(ctx, o.proj.Name, c.Repo)
			if err != nil {
				writeClientErr(o.w, err)
				return
			}
			st, err := writer.GetIssueState(ctx, repo.Spec.URL, token, c.Number)
			if err != nil {
				s.log.ErrorContext(ctx, "restapi: revalidating a close target failed",
					append(reqLogFields(o.r), "task", o.task.Name, "repo", repo.Name,
						"number", c.Number, "error", err)...)
				writeError(o.w, http.StatusBadGateway, "scm read failed")
				return
			}
			if st.Closed {
				continue
			}
			name := tatarav1alpha1.IssueName(c.Repo, c.Number)
			var iss tatarav1alpha1.Issue
			if err := s.c.Get(ctx, types.NamespacedName{Namespace: s.ns, Name: name}, &iss); err != nil {
				writeClientErr(o.w, err)
				return
			}
			if err := s.closeOrParkIssue(ctx, o.proj, o.task, &iss, c.Reason,
				obs.CloseRefusedPathRefine, open); err != nil {
				writeClientErr(o.w, err)
				return
			}
		}
	}

	// links[] adopt the named artifact as a PLAIN owner of this Task: the link
	// holds the GC open and puts the artifact in the umbrella's bundle.
	for _, l := range p.Links {
		if err := s.linkArtifact(ctx, o.proj, o.task, l); err != nil {
			writeClientErr(o.w, err)
			return
		}
	}

	if !o.commit(func(t *tatarav1alpha1.Task) error {
		if err := stage.Enter(t, nil, tatarav1alpha1.StateDone, "", s.now()); err != nil {
			return err
		}
		t.Status.FoldInFlight = nil
		t.Status.FoldInFlightSince = nil
		return nil
	}) {
		return
	}
	o.ok("refine", "folds", len(p.Folds), "closes", len(p.Closes), "links", len(p.Links))
}

// foldMemberBusy is the B.3 liveness gate: a running pod, or a live
// post-approved stage.
func foldMemberBusy(m *tatarav1alpha1.Task) bool {
	if m.Status.PodName != "" && m.Status.PodStartedAt != nil {
		return true
	}
	switch m.Status.State {
	case tatarav1alpha1.StateRefined, tatarav1alpha1.StateUnderImplementation,
		tatarav1alpha1.StateAwaitingReview, tatarav1alpha1.StateMerged,
		tatarav1alpha1.StateDeployed:
		return true
	}
	return false
}

var errFoldUnverified = errors.New("restapi: fold adoption could not be verified")

// foldMembers runs the B.3 fold sequence IN ORDER:
//
//  1. status.foldInFlight = [M1..Mn]                    (one Status().Update)
//  2. for each artifact A owned by Mi: ONE Update on A - append U
//     (controller=false), rewrite Mi to controller=false, rewrite U to
//     controller=true. The API server rejects two controller=true refs, so the
//     swap MUST be one PUT (own.HandOverController).
//  3. RE-LIST every named artifact; VERIFY U is a solid owner with
//     controller=true. On ANY mismatch: -> failed(fold-adoption-unverified),
//     foldInFlight cleared, members NOT deleted. Nothing is lost; a human sees
//     a failed umbrella.
//  4. only then: delete M1..Mn
//  5. foldInFlight = []
//
// A crash between 2 and 4 is safe and idempotent. A crash after 4 leaves
// foldInFlight set; the reconciler clears it once the members are gone. The
// reaper SKIPS any Task named in a live umbrella's foldInFlight.
func (s *Server) foldMembers(ctx context.Context, proj *tatarav1alpha1.Project, umbrella *tatarav1alpha1.Task, members []*tatarav1alpha1.Task) error {
	names := make([]string, 0, len(members))
	for _, m := range members {
		names = append(names, m.Name)
	}

	spiller := s.spillerForOrNil(proj)

	// STEP 1. The marker is ANCHORED as it is written: foldInFlightSince is what
	// the reaper's TTL measures, and an unanchored marker is the #467 shape -
	// indistinguishable from one written a second ago, so unreleasable.
	key := types.NamespacedName{Namespace: s.ns, Name: umbrella.Name}
	since := metav1.NewTime(s.now())
	if err := objbudget.FitTask(ctx, s.c, spiller, key, func(t *tatarav1alpha1.Task) {
		t.Status.FoldInFlight = names
		t.Status.FoldInFlightSince = &since
	}); err != nil {
		return err
	}

	// STEP 2.
	var adopted []client.Object
	for _, m := range members {
		issues, err := s.ownedIssues(ctx, m)
		if err != nil {
			return err
		}
		for i := range issues {
			iss := issues[i]
			if err := s.adopt(ctx, &iss, m, umbrella); err != nil {
				return err
			}
			// The umbrella's Status.IssueRefs is what every downstream
			// consumer reads (the C.6 approval citation check, the reaper's
			// owned-set, the agent bundle) - NOT ownerRefs. Adoption without
			// this leaves adopted work unguarded and absent from the bundle.
			if err := s.appendTaskRefFor(ctx, proj, umbrella.Name, &iss); err != nil {
				return err
			}
			adopted = append(adopted, &tatarav1alpha1.Issue{
				ObjectMeta: metav1.ObjectMeta{Name: iss.Name, Namespace: s.ns},
			})
		}
		mrs, err := s.ownedMRs(ctx, m)
		if err != nil {
			return err
		}
		for i := range mrs {
			mr := mrs[i]
			if err := s.adopt(ctx, &mr, m, umbrella); err != nil {
				return err
			}
			if err := s.appendTaskRefFor(ctx, proj, umbrella.Name, &mr); err != nil {
				return err
			}
			adopted = append(adopted, &tatarav1alpha1.MergeRequest{
				ObjectMeta: metav1.ObjectMeta{Name: mr.Name, Namespace: s.ns},
			})
		}
	}

	// STEP 3: RE-LIST and VERIFY. Adopt, verify, THEN delete.
	for _, obj := range adopted {
		fresh := obj.DeepCopyObject().(client.Object)
		if err := s.c.Get(ctx, client.ObjectKeyFromObject(obj), fresh); err != nil {
			return errFoldUnverified
		}
		ctrl, ok := own.ControllerOwner(fresh)
		if !ok || ctrl != umbrella.Name {
			return errFoldUnverified
		}
	}

	// STEP 4: only NOW are the members deleted.
	for _, m := range members {
		if err := s.c.Delete(ctx, m); err != nil && client.IgnoreNotFound(err) != nil {
			return err
		}
	}

	// STEP 5.
	return objbudget.FitTask(ctx, s.c, spiller, key, func(t *tatarav1alpha1.Task) {
		t.Status.FoldInFlight = nil
		t.Status.FoldInFlightSince = nil
	})
}

// adopt is the single-PUT controller swap: append the umbrella as a plain
// owner, then hand the controller flag over, in ONE Update. Two controller=true
// refs are a 422 at admission, so the demote and the promote CANNOT be two PUTs.
func (s *Server) adopt(ctx context.Context, obj client.Object, from, to *tatarav1alpha1.Task) error {
	own.AddPlainOwner(obj, to)
	if err := own.HandOverController(obj, from, to); err != nil {
		return err
	}
	if err := s.c.Update(ctx, obj); err != nil {
		return fmt.Errorf("adopt %s onto %s: %w", obj.GetName(), to.Name, err)
	}
	return nil
}

// linkArtifact appends the umbrella as a PLAIN owner of a linked Issue/MR. A
// plain owner's only job is to hold the GC open; the controller flag - and with
// it the authorization to write to the forge - is untouched.
//
// UNLESS THERE IS NO CONTROLLER FLAG TO LEAVE UNTOUCHED (issue #536). That doc
// comment's premise is sound only when a controller owner EXISTS, and against a
// ZERO-OWNER artifact it does not: own.AddPlainOwner appends Controller unset
// whether refs is empty or not, so this used to write the object's FIRST and
// ONLY ownerRef as plain and break B.2 rule 5 outright. A zero-owner artifact is
// not an error state - it is the reaper's designed hand-off to the sweep
// (reaper.go's drop branch: "the next sweep re-mints and adopts") - and that
// window is routinely hours wide, so links[] lands in it. iss-tatara-operator-526
// on 2026-08-07 06:13:16Z: 1 h 41 m after the drop, a refine outcome's links[]
// tripped the repair guard 2.5 s later.
//
// So a link onto an unclaimed artifact CLAIMS it, in ONE Update, exactly as the
// fold's adopt does - never leaving a sole plain owner as a reachable end state.
// Claiming rather than refusing, because the umbrella is definitionally live (it
// is submitting an outcome right now) and so is a valid controller, while
// refusing would fail a legitimate link with no recovery until the sweep runs;
// and claiming rather than promoting some pre-existing plain owner, because that
// is RepairZeroController's last-resort heuristic and it needs a liveness read
// this path does not have. When the umbrella goes terminal the reaper releases
// the artifact back to the sweep on the normal B.5 path.
//
// The write goes through controller.MutateArtifactOwnerRefs (fresh Get +
// RetryOnConflict): it was a bare Update on a cached read, the same discipline
// gap issue #524 documents on the reaper's release.
func (s *Server) linkArtifact(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, l linkRef) error {
	var obj client.Object
	if l.IsPR {
		mr := &tatarav1alpha1.MergeRequest{}
		mr.Name, mr.Namespace = tatarav1alpha1.MergeRequestName(l.Repo, l.Number), s.ns
		if err := controller.MutateArtifactOwnerRefs(ctx, s.c, mr, linkOwnership[tatarav1alpha1.MergeRequest](task)); err != nil {
			return fmt.Errorf("link %s onto %s: %w", mr.Name, task.Name, err)
		}
		obj = mr
	} else {
		iss := &tatarav1alpha1.Issue{}
		iss.Name, iss.Namespace = tatarav1alpha1.IssueName(l.Repo, l.Number), s.ns
		if err := controller.MutateArtifactOwnerRefs(ctx, s.c, iss, linkOwnership[tatarav1alpha1.Issue](task)); err != nil {
			return fmt.Errorf("link %s onto %s: %w", iss.Name, task.Name, err)
		}
		obj = iss
	}
	return s.appendTaskRefFor(ctx, proj, task.Name, obj)
}

// linkOwnership is the owner-ref half of a links[] entry, applied to the FRESH
// copy inside MutateArtifactOwnerRefs: append task as a plain owner, and - only
// when the artifact carries no controller owner at all - claim the flag in the
// SAME Update. See linkArtifact for why claiming is the right answer there.
func linkOwnership[T any, PT interface {
	client.Object
	*T
}](task *tatarav1alpha1.Task) func(PT) error {

	return func(fresh PT) error {
		added := own.AddPlainOwner(fresh, task)
		if _, owned := own.ControllerOwner(fresh); owned {
			if !added {
				return controller.ErrOwnerRefsUnchanged
			}
			return nil
		}
		return own.HandOverController(fresh, nil, task)
	}
}

func (s *Server) appendTaskRefFor(ctx context.Context, proj *tatarav1alpha1.Project, taskName string, obj client.Object) error {
	if _, ok := obj.(*tatarav1alpha1.MergeRequest); ok {
		return s.appendTaskRef(ctx, proj, taskName, "", obj.GetName())
	}
	return s.appendTaskRef(ctx, proj, taskName, obj.GetName(), "")
}
