package restapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
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

type implementPayload struct {
	Action             string   `json:"action"`
	Title              string   `json:"title,omitempty"`
	Body               string   `json:"body,omitempty"`
	ChangeSignificance string   `json:"changeSignificance,omitempty"`
	MergeOrder         []string `json:"mergeOrder,omitempty"`
	Reason             string   `json:"reason,omitempty"`
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
	Line     int    `json:"line,omitempty"`
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

type clarifyPayload struct {
	Decision string `json:"decision"`
	Reason   string `json:"reason"`
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

type incidentPayload struct {
	Action     string         `json:"action"`
	AlertRules []string       `json:"alertRules"`
	Issue      *incidentIssue `json:"issue,omitempty"`
	Reason     string         `json:"reason"`
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
		writeDecodeError(w, r, err)
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

	// IDEMPOTENCY FIRST, before the terminal-stage and kind gates, and CLAIMED
	// atomically before any forge/child-mint side effect (C7): the handler runs
	// on every replica, so a stale top-of-handler read of a stamped-only-at-commit
	// fingerprint admits two concurrent identical POSTs straight through to the
	// same forge write / child-mint / ReviewRounds increment. The claim is a
	// LEASE: claimOutcomeFingerprint re-Gets the Task fresh and, under
	// RetryOnConflict, reports whether the fingerprint is COMMITTED (replay),
	// claimed and IN FLIGHT on another replica (409 retry), or free/orphaned (it
	// stamps and we proceed). A TTL-stopped pod's retry of a COMMITTED outcome
	// must not 409 the Task into failure - and by the time it retries, both
	// status.stage and status.agentKind have moved on, so both gates below would
	// refuse it.
	fp := outcomeFingerprint(env.Kind, env.Payload)
	key := types.NamespacedName{Namespace: s.ns, Name: name}
	task, state, err := claimOutcomeFingerprint(ctx, s.c, key, fp, s.now())
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

	// oc is built BEFORE the two gates so both can release the claim they hold:
	// they run before any kind handler and stamp nothing, so they are class B.
	// oc.proj is nil until the lookup below; neither gate reads it.
	oc := &outcomeCtx{s: s, w: w, r: r, task: task, fp: fp, kind: env.Kind}
	if tatarav1alpha1.StageTerminal(task) || task.Status.Stage == tatarav1alpha1.StageDelivered {
		oc.conflict("task is in a terminal stage", "terminal-stage")
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
	case "implement", "documentation":
		var p implementPayload
		if !oc.decode(env.Payload, &p) {
			return
		}
		oc.implement(p)
	case "review":
		var p reviewPayload
		if !oc.decode(env.Payload, &p) {
			return
		}
		oc.review(p)
	case "clarify":
		var p clarifyPayload
		if !oc.decode(env.Payload, &p) {
			return
		}
		oc.clarify(p)
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

// claimOutcomeFingerprint atomically claims fp against a FRESH re-read of the
// Task, before any forge/child-mint side effect (C7), and reports which of the
// three states it found.
//
// THE CLAIM-FIRST ORDERING IS LOAD-BEARING AND MUST NOT MOVE. The handler runs on
// every replica, so a stale top-of-handler read of a stamped-only-at-commit
// fingerprint admits two concurrent identical POSTs straight through to the same
// forge write / child-mint / ReviewRounds increment. Optimistic concurrency lets
// exactly one of two concurrent identical POSTs win the Update; the loser now
// reads back a fresh bare claim and is told to RETRY (409) rather than being
// answered 200 with nothing done.
//
// The claim is a LEASE, not a tombstone: Reason carries claimed-vs-committed and
// LastTransitionTime carries the expiry, both on fields that already existed.
func claimOutcomeFingerprint(ctx context.Context, c client.Client, key types.NamespacedName,
	fp string, now time.Time) (*tatarav1alpha1.Task, outcomeClaimState, error) {
	fresh := &tatarav1alpha1.Task{}
	state := claimWon
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		state = claimWon
		if err := c.Get(ctx, key, fresh); err != nil {
			return err
		}
		if cond := tatarav1alpha1.OutcomeCondition(fresh); cond != nil &&
			cond.Status == metav1.ConditionTrue && cond.Message == fp {
			switch {
			case cond.Reason != tatarav1alpha1.OutcomeReasonClaimed:
				state = claimCommitted
				return nil
			case now.Sub(cond.LastTransitionTime.Time) < tatarav1alpha1.OutcomeClaimTTL:
				state = claimInFlight
				return nil
			}
			// An ORPHANED STUB: the process died between the claim and the commit,
			// so no release ever ran. RE-CLAIM it - refresh LastTransitionTime and
			// fall through. Two replicas racing the re-claim is safe: one wins the
			// Update, the other conflicts, re-Gets, sees a claim younger than the
			// TTL and answers claimInFlight.
		}
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
		writeDecodeError(o.w, o.r, err)
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
	ctx := o.r.Context()
	s := o.s
	key := types.NamespacedName{Namespace: s.ns, Name: o.task.Name}
	var mutErr error
	from := o.task.Status.Stage
	var to, toReason string
	err := objbudget.FitTask(ctx, s.c, s.spillerForOrNil(o.proj), key, func(t *tatarav1alpha1.Task) {
		if mutate != nil {
			if err := mutate(t); err != nil {
				mutErr = err
				return
			}
		}
		to, toReason = t.Status.Stage, t.Status.StageReason
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
	if to != from {
		s.log.InfoContext(ctx, "stage transition",
			append(reqLogFields(o.r), "action", "stage_transition", "task", o.task.Name,
				"from", from, "to", to, "stage_reason", toReason)...)
	}
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
			"kind", o.kind, "outcome", action, "stage", fresh.Status.Stage), fields...)...)
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

	switch p.Action {
	case "submitted":
		if strings.TrimSpace(p.Title) == "" || strings.TrimSpace(p.Body) == "" ||
			strings.TrimSpace(p.ChangeSignificance) == "" {
			o.bad("action=submitted requires title, body and changeSignificance", "missing-field")
			return
		}
		if p.Reason != "" {
			o.bad("reason is only for action=declined", "unexpected-field")
			return
		}
		if !validChangeSignificance[p.ChangeSignificance] {
			o.bad("changeSignificance must be one of major, minor, patch", "bad-significance")
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

	mrs, err := s.ownedMRs(ctx, o.task)
	if err != nil {
		writeClientErr(o.w, err)
		return
	}

	if p.Action == "declined" {
		to, reason := tatarav1alpha1.StageParked, stage.ReasonImplementDeclined
		if o.kind == "documentation" {
			// A declined documentation batch is DELIVERED, not parked: there was
			// nothing to document (F.3).
			to, reason = tatarav1alpha1.StageDelivered, ""
		}
		if !o.commit(func(t *tatarav1alpha1.Task) error {
			// stage.Enter FIRST: objbudget.FitTask persists whatever this
			// closure mutated even when it returns an error, so a note appended
			// before a REFUSED transition would land - and, now that a class-B
			// rejection releases the claim, land AGAIN on every retry.
			if err := stage.Enter(t, mrs, to, reason, s.now()); err != nil {
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
		if err := stage.Enter(t, mrs, tatarav1alpha1.StageReviewing, "", s.now()); err != nil {
			return err
		}
		agentNote(t, o.kind, "note", "submitted: "+p.Title+"\n\n"+p.Body, s.now())
		return nil
	}) {
		return
	}
	o.ok("submitted", "merge_order", strings.Join(p.MergeOrder, ","),
		"change_significance", p.ChangeSignificance)
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
			Path: f.Path, Line: f.Line, Body: f.Body, Severity: f.Severity,
		})
	}
	return out
}

// --- clarify --------------------------------------------------------------

func (o *outcomeCtx) clarify(p clarifyPayload) {
	ctx := o.r.Context()
	s := o.s

	switch p.Decision {
	case "implement", "close", "discuss":
	default:
		o.bad("decision must be one of implement, close, discuss", "bad-decision")
		return
	}
	if strings.TrimSpace(p.Reason) == "" {
		o.bad("reason is required on every clarify decision", "missing-field")
		return
	}

	mrs, err := s.ownedMRs(ctx, o.task)
	if err != nil {
		writeClientErr(o.w, err)
		return
	}

	switch p.Decision {
	case "discuss":
		if !o.commit(func(t *tatarav1alpha1.Task) error {
			if err := stage.Enter(t, mrs, tatarav1alpha1.StageParked, stage.ReasonAwaitingHuman, s.now()); err != nil {
				return err
			}
			agentNote(t, o.kind, "note", "discuss: "+p.Reason, s.now())
			return nil
		}) {
			return
		}
		o.ok("discuss")
		return
	case "close":
		// The OPERATOR closes the issue; the agent never does it from here.
		// The close is queued as a pending comment intent on every owned Issue,
		// drained by the Issue reconciler.
		issues, err := s.ownedIssues(ctx, o.task)
		if err != nil {
			writeClientErr(o.w, err)
			return
		}
		for i := range issues {
			if err := s.queueIssueClose(ctx, o.proj, &issues[i], o.task.Name, p.Reason); err != nil {
				writeClientErr(o.w, err)
				return
			}
		}
		if !o.commit(func(t *tatarav1alpha1.Task) error {
			if err := stage.Enter(t, mrs, tatarav1alpha1.StageRejected, stage.ReasonDeclined, s.now()); err != nil {
				return err
			}
			agentNote(t, o.kind, "note", "close: "+p.Reason, s.now())
			return nil
		}) {
			return
		}
		o.ok("close")
		return
	}

	// decision=implement. APPROVAL IS IN NO SCHEMA: the agent reports its
	// decision and the operator INDEPENDENTLY verifies the C.6 grammar - both
	// the TEXT (anchored whole-line) and the SCOPE (EVERY owned Issue, not one:
	// fix H9).
	issues, err := s.ownedIssues(ctx, o.task)
	if err != nil {
		writeClientErr(o.w, err)
		return
	}
	granted, evidence := s.verifyApprovalScope(ctx, o.proj, issues)
	if !granted {
		if !o.commit(func(t *tatarav1alpha1.Task) error {
			if err := stage.Enter(t, mrs, tatarav1alpha1.StageParked, stage.ReasonIdentityUnverified, s.now()); err != nil {
				return err
			}
			agentNote(t, o.kind, "note", "implement: "+p.Reason, s.now())
			return nil
		}) {
			return
		}
		s.log.WarnContext(ctx, "restapi: clarify reported approval but the C.6 grammar did not pass on every owned issue",
			append(reqLogFields(o.r), "task", o.task.Name, "issues", len(issues))...)
		o.ok("implement-unverified")
		return
	}

	for i := range issues {
		iss := &issues[i]
		ev := evidence[iss.Name]
		// Count the auto-approval TRANSITION (an issue not already approved): the
		// last human gate is being removed, so it must be queryable without
		// log-scraping (hard rule 13). This is the primary auto-approve site - a
		// brainstorm/incident proposal reaching implement via clarify submit.
		if ev != nil && ev.Auto && iss.Status.Status != "approved" {
			if kind := tatarav1alpha1.ProposalKindFromBody(iss.Status.Body); kind != "" {
				s.metrics.AutoApproveTotal(kind)
			}
		}
		key := types.NamespacedName{Namespace: s.ns, Name: iss.Name}
		if err := objbudget.FitIssue(ctx, s.c, s.spillerForOrNil(o.proj), key, func(is *tatarav1alpha1.Issue) {
			is.Status.Status = "approved"
			is.Status.Approval = ev
		}); err != nil {
			writeClientErr(o.w, err)
			return
		}
	}
	if !o.commit(func(t *tatarav1alpha1.Task) error {
		if err := stage.Enter(t, mrs, tatarav1alpha1.StageApproved, "", s.now()); err != nil {
			return err
		}
		agentNote(t, o.kind, "note", "implement: "+p.Reason, s.now())
		return nil
	}) {
		return
	}
	o.ok("implement", "issues", len(issues))
}

// verifyApprovalScope runs the C.6 grammar over EVERY owned Issue. The empty
// set is NOT a licence: a clarify Task with no Issue has nothing to approve and
// is refused.
//
// A nil verifier FAILS CLOSED.
func (s *Server) verifyApprovalScope(ctx context.Context, proj *tatarav1alpha1.Project,
	issues []tatarav1alpha1.Issue) (bool, map[string]*tatarav1alpha1.ApprovalEvidence) {
	if len(issues) == 0 || s.approval == nil {
		return false, nil
	}
	out := make(map[string]*tatarav1alpha1.ApprovalEvidence, len(issues))
	for i := range issues {
		ev, ok := s.approval.VerifyApproval(ctx, proj, &issues[i])
		if !ok {
			return false, nil
		}
		out[issues[i].Name] = ev
	}
	return true, out
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
			RequestID: requestID, Action: "comment", Body: closeIntentBody(reason),
		})
	})
}

func closeIntentBody(reason string) string {
	return "<!-- tatara-close -->\n" + reason
}

// --- brainstorm -----------------------------------------------------------

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
	case "skip":
		if strings.TrimSpace(p.Reason) == "" {
			o.bad("action=skip requires a non-empty reason", "missing-field")
			return
		}
	default:
		o.bad("action must be one of propose, skip", "bad-action")
		return
	}

	if p.Action == "skip" {
		// documentedBy stays EMPTY (fix 25): a cron brainstorm that correctly
		// says "nothing novel" must not spawn a docs pod, a docs PR about
		// nothing, a review, a merge and a release - every day.
		if !o.commit(func(t *tatarav1alpha1.Task) error {
			if err := stage.Enter(t, nil, tatarav1alpha1.StageDelivered, "", s.now()); err != nil {
				return err
			}
			agentNote(t, o.kind, "note", "skip: "+p.Reason, s.now())
			return nil
		}) {
			return
		}
		o.ok("skip")
		return
	}

	// Each proposal becomes its OWN new clarify Task, owning its OWN Issue
	// (F.3). brainstorm files issues through submit_outcome, not issue_write,
	// so the proposal cap and dedup still apply.
	writer, token, ok := s.projectSCMWriterAndToken(o.w, o.r, o.proj)
	if !ok {
		return
	}
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
		created, err := writer.CreateIssue(ctx, repo.Spec.URL, token, scm.IssueReq{Title: pr.Title, Body: body})
		controller.RecordSCM(s.metrics, providerOf(o.proj), "create_issue", err)
		if err != nil {
			s.log.ErrorContext(ctx, "restapi: filing a brainstorm proposal failed",
				append(reqLogFields(o.r), "task", o.task.Name, "repo", repo.Name, "error", err)...)
			writeError(o.w, http.StatusBadGateway, "scm write failed")
			return
		}
		number := issueRefNumber(created.Ref)
		if number == 0 {
			writeError(o.w, http.StatusBadGateway, "scm returned no issue number")
			return
		}
		child, err := s.mintClarifyTask(ctx, o.proj, repo, pr, number, created.URL)
		if err != nil {
			writeClientErr(o.w, err)
			return
		}
		if err := s.mintIssueCR(ctx, o.proj, repo, child, number, created.URL, pr.Title, body, nil); err != nil {
			writeClientErr(o.w, err)
			return
		}
		spawned = append(spawned, child.Name)
	}

	if !o.commit(func(t *tatarav1alpha1.Task) error {
		if err := stage.Enter(t, nil, tatarav1alpha1.StageDelivered, "", s.now()); err != nil {
			return err
		}
		agentNote(t, o.kind, "note", "proposed: "+strings.Join(spawned, ", "), s.now())
		return nil
	}) {
		return
	}
	o.ok("propose", "spawned", strings.Join(spawned, ","))
}

// mintClarifyTask creates the clarify Task a brainstorm proposal becomes.
func (s *Server) mintClarifyTask(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, pr proposalPayload, number int, url string) (*tatarav1alpha1.Task, error) {
	name := tatarav1alpha1.TaskName(proj.Name, "clarify", s.now(), rand.String(5))
	t := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.ns},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: repo.Name, Kind: "clarify",
			Goal: pr.Title + "\n\n" + pr.Body,
			Source: &tatarav1alpha1.TaskSource{
				Provider: providerOf(proj), IssueRef: issueRef(repo, number),
				URL: url, Number: number,
			},
		},
	}
	if err := s.c.Create(ctx, t); err != nil {
		return nil, fmt.Errorf("create clarify task %s: %w", name, err)
	}
	return t, nil
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
		if p.Issue == nil || p.Issue.Repo == "" ||
			strings.TrimSpace(p.Issue.Title) == "" || strings.TrimSpace(p.Issue.Body) == "" {
			o.bad("action=file_issue requires issue.repo, issue.title and issue.body", "missing-field")
			return
		}
		if p.Issue.Parent != nil && (p.Issue.Parent.Repo == "" || p.Issue.Parent.Number == 0) {
			o.bad("issue.parent requires repo and number", "bad-parent")
			return
		}
	case "false_positive":
		if p.Issue != nil {
			o.bad("issue is only for action=file_issue", "unexpected-field")
			return
		}
	default:
		o.bad("action must be one of file_issue, false_positive", "bad-action")
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
			if err := stage.Enter(t, nil, tatarav1alpha1.StageRejected, stage.ReasonFalsePositive, s.now()); err != nil {
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

	// The tracker Issue is created under THIS Task (F.3), and the Task then
	// goes to clarifying: the human decides whether it is worked.
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
	issueReq := scm.IssueReq{Title: p.Issue.Title, Body: body}
	if ruleKey != "" {
		issueReq.Labels = append(issueReq.Labels, forgeAlertRulePrefix+ruleKey)
	}
	created, err := writer.CreateIssue(ctx, repo.Spec.URL, token, issueReq)
	controller.RecordSCM(s.metrics, providerOf(o.proj), "create_issue", err)
	if err != nil {
		s.log.ErrorContext(ctx, "restapi: filing the incident tracker issue failed",
			append(reqLogFields(o.r), "task", o.task.Name, "repo", repo.Name, "error", err)...)
		writeError(o.w, http.StatusBadGateway, "scm write failed")
		return
	}
	number := issueRefNumber(created.Ref)
	if number == 0 {
		writeError(o.w, http.StatusBadGateway, "scm returned no issue number")
		return
	}
	var crLabels map[string]string
	if ruleKey != "" {
		crLabels = map[string]string{queue.LabelAlertRuleKey: ruleKey}
	}
	if err := s.mintIssueCR(ctx, o.proj, repo, o.task, number, created.URL, p.Issue.Title, body, crLabels); err != nil {
		writeClientErr(o.w, err)
		return
	}

	if p.Issue.Parent != nil {
		s.linkIncidentParent(ctx, o, writer, token, created.Ref, p.Issue.Parent)
	}

	if !o.commit(func(t *tatarav1alpha1.Task) error {
		if err := stage.Enter(t, nil, tatarav1alpha1.StageClarifying, "", s.now()); err != nil {
			return err
		}
		agentNote(t, o.kind, "note", "file_issue: "+p.Reason, s.now())
		return nil
	}) {
		return
	}
	o.ok("file_issue", "repo", repo.Name, "number", number)
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
		if ctrl, ok := own.ControllerOwner(&iss); ok && ctrl != o.task.Name {
			o.conflict("issue has an active task", "close-target-live")
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
					return stage.Enter(t, nil, tatarav1alpha1.StageFailed,
						stage.ReasonFoldAdoptionUnverified, s.now())
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
			if err := s.queueIssueClose(ctx, o.proj, &iss, o.task.Name, c.Reason); err != nil {
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
		if err := stage.Enter(t, nil, tatarav1alpha1.StageDelivered, "", s.now()); err != nil {
			return err
		}
		t.Status.FoldInFlight = nil
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
	switch m.Status.Stage {
	case tatarav1alpha1.StageApproved, tatarav1alpha1.StageImplementing,
		tatarav1alpha1.StageReviewing, tatarav1alpha1.StageMerging,
		tatarav1alpha1.StageDeploying:
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

	// STEP 1.
	key := types.NamespacedName{Namespace: s.ns, Name: umbrella.Name}
	if err := objbudget.FitTask(ctx, s.c, spiller, key, func(t *tatarav1alpha1.Task) {
		t.Status.FoldInFlight = names
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
			// consumer reads (the C.6 approval grammar, the reaper's
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
func (s *Server) linkArtifact(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, l linkRef) error {
	var obj client.Object
	if l.IsPR {
		obj = &tatarav1alpha1.MergeRequest{}
		if err := s.c.Get(ctx, types.NamespacedName{
			Namespace: s.ns, Name: tatarav1alpha1.MergeRequestName(l.Repo, l.Number),
		}, obj); err != nil {
			return err
		}
	} else {
		obj = &tatarav1alpha1.Issue{}
		if err := s.c.Get(ctx, types.NamespacedName{
			Namespace: s.ns, Name: tatarav1alpha1.IssueName(l.Repo, l.Number),
		}, obj); err != nil {
			return err
		}
	}
	if !own.AddPlainOwner(obj, task) {
		return nil
	}
	if err := s.c.Update(ctx, obj); err != nil {
		return fmt.Errorf("link %s onto %s: %w", obj.GetName(), task.Name, err)
	}
	return s.appendTaskRefFor(ctx, proj, task.Name, obj)
}

func (s *Server) appendTaskRefFor(ctx context.Context, proj *tatarav1alpha1.Project, taskName string, obj client.Object) error {
	if _, ok := obj.(*tatarav1alpha1.MergeRequest); ok {
		return s.appendTaskRef(ctx, proj, taskName, "", obj.GetName())
	}
	return s.appendTaskRef(ctx, proj, taskName, obj.GetName(), "")
}
