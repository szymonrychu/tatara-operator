package restapi

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// NO AGENT LEAVES A DIRTY PR (PR B1).
//
// Until this file existed, submit_outcome(kind=implement, action=submitted) and
// submit_outcome(kind=review, verdict=approve) made exactly ONE forge call
// between them - a best-effort GetPRHead - and neither of them ever asked
// whether the change they were accepting could actually merge. A pod that had
// pushed a commit whose pipeline failed sixty seconds later still advanced the
// Task to awaiting-review, spawned a review pod on a red change, and only the
// merge corridor (ci_gate.go, edge.To == merged) ever noticed - three stages and
// one whole review round later.
//
// THE AGENT IS STILL IN-TURN WHEN THIS RUNS, and that is the entire point. This
// evaluation happens in the handler's READ-ONLY VALIDATION PHASE, strictly
// before outcomeCtx.claim(), so a refusal writes NOTHING: no claim, no note, no
// stage transition, no counted turn, no park. The agent gets a structured 409
// naming the axis that failed, fixes it, and submits again - which is the
// cheapest place in the whole platform to fix a red pipeline, because the pod
// that wrote the code is still running with the workspace still checked out.
//
// THREE AXES, AND ONLY ONE OF THEM IS CI.
//
//   - CI at the pushed head. red refuses; pending/running is NOT a verdict and
//     is answered by the caller's CI hold, never by a refusal.
//   - Mergeability. Only MergeStateDirty - a real conflict with the base branch
//     - refuses. MergeStateBlocked is deliberately NOT a refusal: on GitHub it
//     is what a failing REQUIRED check reports, so refusing on it would double
//     count the CI axis and would also refuse a PR blocked purely on a review
//     approval the agent cannot give itself. MergeStateBehind is not a refusal
//     either: "behind base" is a rebase the merge corridor's own self-heal
//     already owns, and it is the normal state of every PR opened against a
//     moving default branch.
//   - Unresolved change requests. See unresolvedChangeRequest.
//
// IT FAILS OPEN ON A READ ERROR. A forge blip must not cost an agent its
// submission: the whole turn's work is in that outcome, and refusing it strands
// the change on a branch nobody will look at again. On any read failure the
// outcome is accepted exactly as it was before this file existed, the failure is
// logged at WARN, and the B3 gate at awaiting-review catches a red pipeline one
// rung later. This mirrors the reviewing-side ci_gate decision verbatim: the
// permissive path is the one that is safe to get wrong.

// The blocker vocabulary. It is CLOSED and it is a cross-repo contract: it lands
// verbatim in the 409 body an agent reads, so a new member is a new thing an
// agent has to be taught to fix.
const (
	blockerCIRed            = "ci-red"
	blockerConflict         = "conflict"
	blockerUnresolvedReview = "unresolved-review"
)

// mrReadiness is one owned MR judged at its LIVE head.
type mrReadiness struct {
	repo     string
	number   int
	headSHA  string
	blockers []string
	// ciPending is true when CI at that head has NOT reached a verdict. It is
	// meaningless when blockers is non-empty: a refusal outranks a hold.
	ciPending bool
	// detail is the one human sentence the agent is shown for this MR.
	detail string
}

// blockedMR is one refused MR on the wire.
type blockedMR struct {
	Repo     string   `json:"repo"`
	Number   int      `json:"number"`
	HeadSHA  string   `json:"headSHA,omitempty"`
	Blockers []string `json:"blockers"`
	Detail   string   `json:"detail"`
}

// prNotReadyResponse is the STRUCTURED 409 body a refused submission gets. It is
// shaped after headMovedResponse for the same reason: tatara-cli keys on the
// `reason` field to render a 409 as actionable guidance rather than a hard tool
// failure, and `error` carries the same sentence for any client that does not.
//
// The field names are load-bearing across repos.
type prNotReadyResponse struct {
	Reason  string      `json:"reason"`
	Error   string      `json:"error"`
	Blocked []blockedMR `json:"blocked"`
	Message string      `json:"message"`
}

// unresolvedChangeRequest is the third axis, and it is NOT a read of forge
// thread-resolution state. That was the obvious implementation and it is a
// deadlock: nothing in this platform ever resolves a review thread - there is no
// resolve primitive on SCMWriter, no mr_write action for it, and the operator's
// own PostReview only ever OPENS threads - so an agent refused for unresolved
// threads could never clear the refusal, and every PR that ever took one
// request_changes round would be permanently unsubmittable.
//
// What IS observable, and is what the requirement actually means, is whether the
// change requests have been ANSWERED WITH CODE. An MR carrying an accepted
// request_changes verdict (status=needs-changes) whose live head is STILL the
// head that was reviewed has had nothing pushed at it since the findings landed.
// That is a submission claiming to have addressed a review while the diff the
// reviewer read is byte-identical, and the agent clears it the only way it can:
// by pushing the fix.
//
// It is silent when ReviewedSHA is empty (never reviewed) and when the head has
// moved (the fix is pushed; judging whether it is a GOOD fix is the next review
// round's job, not this gate's).
func unresolvedChangeRequest(mr *tatarav1alpha1.MergeRequest, liveHead string) bool {
	return mr.Status.Status == "needs-changes" &&
		mr.Status.ReviewedSHA != "" &&
		liveHead != "" &&
		liveHead == mr.Status.ReviewedSHA
}

// readinessScope names WHICH axes a call site judges. The CI and conflict axes
// are universal; the change-request axis is not.
type readinessScope struct{ changeRequests bool }

var (
	// submitScope is the implement submission: all three axes.
	submitScope = readinessScope{changeRequests: true}
	// approveScope is the review approval, and it deliberately DROPS the
	// change-request axis. A reviewer cannot push, so refusing its approval for
	// "nothing was pushed since the last round" would leave it with no legal
	// outcome on that PR at all - and the in-cluster reviewer is documented-flaky,
	// so a round that decides the previous round's findings were wrong is a real
	// and correct outcome, not a contract violation.
	approveScope = readinessScope{changeRequests: false}
)

// evaluateReadiness judges every open owned MR at its live head. ok=false means
// the forge could not be read at all and the caller must FAIL OPEN.
//
// It writes NOTHING - not to the forge, not to etcd - and it must stay that way:
// it runs before the claim, so every one of its exits has to be a pure no-op.
func (o *outcomeCtx) evaluateReadiness(ctx context.Context,
	open []tatarav1alpha1.MergeRequest, scope readinessScope) ([]mrReadiness, bool) {

	s := o.s
	// The discardWriter is the same fail-open device the record_bot_head stamp
	// uses: projectSCMWriterAndToken writes an HTTP error to whatever
	// ResponseWriter it is handed, and o.w must carry exactly ONE response.
	writer, token, ok := s.projectSCMWriterAndToken(&discardWriter{}, o.r, o.proj)
	if !ok {
		s.log.WarnContext(ctx, "restapi: pr readiness skipped: scm writer/token resolution failed",
			"action", "pr_readiness_skip", "reason", "scm_resolution", "task", o.task.Name, "kind", o.kind)
		return nil, false
	}
	out := make([]mrReadiness, 0, len(open))
	for i := range open {
		mr := &open[i]
		repo, err := s.repoCR(ctx, o.proj.Name, mr.Spec.RepositoryRef)
		if err != nil {
			s.log.WarnContext(ctx, "restapi: pr readiness skipped: repository lookup failed",
				"action", "pr_readiness_skip", "reason", "repo_lookup", "task", o.task.Name,
				"mr", mr.Name, "error", err)
			return nil, false
		}
		st, err := writer.GetPRState(ctx, repo.Spec.URL, token, mr.Spec.Number)
		if err != nil {
			s.log.WarnContext(ctx, "restapi: pr readiness skipped: live pr state read failed",
				"action", "pr_readiness_skip", "reason", "get_pr_state", "task", o.task.Name,
				"mr", mr.Name, "error", err)
			return nil, false
		}
		if st.Merged || st.Closed {
			// A terminal MR has no readiness to judge and cannot be fixed by the
			// agent. The terminal carve-outs upstream own that shape.
			continue
		}
		r := mrReadiness{repo: mr.Spec.RepositoryRef, number: mr.Spec.Number, headSHA: st.HeadSHA}
		var details []string
		switch st.CIStatus {
		case "failure":
			r.blockers = append(r.blockers, blockerCIRed)
			details = append(details, fmt.Sprintf(
				"CI is RED at the pushed head %s: read the failing job (`scm_read(kind=\"ci\", repo=%q, number=%d)`), fix it, push, and submit again",
				shortSHA(st.HeadSHA), mr.Spec.RepositoryRef, mr.Spec.Number))
		case "pending":
			r.ciPending = true
		}
		// GetMergeState is a SECOND call and it is worth it: PRState carries no
		// mergeability at all (see its doc comment - GitHub's mergeable_state is
		// deliberately not part of it), so without this the conflict axis does not
		// exist. A failure here is NOT fatal to the other two axes: an unknown
		// merge state is the forge saying "still computing", which is the answer
		// on any PR pushed seconds ago.
		ms, err := writer.GetMergeState(ctx, repo.Spec.URL, token, mr.Spec.Number)
		switch {
		case err != nil:
			s.log.WarnContext(ctx, "restapi: pr readiness: merge state unreadable; the conflict axis is skipped for this MR",
				"action", "pr_readiness_merge_state_failed", "task", o.task.Name,
				"mr", mr.Name, "error", err)
		case ms == scm.MergeStateDirty:
			r.blockers = append(r.blockers, blockerConflict)
			details = append(details, fmt.Sprintf(
				"the branch CONFLICTS with its base: rebase or merge the base into %s, resolve every conflict, push, and submit again",
				mr.Status.HeadBranch))
		}
		if scope.changeRequests && unresolvedChangeRequest(mr, st.HeadSHA) {
			r.blockers = append(r.blockers, blockerUnresolvedReview)
			details = append(details, fmt.Sprintf(
				"the review requested changes at %s and the head has not moved since: nothing was pushed to answer the findings",
				shortSHA(mr.Status.ReviewedSHA)))
		}
		r.detail = strings.Join(details, "; ")
		out = append(out, r)
	}
	return out, true
}

// refuseNotReady writes the structured 409 and returns true when any MR is
// blocked. It is a PURE no-op otherwise.
//
// IT IS IDEMPOTENT BY CONSTRUCTION. It never claims the fingerprint and never
// writes, so an identical resubmit re-evaluates against the LIVE forge rather
// than replaying a cached verdict - which is exactly what an agent that has just
// pushed a fix needs, and what a cached 200 replay would have denied it.
func (o *outcomeCtx) refuseNotReady(ctx context.Context, rd []mrReadiness, verb string) bool {
	var blocked []blockedMR
	axes := map[string]bool{}
	for _, r := range rd {
		if len(r.blockers) == 0 {
			continue
		}
		for _, b := range r.blockers {
			axes[b] = true
		}
		blocked = append(blocked, blockedMR{
			Repo: r.repo, Number: r.number, HeadSHA: r.headSHA,
			Blockers: append([]string(nil), r.blockers...), Detail: r.detail,
		})
	}
	if len(blocked) == 0 {
		return false
	}
	names := make([]string, 0, len(axes))
	for a := range axes {
		names = append(names, a)
	}
	sort.Strings(names)

	var b strings.Builder
	fmt.Fprintf(&b, "This %s was NOT accepted: the merge request(s) it names are not ready to be handed on (%s). ",
		verb, strings.Join(names, ", "))
	for _, m := range blocked {
		fmt.Fprintf(&b, "%s!%d - %s. ", m.Repo, m.Number, m.Detail)
	}
	b.WriteString("Your turn is NOT over and nothing was recorded: fix every item above, push, and submit the same outcome again.")
	msg := b.String()

	// The counter reason is the AXIS SET, not a flat "not-ready": the whole
	// operational question this refusal raises is which of the three keeps
	// firing, and a single label answers none of it.
	obs.RestOutcomeRejectedTotal.WithLabelValues(o.kind, "not-ready:"+strings.Join(names, "+")).Inc()
	// release() is a documented no-op before the claim; it is called for the same
	// reason head-moved calls it - so that a future edit which moves the claim
	// earlier cannot silently leave this path holding one.
	o.release()
	o.s.log.InfoContext(ctx, "restapi: outcome refused: the merge request is not ready to hand on",
		append(reqLogFields(o.r), "action", "outcome_not_ready", "task", o.task.Name,
			"kind", o.kind, "axes", strings.Join(names, ","), "mrs", len(blocked))...)
	writeJSON(o.w, http.StatusConflict, prNotReadyResponse{
		Reason: "pr-not-ready", Error: msg, Blocked: blocked, Message: msg,
	})
	return true
}

// anyCIPending reports whether any judged MR is still waiting on a CI verdict.
func anyCIPending(rd []mrReadiness) bool {
	for _, r := range rd {
		if r.ciPending {
			return true
		}
	}
	return false
}

// shortSHA renders a commit for prose. Full SHAs make the refusal message
// unreadable and the agent has the full one in the blocked[] entry.
func shortSHA(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}
