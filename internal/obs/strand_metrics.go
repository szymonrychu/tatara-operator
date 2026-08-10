package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// IssueCloseRefusedTotal counts issue closes the C.1 CLOSE INVARIANT refused
// because the owning Task still had a live MergeRequest, so the issue was
// PARKED instead. `path` names WHICH close path was refused - the shapes are
// independent and their runbooks differ:
//
//	gate-rejected  the implement gate's rejected(declined) arm (outcome.go)
//	refine-closes  a refine outcome's closes[] list (outcome.go)
//
// Steady state is ZERO. A non-zero rate is not an error - it is the invariant
// doing its job - but a SUSTAINED one means agents are routinely declining
// issues while their own PRs are still open, which is a PR-B (agent owns its
// PR until it is clean) regression, not a C.1 one.
var IssueCloseRefusedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_issue_close_refused_total",
	Help: "Issue closes refused because the owning task still had an open merge request, by project and close path (contract C.1).",
}, []string{"project", "path"})

// TerminalIssueReleasedTotal counts still-open Issues that a Task's TERMINAL
// transition released at the transition choke point: terminal notice posted,
// tatara-parked stamped, ownerRef dropped.
//
// It is the observable half of "a terminal task never silently drops an open
// issue". Before this existed, a `done` Task's still-open issue got NOTHING -
// reapDelivered never ran the terminal sequence - and since 4ee5c7f the sweep
// released the ownerRef anyway, so MintStage fell through its label gate and
// re-minted the issue ACTIVE, with a pod, on every pass, unbounded. That loop
// was invisible: every individual mint looked like an ordinary intake.
//
// `outcome` is "released" for the release that happened, or "error" when the
// forge write failed and the reaper's blocking backstop still owes it.
var TerminalIssueReleasedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_terminal_issue_released_total",
	Help: "Still-open issues released by a task's terminal transition, by task state and outcome (contract B.6).",
}, []string{"state", "outcome"})

// StrandedParkReentryTotal counts the C.3 AUTOMATIC re-entries of a park no
// human ever un-parks, by park reason and outcome:
//
//	reentered  the stranded park was collected early and the issue re-minted
//	exhausted  the per-issue automatic-re-entry budget is spent; the issue is
//	           labelled tatara-parked and left in the backlog as a REAL dead end
//
// BOTH members matter and neither is an error. `reentered` climbing without a
// matching `exhausted` is the platform recovering; `exhausted` climbing is the
// bound holding, which is the entire safety argument for letting UnparkNever
// stop meaning permanent. What WOULD be a bug is `reentered` climbing past
// MaxAutoReentries per issue, which is what the bound - persisted on the Issue
// CR, because the Task is reaped on every lap - exists to make impossible.
var StrandedParkReentryTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_stranded_park_reentry_total",
	Help: "Automatic re-entries of a no-re-entry park, by project, park reason and outcome (contract F.6).",
}, []string{"project", "park_reason", "outcome"})

// The closed {path} vocabulary of IssueCloseRefusedTotal and the closed
// {outcome} vocabularies of the other two counters above.
const (
	CloseRefusedPathGate   = "gate-rejected"
	CloseRefusedPathRefine = "refine-closes"

	TerminalIssueReleased       = "released"
	TerminalIssueReleaseError   = "error"
	StrandedParkReentered       = "reentered"
	StrandedParkBudgetExhausted = "exhausted"
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		IssueCloseRefusedTotal,
		TerminalIssueReleasedTotal,
		StrandedParkReentryTotal,
	)
}
