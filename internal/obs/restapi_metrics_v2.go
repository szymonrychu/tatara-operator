package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Metrics for the contract-C REST surface. Every new failure mode on the agent
// hot path gets one: an agent that cannot terminate its Task, or that is
// silently refused a write, is invisible without them.
var (
	// RestOutcomeAcceptedTotal counts ACCEPTED submit_outcome calls.
	// outcome is the payload's action/verdict/decision.
	RestOutcomeAcceptedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operator_rest_outcome_accepted_total",
		Help: "Accepted submit_outcome calls, by agent kind and outcome.",
	}, []string{"kind", "outcome"})

	// RestOutcomeRejectedTotal counts REFUSED submit_outcome calls. A non-zero
	// head-moved or review-coverage rate is a real signal, not noise: it means
	// agents are reviewing stale heads or under-reporting their coverage.
	RestOutcomeRejectedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operator_rest_outcome_rejected_total",
		Help: "Refused submit_outcome calls, by agent kind and refusal reason.",
	}, []string{"kind", "reason"})

	// RestOwnershipRefusedTotal counts controller-ownership refusals on
	// issue_write / mr_write. ANY value means an agent tried to write to an
	// artifact another Task owns (contract fix 7).
	RestOwnershipRefusedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operator_rest_ownership_refused_total",
		Help: "Forge writes refused because the calling Task does not control the artifact.",
	}, []string{"target"})

	// RestCIReadTotal counts scm_read(kind=ci) calls by whether they left the
	// cluster. It is how the C.2.10 pacer is observed: a live/cached ratio that
	// climbs means agents are polling faster than the 20s floor.
	RestCIReadTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operator_rest_ci_read_total",
		Help: "scm_read(kind=ci) calls, by whether the result came from the forge or the pacer.",
	}, []string{"result"})

	// RestTakeoverErrorTotal counts the internal-error (500) branches inside
	// the OP9 takeover endpoint (mrTakeover), by stage: mint
	// (MintOrUnparkTakeoverTask), ownerref (moving the MR mirror's controller
	// ownership onto the takeover Task), stamp (the ownership-flip status
	// write). Every one of these was log-only before this counter - a real
	// operator-side failure on the takeover hot path (the caller already got
	// a 500) was otherwise invisible.
	RestTakeoverErrorTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operator_rest_takeover_error_total",
		Help: "Internal errors in the OP9 mr-takeover endpoint, by stage.",
	}, []string{"stage"})

	// RestNotesRehydrateFailedTotal counts spilled note batches that
	// task_context(notes=all) could not read back from tatara-memory. The read
	// now serves the notes it HAS instead of 502-ing the whole response, so
	// without this counter a permanently unreadable batch would be invisible:
	// every agent would just quietly get a shorter history than it asked for.
	// Labelled by project because each Project runs its own memory endpoint and
	// one project's outage must not read as the fleet's.
	RestNotesRehydrateFailedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operator_restapi_notes_rehydrate_failed_total",
		Help: "Spilled note batches task_context(notes=all) could not rehydrate from tatara-memory, by project.",
	}, []string{"project"})

	// RestTitleClampedTotal counts agent-supplied ISSUE titles the restapi
	// boundary REWROTE before handing them to the forge (#529), by call site.
	// The clamp is the fix for a GitLab 400 that used to discard a whole
	// submitted outcome, so it is expected to engage routinely - which is exactly
	// why it needs a counter. Without one the platform silently edits what an
	// agent wrote, and the only trace is a log line on the failure path that, by
	// construction, no longer fires.
	//
	// It counts every rewrite, not only the over-cap cut: a title that changed at
	// all is a title the forge did not receive verbatim. site is the boundary the
	// title came through, so a rollout can be read per path rather than as one
	// fleet-wide number.
	RestTitleClampedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operator_rest_title_clamped_total",
		Help: "Agent-supplied issue titles rewritten by the forge-title clamp, by call site (#529).",
	}, []string{"site"})

	// RestTakeoverRefusedTotal counts mr_takeover_request calls REFUSED because
	// the merge request's takeover Task is parked for a reason nothing can
	// resume (#604 review). It is separate from RestTakeoverErrorTotal on
	// purpose: that one counts operator faults (a mint, owner-ref or stamp write
	// that failed), this one counts a CORRECT refusal - and conflating a 409 the
	// design intends with a 500 it does not would make both unreadable.
	//
	// It is the endpoint's only PERMANENT refusal: a merge request nobody can
	// take over until the Task ages out at ParkRetention. Labelled by park
	// reason, because "which terminal is stranding maintainers" is the only
	// interesting question about it - and it is the series that makes this dead
	// end visible to the same sweep that checks whether takeover has ever fired
	// at all.
	//
	// NOT pre-seeded. The label is a park reason, and pre-seeding would mint a
	// series per UnparkNever/UnparkRetired reason for a path that has never once
	// fired in production; the query that matters here is a sum over retention,
	// not a rate on a named reason.
	RestTakeoverRefusedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "operator_rest_takeover_refused_total",
		Help: "mr_takeover_request calls refused because the takeover task is parked unresumably, by park reason.",
	}, []string{"reason"})
)

// The call sites of RestTitleClampedTotal, pre-seeded below so a healthy
// operator exposes a zero baseline from startup rather than a rate alert with no
// series to evaluate on the first clamp.
const (
	TitleSiteIncidentFileIssue = "incident_file_issue"
	TitleSiteBrainstormPropose = "brainstorm_propose"
	TitleSiteIssueCreate       = "issue_create"
	TitleSiteIssueEdit         = "issue_edit"
	// TitleSiteMREdit is the submit_outcome title on its way to the merge
	// request. It is the one non-issue site: a forge caps a merge request title
	// at the same length it caps an issue title, so the same clamp applies.
	TitleSiteMREdit = "mr_edit"
)

func init() {
	metrics.Registry.MustRegister(
		RestOutcomeAcceptedTotal,
		RestOutcomeRejectedTotal,
		RestOwnershipRefusedTotal,
		RestCIReadTotal,
		RestTakeoverErrorTotal,
		RestNotesRehydrateFailedTotal,
		RestTitleClampedTotal,
		RestTakeoverRefusedTotal,
	)
	// Pre-seed the three real takeover-error stage label sets so a healthy
	// operator exposes a zero baseline from startup (metric-wiring audit
	// convention, issue #370) rather than a rate alert with no series to
	// evaluate on the first real failure.
	RestTakeoverErrorTotal.WithLabelValues("mint")
	RestTakeoverErrorTotal.WithLabelValues("ownerref")
	RestTakeoverErrorTotal.WithLabelValues("stamp")
	RestTitleClampedTotal.WithLabelValues(TitleSiteIncidentFileIssue)
	RestTitleClampedTotal.WithLabelValues(TitleSiteBrainstormPropose)
	RestTitleClampedTotal.WithLabelValues(TitleSiteIssueCreate)
	RestTitleClampedTotal.WithLabelValues(TitleSiteIssueEdit)
	RestTitleClampedTotal.WithLabelValues(TitleSiteMREdit)
}
