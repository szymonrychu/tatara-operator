package v1alpha1

import (
	"sort"
	"strings"
)

// THE CI-DECLINE EVIDENCE VOCABULARY. It is deliberately NOT
// MergeRequestStatus.CIStatus: this is an AGGREGATE over the merge requests ONE
// Task owns, so it needs a value the per-MR enum cannot express ("every one of
// them is green"), and it must never be validated against that enum.
//
// Three members, closed. They are what tatara.dev/decline-ci carries.
const (
	// CIEvidenceRed: at least one owned merge request read `red`. It is the ONLY
	// value that is POSITIVE evidence of an infrastructure block rather than the
	// absence of evidence, and that asymmetry is the whole safety argument for
	// the recovery driver: the operator's own submission gate
	// (restapi/readiness.go) refuses a submit on a RED pipeline and on nothing
	// else - `pending` is explicitly accepted, with a hold - so a decline made
	// against anything but red was not a decline the platform forced.
	CIEvidenceRed = "red"
	// CIEvidenceGreen: EVERY owned merge request read `green`. A decline made
	// here is a verdict on the CHANGE, and it stays permanent forever.
	CIEvidenceGreen = "green"
	// CIEvidenceUnknown: no red, but not all green either - pending, running,
	// none, or never observed. An absence of evidence, and it is treated as one.
	CIEvidenceUnknown = "unknown"
)

// CIDeclineEvidence summarises the merge requests a Task owns into the two
// facts that survive a decline: WHAT CI SAID and WHICH CODE IT SAID IT ABOUT.
//
// IT IS CALLED ON BOTH SIDES OF THE BOUND and that is why it lives here rather
// than in either caller. The decline (internal/restapi/outcome.go) stamps what
// it returns; the recovery driver (internal/controller/unpark.go) re-derives it
// from the live mirrors and compares. Two implementations of the same
// aggregation would not fail loudly if they drifted - they would compare unequal
// and silently disable the recovery - so there is exactly one.
//
// THE SET IS "THE MERGE REQUESTS TATARA MAY ACT ON": open, and ownership tatara.
// Both filters are load-bearing and both are applied HERE, not by the caller:
//   - an `external` merge request is a HUMAN's. The platform reviews it and
//     never pushes to it, so it can neither justify a recovery nor be recovered.
//   - a merged or closed merge request has left the flow; re-driving work
//     against it is the shape stage.Unpark's own DeclineMergedMR refuses.
//
// An empty state reads as OPEN, matching restapi's openMRs: a mirror minted
// before its first forge sync is in flight, not terminal.
//
// heads is an OPAQUE FINGERPRINT - `<mrName>@<headSHA>` per member, sorted by
// name, comma-joined. It is only ever compared for equality and is never parsed.
// Sorting is what makes it independent of the order the caller's loader happened
// to return. Any member with an unknown head voids the WHOLE fingerprint (both
// returns are ""): "the same code the agent declined at" is the claim the
// fingerprint exists to make, and a blank head cannot make it.
//
// Both returns are "" when the Task owns nothing in the set. A caller stamping
// evidence must REMOVE the annotations in that case rather than leave the
// previous decline's, which would be a red from a world that no longer exists.
func CIDeclineEvidence(mrs []MergeRequest) (ci, heads string) {
	live := make([]*MergeRequest, 0, len(mrs))
	for i := range mrs {
		mr := &mrs[i]
		if mr.Status.Ownership != OwnershipTatara {
			continue
		}
		if mr.Status.State != "" && mr.Status.State != "open" {
			continue
		}
		live = append(live, mr)
	}
	if len(live) == 0 {
		return "", ""
	}
	sort.Slice(live, func(i, j int) bool { return live[i].Name < live[j].Name })

	anyRed, allGreen := false, true
	parts := make([]string, 0, len(live))
	for _, mr := range live {
		switch mr.Status.CIStatus {
		case "red":
			anyRed = true
			allGreen = false
		case "green":
		default:
			allGreen = false
		}
		if mr.Status.HeadSHA == "" {
			return "", ""
		}
		parts = append(parts, mr.Name+"@"+mr.Status.HeadSHA)
	}
	switch {
	case anyRed:
		ci = CIEvidenceRed
	case allGreen:
		ci = CIEvidenceGreen
	default:
		ci = CIEvidenceUnknown
	}
	return ci, strings.Join(parts, ",")
}
