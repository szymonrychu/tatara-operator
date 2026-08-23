// Copyright 2026 tatara authors.

package v1alpha1

// AutoApproveOff is the ceiling value that disables the auto-approve carve-out
// entirely, and it is what an EMPTY autoApproveMaxSignificance reads as. The
// field replaced a boolean, so every Project CR written by an older build
// carries the empty string for it: reading that as anything but off would turn
// a CRD upgrade into a silent grant of the widest possible ceiling.
const AutoApproveOff = "off"

// SignificanceRank orders the three semver levels a change can declare.
//
// EXPORTED BECAUSE THERE ARE NOW TWO READERS. It was a private table in
// internal/restapi, consulted only by the review-escalation clause ("a review
// may raise significance, never lower it"). The auto-approve ceiling is the
// second reader and it must order the levels identically - a ceiling of `minor`
// that admitted `major` because the two tables disagreed would release exactly
// the change the ceiling exists to hold. One table, two readers.
var SignificanceRank = map[string]int{"patch": 1, "minor": 2, "major": 3}

// AutoApproveCeiling is Project.spec.autoApproveMaxSignificance with the empty
// string folded onto off. Call it rather than reading the field: the zero value
// is load-bearing and must not be interpreted at each call site.
func AutoApproveCeiling(p *Project) string {
	if p == nil || p.Spec.AutoApproveMaxSignificance == "" {
		return AutoApproveOff
	}
	return p.Spec.AutoApproveMaxSignificance
}

// AutoApproveCeilingRank ranks a ceiling on the SignificanceRank scale, with off
// at 0 - strictly below `patch`, which is what makes `off` refuse every declared
// level rather than admitting the smallest one.
func AutoApproveCeilingRank(ceiling string) int {
	if ceiling == "" || ceiling == AutoApproveOff {
		return 0
	}
	return SignificanceRank[ceiling]
}

// AutoApproveOverCeiling reports whether a DECLARED change significance sits
// above what this project will auto-approve.
//
// DECLARED IS THE OPERATIVE WORD, AND IT BOUNDS WHAT THIS CEILING IS FOR.
// `change_significance` is whatever the agent says it is: validChangeSignificance
// checks membership in the enum and nothing in the operator compares the value
// to the diff, so a `patch` declaration walks past a `minor` ceiling carrying a
// breaking change. A review outcome may also RAISE
// MergeRequest.status.significance past the ceiling with no re-check, because
// that handler's Task owns the merge requests and not the Issues the evidence
// lives on. The ceiling is therefore a bound on how much tatara ships by honest
// ACCIDENT, not a containment boundary against an agent trying to get past it;
// `off` is that boundary. Said the same way in
// docs/operations/security/approval-gates.md so the two cannot drift.
//
// AN EMPTY SIGNIFICANCE IS NEVER OVER. `change_significance` does not exist on
// the wire until submit_outcome(action=submitted): mr_write(action=open) runs
// before the agent has declared one, and the same verdict function serves both.
// The ceiling therefore bites at SUBMIT, where the level is known, and the open
// path only enforces the two evidence blockers.
//
// An unrecognised non-empty level fails CLOSED. validChangeSignificance already
// rejects one at the wire, so this is the layer that survives a change to that
// one rather than the operative check.
func AutoApproveOverCeiling(p *Project, significance string) bool {
	if significance == "" {
		return false
	}
	rank, known := SignificanceRank[significance]
	if !known {
		return true
	}
	return rank > AutoApproveCeilingRank(AutoApproveCeiling(p))
}
