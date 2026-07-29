package stage

import (
	"testing"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func ciRedTask(kind, stg string, reentries int) *v1alpha1.Task {
	return &v1alpha1.Task{
		Spec:   v1alpha1.TaskSpec{Kind: kind},
		Status: v1alpha1.TaskStatus{Stage: stg, CIRedReentries: reentries},
	}
}

func ciRedMRs(states ...string) []v1alpha1.MergeRequest {
	out := make([]v1alpha1.MergeRequest, 0, len(states))
	for _, s := range states {
		out = append(out, v1alpha1.MergeRequest{
			Status: v1alpha1.MergeRequestStatus{State: s},
		})
	}
	return out
}

// CIRed is CYCLE 5, and its whole point is that it TERMINATES: an implement lap
// under the cap, failed(ci-blocked) at it, and a park the moment re-implementing
// would touch already-merged work.
func TestCIRedRouting(t *testing.T) {
	tests := []struct {
		name       string
		kind       string
		reentries  int
		mrs        []v1alpha1.MergeRequest
		wantTo     string
		wantReason string
		wantCount  int
	}{
		{
			name: "first lap re-implements", kind: "implement", reentries: 0,
			mrs: ciRedMRs("open"), wantTo: v1alpha1.StageImplementing,
			wantReason: ReasonCIRed, wantCount: 1,
		},
		{
			name: "last lap under the cap still re-implements", kind: "implement",
			reentries: v1alpha1.MaxCIRedReentries - 1, mrs: ciRedMRs("open"),
			wantTo: v1alpha1.StageImplementing, wantReason: ReasonCIRed,
			wantCount: v1alpha1.MaxCIRedReentries,
		},
		{
			name: "at the cap it fails", kind: "implement",
			reentries: v1alpha1.MaxCIRedReentries, mrs: ciRedMRs("open"),
			wantTo: v1alpha1.StageFailed, wantReason: ReasonCIBlocked,
			wantCount: v1alpha1.MaxCIRedReentries,
		},
		{
			name: "an already-merged repo parks instead of re-implementing", kind: "implement",
			reentries: 0, mrs: ciRedMRs("merged", "open"), wantTo: v1alpha1.StageParked,
			wantReason: ReasonCIRed, wantCount: 0,
		},
		{
			name: "kind=review never re-implements a human's PR", kind: "review",
			reentries: 0, mrs: ciRedMRs("open"), wantTo: v1alpha1.StageParked,
			wantReason: ReasonAwaitingHuman, wantCount: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := ciRedTask(tc.kind, v1alpha1.StageMerging, tc.reentries)
			edge, ok := CIRed(task, tc.mrs, v1alpha1.MaxCIRedReentries)
			if !ok {
				t.Fatal("CIRed returned ok=false; it always yields an edge")
			}
			if edge.To != tc.wantTo || edge.Reason != tc.wantReason {
				t.Fatalf("edge = %s(%s), want %s(%s)", edge.To, edge.Reason, tc.wantTo, tc.wantReason)
			}
			if task.Status.CIRedReentries != tc.wantCount {
				t.Fatalf("ciRedReentries = %d, want %d", task.Status.CIRedReentries, tc.wantCount)
			}
		})
	}
}

// Every edge CIRed can produce must exist in the F.3 table from BOTH stages that
// take it, or Enter refuses it and the gate becomes an error loop.
func TestCIRedEdgesAreLegalFromBothSides(t *testing.T) {
	for _, from := range []string{v1alpha1.StageReviewing, v1alpha1.StageMerging} {
		for _, to := range []string{v1alpha1.StageImplementing, v1alpha1.StageParked, v1alpha1.StageFailed} {
			if !Legal(from, to) {
				t.Fatalf("%s -> %s is not in the F.3 table", from, to)
			}
		}
	}
	for _, r := range []string{ReasonCIRed, ReasonCIBlocked} {
		if !ValidReason(r) {
			t.Fatalf("%q is not in the F.5 closed set", r)
		}
	}
	// parked(ci-red) is a DEAD END on purpose: an F.6 re-entry would put the Task
	// straight back on the merge path it just proved it cannot take.
	if HasReentry(ReasonCIRed) {
		t.Fatal("ci-red must not be an F.6 re-entry reason")
	}
}
