package controller

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestProposalPendingChanged(t *testing.T) {
	tests := []struct {
		name     string
		old, new tatarav1alpha1.Issue
		want     bool
	}{
		{"approved leaves the pending set",
			iss("brainstorm", "open", "new", "r1"), iss("brainstorm", "open", "approved", "r1"), true},
		{"closed leaves the pending set",
			iss("brainstorm", "open", "new", "r1"), iss("brainstorm", "closed", "new", "r1"), true},
		{"rejected leaves the pending set",
			iss("brainstorm", "open", "new", "r1"), iss("brainstorm", "open", "rejected", "r1"), true},
		{"a mirror resync with no verdict change is not an event",
			iss("brainstorm", "open", "new", "r1"), iss("brainstorm", "open", "new", "r1"), false},
		{"a new proposal ENTERING the pending set is also an event (it changes the level)",
			iss("brainstorm", "closed", "new", "r1"), iss("brainstorm", "open", "new", "r1"), true},
		{"an incident issue is never a brainstorm event",
			iss("incident", "open", "new", "r1"), iss("incident", "closed", "new", "r1"), false},
		{"an unstamped issue that gets backfilled to brainstorm is an event",
			iss("", "open", "new", "r1"), iss("brainstorm", "open", "new", "r1"), true},
		// O6 review Minor 5: pins the deviation-3 narrowing. This issue is
		// bot-authored, carries the in-body marker AND the integrity anchor
		// (issBody sets ProposalBodyHash whenever the body parses to a
		// non-empty kind) - i.e. it IS a real, countable brainstorm proposal
		// under proposalPending(iss, testBotLogin) - but Spec.ProposalKind was
		// never stamped. The watch predicate has no botLogin to resolve
		// (deviation 3), so it can never see this issue as pending via either
		// its early Spec.ProposalKind gate or the botLogin="" fallback read,
		// and an approval verdict on it is correctly NOT treated as an event.
		{"a countable-but-unstamped issue (body marker + anchor, bot-authored) is narrowed away by the watch's empty botLogin",
			issBody("", "open", "new", "r1", markedBody("brainstorm")),
			issBody("", "open", "approved", "r1", markedBody("brainstorm")), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o, n := tc.old, tc.new
			if got := proposalPendingChanged(&o, &n); got != tc.want {
				t.Fatalf("proposalPendingChanged = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestProposalVerdictPredicate directly exercises the predicate.Funcs
// proposalVerdictPredicate wires onto the Watches(&Issue{}) edge - the only
// thing standing between an Issue write and a Project reconcile. The O6
// envtest suite drives ProjectReconciler.Reconcile directly and never starts
// a manager, so it does NOT exercise this predicate or the Watches wiring
// itself (O6 review Minor 7); this is the actual coverage for both.
func TestProposalVerdictPredicate(t *testing.T) {
	pred := proposalVerdictPredicate()

	t.Run("create", func(t *testing.T) {
		pending := iss("brainstorm", "open", "new", "r1")
		if !pred.Create(event.CreateEvent{Object: &pending}) {
			t.Fatal("want a create of an already-pending proposal to be admitted")
		}
		decided := iss("brainstorm", "open", "approved", "r1")
		if pred.Create(event.CreateEvent{Object: &decided}) {
			t.Fatal("want a create of an already-decided proposal to be dropped")
		}
	})

	t.Run("update", func(t *testing.T) {
		old := iss("brainstorm", "open", "new", "r1")
		approved := iss("brainstorm", "open", "approved", "r1")
		if !pred.Update(event.UpdateEvent{ObjectOld: &old, ObjectNew: &approved}) {
			t.Fatal("want an approval update to be admitted")
		}
		resync := iss("brainstorm", "open", "new", "r1")
		if pred.Update(event.UpdateEvent{ObjectOld: &old, ObjectNew: &resync}) {
			t.Fatal("want a no-op mirror resync update to be dropped")
		}
	})

	t.Run("delete", func(t *testing.T) {
		// O6 review Minor 6: DeleteFunc gates on proposalPending, matching
		// CreateFunc - not the bare ProposalKind check it used to.
		pending := iss("brainstorm", "open", "new", "r1")
		if !pred.Delete(event.DeleteEvent{Object: &pending}) {
			t.Fatal("want a delete of a still-pending proposal to be admitted (its slot is freed)")
		}
		approved := iss("brainstorm", "open", "approved", "r1")
		if pred.Delete(event.DeleteEvent{Object: &approved}) {
			t.Fatal("want a delete of an already-decided (approved) proposal to be dropped - it frees no slot")
		}
		incident := iss("incident", "open", "new", "r1")
		if pred.Delete(event.DeleteEvent{Object: &incident}) {
			t.Fatal("want a delete of a non-brainstorm issue to be dropped")
		}
	})

	t.Run("generic is always dropped", func(t *testing.T) {
		pending := iss("brainstorm", "open", "new", "r1")
		if pred.Generic(event.GenericEvent{Object: &pending}) {
			t.Fatal("want GenericFunc to always return false")
		}
	})
}

func TestIssueToProject(t *testing.T) {
	in := iss("brainstorm", "open", "new", "r1")
	got := issueToProject(t.Context(), client.Object(&in))
	if len(got) != 1 {
		t.Fatalf("want 1 request, got %d", len(got))
	}
	if got[0].Name != "demo" || got[0].Namespace != "tatara" {
		t.Fatalf("request = %v, want tatara/demo", got[0])
	}
	// A non-proposal Issue must not wake the Project reconciler at all.
	other := iss("", "open", "new", "r1")
	if n := len(issueToProject(t.Context(), client.Object(&other))); n != 0 {
		t.Fatalf("want 0 requests for an unstamped issue, got %d", n)
	}
}
