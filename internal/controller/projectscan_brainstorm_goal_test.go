package controller

import (
	"strings"
	"testing"
)

func TestBrainstormGoalDropsCommentPathAddsEarlyExit(t *testing.T) {
	g := brainstormGoalProject([]string{"o/a", "o/b"}, "STATE", "", 3)
	if strings.Contains(g, "comment_on_issue") {
		t.Fatal("brainstorm goal must NOT instruct comment_on_issue (path-2 dropped)")
	}
	if !strings.Contains(g, "action=skip") {
		t.Fatal("brainstorm goal must instruct the submit_outcome(action=skip) early-exit")
	}
	if !strings.Contains(g, "action=propose") {
		t.Fatal("brainstorm goal must keep the submit_outcome(action=propose) path")
	}
	// Proposal must decompose into sub-problems and offer options per piece.
	for _, want := range []string{"sub-problem", "OPTIONS", "recommended"} {
		if !strings.Contains(g, want) {
			t.Fatalf("brainstorm goal must require decomposition+options; missing %q", want)
		}
	}
}

// The brainstorm goal used to open with "HANDOFF CONTINUATION (do this FIRST):
// call `list_handoffs` ... call `get_handoff` and propose continuing it". Two
// defects in one paragraph. Neither tool has ever existed, so agents silently
// fell back to task_list; and reading a prior handoff BEFORE deriving scope let
// one cycle's narrowing bind the next. Project mtg inherited a single-deck scope
// through four consecutive cycles that each re-derived it correctly and each
// skipped. This is the guard that neither half comes back.
func TestBrainstormGoalTreatsHandoffsAsEvidenceNotScope(t *testing.T) {
	g := brainstormGoalProject([]string{"o/a", "o/b"}, "STATE", "", 3)

	// The tools it names must exist. list_handoffs and get_handoff never did.
	for _, gone := range []string{"list_handoffs", "get_handoff"} {
		if strings.Contains(g, gone) {
			t.Fatalf("brainstorm goal still names the nonexistent tool %q", gone)
		}
	}
	for _, want := range []string{"task_list", "task_context"} {
		if !strings.Contains(g, want) {
			t.Fatalf("brainstorm goal must name the real tool %q for reading prior cycles", want)
		}
	}

	// A handoff is evidence, and scope is re-derived from the mandate.
	for _, want := range []string{
		"PRIOR-CYCLE EVIDENCE",
		"EVIDENCE, not instructions",
		"never a scope decision",
		"Re-derive",
	} {
		if !strings.Contains(g, want) {
			t.Fatalf("brainstorm goal missing the evidence/re-derive framing %q", want)
		}
	}

	// The widen rule is the half that breaks a wedge: re-derivation alone did
	// not, because mtg's agents re-derived correctly and still landed on the
	// same target every time.
	for _, want := range []string{
		"WIDEN ON REPEAT",
		"two or more consecutive cycles",
		"You MUST widen",
	} {
		if !strings.Contains(g, want) {
			t.Fatalf("brainstorm goal missing the widen-on-repeat rule %q", want)
		}
	}

	// Ordering is load-bearing and INVERTED from the old goal: the mandate is
	// where scope comes from, so it must be read before any prior-cycle note.
	// Reading the handoff first is the mechanism of the wedge.
	if strings.Index(g, "MANDATE:") > strings.Index(g, "PRIOR-CYCLE EVIDENCE") {
		t.Fatal("the MANDATE must precede PRIOR-CYCLE EVIDENCE: scope is derived from the mandate, " +
			"and a note read first is a note that binds")
	}
	if strings.Index(g, "PRIOR-CYCLE EVIDENCE") > strings.Index(g, "WIDEN ON REPEAT") {
		t.Fatal("WIDEN ON REPEAT reads the prior-cycle evidence, so it must follow it")
	}
}

func TestBrainstormGoalMandatesCouncilSkill(t *testing.T) {
	g := brainstormGoalProject([]string{"o/a", "o/b"}, "STATE", "", 3)
	for _, want := range []string{
		"tatara-council-brainstorm",
		"tatara-code-quality-proposal",
		"emits the single terminal action",
	} {
		if !strings.Contains(g, want) {
			t.Fatalf("brainstorm goal must mandate council skill; missing %q", want)
		}
	}
	// The council mandate must precede the existing repo-awareness body.
	if strings.Index(g, "tatara-council-brainstorm") > strings.Index(g, "READ REAL CODE") {
		t.Fatal("council mandate must be prepended before the READ REAL CODE body")
	}
}
