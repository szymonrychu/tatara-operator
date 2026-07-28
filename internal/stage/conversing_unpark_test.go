package stage_test

import (
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// A comment on a parked conversation should reach a LIVE agent, not merely
// re-enter a stage that will post one reply and die. When there is no room under
// the per-project conversing ceiling the rule falls back to exactly what it did
// before, so the ceiling degrades the experience rather than dropping the event.
func TestUnpark_ConversingTargets(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name   string
		reason string
		kind   string
		room   bool
		issues []v1alpha1.Issue
		target string
	}{
		{
			name: "awaiting-human, unapproved issue, room", reason: stage.ReasonAwaitingHuman, kind: "clarify",
			room: true, issues: []v1alpha1.Issue{openIssue("open")}, target: v1alpha1.StageConversing,
		},
		{
			name: "awaiting-human, unapproved issue, NO room", reason: stage.ReasonAwaitingHuman, kind: "clarify",
			room: false, issues: []v1alpha1.Issue{openIssue("open")}, target: v1alpha1.StageClarifying,
		},
		{
			name: "awaiting-human, every issue approved, room: implementing still wins", reason: stage.ReasonAwaitingHuman, kind: "clarify",
			room: true, issues: []v1alpha1.Issue{openIssue("approved")}, target: v1alpha1.StageImplementing,
		},
		{
			name: "review kind stays on the reviewing rule", reason: stage.ReasonAwaitingHuman, kind: "review",
			room: true, target: v1alpha1.StageReviewing,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := parkedTask(tc.kind, tc.reason)
			humanEvent(task)
			target, decline := stage.UnparkDetailed(stage.UnparkInput{
				Task:              task,
				Issues:            tc.issues,
				ConversingHasRoom: tc.room,
				Now:               now,
			})
			if target != tc.target {
				t.Fatalf("target = %q (decline %q), want %q", target, decline, tc.target)
			}
		})
	}
}

// identity-unverified with a passing grammar goes to implementing, never to
// conversing: it is the one park reason sitting directly in front of "write code
// and merge it to prod", and a conversation is not what a maintainer asked for
// when they approved.
func TestUnpark_IdentityUnverifiedNeverConverses(t *testing.T) {
	task := parkedTask("clarify", stage.ReasonIdentityUnverified)
	humanEvent(task)
	target, _ := stage.UnparkDetailed(stage.UnparkInput{
		Task:              task,
		Issues:            []v1alpha1.Issue{openIssue("approved")},
		GrammarPassed:     true,
		ConversingHasRoom: true,
		Now:               time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC),
	})
	if target != v1alpha1.StageImplementing {
		t.Fatalf("target = %q, want implementing", target)
	}
}

// A failing grammar on identity-unverified with a human comment waiting is a
// CONVERSATION, not a dead end: the human said something the grammar could not
// read as approval, and an agent should read it.
func TestUnpark_IdentityUnverifiedWithoutGrammarConversesWhenThereIsRoom(t *testing.T) {
	task := parkedTask("clarify", stage.ReasonIdentityUnverified)
	humanEvent(task)
	target, _ := stage.UnparkDetailed(stage.UnparkInput{
		Task:              task,
		Issues:            []v1alpha1.Issue{openIssue("open")},
		GrammarPassed:     false,
		ConversingHasRoom: true,
		Now:               time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC),
	})
	if target != v1alpha1.StageConversing {
		t.Fatalf("target = %q, want conversing", target)
	}
}

// 2026-07-28 security review IMPORTANT 2: a kind=review Task owns ZERO
// Issues, so verifyApprovalScope (internal/restapi/outcome.go) can NEVER pass
// for it - every decision=implement from a review-kind conversation bounces
// straight back here. Without the SAME three guards the sibling
// ReasonAwaitingHuman review branch carries (merged-MR, round cap, round
// increment), a stuck kind=review Task would re-enter conversing on every
// subsequent human comment and spawn one pod per comment forever, capped
// only by maxTurnsPerTask (300).
func TestUnpark_IdentityUnverifiedReviewKindGuardedLikeAwaitingHuman(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)

	t.Run("review kind, no merged MR, under the round cap: conversing, round incremented", func(t *testing.T) {
		task := parkedTask("review", stage.ReasonIdentityUnverified)
		humanEvent(task)
		task.Status.HumanReviewRounds = 2
		target, decline := stage.UnparkDetailed(stage.UnparkInput{
			Task: task, MRs: []v1alpha1.MergeRequest{openMR()},
			GrammarPassed: false, ConversingHasRoom: true, Now: now,
		})
		if target != v1alpha1.StageConversing {
			t.Fatalf("target = %q (decline %q), want conversing", target, decline)
		}
		if task.Status.HumanReviewRounds != 3 {
			t.Fatalf("HumanReviewRounds = %d, want 3 (2 + 1)", task.Status.HumanReviewRounds)
		}
	})

	t.Run("review kind, an owned MR already merged: refused, never re-enters", func(t *testing.T) {
		task := parkedTask("review", stage.ReasonIdentityUnverified)
		humanEvent(task)
		target, decline := stage.UnparkDetailed(stage.UnparkInput{
			Task: task, MRs: []v1alpha1.MergeRequest{mergedMR()},
			GrammarPassed: false, ConversingHasRoom: true, Now: now,
		})
		if target != "" || decline != stage.DeclineMergedMR {
			t.Fatalf("target=%q decline=%q, want (\"\", %q)", target, decline, stage.DeclineMergedMR)
		}
	})

	t.Run("review kind, at the round cap: refused, no runaway pod spawn", func(t *testing.T) {
		task := parkedTask("review", stage.ReasonIdentityUnverified)
		humanEvent(task)
		task.Status.HumanReviewRounds = v1alpha1.MaxHumanReviewRounds
		target, decline := stage.UnparkDetailed(stage.UnparkInput{
			Task: task, MRs: []v1alpha1.MergeRequest{openMR()},
			GrammarPassed: false, ConversingHasRoom: true, Now: now,
		})
		if target != "" || decline != stage.DeclineRoundsExhausted {
			t.Fatalf("target=%q decline=%q, want (\"\", %q)", target, decline, stage.DeclineRoundsExhausted)
		}
		if task.Status.HumanReviewRounds != v1alpha1.MaxHumanReviewRounds {
			t.Fatalf("HumanReviewRounds = %d, want unchanged at %d", task.Status.HumanReviewRounds, v1alpha1.MaxHumanReviewRounds)
		}
	})
}
