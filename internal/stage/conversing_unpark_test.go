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
			name: "review kind, awaiting-human, room: conversing like every other awaiting-human comment", reason: stage.ReasonAwaitingHuman, kind: "review",
			room: true, target: v1alpha1.StageConversing,
		},
		{
			name: "review kind, awaiting-human, NO room: falls back to the reviewing rule", reason: stage.ReasonAwaitingHuman, kind: "review",
			room: false, target: v1alpha1.StageReviewing,
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

// TestUnpark_IdentityUnverifiedAlwaysConverses is the Step C behaviour. The
// arm no longer branches on a grammar verdict: a non-bot event on a parked
// identity-unverified Task enters conversing when there is room, and declines
// with no-conversing-room when there is not. It NEVER enters implementing -
// the only grant path is restapi.verifyApprovalScope (#294: flip the DECISION
// at the chokepoint, never add an EDGE).
func TestUnpark_IdentityUnverifiedAlwaysConverses(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	base := func() *v1alpha1.Task {
		task := parkedTask("clarify", stage.ReasonIdentityUnverified)
		humanEvent(task)
		return task
	}

	// Every open owned Issue already approved: the old code took this straight
	// to implementing. It must not any more.
	target, decline := stage.UnparkDetailed(stage.UnparkInput{
		Task: base(), Issues: []v1alpha1.Issue{openIssue("approved")},
		ActiveTasks: 1, MaxOpenTasks: 6, ConversingHasRoom: true, Now: now,
	})
	if target != v1alpha1.StageConversing || decline != stage.DeclineNone {
		t.Fatalf("approved+room: target = %q decline = %q, want conversing/none", target, decline)
	}

	// No conversing room (D1): a truthful decline, not a lie about a grammar
	// that no longer exists, and not a fallthrough into clarifying that would
	// route around the conversing ceiling.
	target, decline = stage.UnparkDetailed(stage.UnparkInput{
		Task: base(), Issues: []v1alpha1.Issue{openIssue("approved")},
		ActiveTasks: 1, MaxOpenTasks: 6, ConversingHasRoom: false, Now: now,
	})
	if target != "" || decline != stage.DeclineNoConversingRoom {
		t.Fatalf("no room: target = %q decline = %q, want \"\"/no-conversing-room", target, decline)
	}

	// No non-bot event at all: unchanged, still the first guard.
	target, decline = stage.UnparkDetailed(stage.UnparkInput{
		Task:        parkedTask("clarify", stage.ReasonIdentityUnverified),
		ActiveTasks: 1, MaxOpenTasks: 6, ConversingHasRoom: true, Now: now,
	})
	if target != "" || decline != stage.DeclineNoHumanEvent {
		t.Fatalf("no event: target = %q decline = %q, want \"\"/no-human-event", target, decline)
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
			ConversingHasRoom: true, Now: now,
		})
		if target != v1alpha1.StageConversing {
			t.Fatalf("target = %q (decline %q), want conversing", target, decline)
		}
		if task.Status.HumanReviewRounds != 3 {
			t.Fatalf("HumanReviewRounds = %d, want 3 (2 + 1)", task.Status.HumanReviewRounds)
		}
	})

	t.Run("review kind, an owned MR already merged: refused, never re-enters", func(t *testing.T) {
		task := parkedTask("review", stage.ReasonAwaitingHuman)
		humanEvent(task)
		target, decline := stage.UnparkDetailed(stage.UnparkInput{
			Task: task, MRs: []v1alpha1.MergeRequest{mergedMR()},
			ConversingHasRoom: true, Now: now,
		})
		if target != "" || decline != stage.DeclineMergedMR {
			t.Fatalf("target=%q decline=%q, want (\"\", %q)", target, decline, stage.DeclineMergedMR)
		}
	})

	t.Run("review kind, at the round cap: refused, no runaway pod spawn", func(t *testing.T) {
		task := parkedTask("review", stage.ReasonAwaitingHuman)
		humanEvent(task)
		task.Status.HumanReviewRounds = v1alpha1.MaxHumanReviewRounds
		target, decline := stage.UnparkDetailed(stage.UnparkInput{
			Task: task, MRs: []v1alpha1.MergeRequest{openMR()},
			ConversingHasRoom: true, Now: now,
		})
		if target != "" || decline != stage.DeclineRoundsExhausted {
			t.Fatalf("target=%q decline=%q, want (\"\", %q)", target, decline, stage.DeclineRoundsExhausted)
		}
		if task.Status.HumanReviewRounds != v1alpha1.MaxHumanReviewRounds {
			t.Fatalf("HumanReviewRounds = %d, want unchanged at %d", task.Status.HumanReviewRounds, v1alpha1.MaxHumanReviewRounds)
		}
	})
}

// TestUnpark_AwaitingHumanReviewKindGuardedLikeIdentityUnverified is #508: the
// awaiting-human arm used to hard-code StageReviewing for kind=review,
// bypassing the conversing/idle-TTL path (and its ceiling) that every other
// awaiting-human re-entry gets - the maintainer's "review agent stays alive on
// a comment until approve or 1h" never had a mechanism under it. It now
// carries the SAME three guards as the identity-unverified review arm above
// (merged-MR, round cap, round increment), because it is spawning the exact
// same review pod under the exact same runaway-loop risk.
func TestUnpark_AwaitingHumanReviewKindGuardedLikeIdentityUnverified(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)

	t.Run("review kind, no merged MR, under the round cap: conversing, round incremented", func(t *testing.T) {
		task := parkedTask("review", stage.ReasonAwaitingHuman)
		humanEvent(task)
		task.Status.HumanReviewRounds = 2
		target, decline := stage.UnparkDetailed(stage.UnparkInput{
			Task: task, MRs: []v1alpha1.MergeRequest{openMR()},
			ConversingHasRoom: true, Now: now,
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
			ConversingHasRoom: true, Now: now,
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
			ConversingHasRoom: true, Now: now,
		})
		if target != "" || decline != stage.DeclineRoundsExhausted {
			t.Fatalf("target=%q decline=%q, want (\"\", %q)", target, decline, stage.DeclineRoundsExhausted)
		}
		if task.Status.HumanReviewRounds != v1alpha1.MaxHumanReviewRounds {
			t.Fatalf("HumanReviewRounds = %d, want unchanged at %d", task.Status.HumanReviewRounds, v1alpha1.MaxHumanReviewRounds)
		}
	})
}
