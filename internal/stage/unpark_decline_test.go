package stage_test

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

func parkedTask(kind, reason string) *v1alpha1.Task {
	t := &v1alpha1.Task{}
	t.Name = "t"
	t.Spec.Kind = kind
	t.Status.Stage = v1alpha1.StageParked
	t.Status.StageReason = reason
	t.Status.StageEnteredAt = &metav1.Time{Time: time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)}
	return t
}

// unpark_decline_test.go's own doubles, name-scoped to avoid colliding with
// stage_test.go's humanEvent(*Task) / openIssue(status), which have the same
// base name but a different shape.
func unparkDeclineHumanEvent() []v1alpha1.TaskEvent {
	return []v1alpha1.TaskEvent{{At: metav1.Now(), Kind: "issue_comment", Author: "human", Body: "go ahead"}}
}

func unparkDeclineOpenIssue(name, status string) v1alpha1.Issue {
	var iss v1alpha1.Issue
	iss.Name = name
	iss.Status.State = "open"
	iss.Status.Status = status
	return iss
}

// Every decline must name WHICH condition refused. A bare "not ok" is exactly
// what let the cache-lag decline hide as an unremarkable steady-state outcome.
func TestUnparkDetailed_ClassifiesEveryDecline(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name    string
		in      func() stage.UnparkInput
		target  string
		decline string
	}{
		{
			name: "identity-unverified with no human event",
			in: func() stage.UnparkInput {
				return stage.UnparkInput{Task: parkedTask("clarify", stage.ReasonIdentityUnverified), Now: now}
			},
			decline: stage.DeclineNoHumanEvent,
		},
		{
			name: "identity-unverified with a human event but no grammar pass",
			in: func() stage.UnparkInput {
				task := parkedTask("clarify", stage.ReasonIdentityUnverified)
				task.Status.PendingEvents = unparkDeclineHumanEvent()
				return stage.UnparkInput{Task: task, GrammarPassed: false, Now: now}
			},
			decline: stage.DeclineGrammarNotPassed,
		},
		{
			name: "identity-unverified, grammar passed, no open issues",
			in: func() stage.UnparkInput {
				task := parkedTask("clarify", stage.ReasonIdentityUnverified)
				task.Status.PendingEvents = unparkDeclineHumanEvent()
				return stage.UnparkInput{Task: task, GrammarPassed: true, Now: now}
			},
			decline: stage.DeclineNoOpenIssues,
		},
		{
			name: "identity-unverified, grammar passed, an owned issue is unapproved (THE cache-lag shape)",
			in: func() stage.UnparkInput {
				task := parkedTask("clarify", stage.ReasonIdentityUnverified)
				task.Status.PendingEvents = unparkDeclineHumanEvent()
				return stage.UnparkInput{
					Task:          task,
					Issues:        []v1alpha1.Issue{unparkDeclineOpenIssue("iss-a", "open")},
					GrammarPassed: true,
					Now:           now,
				}
			},
			decline: stage.DeclineNotAllApproved,
		},
		{
			name: "identity-unverified, grammar passed, every owned issue approved",
			in: func() stage.UnparkInput {
				task := parkedTask("clarify", stage.ReasonIdentityUnverified)
				task.Status.PendingEvents = unparkDeclineHumanEvent()
				return stage.UnparkInput{
					Task:          task,
					Issues:        []v1alpha1.Issue{unparkDeclineOpenIssue("iss-a", "approved")},
					GrammarPassed: true,
					Now:           now,
				}
			},
			target:  v1alpha1.StageImplementing,
			decline: stage.DeclineNone,
		},
		{
			name: "backlog-sweep over cap",
			in: func() stage.UnparkInput {
				task := parkedTask("clarify", stage.ReasonBacklogSweep)
				task.Status.PendingEvents = unparkDeclineHumanEvent()
				return stage.UnparkInput{Task: task, ActiveTasks: 6, MaxOpenTasks: 6, Now: now}
			},
			decline: stage.DeclineOverCap,
		},
		{
			name: "awaiting-human on a review Task at the round cap",
			in: func() stage.UnparkInput {
				task := parkedTask("review", stage.ReasonAwaitingHuman)
				task.Status.PendingEvents = unparkDeclineHumanEvent()
				task.Status.HumanReviewRounds = v1alpha1.MaxHumanReviewRounds
				return stage.UnparkInput{Task: task, Now: now}
			},
			decline: stage.DeclineRoundsExhausted,
		},
		{
			name: "no-outcome parked from a pre-implement stage",
			in: func() stage.UnparkInput {
				task := parkedTask("clarify", stage.ReasonNoOutcome)
				task.Status.ParkedFromStage = v1alpha1.StageClarifying
				return stage.UnparkInput{Task: task, MaxTurnsPerTask: 300, Now: now}
			},
			decline: stage.DeclineWrongParkedFrom,
		},
		{
			name: "no-outcome at the lifetime turn cap",
			in: func() stage.UnparkInput {
				task := parkedTask("clarify", stage.ReasonNoOutcome)
				task.Status.ParkedFromStage = v1alpha1.StageImplementing
				task.Status.Stats.Turns = 300
				return stage.UnparkInput{Task: task, MaxTurnsPerTask: 300, Now: now}
			},
			decline: stage.DeclineTurnsExhausted,
		},
		{
			name: "a reason with no F.6 re-entry rule",
			in: func() stage.UnparkInput {
				return stage.UnparkInput{Task: parkedTask("clarify", stage.ReasonStageDeadline), Now: now}
			},
			decline: stage.DeclineNoReentry,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target, decline := stage.UnparkDetailed(tc.in())
			if target != tc.target {
				t.Errorf("target = %q, want %q", target, tc.target)
			}
			if decline != tc.decline {
				t.Errorf("decline = %q, want %q", decline, tc.decline)
			}
		})
	}
}

// Unpark keeps its old shape so no existing caller or test has to move.
func TestUnparkDelegatesToUnparkDetailed(t *testing.T) {
	task := parkedTask("clarify", stage.ReasonIdentityUnverified)
	task.Status.PendingEvents = unparkDeclineHumanEvent()
	target, ok := stage.Unpark(stage.UnparkInput{
		Task:          task,
		Issues:        []v1alpha1.Issue{unparkDeclineOpenIssue("iss-a", "approved")},
		GrammarPassed: true,
		Now:           time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC),
	})
	if !ok || target != v1alpha1.StageImplementing {
		t.Fatalf("Unpark = (%q, %v), want (implementing, true)", target, ok)
	}
}
