package controller

import (
	"context"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// conversingHandoffAndPark ends a conversation the way G.7 ends a pod: the agent
// gets ONE handoff turn to write everything the next pod needs, the operator
// writes a synthetic handoff note if the agent cannot, and only THEN does the
// Task park at awaiting-human (which tears the pod down through the ordinary
// choke point).
//
// The order is the whole point. Notes ARE the continuation state, so a park that
// deletes the pod first leaves the journal empty and the next pod starts from
// nothing, redoes the work and burns maxTurnsPerTask. The eviction path (the
// per-project conversing ceiling) is a ROUTINE mechanism rather than a rare edge
// case, so this sequence runs often and must be correct.
//
// cause is "idle" or "evicted" and lands on the log line and the metric; it is
// not otherwise behavioural. The park reason is awaiting-human either way: a
// conversation that ended without a decision is a Task waiting on a human, and
// awaiting-human already has the F.6 re-entry rule that resumes it on the next
// comment.
func (r *TaskReconciler) conversingHandoffAndPark(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest, cause string, now time.Time) error {

	var sp objbudget.Spiller
	if r.SpillerFor != nil {
		sp = r.SpillerFor(proj)
	}
	// A conversing Task with no pod (the pod died, or none was ever admitted) has
	// nothing to hand off from. Park it directly: the handoff is best-effort, the
	// park is not.
	if task.Status.PodStartedAt != nil {
		stopper := &agent.TTLStopper{
			Client:  r.Client,
			Session: r.Session,
			Notes: &agent.FitNoteAppender{
				Client:    r.Client,
				Spiller:   sp,
				Namespace: task.Namespace,
			},
			Namespace: task.Namespace,
			Record:    obs.AgentPodTTLExpired,
		}
		in := agent.TTLStopInput{
			BaseURL:     agent.BaseURL(task, task.Namespace),
			CallbackURL: r.callbackURL(),
			AgentKind:   stage.AgentKindFor(task.Status.Stage),
			// The deadline is NOW: the conversation is over, so the hard cap the
			// stopper computes from it (now + 2*turnTimeout + 60s) bounds this
			// sequence rather than any pod TTL.
			Deadline:    now,
			TurnTimeout: time.Duration(proj.Spec.Agent.TurnTimeoutSeconds) * time.Second,
		}
		outcome, err := stopper.StopWithHandoff(ctx, task, in)
		if err != nil {
			return fmt.Errorf("conversing: handoff stop on %s: %w", task.Name, err)
		}
		log.FromContext(ctx).Info("conversation handed off",
			"action", "conversing_handoff", "resource_id", task.Name,
			"cause", cause, "outcome", outcome)
	}

	if err := EnterStage(ctx, r.Client, sp, r.Metrics, task, mrs,
		tatarav1alpha1.StageParked, stage.ReasonAwaitingHuman, now, nil); err != nil {
		return fmt.Errorf("conversing: park %s after handoff: %w", task.Name, err)
	}
	log.FromContext(ctx).Info("conversation closed",
		"action", "conversing_closed", "resource_id", task.Name, "cause", cause,
		"project", task.Spec.ProjectRef)
	if r.Metrics != nil {
		r.Metrics.ConversingClosed(task.Spec.ProjectRef, cause)
	}
	return nil
}
