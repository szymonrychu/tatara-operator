// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// TerminalIssueReleaser is what the TRANSITION CHOKE POINT calls when a Task
// enters a terminal state (done or rejected) still owning an OPEN Issue: the
// terminal notice comment plus the tatara-parked label, on every one of them.
//
// It is an interface, and EnterStage takes it as an OPTION, because the choke
// point is a free function called from four different reconcilers and none of
// them can be made to carry the reaper. A caller that passes nothing gets the
// old behaviour and the reap path's backstop; a caller that passes one gets the
// treatment AT the transition.
type TerminalIssueReleaser interface {
	ReleaseTerminalIssues(ctx context.Context, task *tatarav1alpha1.Task) error
}

// TerminalReleaser is the one implementation, and it is deliberately NOT a
// second copy of releaseTerminal.
//
// WHAT IT DOES: steps 1 and 2 of the B.6 terminal sequence
// (notifyTerminalIssueWith - the SAME function the reap path calls, not a
// re-implementation) on every still-open Issue the Task controller-owns.
//
// WHAT IT DELIBERATELY DOES NOT DO: step 3, the ownerRef release, and step 4,
// closing the Task's own bot MRs. Both belong to releaseOwnership/closeOwnMRs,
// which need the B.5 handover's map of every LIVE Task in the namespace to
// decide between handing the controller flag to a surviving plain owner and
// dropping the ref outright. Rebuilding that decision here would be exactly the
// duplication that produces two rules which disagree - the failure ourMR's doc
// comment describes - so the reap pass keeps it, and reapOne is now TOTAL over
// terminal Tasks (reapDelivered gained the releaseTerminal call it never had),
// so it lands within one reap interval.
//
// The split is not a compromise on the invariant, because step 2 is the half
// that actually STOPS things: MintStage's outermost gate is the tatara-parked
// label, so the label is what turns the re-mint of a terminal Task's still-open
// issue from ACTIVE-with-a-pod into parked(backlog-sweep)-at-zero-pods. Getting
// it stamped at the transition rather than up to a reap interval later is the
// whole point of doing anything here at all.
type TerminalReleaser struct {
	Client  client.Client
	SCMFor  func(provider string) (scm.SCMWriter, error)
	Metrics *obs.OperatorMetrics
}

// ReleaseTerminalIssues posts the notice and stamps the label on every open
// owned Issue. It is idempotent through the same markers the reap path uses
// (AnnTerminalCommented per (Issue, Task); AddLabel is idempotent on both
// forges), so running it here AND on the reap path costs one extra label call
// and posts no second comment.
func (tr *TerminalReleaser) ReleaseTerminalIssues(ctx context.Context, task *tatarav1alpha1.Task) error {
	if tr == nil || tr.Client == nil || tr.SCMFor == nil {
		return nil
	}
	issues, err := ownedIssueCRs(ctx, tr.Client, task)
	if err != nil {
		return err
	}
	open := issues[:0:0]
	for i := range issues {
		if issues[i].Status.State != "closed" {
			open = append(open, issues[i])
		}
	}
	if len(open) == 0 {
		return nil
	}

	var proj tatarav1alpha1.Project
	if err := tr.Client.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &proj); err != nil {
		return fmt.Errorf("terminal: get project %s: %w", task.Spec.ProjectRef, err)
	}
	provider := "github"
	if proj.Spec.Scm != nil && proj.Spec.Scm.Provider != "" {
		provider = proj.Spec.Scm.Provider
	}
	writer, err := tr.SCMFor(provider)
	if err != nil {
		return fmt.Errorf("terminal: scm writer: %w", err)
	}
	token, err := mirrorSCMToken(ctx, tr.Client, &proj)
	if err != nil {
		return err
	}

	for i := range open {
		iss := &open[i]
		var repo tatarav1alpha1.Repository
		if err := tr.Client.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: iss.Spec.RepositoryRef}, &repo); err != nil {
			return fmt.Errorf("terminal: get repository %s: %w", iss.Spec.RepositoryRef, err)
		}
		if err := notifyTerminalIssueWith(ctx, tr.Client, tr.Metrics, &proj, writer, token, task, iss, &repo); err != nil {
			return err
		}
		obs.TerminalIssueReleasedTotal.WithLabelValues(task.Status.State, obs.TerminalIssueReleased).Inc()
		log.FromContext(ctx).Info("terminal transition parked a still-open owned issue",
			"action", "terminal_issue_parked", "resource_id", iss.Name, "task", task.Name,
			"state", task.Status.State, "kind", task.Spec.Kind, "label", TataraParkedLabel)
	}
	return nil
}

// EnterOption configures one EnterStage call. It is variadic so the four
// existing call sites and every test that drives the choke point keep
// compiling: an option nobody passes must never change what the choke point
// does.
type EnterOption func(*enterOpts)

type enterOpts struct{ terminal TerminalIssueReleaser }

// WithTerminalIssueRelease wires the B.6 terminal treatment into this
// transition. Pass it wherever the caller can reach an SCM writer; the reap
// path remains the blocking backstop either way.
func WithTerminalIssueRelease(tr TerminalIssueReleaser) EnterOption {
	return func(o *enterOpts) { o.terminal = tr }
}

// releaseTerminalIssues is the choke point's call into the option, and it is
// NON-FATAL by design.
//
// The transition has ALREADY been written by the time this runs, so returning
// an error here would fail a state change that has demonstrably happened -
// CloseIssuesOnDelivery would skip its delivered log and its merge-cursor
// clear over a forge 502, and the caller's retry would re-enter a state it is
// already in (a documented no-op). The forge write is owed, not lost: the reap
// pass runs the same sequence BLOCKING, with retries, on every terminal Task,
// and it is the thing that is allowed to hold a reconcile open until the forge
// answers. What is owed is COUNTED, on outcome="error", so an owed release is
// visible rather than merely logged.
func releaseTerminalIssues(ctx context.Context, o enterOpts, task *tatarav1alpha1.Task, to string) {
	if o.terminal == nil || !tatarav1alpha1.TaskIsTerminalOutcome(to) {
		return
	}
	if err := o.terminal.ReleaseTerminalIssues(ctx, task); err != nil {
		obs.TerminalIssueReleasedTotal.WithLabelValues(to, obs.TerminalIssueReleaseError).Inc()
		log.FromContext(ctx).Error(err,
			"terminal transition could not park its still-open owned issues; the reap pass still owes it",
			"action", "terminal_issue_park_failed", "resource_id", task.Name,
			"state", to, "kind", task.Spec.Kind)
	}
}
