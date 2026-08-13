package controller

import (
	"context"
	"fmt"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// The resume triggers, in the order the design table lists them. They are the
// VALUE of AnnBrainstormResume and the "trigger" log field; nothing branches on
// them, so an unrecognised value is not an error - it reads as a manual
// override, which is exactly what a human writing the annotation by hand is.
const (
	ResumeTriggerPush              = "push"
	ResumeTriggerMerge             = "merge"
	ResumeTriggerMaintainerComment = "maintainer-comment"
	ResumeTriggerMaintainerClose   = "maintainer-close"
	ResumeTriggerManual            = "manual"
)

// brainstormResumeKind reports whether an MR merged for a Task of this kind is
// the project MOVING, and therefore worth re-opening the idea space for.
//
// It is keyed on Task.Spec.Kind, a closed CRD enum the operator writes itself
// (task_types.go), and NEVER on a commit message or on git author metadata:
// ListCommits returns raw `git config user.name`, it is forgeable, and
// Conventional Commits' own spec concedes that a commit which does not meet the
// convention "will be missed by tools". The doc-sync exclusion falls out of the
// Kind field rather than out of parsing what a commit says about itself.
//
// The allowlist is deliberately narrow, but not narrow on the axis it looks
// narrow on: it is "did an MR merge for this Task land real code", not "is
// this an implement-shaped Task". clarify is the full proposal-to-implementation
// lifecycle (the "implement" of the design table - note that implement is an
// AGENT kind, Task.Status.AgentKind, and never a Task.Spec.Kind), and incident
// is a fix landing. upgrade lands real code too - a dependency hop that moves a
// pin AND the config that has to move with it is the project moving. takeover is
// included for the SAME reason: a takeover Task
// lands real code changes on behalf of real task work, so the project moved,
// exactly as much as an implement or incident merge does - do not "simplify" it
// back out. documentation, refine and review are not: they are housekeeping
// kinds whose merges do not represent the project moving on its own idea
// space, documentation above all (see the regression test).
func brainstormResumeKind(kind string) bool {
	switch kind {
	case "implement", "incident", "takeover", "upgrade":
		return true
	}
	return false
}

// StampBrainstormResume records trigger on projName's AnnBrainstormResume
// annotation, AND unconditionally stamps Status.LastMovementAt. The Project
// reconcile (clearBrainstormPauseIfRequested) is the SINGLE consumer of the
// annotation: it clears the pause and removes the annotation.
//
// The annotation write is a NO-OP on a project that is not paused. Every
// webhook delivery would otherwise be an unconditional write to the Project
// object, for an annotation with no reconcile effect until the project
// actually pauses.
//
// Status.LastMovementAt is different: it is stamped on EVERY call, paused or
// not (I3 fix round). Before this, a merge/push/maintainer trigger landing
// WHILE a brainstorm session was in flight for a project that was NOT YET
// paused was silently discarded entirely - nothing recorded it anywhere - so
// that session's own eventual exhausted verdict could pause a project that
// had already moved, and it stayed paused until the NEXT qualifying event.
// That is the OPPOSITE of the design's intended fail direction ("over-resumes
// rather than under-resumes", see ResumeBrainstormOnPush's own comment).
// internal/restapi's exhausted outcome handler (setBrainstormPause) now reads
// this field, freshly, and refuses to pause when it is newer than the
// session's own Task start - it is the durable signal that comparison needs,
// and unlike the annotation it can never be a no-op: a session can be live on
// an unpaused project at any time.
//
// It lives in internal/controller and is exported because internal/webhook
// imports this package (one way - internal/controller must never import
// internal/webhook), and all five triggers must write the same fields through
// the same code.
func StampBrainstormResume(ctx context.Context, c client.Client, ns, projName, trigger string) error {
	if projName == "" || trigger == "" {
		return nil
	}
	key := types.NamespacedName{Namespace: ns, Name: projName}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var proj tatarav1alpha1.Project
		if err := c.Get(ctx, key, &proj); err != nil {
			return fmt.Errorf("brainstorm resume: get project %s: %w", projName, err)
		}
		now := metav1.Now()
		proj.Status.LastMovementAt = &now
		return c.Status().Update(ctx, &proj)
	}); err != nil {
		return fmt.Errorf("brainstorm resume: stamp movement on project %s: %w", projName, err)
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var proj tatarav1alpha1.Project
		if err := c.Get(ctx, key, &proj); err != nil {
			return fmt.Errorf("brainstorm resume: get project %s: %w", projName, err)
		}
		if proj.Status.BrainstormPausedAt == nil {
			return nil
		}
		if proj.Annotations[tatarav1alpha1.AnnBrainstormResume] == trigger {
			return nil
		}
		if proj.Annotations == nil {
			proj.Annotations = map[string]string{}
		}
		proj.Annotations[tatarav1alpha1.AnnBrainstormResume] = trigger
		if err := c.Update(ctx, &proj); err != nil {
			return fmt.Errorf("brainstorm resume: annotate project %s: %w", projName, err)
		}
		return nil
	})
}

// OperatorLandedSHA reports whether sha is a merge commit THIS operator
// produced for projName, read from its own MergeRequest mirrors. This is the
// identity-scoped exclusion the push trigger needs - the same shape as GitHub's
// rule that GITHUB_TOKEN pushes do not re-trigger workflows - and it inspects
// no commit and no author.
//
// Plain List + filter, matching projectProposalIssues and ownedIssueCRs: the
// MergeRequest CR set per namespace is small and there is no projectRef field
// index to piggyback on.
func OperatorLandedSHA(ctx context.Context, c client.Reader, ns, projName, sha string) (bool, error) {
	if sha == "" {
		return false, nil
	}
	var list tatarav1alpha1.MergeRequestList
	if err := c.List(ctx, &list, client.InNamespace(ns)); err != nil {
		return false, fmt.Errorf("brainstorm resume: list mergerequests for %s: %w", projName, err)
	}
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == projName && list.Items[i].Status.MergedSHA == sha {
			return true, nil
		}
	}
	return false, nil
}

// ResumeBrainstormOnPush is the push trigger: a push whose head SHA the
// operator did not itself land resumes a paused brainstorm.
//
// It fails CLOSED on an empty head SHA - that is not evidence of anything, and
// four other triggers remain. It fails OPEN on a MergeRequest that has already
// been garbage-collected, resuming a project whose head the operator really did
// land; that over-resumes rather than under-resumes, which is the correct
// direction to fail given the prior art (Renovate and Dependabot both pause on
// human disengagement and never on "nothing to do"). A force-push or branch
// rewrite resumes for the same reason.
func ResumeBrainstormOnPush(ctx context.Context, c client.Client, ns, projName, headSHA string) error {
	if headSHA == "" {
		return nil
	}
	landed, err := OperatorLandedSHA(ctx, c, ns, projName, headSHA)
	if err != nil {
		return err
	}
	if landed {
		return nil
	}
	return StampBrainstormResume(ctx, c, ns, projName, ResumeTriggerPush)
}

// clearBrainstormPauseIfRequested consumes AnnBrainstormResume: it clears the
// pause fields, then removes the annotation. That ORDER is load-bearing. If the
// annotation were removed first and the status write then failed, the project
// would stay paused with its one-shot signal already consumed - wedged until a
// human noticed. In this order a failed annotation removal costs one redundant
// clear on the next pass, which is a no-op.
func (r *ProjectReconciler) clearBrainstormPauseIfRequested(ctx context.Context, proj *tatarav1alpha1.Project) error {
	trigger, ok := proj.Annotations[tatarav1alpha1.AnnBrainstormResume]
	if !ok {
		return nil
	}
	if trigger == "" {
		trigger = ResumeTriggerManual
	}
	l := log.FromContext(ctx)
	key := types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}

	if proj.Status.BrainstormPausedAt != nil {
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tatarav1alpha1.Project{}
			if err := r.Get(ctx, key, fresh); err != nil {
				return err
			}
			if fresh.Status.BrainstormPausedAt == nil && fresh.Status.BrainstormPauseReason == "" {
				return nil
			}
			fresh.Status.BrainstormPausedAt = nil
			fresh.Status.BrainstormPauseReason = ""
			return r.Status().Update(ctx, fresh)
		}); err != nil {
			return fmt.Errorf("brainstorm resume: clear pause on %s: %w", proj.Name, err)
		}
		proj.Status.BrainstormPausedAt = nil
		proj.Status.BrainstormPauseReason = ""
		l.Info("brainstorm: pause cleared",
			"action", "brainstorm_resumed", "resource_id", proj.Name, "trigger", trigger)
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Project{}
		if err := r.Get(ctx, key, fresh); err != nil {
			return err
		}
		if _, still := fresh.Annotations[tatarav1alpha1.AnnBrainstormResume]; !still {
			return nil
		}
		delete(fresh.Annotations, tatarav1alpha1.AnnBrainstormResume)
		return r.Update(ctx, fresh)
	}); err != nil {
		return fmt.Errorf("brainstorm resume: remove annotation on %s: %w", proj.Name, err)
	}
	delete(proj.Annotations, tatarav1alpha1.AnnBrainstormResume)
	return nil
}
