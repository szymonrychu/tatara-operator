package controller

import (
	"context"
	"fmt"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// lifecycleLabels returns the three managed labels for the project, applying the
// tatara-idea/tatara-approved/tatara-rejected defaults when a field is empty.
func lifecycleLabels(s *tatarav1alpha1.ScmSpec) (idea, approved, rejected string) {
	idea, approved, rejected = "tatara-idea", "tatara-approved", "tatara-rejected"
	if s == nil {
		return
	}
	if s.IdeaLabel != "" {
		idea = s.IdeaLabel
	}
	if s.ApprovedLabel != "" {
		approved = s.ApprovedLabel
	}
	if s.RejectedLabel != "" {
		rejected = s.RejectedLabel
	}
	return
}

// setLifecycleLabel ensures exactly `desired` of the three managed labels is
// present on the task's source issue: it adds `desired` if absent and removes
// the other two managed labels if present. It never touches any non-managed
// label. Idempotent. AddLabel failures are returned (caller requeues);
// RemoveLabel failures are logged and tolerated.
func (r *TaskReconciler) setLifecycleLabel(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, desired string) error {
	if task.Spec.Source == nil || task.Spec.Source.IssueRef == "" {
		return nil
	}
	l := log.FromContext(ctx)
	idea, approved, rejected := lifecycleLabels(proj.Spec.Scm)
	managed := []string{idea, approved, rejected}
	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return fmt.Errorf("set label: %w", err)
	}
	issueRef := task.Spec.Source.IssueRef

	current := map[string]bool{}
	if r.ReaderFor != nil {
		if reader, rerr := r.ReaderFor(provider, token); rerr == nil {
			if owner, name, oerr := scm.OwnerRepo(repo.Spec.URL); oerr == nil {
				if issues, lerr := reader.ListOpenIssues(ctx, owner, name); lerr == nil {
					for _, iss := range issues {
						if fmt.Sprintf("%s#%d", iss.Repo, iss.Number) == issueRef {
							for _, lb := range iss.Labels {
								current[lb] = true
							}
							break
						}
					}
				}
			}
		}
	}

	if !current[desired] {
		if aerr := writer.AddLabel(ctx, token, issueRef, desired); aerr != nil {
			r.recordSCM(provider, "add_label", aerr)
			return fmt.Errorf("set label add %q: %w", desired, aerr)
		}
		r.recordSCM(provider, "add_label", nil)
	}
	for _, lb := range managed {
		if lb == desired || !current[lb] {
			continue
		}
		if rerr := writer.RemoveLabel(ctx, token, issueRef, lb); rerr != nil {
			r.recordSCM(provider, "remove_label", rerr)
			l.Info("set label: remove other label failed (non-fatal)",
				"action", "scm_set_label", "resource_id", task.Name, "issue_ref", issueRef, "label", lb, "err", rerr.Error())
			continue
		}
		r.recordSCM(provider, "remove_label", nil)
	}
	l.Info("lifecycle label set", "action", "scm_set_label",
		"resource_id", task.Name, "issue_ref", issueRef, "label", desired)
	return nil
}
