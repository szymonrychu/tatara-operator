package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// Writer opens changes and comments on the originating work item. Implemented
// by the per-provider scm clients; faked in tests.
type Writer interface {
	OpenChange(ctx context.Context, repoURL, token, sourceBranch, targetBranch, title, body string) (string, error)
	Comment(ctx context.Context, token, issueRef, body string) error
}

// doWriteBack opens a PR/MR for each Project repo that has the task branch,
// comments the primary issue with all PR links, and records them on the Task
// status. It is called when WritebackPending is True and prURL is not yet set.
// Permanent SCM errors (4xx) per repo are logged and skipped; transient errors
// are returned for requeue.
func (r *TaskReconciler) doWriteBack(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	// Idempotency guard: already done.
	if task.Status.PrURL != "" {
		r.clearWritebackPending(ctx, task, "AlreadyWritten", "pr/mr url already set")
		return ctrl.Result{}, nil
	}

	var proj tatarav1alpha1.Project
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &proj); err != nil {
		return ctrl.Result{}, fmt.Errorf("writeback: get project: %w", err)
	}

	var primaryRepo tatarav1alpha1.Repository
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &primaryRepo); err != nil {
		return ctrl.Result{}, fmt.Errorf("writeback: get repository: %w", err)
	}

	provider := ""
	if task.Spec.Source != nil {
		provider = task.Spec.Source.Provider
	}
	if provider == "" {
		provider = providerForRemote(ctx, primaryRepo.Spec.URL)
	}

	writer, err := r.SCMFor(provider)
	if err != nil {
		l.Error(err, "writeback: select scm writer", "provider", provider)
		r.clearWritebackPending(ctx, task, "SCMError", fmt.Sprintf("scm writer: %v", err))
		return ctrl.Result{}, nil
	}

	token, err := r.scmToken(ctx, task.Namespace, proj.Spec.ScmSecretRef)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("writeback: scm token: %w", err)
	}

	// Gather all Project repos; primary first, then the rest.
	allRepos, err := r.projectRepos(ctx, &proj)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("writeback: list project repos: %w", err)
	}
	// Build an ordered list with the primary first.
	ordered := make([]tatarav1alpha1.Repository, 0, len(allRepos))
	ordered = append(ordered, primaryRepo)
	for i := range allRepos {
		if allRepos[i].Name != primaryRepo.Name {
			ordered = append(ordered, allRepos[i])
		}
	}

	sourceBranch := taskBranch(task)
	title := firstLine(task.Spec.Goal)
	body := writeBackBody(task)

	var prURLs []string
	var lastSkipStatus int
	for _, repo := range ordered {
		prURL, openErr := writer.OpenChange(ctx, repo.Spec.URL, token, sourceBranch, repo.Spec.DefaultBranch, title, body)
		if openErr != nil {
			var he *scm.HTTPError
			if errors.As(openErr, &he) && he.Status >= 400 && he.Status < 500 {
				// 4xx permanent: branch missing or no diff - skip this repo.
				l.Info("writeback: skipping repo (4xx)", "repo", repo.Name, "status", he.Status)
				lastSkipStatus = he.Status
				continue
			}
			return ctrl.Result{}, fmt.Errorf("writeback: open change for %s: %w", repo.Name, openErr)
		}
		l.Info("writeback: pr/mr opened", "task", task.Name, "repo", repo.Name, "pr_url", prURL)
		prURLs = append(prURLs, prURL)
	}

	if len(prURLs) == 0 {
		// No repo had the branch / no code change: still post the agent's result
		// to the issue, so report/question/verify tasks surface their answer
		// (otherwise the work is invisible - no PR and no comment).
		if task.Spec.Source != nil && task.Spec.Source.IssueRef != "" {
			summary := task.Status.ResultSummary
			if summary == "" {
				summary = task.Spec.Goal
			}
			if err := writer.Comment(ctx, token, task.Spec.Source.IssueRef, summary); err != nil {
				l.Error(err, "writeback: comment result on work item (non-fatal)",
					"issue_ref", task.Spec.Source.IssueRef)
			}
		}
		msg := "no PR opened; result commented on the issue"
		if lastSkipStatus != 0 {
			msg = fmt.Sprintf("PR/MR could not be opened or already exists: %d", lastSkipStatus)
		}
		r.clearWritebackPending(ctx, task, "WritebackSkipped", msg)
		return ctrl.Result{}, nil
	}

	// Record primary PR URL (first in list) and all URLs in the condition message.
	task.Status.PrURL = prURLs[0]
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type:               "WritebackPending",
		Status:             metav1.ConditionFalse,
		Reason:             "Written",
		Message:            "pr/mr opened: " + strings.Join(prURLs, " "),
		ObservedGeneration: task.Generation,
	})
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("writeback: update prURL: %w", err)
	}

	// Comment on the originating issue with all PR links (non-fatal).
	if task.Spec.Source != nil && task.Spec.Source.IssueRef != "" {
		resultSummary := task.Status.ResultSummary
		if resultSummary == "" {
			resultSummary = task.Spec.Goal
		}
		commentBody := "Done - opened PR/MR:\n" + strings.Join(prURLs, "\n") + "\n\n" + resultSummary
		if err := writer.Comment(ctx, token, task.Spec.Source.IssueRef, commentBody); err != nil {
			l.Error(err, "writeback: comment on work item (non-fatal)",
				"issue_ref", task.Spec.Source.IssueRef)
			// Non-fatal: PRs exist; continue.
		}
	}

	return ctrl.Result{}, nil
}

// clearWritebackPending sets WritebackPending=False and updates status.
func (r *TaskReconciler) clearWritebackPending(ctx context.Context, task *tatarav1alpha1.Task, reason, msg string) {
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type:               "WritebackPending",
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: task.Generation,
	})
	if err := r.Status().Update(ctx, task); err != nil {
		log.FromContext(ctx).Error(err, "writeback: clear WritebackPending", "task", task.Name)
	}
}

// providerForRemote is a best-effort heuristic used only when
// Task.spec.source.provider is unset. Prefer setting that field explicitly.
func providerForRemote(ctx context.Context, remote string) string {
	lower := strings.ToLower(remote)
	if strings.Contains(lower, "gitlab") {
		return "gitlab"
	}
	if strings.Contains(lower, "github") {
		return "github"
	}
	log.FromContext(ctx).Info("writeback: provider unknown from remote URL, defaulting to github",
		"remote", remote)
	return "github"
}

// taskBranch returns the deterministic branch name for a Task's agent run.
// Convention: tatara/task-<task-name>. The branch is communicated to the agent
// via the turn prompts (turnloop.go planTurnText/turnText) and TASK_BRANCH env;
// the operator opens the PR/MR targeting this same branch.
func taskBranch(t *tatarav1alpha1.Task) string {
	return agent.TaskBranch(t)
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 72 {
		s = s[:72]
	}
	if s == "" {
		return "tatara automated change"
	}
	return s
}

func writeBackBody(t *tatarav1alpha1.Task) string {
	b := t.Status.ResultSummary
	if b == "" {
		b = t.Spec.Goal
	}
	if t.Spec.Source != nil && t.Spec.Source.URL != "" {
		b += "\n\nSource: " + t.Spec.Source.URL
	}
	return b
}

func (r *TaskReconciler) scmToken(ctx context.Context, ns, ref string) (string, error) {
	var sec corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: ref}, &sec); err != nil {
		return "", fmt.Errorf("get scm secret: %w", err)
	}
	v, ok := sec.Data["token"]
	if !ok {
		return "", fmt.Errorf("scm secret %q missing token key", ref)
	}
	return string(v), nil
}
