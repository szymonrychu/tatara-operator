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
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// Writer opens changes and comments on the originating work item. Implemented
// by the per-provider scm clients; faked in tests.
type Writer interface {
	OpenChange(ctx context.Context, repoURL, token, sourceBranch, targetBranch, title, body string) (string, error)
	Comment(ctx context.Context, token, issueRef, body string) error
}

// doWriteBack opens a PR/MR for the task and optionally comments on the source
// issue. It is called when WritebackPending is True and prURL is not yet set.
// Permanent SCM errors (4xx) are recorded without requeue; transient errors are
// returned for requeue.
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

	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return ctrl.Result{}, fmt.Errorf("writeback: get repository: %w", err)
	}

	provider := ""
	if task.Spec.Source != nil {
		provider = task.Spec.Source.Provider
	}
	if provider == "" {
		provider = providerForRemote(repo.Spec.URL)
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

	sourceBranch := taskBranch(task)
	title := firstLine(task.Spec.Goal)
	body := writeBackBody(task)

	prURL, err := writer.OpenChange(ctx, repo.Spec.URL, token, sourceBranch, repo.Spec.DefaultBranch, title, body)
	if err != nil {
		l.Error(err, "writeback: open change", "task", task.Name)
		// Permanent 4xx: record failure and clear the condition (no infinite requeue).
		var he *scm.HTTPError
		if errors.As(err, &he) && he.Status >= 400 && he.Status < 500 {
			r.clearWritebackPending(ctx, task, "OpenChangeFailed", fmt.Sprintf("open change: %v", err))
			return ctrl.Result{}, nil
		}
		// Also accept any error that implements IsPermanent() bool.
		type permanenter interface{ IsPermanent() bool }
		if p, ok := err.(permanenter); ok && p.IsPermanent() {
			r.clearWritebackPending(ctx, task, "OpenChangeFailed", fmt.Sprintf("open change permanent: %v", err))
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("writeback: open change: %w", err)
	}

	// Record prURL and clear the condition atomically via Status().Update.
	task.Status.PrURL = prURL
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type:               "WritebackPending",
		Status:             metav1.ConditionFalse,
		Reason:             "Written",
		Message:            "pr/mr opened: " + prURL,
		ObservedGeneration: task.Generation,
	})
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("writeback: update prURL: %w", err)
	}

	l.Info("writeback: pr/mr opened",
		"task", task.Name, "pr_url", prURL,
		"has_source", task.Spec.Source != nil && task.Spec.Source.IssueRef != "")

	// Comment on the originating work item (non-fatal).
	if task.Spec.Source != nil && task.Spec.Source.IssueRef != "" {
		commentBody := writeBackBody(task) + "\n\nPR/MR: " + prURL
		if err := writer.Comment(ctx, token, task.Spec.Source.IssueRef, commentBody); err != nil {
			l.Error(err, "writeback: comment on work item (non-fatal)",
				"issue_ref", task.Spec.Source.IssueRef)
			// Non-fatal: PR exists; continue.
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

func providerForRemote(remote string) string {
	if strings.Contains(strings.ToLower(remote), "gitlab") {
		return "gitlab"
	}
	return "github"
}

// taskBranch returns the deterministic branch name for a Task's agent run.
// Convention: tatara/task-<task-name>. The wrapper pod is told to push to
// this branch via REPO_BRANCH env; M5 uses this same convention for write-back.
func taskBranch(t *tatarav1alpha1.Task) string {
	return "tatara/task-" + t.Name
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
