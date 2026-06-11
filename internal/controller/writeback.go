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

// Writer is the SCM egress contract the reconciler uses. It is the full
// scm.SCMWriter; SCMFor returns it and tests fake it.
type Writer = scm.SCMWriter

// doWriteBack opens a PR/MR for each Project repo that has the task branch,
// comments the primary issue with all PR links, and records them on the Task
// status. It is called when WritebackPending is True and prURL is not yet set.
// Permanent SCM errors (4xx) per repo are logged and skipped; transient errors
// are returned for requeue.
func (r *TaskReconciler) doWriteBack(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	// Idempotency guard: already done.
	if task.Status.PrURL != "" {
		r.clearWritebackPending(ctx, task, "AlreadyWritten", "pr/mr url already set")
		return ctrl.Result{}, nil
	}

	switch task.Spec.Kind {
	case "review":
		return r.writeBackReview(ctx, task)
	case "selfImprove":
		return r.writeBackSelfImprove(ctx, task)
	case "triageIssue":
		return r.writeBackIssue(ctx, task)
	case "brainstorm":
		// Brainstorm proposals are created via propose_issue which spawns child
		// Tasks. The brainstorm Task itself never opens a PR.
		r.clearWritebackPending(ctx, task, "BrainstormProposed", "brainstorm proposals created via propose_issue; no PR to open")
		return ctrl.Result{}, nil
	default:
		// implement and other future kinds that open a change.
	}

	return r.writeBackOpenChange(ctx, task)
}

// writeBackOpenChange opens a PR/MR for each Project repo that has the task
// branch, comments the primary issue with all PR links, and records them on
// the Task status. Shared by the default (implement/brainstorm) path and the
// triageIssue-implement path.
func (r *TaskReconciler) writeBackOpenChange(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

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
	return b + "\n\n" + tataraAuthoredMarker
}

const tataraAuthoredMarker = "<!-- tatara-authored -->"

// createProposal opens the proposed issue with the approval label, places it
// on the board in the "Proposed" column, records the Source + DiscoveredIssues,
// and stays in AwaitingApproval. It is the only SCM egress for proposals.
func (r *TaskReconciler) createProposal(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	if proj.Spec.Scm == nil {
		return ctrl.Result{}, fmt.Errorf("proposal: project %q has no scm spec", proj.Name)
	}
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.ProposedIssue.RepositoryRef}, &repo); err != nil {
		return ctrl.Result{}, fmt.Errorf("proposal: get repository: %w", err)
	}
	writer, err := r.SCMFor(proj.Spec.Scm.Provider)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("proposal: scm writer: %w", err)
	}
	token, err := r.scmToken(ctx, task.Namespace, proj.Spec.ScmSecretRef)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("proposal: scm token: %w", err)
	}
	label := proj.Spec.Scm.ApprovalLabel
	if label == "" {
		label = "tatara/awaiting-approval"
	}
	body := task.Spec.ProposedIssue.Body + "\n\n" + tataraAuthoredMarker
	ref, err := writer.CreateIssue(ctx, repo.Spec.URL, token, scm.IssueReq{
		Title:  task.Spec.ProposedIssue.Title,
		Body:   body,
		Labels: []string{label},
	})
	if err != nil {
		r.Metrics.SCMWrite(proj.Spec.Scm.Provider, "create_issue", "error")
		return ctrl.Result{}, fmt.Errorf("proposal: create issue: %w", err)
	}
	r.Metrics.SCMWrite(proj.Spec.Scm.Provider, "create_issue", "ok")
	if proj.Spec.Scm.Board != nil {
		board := boardRefFromSpec(proj.Spec.Scm)
		if err := writer.AddBoardItem(ctx, token, board, ref.URL); err != nil {
			l.Error(err, "proposal: add board item (non-fatal)")
			r.Metrics.SCMWrite(proj.Spec.Scm.Provider, "board_add", "error")
		} else {
			r.Metrics.SCMWrite(proj.Spec.Scm.Provider, "board_add", "ok")
			if err := writer.SetBoardColumn(ctx, token, board, ref.URL, "Proposed"); err != nil {
				l.Error(err, "proposal: set board column (non-fatal)")
				r.Metrics.SCMWrite(proj.Spec.Scm.Provider, "board_column", "error")
			} else {
				r.Metrics.SCMWrite(proj.Spec.Scm.Provider, "board_column", "ok")
			}
		}
	}
	task.Spec.Source = &tatarav1alpha1.TaskSource{
		Provider:    proj.Spec.Scm.Provider,
		IssueRef:    ref.Ref,
		URL:         ref.URL,
		Number:      0,
		IsPR:        false,
		AuthorLogin: proj.Spec.Scm.BotLogin,
	}
	if err := r.Update(ctx, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("proposal: record source: %w", err)
	}
	task.Status.Phase = "AwaitingApproval"
	task.Status.DiscoveredIssues = append(task.Status.DiscoveredIssues, ref.URL)
	now := metav1.Now()
	task.Status.GateEnteredAt = &now
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type:    tatarav1alpha1.ConditionApprovalApproved,
		Status:  metav1.ConditionFalse,
		Reason:  "AwaitingHuman",
		Message: "issue opened with approval label; awaiting removal",
	})
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("proposal: record status: %w", err)
	}
	l.Info("proposal issue opened", "action", "scm_propose_issue",
		"resource_id", task.Name, "project", proj.Name, "issue_ref", ref.Ref)
	return ctrl.Result{RequeueAfter: capRequeue}, nil
}

func boardRefFromSpec(s *tatarav1alpha1.ScmSpec) scm.BoardRef {
	b := scm.BoardRef{Provider: s.Provider, Owner: s.Owner}
	if s.Board != nil {
		b.GitHubProjectNumber = s.Board.GitHubProjectNumber
		b.GitLabBoardID = s.Board.GitLabBoardID
		b.StatusField = s.Board.StatusField
	}
	return b
}

// scmContext resolves project, primary repo, writer, token, and provider for a Task.
func (r *TaskReconciler) scmContext(ctx context.Context, task *tatarav1alpha1.Task) (tatarav1alpha1.Project, tatarav1alpha1.Repository, scm.SCMWriter, string, string, error) {
	var proj tatarav1alpha1.Project
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &proj); err != nil {
		return proj, tatarav1alpha1.Repository{}, nil, "", "", fmt.Errorf("writeback: get project: %w", err)
	}
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return proj, repo, nil, "", "", fmt.Errorf("writeback: get repository: %w", err)
	}
	provider := ""
	if task.Spec.Source != nil {
		provider = task.Spec.Source.Provider
	}
	if provider == "" {
		provider = providerForRemote(ctx, repo.Spec.URL)
	}
	writer, err := r.SCMFor(provider)
	if err != nil {
		return proj, repo, nil, "", provider, fmt.Errorf("writeback: scm writer: %w", err)
	}
	token, err := r.scmToken(ctx, task.Namespace, proj.Spec.ScmSecretRef)
	if err != nil {
		return proj, repo, writer, "", provider, fmt.Errorf("writeback: scm token: %w", err)
	}
	return proj, repo, writer, token, provider, nil
}

func (r *TaskReconciler) recordSCM(provider, verb string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	r.Metrics.SCMWrite(provider, verb, result)
}

// writeBackReview reads Status.ReviewVerdict and posts exactly one verb set.
// Never calls OpenChange.
func (r *TaskReconciler) writeBackReview(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	v := task.Status.ReviewVerdict
	if v == nil || task.Spec.Source == nil {
		r.clearWritebackPending(ctx, task, "NoVerdict", "review task without a verdict")
		return ctrl.Result{}, nil
	}
	proj, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	_ = proj
	number := task.Spec.Source.Number
	switch v.Decision {
	case "approve":
		err = writer.Approve(ctx, repo.Spec.URL, token, number, v.Body)
		r.recordSCM(provider, "approve", err)
	case "request_changes":
		err = writer.RequestChanges(ctx, repo.Spec.URL, token, number, v.Body)
		r.recordSCM(provider, "request_changes", err)
		if err == nil && len(v.Suggestions) > 0 {
			serr := writer.Suggest(ctx, repo.Spec.URL, token, number, toSCMSuggestions(v.Suggestions))
			r.recordSCM(provider, "suggest", serr)
		}
	case "comment":
		err = writer.Comment(ctx, token, task.Spec.Source.IssueRef, v.Body)
		r.recordSCM(provider, "comment", err)
	default:
		err = fmt.Errorf("unknown review decision %q", v.Decision)
	}
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("writeback review: %w", err)
	}
	l.Info("review verdict posted", "action", "scm_review", "resource_id", task.Name, "decision", v.Decision)
	r.clearWritebackPending(ctx, task, "Reviewed", "review verdict posted: "+v.Decision)
	return ctrl.Result{}, nil
}

// writeBackSelfImprove reads Status.PROutcome and merges or closes the PR per policy.
// Never calls OpenChange.
func (r *TaskReconciler) writeBackSelfImprove(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	out := task.Status.PROutcome
	if out == nil || task.Spec.Source == nil {
		r.clearWritebackPending(ctx, task, "NoOutcome", "selfImprove task without an outcome")
		return ctrl.Result{}, nil
	}
	proj, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	number := task.Spec.Source.Number

	// Authorship gate (security boundary): hard-require the live PR/MR author to
	// be the project bot before merging OR closing, regardless of MergePolicy.
	// The agent must never act on a PR it does not own.
	if proj.Spec.Scm == nil || proj.Spec.Scm.BotLogin == "" {
		r.clearWritebackPending(ctx, task, "AuthorshipWithheld", "project has no scm.botLogin")
		return ctrl.Result{}, nil
	}
	st, perr := writer.GetPRState(ctx, repo.Spec.URL, token, number)
	if perr != nil {
		return ctrl.Result{}, fmt.Errorf("writeback selfImprove: authorship gate: %w", perr)
	}
	if st.Author != proj.Spec.Scm.BotLogin {
		l.Info("self-improve write-back withheld: PR not bot-authored",
			"action", "scm_authorship_withheld", "resource_id", task.Name, "author", st.Author)
		r.clearWritebackPending(ctx, task, "AuthorshipWithheld",
			"PR/MR author is not the project bot login")
		return ctrl.Result{}, nil
	}

	switch out.Action {
	case "close":
		err = writer.ClosePR(ctx, repo.Spec.URL, token, number, out.Reason)
		r.recordSCM(provider, "close", err)
	case "merge":
		ok, merr := r.mergeAllowed(ctx, &proj, repo, writer, token, number)
		if merr != nil {
			return ctrl.Result{}, merr
		}
		if !ok {
			l.Info("self-improve merge withheld: policy not satisfied", "action", "scm_merge_withheld", "resource_id", task.Name)
			r.clearWritebackPending(ctx, task, "MergeWithheld", "merge policy not satisfied")
			return ctrl.Result{}, nil
		}
		err = writer.Merge(ctx, repo.Spec.URL, token, number, "squash")
		r.recordSCM(provider, "merge", err)
	default:
		err = fmt.Errorf("unknown pr outcome %q", out.Action)
	}
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("writeback selfImprove: %w", err)
	}
	l.Info("self-improve outcome applied", "action", "scm_pr_outcome", "resource_id", task.Name, "outcome", out.Action)
	r.clearWritebackPending(ctx, task, "PROutcomeApplied", "pr outcome applied: "+out.Action)
	return ctrl.Result{}, nil
}

// writeBackIssue applies a triageIssue Task's IssueOutcome: close calls
// CloseIssue with the agent's comment; implement records the marker only (the
// PR opened during the agent run is the artifact, re-entering the author-gated
// path). Never calls OpenChange.
func (r *TaskReconciler) writeBackIssue(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	out := task.Status.IssueOutcome
	if out == nil || task.Spec.Source == nil {
		r.clearWritebackPending(ctx, task, "NoOutcome", "triageIssue task without an outcome")
		return ctrl.Result{}, nil
	}
	// Safety gate: triageIssue must never close a PR.
	if task.Spec.Source.IsPR {
		l.Error(fmt.Errorf("triageIssue source is a PR"), "writeback issue: refusing to close a PR",
			"action", "scm_issue_refused_pr", "resource_id", task.Name, "number", task.Spec.Source.Number)
		r.clearWritebackPending(ctx, task, "IssueRefusedPR", "triageIssue source is a PR; CloseIssue withheld")
		return ctrl.Result{}, nil
	}
	// Re-assert kind (defence-in-depth).
	if task.Spec.Kind != "triageIssue" {
		l.Error(fmt.Errorf("unexpected kind %q in writeBackIssue", task.Spec.Kind), "writeback issue: wrong kind",
			"action", "scm_issue_wrong_kind", "resource_id", task.Name)
		r.clearWritebackPending(ctx, task, "IssueWrongKind", "writeBackIssue called for non-triageIssue task")
		return ctrl.Result{}, nil
	}
	if out.Action == "implement" {
		r.Metrics.IssueOutcome("implement")
		l.Info("issue outcome implement: opening PR from agent branch", "action", "scm_issue_outcome", "resource_id", task.Name, "outcome", "implement")
		// Route through the shared OpenChange path so the agent's pushed branch
		// becomes a tatara-authored PR re-entering the author-gated review/merge path.
		return r.writeBackOpenChange(ctx, task)
	}
	// close
	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	repoSlug, _, perr := repoSlugFromURL(repo.Spec.URL, provider)
	if perr != nil {
		return ctrl.Result{}, perr
	}
	if cerr := writer.CloseIssue(ctx, token, repoSlug, task.Spec.Source.Number, out.Comment); cerr != nil {
		r.recordSCM(provider, "close_issue", cerr)
		return ctrl.Result{}, fmt.Errorf("writeback issue close: %w", cerr)
	}
	r.recordSCM(provider, "close_issue", nil)
	r.Metrics.IssueOutcome("close")
	l.Info("issue closed", "action", "scm_issue_outcome", "resource_id", task.Name, "outcome", "close", "number", task.Spec.Source.Number)
	r.clearWritebackPending(ctx, task, "IssueClosed", "issue closed with comment")
	return ctrl.Result{}, nil
}

// repoSlugFromURL derives the provider-correct repo slug (owner/name for
// GitHub, group/proj path for GitLab) that CloseIssue expects.
func repoSlugFromURL(repoURL, provider string) (string, string, error) {
	if provider == "gitlab" {
		proj, err := scm.GitLabProjectPath(repoURL)
		return proj, "", err
	}
	owner, name, err := scm.OwnerRepo(repoURL)
	return owner + "/" + name, "", err
}

// selfImproveBotAuthored reports whether the selfImprove PR/MR is actually
// authored by the project's bot login, by consulting the live PR state. It is
// the authoritative pre-spawn authorship gate: the agent must never be allowed
// to push to / merge / close a PR it does not own.
func (r *TaskReconciler) selfImproveBotAuthored(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (bool, error) {
	if proj.Spec.Scm == nil || proj.Spec.Scm.BotLogin == "" {
		return false, fmt.Errorf("authorship gate: project %q has no scm.botLogin", proj.Name)
	}
	if task.Spec.Source == nil {
		return false, fmt.Errorf("authorship gate: selfImprove task %q has no source", task.Name)
	}
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return false, fmt.Errorf("authorship gate: get repository: %w", err)
	}
	provider := task.Spec.Source.Provider
	if provider == "" {
		provider = providerForRemote(ctx, repo.Spec.URL)
	}
	writer, err := r.SCMFor(provider)
	if err != nil {
		return false, fmt.Errorf("authorship gate: scm writer: %w", err)
	}
	token, err := r.scmToken(ctx, task.Namespace, proj.Spec.ScmSecretRef)
	if err != nil {
		return false, fmt.Errorf("authorship gate: scm token: %w", err)
	}
	st, err := writer.GetPRState(ctx, repo.Spec.URL, token, task.Spec.Source.Number)
	if err != nil {
		return false, fmt.Errorf("authorship gate: get pr state: %w", err)
	}
	return st.Author == proj.Spec.Scm.BotLogin, nil
}

// mergeAllowed enforces MergePolicy. autoMergeOnGreenCI merges only when CI is
// present and green; CI absent falls back to afterApproval (trusts pr_outcome=merge
// as the agent's relay of an approving signal).
func (r *TaskReconciler) mergeAllowed(ctx context.Context, proj *tatarav1alpha1.Project, repo tatarav1alpha1.Repository, writer scm.SCMWriter, token string, number int) (bool, error) {
	policy := "afterApproval"
	if proj.Spec.Scm != nil && proj.Spec.Scm.MergePolicy != "" {
		policy = proj.Spec.Scm.MergePolicy
	}
	if policy == "autoMergeOnGreenCI" {
		st, err := writer.GetPRState(ctx, repo.Spec.URL, token, number)
		if err != nil {
			return false, fmt.Errorf("merge policy: get pr state: %w", err)
		}
		if st.CIStatus == "success" {
			return true, nil
		}
		if st.CIStatus != "" {
			return false, nil // CI present but not green
		}
		// CI absent -> fall back to afterApproval below.
	}
	// afterApproval: trust pr_outcome=merge as the agent's relay of an approving signal.
	return true, nil
}

func toSCMSuggestions(in []tatarav1alpha1.Suggestion) []scm.Suggestion {
	out := make([]scm.Suggestion, 0, len(in))
	for _, s := range in {
		out = append(out, scm.Suggestion{Path: s.Path, Line: s.Line, Body: s.Body})
	}
	return out
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
