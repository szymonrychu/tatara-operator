package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
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
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "AlreadyWritten", "pr/mr url already set")
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
		// Only claim BrainstormProposed when at least one proposal child Task
		// exists; otherwise use BrainstormComplete so a no-yield run is visible.
		if r.brainstormHasProposal(ctx, task) {
			return ctrl.Result{}, r.clearWritebackPending(ctx, task, "BrainstormProposed", "brainstorm proposals created via propose_issue; no PR to open")
		}
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "BrainstormComplete", "brainstorm finished with no proposal filed via propose_issue")
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
	// Idempotency guard: if PrURL is already set this function ran successfully on
	// a previous reconcile. Clear WritebackPending and return without re-opening.
	if task.Status.PrURL != "" {
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "AlreadyWritten", "pr/mr url already set")
	}

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
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "SCMError", fmt.Sprintf("scm writer: %v", err))
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
	baseBody := writeBackBody(task)

	// M4: when the agent submitted a change_summary, use PRTitle + PRBody +
	// Delivered block as the MR title/body instead of the M1 defaults.
	if cs := task.Status.ChangeSummary; cs != nil {
		if cs.PRTitle != "" {
			title = cs.PRTitle
		}
		deliveredBody := cs.PRBody
		if cs.DeliveredScope != "" {
			deliveredBody += "\n\n## Delivered\n" + cs.DeliveredScope
		}
		// Preserve the tatara-authored marker so downstream merge-gate logic works.
		deliveredBody += "\n\n" + tataraAuthoredMarker
		baseBody = deliveredBody
	}

	var prURLs []string
	var lastSkipStatus int
	for _, repo := range ordered {
		body := baseBody
		// Append "Closes #N" for the primary repo of an issue-linked lifecycle task
		// so the MR auto-closes the issue on merge.  Never emit this on secondary
		// repos (cross-repo leak) or for non-lifecycle / PR-entry tasks.
		if repo.Name == primaryRepo.Name &&
			task.Spec.Kind == "issueLifecycle" &&
			task.Spec.Source != nil &&
			!task.Spec.Source.IsPR &&
			task.Spec.Source.Number > 0 {
			body = body + "\n\nCloses #" + strconv.Itoa(task.Spec.Source.Number)
		}
		prURL, openErr := writer.OpenChange(ctx, repo.Spec.URL, token, sourceBranch, repo.Spec.DefaultBranch, title, body)
		r.recordSCM(provider, "open_change", openErr)
		if openErr != nil {
			var he *scm.HTTPError
			if errors.As(openErr, &he) && he.Status >= 400 && he.Status < 500 {
				// 4xx permanent: skip this repo. A 422 "No commits" means the
				// implement run produced nothing (empty branch); log it distinctly
				// so a fix that never landed is visible, not masked as a generic skip.
				// A 422 "A pull request already exists" means OpenChange succeeded on a
				// prior reconcile but the PrURL status update failed; recover the
				// existing PR URL so the lifecycle path is not mis-routed into the
				// empty-implement / 'refused' branch.
				skipReason := openChangeSkipReason(he)
				if skipReason == "no-change" {
					l.Info("writeback: implement produced no changes (branch has no commits)",
						"action", "writeback_no_change", "repo", repo.Name, "task", task.Name, "branch", sourceBranch)
					r.Metrics.WritebackOutcome("no_change")
				} else if skipReason == "already-exists" {
					if recovered, rerr := r.recoverExistingPRURL(ctx, token, provider, repo.Spec.URL, sourceBranch); rerr == nil && recovered != "" {
						l.Info("writeback: pr/mr already exists, recovered url",
							"action", "writeback_pr_recovered", "repo", repo.Name, "task", task.Name, "pr_url", recovered)
						prURLs = append(prURLs, recovered)
						// Persist the primary PR URL after recovery so a later transient
						// failure on another repo does not lose this URL.
						if len(prURLs) == 1 {
							if perr := r.persistPrimaryPRURL(ctx, task, prURLs[0]); perr != nil {
								return ctrl.Result{}, perr
							}
						}
						continue
					}
					l.Info("writeback: skipping repo (4xx - already exists, could not recover)",
						"action", "writeback_skip_4xx", "repo", repo.Name, "task", task.Name, "status", he.Status, "path", he.Path, "body", he.Body)
					r.Metrics.WritebackOutcome("skip_4xx")
				} else {
					l.Info("writeback: skipping repo (4xx)",
						"action", "writeback_skip_4xx", "repo", repo.Name, "task", task.Name, "status", he.Status, "path", he.Path, "body", he.Body)
					r.Metrics.WritebackOutcome("skip_4xx")
				}
				lastSkipStatus = he.Status
				continue
			}
			return ctrl.Result{}, fmt.Errorf("writeback: open change for %s: %w", repo.Name, openErr)
		}
		l.Info("writeback: pr/mr opened", "task", task.Name, "repo", repo.Name, "pr_url", prURL)
		r.Metrics.WritebackOutcome("opened")
		prURLs = append(prURLs, prURL)
		// Persist the primary PR URL immediately after the first successful OpenChange
		// so a transient failure on a later repo does not lose the already-opened URL.
		// A requeue then finds PrURL set and skips re-opening.
		if len(prURLs) == 1 {
			if perr := r.persistPrimaryPRURL(ctx, task, prURLs[0]); perr != nil {
				return ctrl.Result{}, perr
			}
		}
	}

	if len(prURLs) == 0 {
		// No repo had the branch / no code change: still post the agent's result
		// to the issue, so report/question/verify tasks surface their answer
		// (otherwise the work is invisible - no PR and no comment).
		// Only surface a real result. An empty ResultSummary means the agent
		// reported nothing; echoing task.Spec.Goal would post the issue body
		// back verbatim (noise), so stay silent. issueLifecycle Implement runs are
		// also skipped here: finishImplement owns those issue comments (silent
		// retries plus a single escalation), so echoing the per-run ResultSummary
		// would spam the issue once per empty retry.
		commented := task.Spec.Source != nil && task.Spec.Source.IssueRef != "" &&
			task.Status.ResultSummary != "" && task.Status.LifecycleState != "Implement"
		if commented {
			cerr := writer.Comment(ctx, token, task.Spec.Source.IssueRef, task.Status.ResultSummary)
			r.recordSCM(provider, "comment", cerr)
			if cerr != nil {
				l.Error(cerr, "writeback: comment result on work item (non-fatal)",
					"issue_ref", task.Spec.Source.IssueRef)
			}
		}
		msg := "no PR opened; no result commented"
		if commented {
			msg = "no PR opened; result commented on the issue"
		}
		if lastSkipStatus != 0 {
			msg = fmt.Sprintf("PR/MR could not be opened or already exists: %d", lastSkipStatus)
		}
		r.Metrics.WritebackOutcome("no_pr")
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "WritebackSkipped", msg)
	}

	// Record primary PR URL (first in list) and all URLs in the condition message.
	// RetryOnConflict ensures this idempotency key lands even when a concurrent
	// lifecycle reconcile has bumped the resource version.
	prURLsMsg := strings.Join(prURLs, " ")
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); gerr != nil {
			return gerr
		}
		fresh.Status.PrURL = prURLs[0]
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               "WritebackPending",
			Status:             metav1.ConditionFalse,
			Reason:             "Written",
			Message:            "pr/mr opened: " + prURLsMsg,
			ObservedGeneration: fresh.Generation,
		})
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("writeback: update prURL: %w", err)
	}

	// Comment on the originating issue with all PR links (non-fatal).
	if task.Spec.Source != nil && task.Spec.Source.IssueRef != "" {
		commentBody := "Done - opened PR/MR:\n" + strings.Join(prURLs, "\n")
		// Append the agent's summary only when it produced one; never fall back
		// to task.Spec.Goal (that just echoes the issue body).
		if task.Status.ResultSummary != "" {
			commentBody += "\n\n" + task.Status.ResultSummary
		}
		cerr := writer.Comment(ctx, token, task.Spec.Source.IssueRef, commentBody)
		r.recordSCM(provider, "comment", cerr)
		if cerr != nil {
			l.Error(cerr, "writeback: comment on work item (non-fatal)",
				"issue_ref", task.Spec.Source.IssueRef)
			// Non-fatal: PRs exist; continue.
		}
	}

	return ctrl.Result{}, nil
}

// persistPrimaryPRURL writes the primary PR URL to Status.PrURL under
// RetryOnConflict. Called after the first successful OpenChange so a transient
// failure on a later repo in the loop does not lose the already-opened URL.
func (r *TaskReconciler) persistPrimaryPRURL(ctx context.Context, task *tatarav1alpha1.Task, prURL string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if fresh.Status.PrURL != "" {
			return nil // already persisted (e.g. concurrent reconcile)
		}
		fresh.Status.PrURL = prURL
		return r.Status().Update(ctx, fresh)
	})
}

// clearWritebackPending sets WritebackPending=False and updates status.
// RetryOnConflict handles concurrent reconcile updates so the clear always lands.
// Returns an error when the clear fails so callers can propagate it and avoid
// treating a non-idempotent egress verb as committed when the marker was not stored.
func (r *TaskReconciler) clearWritebackPending(ctx context.Context, task *tatarav1alpha1.Task, reason, msg string) error {
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               "WritebackPending",
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            msg,
			ObservedGeneration: fresh.Generation,
		})
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		log.FromContext(ctx).Error(err, "writeback: clear WritebackPending", "task", task.Name)
		return err
	}
	return nil
}

// brainstormHasProposal reports whether at least one proposal Task from THIS
// brainstorm run exists. A proposal Task is any Task in the same namespace with
// spec.proposedIssue set and the same spec.projectRef, created by the agent
// calling the propose_issue MCP tool. Proposal Tasks carry no per-run linkage
// (the REST handler owns them to the Project, not the brainstorm Task), so the
// run is scoped by creation time: only proposals created at/after this
// brainstorm Task count, excluding prior-cycle proposals.
//
// For repo-scoped brainstorm tasks (non-empty RepositoryRef) the filter
// additionally restricts to the same repo. For project-scoped tasks (empty
// RepositoryRef) any proposal in the project counts regardless of repo.
func (r *TaskReconciler) brainstormHasProposal(ctx context.Context, task *tatarav1alpha1.Task) bool {
	var list tatarav1alpha1.TaskList
	err := r.List(ctx, &list,
		client.InNamespace(task.Namespace),
		client.MatchingFields{taskIndexProjectRef: task.Spec.ProjectRef},
	)
	if err != nil && isFieldSelectorUnsupported(err) {
		// Fall back to full-namespace scan when the field index is not registered.
		// In a managed runtime this means the index was never registered; log WARN
		// so the misconfiguration is visible rather than silently degrading every
		// brainstorm dedup lookup to an unbounded namespace scan.
		log.FromContext(ctx).Info("writeback: brainstormHasProposal: field index unsupported, falling back to full-namespace scan (expected only in test environments without a manager)",
			"action", "writeback_brainstorm_fallback_scan", "task", task.Name, "namespace", task.Namespace)
		err = r.List(ctx, &list, client.InNamespace(task.Namespace))
	}
	if err != nil {
		log.FromContext(ctx).Error(err, "writeback: brainstormHasProposal: list tasks (treating as no proposal)", "task", task.Name)
		return false
	}
	projectScoped := task.Spec.RepositoryRef == ""
	for i := range list.Items {
		t := &list.Items[i]
		if t.Spec.ProposedIssue == nil {
			continue
		}
		if t.Spec.ProjectRef != task.Spec.ProjectRef {
			continue
		}
		// For repo-scoped brainstorm tasks, only count proposals for the same repo.
		if !projectScoped && t.Spec.RepositoryRef != task.Spec.RepositoryRef {
			continue
		}
		if !t.CreationTimestamp.Before(&task.CreationTimestamp) {
			return true
		}
	}
	return false
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
//
// Duplicate-prevention layers (Fix 4):
//
//	(A) Source-set idempotency guard: if task.Spec.Source.URL is already set the
//	    issue was created on a prior reconcile; skip straight to AwaitingApproval.
//	(B) RetryOnConflict wraps both the Spec.Source r.Update and the status update
//	    so they reliably land even when the API server races with another reconcile.
//	(C) Title-level idempotency: before calling CreateIssue, list open issues via
//	    the reader and skip creation if an open issue with the same title exists;
//	    set Source to the existing issue and proceed to AwaitingApproval.
//
// resolveRepository finds the Repository CR for a ProposedIssue.RepositoryRef.
// The brainstorm agent may pass either the CR name ("tatara-cli") or the SCM
// slug ("owner/tatara-cli"); both resolve here. Slug match is by the owner/repo
// of each Project Repository's URL. Without this, an agent that passes the slug
// makes createProposal 404 ("Repository not found"), the proposal never opens an
// issue, and the issue-count backpressure never trips.
func (r *TaskReconciler) resolveRepository(ctx context.Context, namespace, projectRef, ref string) (tatarav1alpha1.Repository, error) {
	var repo tatarav1alpha1.Repository
	// A bare CR name has no slash; a slug ("owner/repo") would be rejected by the
	// API server as an invalid name, so only attempt a direct Get for bare names.
	if !strings.Contains(ref, "/") {
		if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref}, &repo); err == nil {
			return repo, nil
		} else if !apierrors.IsNotFound(err) {
			return repo, err
		}
	}
	var list tatarav1alpha1.RepositoryList
	if err := r.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return repo, err
	}
	for i := range list.Items {
		it := list.Items[i]
		if it.Spec.ProjectRef != projectRef {
			continue
		}
		if it.Name == ref {
			return it, nil
		}
		if owner, name, oerr := scm.OwnerRepo(it.Spec.URL); oerr == nil && owner+"/"+name == ref {
			return it, nil
		}
	}
	return repo, fmt.Errorf("no Repository matches %q (tried CR name and owner/repo slug)", ref)
}

func (r *TaskReconciler) createProposal(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	if proj.Spec.Scm == nil {
		return ctrl.Result{}, fmt.Errorf("proposal: project %q has no scm spec", proj.Name)
	}

	// (A) Idempotency guard: Source already recorded means CreateIssue already ran.
	if task.Spec.Source != nil && task.Spec.Source.URL != "" {
		l.Info("proposal skipped: source already set",
			"action", "scm_propose_skip_source_set", "resource_id", task.Name,
			"issue_url", task.Spec.Source.URL)
		return r.completeProposal(ctx, task, task.Spec.Source.URL)
	}

	repo, err := r.resolveRepository(ctx, task.Namespace, proj.Name, task.Spec.ProposedIssue.RepositoryRef)
	if err != nil {
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

	// (C) Title-level idempotency: skip CreateIssue if an open issue with the
	// same title already exists. Matching on exact title among tatara-authored
	// issues is safe because the title is deterministic (brainstorm generates it).
	// Body/marker check is skipped: if a human opened an issue with the exact
	// same title it is still safer to track it than to create a duplicate.
	proposalTitle := task.Spec.ProposedIssue.Title
	if r.ReaderFor != nil {
		if existing, found, rerr := r.findOpenIssueByTitle(ctx, proj, repo.Spec.URL, token, proposalTitle); rerr != nil {
			l.Error(rerr, "proposal: list open issues for dedup check (non-fatal, proceeding with create)")
		} else if found {
			l.Info("proposal skipped: duplicate exists",
				"action", "scm_propose_skip_duplicate",
				"resource_id", task.Name,
				"existing_number", existing.Number)
			return r.recordExistingProposal(ctx, proj, task, existing, repo.Spec.URL)
		}
	}

	brainstorming, _, _, _ := lifecycleLabels(proj.Spec.Scm)
	label := brainstorming
	body := task.Spec.ProposedIssue.Body + "\n\n" + tataraAuthoredMarker
	ref, err := writer.CreateIssue(ctx, repo.Spec.URL, token, scm.IssueReq{
		Title:  proposalTitle,
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

	src := &tatarav1alpha1.TaskSource{
		Provider:    proj.Spec.Scm.Provider,
		IssueRef:    ref.Ref,
		URL:         ref.URL,
		Number:      0,
		IsPR:        false,
		AuthorLogin: proj.Spec.Scm.BotLogin,
	}

	// (B) RetryOnConflict: record Spec.Source; re-Get the task inside the closure
	// so the write lands even when another reconcile has bumped the resource version.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); gerr != nil {
			return gerr
		}
		fresh.Spec.Source = src
		return r.Update(ctx, fresh)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("proposal: record source: %w", err)
	}

	l.Info("proposal issue opened", "action", "scm_propose_issue",
		"resource_id", task.Name, "project", proj.Name, "issue_ref", ref.Ref)

	return r.completeProposal(ctx, task, ref.URL)
}

// completeProposal marks the brainstorm proposal Task Succeeded after the idea
// issue has been opened. The issue (now carrying the idea label) flows through
// the normal issue lifecycle from here; there is no AwaitingApproval parking.
func (r *TaskReconciler) completeProposal(ctx context.Context, task *tatarav1alpha1.Task, issueURL string) (ctrl.Result, error) {
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); gerr != nil {
			return gerr
		}
		fresh.Status.Phase = "Succeeded"
		present := false
		for _, u := range fresh.Status.DiscoveredIssues {
			if u == issueURL {
				present = true
				break
			}
		}
		if !present {
			fresh.Status.DiscoveredIssues = append(fresh.Status.DiscoveredIssues, issueURL)
		}
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               "WritebackPending",
			Status:             metav1.ConditionFalse,
			Reason:             "BrainstormProposed",
			Message:            "proposal issue opened with idea label",
			ObservedGeneration: fresh.Generation,
		})
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("proposal: complete: %w", err)
	}
	return ctrl.Result{}, nil
}

// recordExistingProposal wires the task to an existing open issue that matches
// the proposal title, skipping CreateIssue. Used by the (C) title-dedup path.
// repoURL is the configured Repository URL; the base (scheme+host) is derived
// from it so self-hosted GitLab instances produce a correct issue URL instead
// of the hardcoded gitlab.com host.
func (r *TaskReconciler) recordExistingProposal(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, existing scm.IssueRef, repoURL string) (ctrl.Result, error) {
	issueURL := issueURLFromRepoURL(repoURL, proj.Spec.Scm.Provider, existing.Repo, existing.Number)
	issueRef := fmt.Sprintf("%s#%d", existing.Repo, existing.Number)

	src := &tatarav1alpha1.TaskSource{
		Provider:    proj.Spec.Scm.Provider,
		IssueRef:    issueRef,
		URL:         issueURL,
		Number:      existing.Number,
		IsPR:        false,
		AuthorLogin: proj.Spec.Scm.BotLogin,
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); gerr != nil {
			return gerr
		}
		fresh.Spec.Source = src
		return r.Update(ctx, fresh)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("proposal: record existing source: %w", err)
	}

	return r.completeProposal(ctx, task, issueURL)
}

// findOpenIssueByTitle lists open issues for the repo and returns the first
// one whose Title matches proposalTitle exactly. Returns (zero, false, nil)
// when no match is found, (zero, false, err) on list failure.
func (r *TaskReconciler) findOpenIssueByTitle(ctx context.Context, proj *tatarav1alpha1.Project, repoURL, token, proposalTitle string) (scm.IssueRef, bool, error) {
	reader, err := r.ReaderFor(proj.Spec.Scm.Provider, token)
	if err != nil {
		return scm.IssueRef{}, false, fmt.Errorf("proposal: reader for %s: %w", proj.Spec.Scm.Provider, err)
	}
	// Derive the provider-correct project path.
	// For GitLab, use the full project path (supports subgroups) derived from the
	// repo URL. owner+"/"+repo produces "owner/" when OwnerRepo errors (GitLab
	// subgroup URLs) which 404s, so we use GitLabProjectPath directly.
	// For GitHub, owner+"/"+repo is the correct two-segment slug.
	var owner, repo string
	if proj.Spec.Scm != nil && proj.Spec.Scm.Provider == "gitlab" {
		glPath, gerr := scm.GitLabProjectPath(repoURL)
		if gerr != nil {
			return scm.IssueRef{}, false, fmt.Errorf("proposal: gitlab project path: %w", gerr)
		}
		// ListOpenIssues for GitLab expects owner=full-project-path, repo="".
		owner = glPath
		repo = ""
	} else {
		owner, repo, err = scm.OwnerRepo(repoURL)
		if err != nil {
			return scm.IssueRef{}, false, fmt.Errorf("proposal: owner/repo from url: %w", err)
		}
	}
	issues, err := reader.ListOpenIssues(ctx, owner, repo)
	r.recordSCM(proj.Spec.Scm.Provider, "list_open_issues", err)
	if err != nil {
		return scm.IssueRef{}, false, fmt.Errorf("proposal: list open issues: %w", err)
	}
	for _, iss := range issues {
		if !iss.IsPR && iss.Title == proposalTitle {
			return iss, true, nil
		}
	}
	return scm.IssueRef{}, false, nil
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
// It must not be called for project-scoped tasks (empty RepositoryRef).
func (r *TaskReconciler) scmContext(ctx context.Context, task *tatarav1alpha1.Task) (tatarav1alpha1.Project, tatarav1alpha1.Repository, scm.SCMWriter, string, string, error) {
	var proj tatarav1alpha1.Project
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &proj); err != nil {
		return proj, tatarav1alpha1.Repository{}, nil, "", "", fmt.Errorf("writeback: get project: %w", err)
	}
	if task.Spec.RepositoryRef == "" {
		return proj, tatarav1alpha1.Repository{}, nil, "", "", fmt.Errorf("writeback: scmContext called for project-scoped task %q (empty repositoryRef)", task.Name)
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
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "NoVerdict", "review task without a verdict")
	}
	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	number := task.Spec.Source.Number
	var verbSent bool
	switch v.Decision {
	case "approve":
		err = writer.Approve(ctx, repo.Spec.URL, token, number, v.Body)
		r.recordSCM(provider, "approve", err)
		verbSent = err == nil
	case "request_changes":
		err = writer.RequestChanges(ctx, repo.Spec.URL, token, number, v.Body)
		r.recordSCM(provider, "request_changes", err)
		verbSent = err == nil
		if err == nil && len(v.Suggestions) > 0 {
			serr := writer.Suggest(ctx, repo.Spec.URL, token, number, toSCMSuggestions(v.Suggestions))
			r.recordSCM(provider, "suggest", serr)
		}
	case "comment":
		// Build the comment target from repo URL + PR number (same addressing as
		// approve/request_changes). IssueRef may be the originating issue rather
		// than the PR, or empty, so derive a consistent ref here.
		slug, _, serr := repoSlugFromURL(repo.Spec.URL, provider)
		if serr != nil {
			return ctrl.Result{}, fmt.Errorf("writeback review comment: derive slug: %w", serr)
		}
		prRef := fmt.Sprintf("%s#%d", slug, number)
		err = writer.Comment(ctx, token, prRef, v.Body)
		r.recordSCM(provider, "comment", err)
		verbSent = err == nil
	default:
		err = fmt.Errorf("unknown review decision %q", v.Decision)
	}
	if err != nil {
		// If the verb reached the server (verbSent) but a later persistence step
		// fails, clear WritebackPending before returning so a requeue does not
		// re-post the same non-idempotent verb (duplicate approve/request_changes).
		if verbSent {
			// Propagate the clear error: if the clear fails the reconciler will
			// requeue; the verbSent guard above means on requeue we detect the
			// verb already landed and will not re-post it. Without propagating,
			// WritebackPending stays True and the verb is re-sent on every reconcile.
			if cerr := r.clearWritebackPending(ctx, task, "Reviewed", "review verdict posted: "+v.Decision); cerr != nil {
				return ctrl.Result{}, cerr
			}
		}
		return ctrl.Result{}, fmt.Errorf("writeback review: %w", err)
	}
	l.Info("review verdict posted", "action", "scm_review", "resource_id", task.Name, "decision", v.Decision)
	return ctrl.Result{}, r.clearWritebackPending(ctx, task, "Reviewed", "review verdict posted: "+v.Decision)
}

// writeBackSelfImprove reads Status.PROutcome and merges or closes the PR per policy.
// Never calls OpenChange.
func (r *TaskReconciler) writeBackSelfImprove(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	out := task.Status.PROutcome
	if out == nil || task.Spec.Source == nil {
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "NoOutcome", "selfImprove task without an outcome")
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
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "AuthorshipWithheld", "project has no scm.botLogin")
	}
	st, perr := writer.GetPRState(ctx, repo.Spec.URL, token, number)
	r.recordSCM(provider, "get_pr_state", perr)
	if perr != nil {
		return ctrl.Result{}, fmt.Errorf("writeback selfImprove: authorship gate: %w", perr)
	}
	if st.Author != proj.Spec.Scm.BotLogin {
		l.Info("self-improve write-back withheld: PR not bot-authored",
			"action", "scm_authorship_withheld", "resource_id", task.Name, "author", st.Author)
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "AuthorshipWithheld",
			"PR/MR author is not the project bot login")
	}

	switch out.Action {
	case "close":
		// Short-circuit: if the PR is already closed (e.g. clearWritebackPending
		// failed on a prior reconcile), skip ClosePR to avoid re-posting the close
		// comment on an already-closed PR. st was fetched by the authorship gate above.
		if !st.Closed {
			err = writer.ClosePR(ctx, repo.Spec.URL, token, number, out.Reason)
			r.recordSCM(provider, "close", err)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("writeback selfImprove: %w", err)
			}
		}
		// Clear WritebackPending immediately after a successful state change so
		// a requeue triggered by a transient comment failure does not re-call
		// ClosePR (which would re-post the close comment on an already-closed PR).
		l.Info("self-improve outcome applied", "action", "scm_pr_outcome", "resource_id", task.Name, "outcome", out.Action)
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "PROutcomeApplied", "pr outcome applied: "+out.Action)
	case "merge":
		if !r.mergeAllowed(&proj, st) {
			l.Info("self-improve merge withheld: policy not satisfied", "action", "scm_merge_withheld", "resource_id", task.Name)
			return ctrl.Result{}, r.clearWritebackPending(ctx, task, "MergeWithheld", "merge policy not satisfied")
		}
		_, err = writer.Merge(ctx, repo.Spec.URL, token, number, "squash")
		r.recordSCM(provider, "merge", err)
		// ErrMergeConflict -> merge conflict on an in-flight task.
		// Do NOT return the error: that would trigger controller-runtime backoff loop.
		// Before treating it as a conflict, re-check st.Merged: if the PR was
		// already merged on a prior reconcile (clearWritebackPending failed then),
		// treat as success rather than mis-labelling a completed merge as a conflict.
		if errors.Is(err, scm.ErrMergeConflict) {
			freshSt, stErr := writer.GetPRState(ctx, repo.Spec.URL, token, number)
			r.recordSCM(provider, "get_pr_state", stErr)
			if stErr == nil && freshSt.Merged {
				l.Info("self-improve merge: PR already merged; treating as success",
					"action", "scm_pr_outcome", "resource_id", task.Name, "outcome", "merge")
				return ctrl.Result{}, r.clearWritebackPending(ctx, task, "PROutcomeApplied", "pr outcome applied: merge (already merged)")
			}
			l.Info("self-improve merge conflict; clearing writeback pending",
				"action", "scm_selfimprove_conflict", "resource_id", task.Name)
			// Swallow the clear error here: MergeConflict is a terminal re-triage
			// signal; best-effort clear is acceptable and avoids a backoff loop.
			_ = r.clearWritebackPending(ctx, task, "MergeConflict", "merge conflict; left for re-triage")
			return ctrl.Result{}, nil
		}
	default:
		err = fmt.Errorf("unknown pr outcome %q", out.Action)
	}
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("writeback selfImprove: %w", err)
	}
	l.Info("self-improve outcome applied", "action", "scm_pr_outcome", "resource_id", task.Name, "outcome", out.Action)
	return ctrl.Result{}, r.clearWritebackPending(ctx, task, "PROutcomeApplied", "pr outcome applied: "+out.Action)
}

// writeBackIssue applies a triageIssue Task's IssueOutcome: close calls
// CloseIssue with the agent's comment; implement records the marker only (the
// PR opened during the agent run is the artifact, re-entering the author-gated
// path). Never calls OpenChange.
func (r *TaskReconciler) writeBackIssue(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	out := task.Status.IssueOutcome
	if out == nil || task.Spec.Source == nil {
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "NoOutcome", "triageIssue task without an outcome")
	}
	// Safety gate: triageIssue must never close a PR.
	if task.Spec.Source.IsPR {
		l.Error(fmt.Errorf("triageIssue source is a PR"), "writeback issue: refusing to close a PR",
			"action", "scm_issue_refused_pr", "resource_id", task.Name, "number", task.Spec.Source.Number)
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "IssueRefusedPR", "triageIssue source is a PR; CloseIssue withheld")
	}
	// Re-assert kind (defence-in-depth).
	if task.Spec.Kind != "triageIssue" {
		l.Error(fmt.Errorf("unexpected kind %q in writeBackIssue", task.Spec.Kind), "writeback issue: wrong kind",
			"action", "scm_issue_wrong_kind", "resource_id", task.Name)
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "IssueWrongKind", "writeBackIssue called for non-triageIssue task")
	}
	if out.Action == "implement" {
		r.Metrics.IssueOutcome("implement")
		l.Info("issue outcome implement: opening PR from agent branch", "action", "scm_issue_outcome", "resource_id", task.Name, "outcome", "implement")
		// Route through the shared OpenChange path so the agent's pushed branch
		// becomes a tatara-authored PR re-entering the author-gated review/merge path.
		return r.writeBackOpenChange(ctx, task)
	}
	// close
	// Invariant: never close an issue that has an unmerged code change. Only the
	// merged-and-green lifecycle (handleMainCI) may close such an issue.
	if hasUnmergedChange(task) {
		l.Info("issue close withheld: triageIssue has an unmerged change",
			"action", "scm_close_withheld", "resource_id", task.Name, "number", task.Spec.Source.Number,
			"pr_url", task.Status.PrURL, "head_branch", task.Status.HeadBranch)
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "CloseWithheldUnmerged", "issue has an unmerged change; close withheld")
	}
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
	return ctrl.Result{}, r.clearWritebackPending(ctx, task, "IssueClosed", "issue closed with comment")
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
	r.recordSCM(provider, "get_pr_state", err)
	if err != nil {
		return false, fmt.Errorf("authorship gate: get pr state: %w", err)
	}
	return st.Author == proj.Spec.Scm.BotLogin, nil
}

// mergeAllowed enforces MergePolicy. autoMergeOnGreenCI merges only when CI is
// present and green; CI absent falls back to afterApproval (trusts pr_outcome=merge
// as the agent's relay of an approving signal).
// st is the PR state already fetched by the authorship gate; passing it avoids
// a second GetPRState call on the hot merge path.
//
// afterApproval is an intentional trust-the-agent policy: the bot's pr_outcome=merge
// signal is treated as the agent relaying an approving signal (human review happened
// outside this gate). It does NOT consult live PR review state. If real approval
// gating is required, use autoMergeOnGreenCI combined with a branch protection rule
// requiring an approved review before CI can pass.
func (r *TaskReconciler) mergeAllowed(proj *tatarav1alpha1.Project, st scm.PRState) bool {
	policy := "afterApproval"
	if proj.Spec.Scm != nil && proj.Spec.Scm.MergePolicy != "" {
		policy = proj.Spec.Scm.MergePolicy
	}
	if policy == "autoMergeOnGreenCI" {
		if st.CIStatus == "success" {
			return true
		}
		if st.CIStatus != "" {
			return false // CI present but not green
		}
		// CI absent -> fall back to afterApproval below.
	}
	// afterApproval: trust pr_outcome=merge as the agent's relay of an approving signal.
	return true
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

// openChangeSkipReason classifies a 4xx OpenChange failure.
// "no-change": 422 "No commits between" - implement produced no commits.
// "already-exists": 422 "A pull request already exists" - PR was opened on a
// prior reconcile but PrURL status update failed; caller should recover the URL.
// "skip-4xx": any other 4xx permanent failure.
func openChangeSkipReason(he *scm.HTTPError) string {
	if he.Status == 422 && strings.Contains(he.Body, "No commits between") {
		return "no-change"
	}
	if he.Status == 422 && strings.Contains(he.Body, "A pull request already exists") {
		return "already-exists"
	}
	return "skip-4xx"
}

// recoverExistingPRURL finds the URL of an already-open PR for sourceBranch in
// the given repo. Called when OpenChange returns 422 "A pull request already
// exists" so the lifecycle path adopts the existing PR instead of treating the
// task as empty/refused. Returns ("", nil) when no matching PR is found.
func (r *TaskReconciler) recoverExistingPRURL(ctx context.Context, token, provider, repoURL, sourceBranch string) (string, error) {
	if r.ReaderFor == nil {
		log.FromContext(ctx).Info("writeback: cannot recover existing PR URL: no reader wired; recovery degraded to skip_4xx",
			"action", "writeback_recovery_no_reader", "repo_url", repoURL, "branch", sourceBranch)
		return "", nil
	}
	reader, err := r.ReaderFor(provider, token)
	if err != nil {
		return "", err
	}
	var owner, repo string
	if provider == "gitlab" {
		owner, err = scm.GitLabProjectPath(repoURL)
		if err != nil {
			return "", err
		}
	} else {
		owner, repo, err = scm.OwnerRepo(repoURL)
		if err != nil {
			return "", err
		}
	}
	prs, err := reader.ListOpenPRs(ctx, owner, repo)
	r.recordSCM(provider, "list_open_prs", err)
	if err != nil {
		return "", err
	}
	for _, pr := range prs {
		// PRRef.HeadBranch is now populated from the list API (GitHub head.ref /
		// GitLab source_branch), so no per-PR GetPRState fan-out is needed.
		if pr.HeadBranch == sourceBranch {
			// Construct the HTML PR URL from the repo URL base + PR number.
			slug, _, serr := repoSlugFromURL(repoURL, provider)
			if serr != nil {
				return "", serr
			}
			base, berr := parseRepoBase(repoURL)
			if berr != nil {
				return "", berr
			}
			if provider == "gitlab" {
				return fmt.Sprintf("%s/%s/-/merge_requests/%d", base, slug, pr.Number), nil
			}
			return fmt.Sprintf("%s/%s/pull/%d", base, slug, pr.Number), nil
		}
	}
	return "", nil
}

// issueURLFromRepoURL constructs an issue web URL by deriving the base
// (scheme+host) from repoURL rather than hardcoding github.com or gitlab.com.
// This correctly handles self-hosted GitLab instances.
func issueURLFromRepoURL(repoURL, provider, repo string, number int) string {
	base := "https://github.com"
	if u, err := parseRepoBase(repoURL); err == nil {
		base = u
	} else if provider == "gitlab" {
		base = "https://gitlab.com"
	}
	if provider == "gitlab" {
		return fmt.Sprintf("%s/%s/-/issues/%d", base, repo, number)
	}
	return fmt.Sprintf("%s/%s/issues/%d", base, repo, number)
}

// parseRepoBase returns the scheme+host of repoURL (e.g. "https://gitlab.example.com").
func parseRepoBase(repoURL string) (string, error) {
	if i := strings.Index(repoURL, "://"); i >= 0 {
		rest := repoURL[i+3:]
		if j := strings.IndexByte(rest, '/'); j >= 0 {
			return repoURL[:i+3] + rest[:j], nil
		}
		return repoURL[:i+3] + rest, nil
	}
	return "", fmt.Errorf("no scheme in %q", repoURL)
}
