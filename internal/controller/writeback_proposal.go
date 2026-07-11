package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

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

	// Dedup before CreateIssue, skipping it and wiring Spec.Source to the existing
	// issue when a match is found.
	// (C) Brainstorm proposals dedup by exact title: the title is deterministic
	// (brainstorm generates it). Matching on exact title among tatara-authored
	// issues is safe; the body/marker check is skipped because tracking a
	// human's identically-titled issue is still safer than creating a duplicate.
	// (C') Incident proposals carry agent free-text titles that exact-title dedup
	// cannot catch, so they dedup by Spec.DedupKey instead (item 6): a recurring
	// alert tracks onto the existing open issue recorded by another incident
	// Task carrying the same DedupKey, rather than spawning a near-duplicate
	// investigation.
	proposalTitle := task.Spec.ProposedIssue.Title
	incidentDedup := task.Spec.ProposedIssue.Incident && task.Spec.ProposedIssue.AlertGroup != ""
	var (
		existing      scm.IssueRef
		found         bool
		viaAlertGroup bool
	)
	if incidentDedup {
		if existingURL, ok := r.matchIncidentByDedupKey(ctx, proj, task.Spec.ProposedIssue.AlertGroup, task.Name); ok {
			if repoSlug, num, pok := parseIssueURL(existingURL); pok {
				existing, found, viaAlertGroup = scm.IssueRef{Repo: repoSlug, Number: num}, true, true
			}
		}
	}
	if r.ReaderFor != nil {
		issues, rerr := r.listOpenProposalIssues(ctx, proj, repo.Spec.URL, token)
		if rerr != nil {
			l.Error(rerr, "proposal: list open issues for dedup check (non-fatal, proceeding with create)")
		} else {
			// (C+) Cross-source near-duplicate guard (finding #5): a normalized-title
			// match against ANY open issue - human-, brainstorm-, OR incident-filed -
			// tracks onto it instead of opening a near-duplicate. This is the code-level
			// backstop to the LLM's (context-window-capped) survey, and it applies to
			// BOTH brainstorm and incident proposals so the same problem never lands as
			// three separate issues across sources.
			if !found {
				if e, ok := matchOpenIssueByTitle(issues, proposalTitle); ok {
					existing, found = e, true
				}
			}
		}
	}
	if found {
		if viaAlertGroup {
			l.Info("proposal skipped: alert-group duplicate exists",
				"action", "scm_propose_skip_alert_group",
				"resource_id", task.Name,
				"alert_group", task.Spec.ProposedIssue.AlertGroup,
				"existing_number", existing.Number)
			// (2A) Post a recurrence note so the re-fire stays visible on the
			// tracked issue (the comment's own timestamp records when).
			issueRef := fmt.Sprintf("%s#%d", existing.Repo, existing.Number)
			if _, cerr := r.gatedComment(ctx, proj, &repo, writer, token, proj.Spec.Scm.Provider,
				existing.Number, false, "", issueRef, alertGroupRefireComment(task.Spec.ProposedIssue.AlertGroup)); cerr != nil {
				l.Error(cerr, "proposal: alert-group re-fire comment (non-fatal)", "issue_ref", issueRef)
			}
		} else {
			l.Info("proposal skipped: duplicate exists",
				"action", "scm_propose_skip_duplicate",
				"resource_id", task.Name,
				"existing_number", existing.Number)
		}
		return r.recordExistingProposal(ctx, proj, task, existing, repo.Spec.URL)
	}

	brainstorming, _, _, _ := lifecycleLabels(proj.Spec.Scm)
	labels := []string{brainstorming}
	if task.Spec.ProposedIssue.Incident {
		labels = append(labels, incidentLabel(proj.Spec.Scm))
	}
	body := task.Spec.ProposedIssue.Body
	if sid := task.Spec.ProposedIssue.SystemicID; sid != "" {
		labels = append(labels, "tatara/systemic-"+sid)
		body += fmt.Sprintf("\n\nPart of systemic improvement %s spanning: %s", sid, systemicRepoList(ctx, r, proj))
	}
	if cc := approverMentions(proj, &repo); cc != "" {
		body += "\n\n" + cc
	}
	body += "\n\n" + tataraAuthoredMarker + "\n" + tataraProposedByMarker(proposalKind(task))
	ref, err := writer.CreateIssue(ctx, repo.Spec.URL, token, scm.IssueReq{
		Title:  proposalTitle,
		Body:   body,
		Labels: labels,
	})
	r.recordSCM(proj.Spec.Scm.Provider, "create_issue", err)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("proposal: create issue: %w", err)
	}

	if proj.Spec.Scm.Board != nil {
		board := boardRefFromSpec(proj.Spec.Scm)
		if err := writer.AddBoardItem(ctx, token, board, ref.URL); err != nil {
			l.Error(err, "proposal: add board item (non-fatal)")
			r.recordSCM(proj.Spec.Scm.Provider, "board_add", err)
		} else {
			r.recordSCM(proj.Spec.Scm.Provider, "board_add", nil)
			if err := writer.SetBoardColumn(ctx, token, board, ref.URL, "Proposed"); err != nil {
				l.Error(err, "proposal: set board column (non-fatal)")
				r.recordSCM(proj.Spec.Scm.Provider, "board_column", err)
			} else {
				r.recordSCM(proj.Spec.Scm.Provider, "board_column", nil)
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

// listOpenProposalIssues lists the repo's open issues using the provider-correct
// project path. For GitLab it derives the full project path (supports subgroups);
// owner+"/"+repo would produce "owner/" when OwnerRepo errors on subgroup URLs,
// which 404s. For GitHub owner/repo is the correct two-segment slug. Shared by
// the title and alert-group dedup paths.
func (r *TaskReconciler) listOpenProposalIssues(ctx context.Context, proj *tatarav1alpha1.Project, repoURL, token string) ([]scm.IssueRef, error) {
	reader, err := r.ReaderFor(proj.Spec.Scm.Provider, token)
	if err != nil {
		return nil, fmt.Errorf("proposal: reader for %s: %w", proj.Spec.Scm.Provider, err)
	}
	var owner, repo string
	if proj.Spec.Scm != nil && proj.Spec.Scm.Provider == "gitlab" {
		glPath, gerr := scm.GitLabProjectPath(repoURL)
		if gerr != nil {
			return nil, fmt.Errorf("proposal: gitlab project path: %w", gerr)
		}
		// ListOpenIssues for GitLab expects owner=full-project-path, repo="".
		owner = glPath
	} else {
		owner, repo, err = scm.OwnerRepo(repoURL)
		if err != nil {
			return nil, fmt.Errorf("proposal: owner/repo from url: %w", err)
		}
	}
	issues, err := reader.ListOpenIssues(ctx, owner, repo)
	r.recordSCM(proj.Spec.Scm.Provider, "list_open_issues", err)
	if err != nil {
		return nil, fmt.Errorf("proposal: list open issues: %w", err)
	}
	return issues, nil
}

// findOpenIssueByTitle lists open issues for the repo and returns the first
// one whose Title matches proposalTitle (exact or normalized near-duplicate).
// Returns (zero, false, nil) when no match is found, (zero, false, err) on list
// failure. Retained as the listing+match wrapper (its GitLab project-path plumbing
// is covered by writeback_r2_test); createProposal uses the pure matchers directly.
func (r *TaskReconciler) findOpenIssueByTitle(ctx context.Context, proj *tatarav1alpha1.Project, repoURL, token, proposalTitle string) (scm.IssueRef, bool, error) {
	issues, err := r.listOpenProposalIssues(ctx, proj, repoURL, token)
	if err != nil {
		return scm.IssueRef{}, false, err
	}
	e, ok := matchOpenIssueByTitle(issues, proposalTitle)
	return e, ok, nil
}

// matchOpenIssueByTitle returns the first non-PR open issue whose Title matches
// want either exactly or after title normalization (the cross-source near-dup
// guard, finding #5). Normalization lower-cases, drops punctuation, and collapses
// whitespace so "Fix: writeback 404 loop" and "fix writeback 404 loop" collide.
func matchOpenIssueByTitle(issues []scm.IssueRef, want string) (scm.IssueRef, bool) {
	nWant := normalizeTitle(want)
	for _, iss := range issues {
		if iss.IsPR {
			continue
		}
		if iss.Title == want || (nWant != "" && normalizeTitle(iss.Title) == nWant) {
			return iss, true
		}
	}
	return scm.IssueRef{}, false
}

// matchIncidentByDedupKey finds another incident Task (excluding selfName) in
// the same project whose DedupKey matches and which has already recorded a
// tracked issue, returning that issue's URL. Replaces the former
// matchOpenIssueByLabel(alertGroupLabel(...)) issue-label lookup (item 6):
// dedup identity now lives on TaskSpec.DedupKey, not an SCM label.
func (r *TaskReconciler) matchIncidentByDedupKey(ctx context.Context, proj *tatarav1alpha1.Project, dedupKey, selfName string) (string, bool) {
	if dedupKey == "" {
		return "", false
	}
	var tasks tatarav1alpha1.TaskList
	if err := r.List(ctx, &tasks, client.InNamespace(proj.Namespace)); err != nil {
		return "", false
	}
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Name == selfName || t.Spec.ProjectRef != proj.Name || t.Spec.Kind != "incident" || t.Spec.DedupKey != dedupKey {
			continue
		}
		if len(t.Status.DiscoveredIssues) > 0 {
			return t.Status.DiscoveredIssues[0], true
		}
	}
	return "", false
}

// normalizeTitle folds a title to a comparison key: lower-case, non-alphanumeric
// runs collapsed to single spaces, trimmed. Empty when the title has no
// alphanumeric content (which never near-matches).
func normalizeTitle(s string) string {
	var b strings.Builder
	prevSpace := true // trims leading space
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevSpace = false
			continue
		}
		if !prevSpace {
			b.WriteByte(' ')
			prevSpace = true
		}
	}
	return strings.TrimRight(b.String(), " ")
}

// alertGroupRefireComment is the short recurrence note posted to the existing
// incident issue when its alert re-fires, so the recurrence stays visible
// without opening a duplicate investigation.
func alertGroupRefireComment(alertGroup string) string {
	return fmt.Sprintf("Alert re-fired (alert-group `%s`). This condition is already tracked by this open incident issue, so no duplicate investigation was opened.", alertGroup)
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

// systemicRepoList returns a comma-joined sorted list of owner/repo slugs for
// all repos in the project. Used for the systemic-improvement footer in
// createProposal. On error it degrades gracefully to an empty list.
func systemicRepoList(ctx context.Context, r *TaskReconciler, proj *tatarav1alpha1.Project) string {
	repos, err := r.projectRepos(ctx, proj)
	if err != nil {
		log.FromContext(ctx).Info("proposal: systemic repo list failed (non-fatal)", "resource_id", proj.Name, "err", err.Error())
		return ""
	}
	slugs := make([]string, 0, len(repos))
	for i := range repos {
		if owner, name, oerr := scm.OwnerRepo(repos[i].Spec.URL); oerr == nil {
			slugs = append(slugs, owner+"/"+name)
		}
	}
	sort.Strings(slugs)
	return strings.Join(slugs, ", ")
}
