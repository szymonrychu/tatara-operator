package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// reviewResolveBudget bounds an umbrella review's wall-clock wait for an
// unresolvable member repo URL to become resolvable before the review parks
// recoverable with a comment naming the stuck member (liveness finding #4). It is
// generous (a member enrollment / transient List error should clear well within
// it) yet finite so the review never error-loops forever.
const reviewResolveBudget = 60 * time.Minute

// recordReviewQuality records the G4 quality-proxy signal for a review
// verdict that was just written back successfully: the verdict itself
// (operator_review_outcome_total) and its finding count
// (operator_review_findings_total), keyed by the model that ran the review.
// Finding count is len(Suggestions) - the only per-review count field on
// ReviewVerdict; AddReviewFindings is a no-op when it is 0.
func (r *TaskReconciler) recordReviewQuality(task *tatarav1alpha1.Task, verdict string, findingCount int) {
	if r.Metrics == nil {
		return
	}
	project, repo, _, _, model := taskTokenLabels(task)
	r.Metrics.RecordReviewOutcome(project, repo, model, verdict)
	r.Metrics.AddReviewFindings(project, repo, model, findingCount)
}

// writeBackReview reads Status.ReviewVerdict and posts exactly one verb set.
// Never calls OpenChange.
func (r *TaskReconciler) writeBackReview(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	v := task.Status.ReviewVerdict
	if v == nil || task.Spec.Source == nil {
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "NoVerdict", "review task without a verdict")
	}
	proj, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	_, approvedLabel, implementationLabel, _ := lifecycleLabels(proj.Spec.Scm)
	number := task.Spec.Source.Number
	var verbSent bool

	// U-D: an umbrella review spans EVERY bot-opened PR in the stream (the
	// role:openedPR ledger members across all repos), not just Spec.Source.Number.
	// members is non-empty only for a stream umbrella review; a per-PR (human /
	// external) review Task carries no openedPR entry and keeps the single-PR logic.
	members := umbrellaPRMembers(task)
	var memberURLs map[string]string
	if len(members) > 0 {
		memberURLs = r.projectRepoURLBySlug(ctx, &proj)
	}

	// Phase 6 / U-D: an approve verdict is withheld unless EVERY spanned PR is
	// mergeable (no conflict, not mergeable-blocked such as failing required
	// checks). If ANY member is dirty/blocked the whole stream routes back to
	// implement via the managed tatara-implementation label - the same terminal
	// effect as request_changes. Fail-open on a merge-state read error: a transient
	// read must not block a genuine approval (the deploy supervisor re-checks
	// mergeability at merge time).
	decision := v.Decision
	if decision == "approve" && len(members) > 0 {
		// U-D: a cross-repo umbrella review must resolve EACH member PR against ITS OWN
		// repo URL. There is no safe fallback to the review Task's own repo URL: an
		// umbrella review spans OTHER repos, so a fallback URL would GetMergeState /
		// Approve PR #m.Number in the WRONG repo (404, or approve an unrelated PR). If
		// ANY member URL is unresolvable (projectRepoURLBySlug List error, or an
		// un-enrolled member repo) the stream is not yet verifiable: withhold approval
		// and requeue by returning an error - approve nothing on a wrong URL and do NOT
		// clear writeback-pending. The state is transient (a List error clears; an
		// enrollment lands), so a requeue re-drives to a real decision.
		for _, m := range members {
			if memberURLs[m.Repo] == "" {
				// Liveness finding #4: an unresolvable member repo URL used to
				// error-loop FOREVER (no deadline, no comment, no park). Bound the
				// retries with a wall-clock deadline: stamp it on first sight and keep
				// requeueing until it elapses; past the deadline park recoverable with
				// an issue comment naming the stuck member and clear writeback-pending
				// so the loop stops.
				if task.Status.ReviewResolveDeadline == nil {
					if serr := r.stampReviewResolveDeadline(ctx, task); serr != nil {
						return ctrl.Result{}, serr
					}
					return ctrl.Result{}, fmt.Errorf(
						"writeback review: umbrella member %s#%d repo URL unresolvable; stamped resolve deadline, withholding approval pending resolution",
						m.Repo, m.Number)
				}
				if time.Now().Before(task.Status.ReviewResolveDeadline.Time) {
					return ctrl.Result{}, fmt.Errorf(
						"writeback review: umbrella member %s#%d repo URL unresolvable; withholding approval pending resolution",
						m.Repo, m.Number)
				}
				l.Info("review: member repo URL unresolvable past resolve deadline; parking recoverable",
					"action", "scm_review_unresolvable_parked", "resource_id", task.Name,
					"pr", m.Number, "repo", m.Repo)
				msg := fmt.Sprintf(
					"tatara: I paused review of this stream - member PR %s#%d lives in a repo I cannot resolve "+
						"(not enrolled as a Repository, or a transient lookup error) and it has not resolved within %s. "+
						"Enroll the member repo (or re-trigger once it resolves) and comment here to resume.",
					m.Repo, m.Number, reviewResolveBudget)
				if perr := r.parkWithComment(ctx, task, writer, token, "review-unresolvable", msg); perr != nil {
					return ctrl.Result{}, perr
				}
				return ctrl.Result{}, r.clearWritebackPending(ctx, task, "ReviewUnresolvable",
					"review parked: member repo URL unresolvable past resolve deadline")
			}
		}
		for _, m := range members {
			if ms, mserr := writer.GetMergeState(ctx, memberURLs[m.Repo], token, m.Number); mserr == nil &&
				(ms == scm.MergeStateDirty || ms == scm.MergeStateBlocked) {
				l.Info("review: approval withheld; umbrella member PR unmergeable, routing back to implement",
					"action", "scm_review_unmergeable", "resource_id", task.Name,
					"pr", m.Number, "repo", m.Repo, "merge_state", string(ms))
				decision = "unmergeable"
				break
			}
		}
	} else if decision == "approve" {
		if ms, mserr := writer.GetMergeState(ctx, repo.Spec.URL, token, number); mserr == nil &&
			(ms == scm.MergeStateDirty || ms == scm.MergeStateBlocked) {
			l.Info("review: approval withheld; PR unmergeable, routing back to implement",
				"action", "scm_review_unmergeable", "resource_id", task.Name, "pr", number, "merge_state", string(ms))
			decision = "unmergeable"
		}
	}

	switch decision {
	case "approve":
		// Approve applies the native PR approval AND the tatara-approved managed
		// label. It NEVER merges: the deploy supervisor is the sole merge caller,
		// gated on green + tatara-approved (CROSS-REPO-CONTRACT handoff transitions).
		// U-D: for an umbrella review, fan the native Approve + tatara-approved label
		// out to EVERY spanned member PR so the deploy supervisor can merge each on
		// green; the per-PR path keeps the single Spec.Source verb.
		if len(members) > 0 {
			// Every member URL was resolved and verified above, so no fallback here.
			var firstErr error
			for _, m := range members {
				aerr := writer.Approve(ctx, memberURLs[m.Repo], token, m.Number, v.Body)
				r.recordSCM(provider, "approve", aerr)
				if aerr != nil {
					if firstErr == nil {
						firstErr = aerr
					}
					l.Error(aerr, "review: approve umbrella member PR (will requeue)",
						"action", "scm_review", "resource_id", task.Name, "pr", m.Number, "repo", m.Repo)
					continue
				}
				if lerr := r.setManagedLabelOnMember(ctx, &proj, writer, token, provider, m, approvedLabel); lerr != nil {
					if firstErr == nil {
						firstErr = lerr
					}
					l.Error(lerr, "review: apply approved label on member (will requeue)",
						"action", "scm_review_label", "resource_id", task.Name, "pr", m.Number, "repo", m.Repo)
				}
				// Best-effort per-MR semver stamp so push-CD can cut the release tag for
				// EVERY member (human MRs included). NEVER feeds firstErr: a semver hiccup
				// must not block the approve verb / tatara-approved fan-out, nor trigger
				// the umbrella approve-failure requeue.
				r.applyReviewSemver(ctx, &proj, task, writer, token, provider, memberURLs[m.Repo], m.Number, v)
			}
			if firstErr != nil {
				// Partial fan-out: at least one member's Approve or tatara-approved label
				// did not land. Do NOT mark the verb sent or clear writeback-pending -
				// return an error to requeue and re-drive until ALL members are
				// approved+labeled. Native Approve is idempotent and setManagedLabelOnMember
				// re-asserts the one-of-4 managed-label set, so re-driving the members that
				// already succeeded is safe.
				err = firstErr
			} else {
				verbSent = true
				r.recordReviewQuality(task, "approved", len(v.Suggestions))
			}
			break
		}
		err = writer.Approve(ctx, repo.Spec.URL, token, number, v.Body)
		r.recordSCM(provider, "approve", err)
		verbSent = err == nil
		if err == nil {
			r.applyReviewLabel(ctx, &proj, task, approvedLabel)
			// Best-effort semver stamp on the (human or bot) source PR - its ONLY
			// stamping opportunity for a human MR. Non-fatal, so it never re-sends the
			// approve verb on a requeue.
			r.applyReviewSemver(ctx, &proj, task, writer, token, provider, repo.Spec.URL, number, v)
			r.recordReviewQuality(task, "approved", len(v.Suggestions))
		}
	case "unmergeable":
		// No PR-review verb: withhold approval and re-add tatara-implementation to
		// route the stream back to implement. This is the only egress action here,
		// so its error propagates for a requeue (the label add is idempotent).
		if lerr := r.setLifecycleLabel(ctx, &proj, task, implementationLabel); lerr != nil {
			return ctrl.Result{}, fmt.Errorf("writeback review unmergeable relabel: %w", lerr)
		}
		r.recordReviewQuality(task, "unmergeable", len(v.Suggestions))
	case "request_changes":
		err = writer.RequestChanges(ctx, repo.Spec.URL, token, number, v.Body)
		r.recordSCM(provider, "request_changes", err)
		verbSent = err == nil
		if err == nil && len(v.Suggestions) > 0 {
			serr := writer.Suggest(ctx, repo.Spec.URL, token, number, toSCMSuggestions(v.Suggestions))
			r.recordSCM(provider, "suggest", serr)
		}
		if err == nil {
			// request_changes re-adds tatara-implementation (routes back to implement).
			r.applyReviewLabel(ctx, &proj, task, implementationLabel)
			r.recordReviewQuality(task, "changes_requested", len(v.Suggestions))
		}
	case "comment":
		// Build the comment target from repo URL + PR number (same addressing as
		// approve/request_changes). IssueRef may be the originating issue rather
		// than the PR, or empty, so derive a consistent ref here.
		slug, _, serr := repoSlugFromURL(repo.Spec.URL, provider)
		if serr != nil {
			return ctrl.Result{}, fmt.Errorf("writeback review comment: derive slug: %w", serr)
		}
		// This is always an MR/PR review. GitLab MRs use the '!' separator and a
		// distinct notes endpoint; a '#' ref routes to /issues/{iid}/notes which
		// 404s (issues and MRs have separate iid spaces). GitHub shares the issue
		// endpoint for PRs, so it stays on '#'.
		sep := "#"
		if provider == "gitlab" {
			sep = "!"
		}
		prRef := fmt.Sprintf("%s%s%d", slug, sep, number)
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

// applyReviewLabel sets the given managed phase label on the review Task's PR via
// setLifecycleLabel (preserving the exactly-one-of-4-managed-labels invariant). It
// is best-effort: the native review verb (Approve/RequestChanges) has already
// landed, so a label failure is logged non-fatally rather than re-sending the
// non-idempotent verb on a requeue. The unmergeable path, which sends NO verb,
// calls setLifecycleLabel directly so its error can propagate.
func (r *TaskReconciler) applyReviewLabel(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, label string) {
	if lerr := r.setLifecycleLabel(ctx, proj, task, label); lerr != nil {
		log.FromContext(ctx).Error(lerr, "review: apply managed label (non-fatal)",
			"action", "scm_review_label", "resource_id", task.Name, "label", label)
	}
}

// stampReviewResolveDeadline records the wall-clock deadline (now + budget) an
// umbrella review waits for an unresolvable member repo URL to become resolvable
// before parking (liveness finding #4). Idempotent: a no-op once stamped, so the
// deadline is anchored to the FIRST unresolvable encounter and copied back so the
// caller observes it.
func (r *TaskReconciler) stampReviewResolveDeadline(ctx context.Context, task *tatarav1alpha1.Task) error {
	return r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if fresh.Status.ReviewResolveDeadline != nil {
			return false
		}
		dl := metav1.NewTime(time.Now().Add(reviewResolveBudget))
		fresh.Status.ReviewResolveDeadline = &dl
		return true
	})
}

// umbrellaPRMembers returns the non-terminal role:openedPR PR members of an
// umbrella review Task's ledger - the cross-repo set of bot-opened PRs the review
// spans (U-D). It is empty for a per-PR (human / external) review Task, whose only
// PR ledger entry is the role:reviewed source PR; that path keeps the single-PR
// verdict logic on Spec.Source.
func umbrellaPRMembers(task *tatarav1alpha1.Task) []tatarav1alpha1.WorkItemRef {
	var out []tatarav1alpha1.WorkItemRef
	for _, wi := range task.Status.WorkItems {
		if wi.Kind == tatarav1alpha1.WorkItemPR && wi.Role == tatarav1alpha1.RoleOpenedPR &&
			wi.Number > 0 && !isWITerminal(wi.State) {
			out = append(out, wi)
		}
	}
	return out
}

// hasPRMember reports whether task's ledger already tracks the PR (repo, number).
func hasPRMember(task *tatarav1alpha1.Task, repo string, number int) bool {
	for _, wi := range task.Status.WorkItems {
		if wi.Kind == tatarav1alpha1.WorkItemPR && wi.Repo == repo && wi.Number == number {
			return true
		}
	}
	return false
}

// seedReviewSpanFromUmbrella copies the sibling implement/clarify umbrella's
// role:openedPR members (the PRs opened on this review's shared head branch across
// every repo) into the stream review Task's ledger, so writeBackReview's
// approve/withhold decision spans the whole cross-repo stream (U-C/U-D). It is
// idempotent - a no-op once every member is present - and best-effort: a copy failure
// is logged and retried on the next reconcile.
func (r *TaskReconciler) seedReviewSpanFromUmbrella(ctx context.Context, task *tatarav1alpha1.Task, branch string) {
	l := log.FromContext(ctx)
	var tasks tatarav1alpha1.TaskList
	if err := r.List(ctx, &tasks, client.InNamespace(task.Namespace)); err != nil {
		return
	}
	var members []tatarav1alpha1.WorkItemRef
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.ProjectRef != task.Spec.ProjectRef || t.Name == task.Name {
			continue
		}
		if t.Spec.Kind != "implement" && t.Spec.Kind != "clarify" {
			continue
		}
		for _, wi := range t.Status.WorkItems {
			if wi.Role == tatarav1alpha1.RoleOpenedPR && wi.Kind == tatarav1alpha1.WorkItemPR &&
				wi.HeadBranch == branch && wi.Number > 0 {
				members = append(members, wi)
			}
		}
	}
	need := false
	for _, m := range members {
		if !hasPRMember(task, m.Repo, m.Number) {
			need = true
			break
		}
	}
	if !need {
		return
	}
	if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		wrote := false
		for _, m := range members {
			if hasPRMember(fresh, m.Repo, m.Number) {
				continue
			}
			UpsertWorkItem(fresh, tatarav1alpha1.WorkItemRef{
				Provider: m.Provider, Repo: m.Repo, Number: m.Number,
				Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR,
				// Copy the source member's real state: a sibling PR already merged/closed
				// (still carrying HeadBranch==branch) must NOT be seeded as live, or
				// umbrellaPRMembers would treat a terminal PR as an approvable member.
				State: m.State, Title: m.Title, HeadBranch: m.HeadBranch, HeadSHA: m.HeadSHA,
			})
			wrote = true
		}
		return wrote
	}); err != nil {
		l.Info("review: seed span from umbrella failed (non-fatal)", "resource_id", task.Name, "err", err.Error())
	}
}

// projectRepoURLBySlug maps each enrolled project repo's "owner/repo" slug to its
// clone URL, so an umbrella review can resolve the per-member repo URL for a
// cross-repo GetMergeState / Approve call. Returns nil on a list error (callers
// fall back to the Task's own repo URL).
func (r *TaskReconciler) projectRepoURLBySlug(ctx context.Context, proj *tatarav1alpha1.Project) map[string]string {
	repos, err := r.projectRepos(ctx, proj)
	if err != nil {
		return nil
	}
	m := make(map[string]string, len(repos))
	for i := range repos {
		if slug, serr := scm.RepoSlugFromURL(repos[i].Spec.URL); serr == nil {
			m[slug] = repos[i].Spec.URL
		}
	}
	return m
}

// setManagedLabelOnMember applies the desired managed phase label to a single
// umbrella PR member (its provider-correct ref), preserving the exactly-one-of-4
// managed-labels invariant by removing the other managed phase labels. It does NOT
// read the member's current label set first - a per-member list would be a heavy
// extra SCM call - so it adds + removes unconditionally, exactly the setLifecycleLabel
// "unknown current" branch: AddLabel is idempotent and RemoveLabel is best-effort
// (tolerates the label being absent). Used by the umbrella approve fan-out.
func (r *TaskReconciler) setManagedLabelOnMember(ctx context.Context, proj *tatarav1alpha1.Project, writer scm.SCMWriter, token, provider string, wi tatarav1alpha1.WorkItemRef, desired string) error {
	ref := umbrellaMemberRef(wi)
	if aerr := writer.AddLabel(ctx, token, ref, desired); aerr != nil {
		if isPermanentTargetGone(aerr) {
			r.recordSCMGone(provider, "add_label", aerr)
			return nil
		}
		r.recordSCM(provider, "add_label", aerr)
		return fmt.Errorf("member label add %q on %s: %w", desired, ref, aerr)
	}
	r.recordSCM(provider, "add_label", nil)
	for _, lb := range managedPhaseLabels(proj.Spec.Scm) {
		if lb == desired {
			continue
		}
		if rerr := writer.RemoveLabel(ctx, token, ref, lb); rerr != nil {
			r.recordSCM(provider, "remove_label", rerr)
			continue
		}
		r.recordSCM(provider, "remove_label", nil)
	}
	return nil
}

// applyReviewSemver stamps a semver:<level> label on an approved member/source PR
// so push-CD can cut the release tag for EVERY MR in the stream - including
// human/maintainer MRs that carry no bot change_significance (the review approve is
// their ONLY stamping opportunity). Level ladder, per MR: (1) RESPECT an existing
// semver:* label; (2) the review verdict's per-MR SemverAssignment; (3) the member's
// implement-agent change_significance (bot MRs); (4) patch. Best-effort and
// GitHub-only (the cd-release cascade is GitHub-only, mirroring applySemverAutoMerge /
// ensureSemverLabelBeforeMerge): every SCM error is logged non-fatally so it never
// blocks the approve verb or the tatara-approved fan-out, and never triggers the
// umbrella approve-failure requeue.
func (r *TaskReconciler) applyReviewSemver(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, writer scm.SCMWriter, token, provider, repoURL string, number int, v *tatarav1alpha1.ReviewVerdict) {
	if provider != "github" || number <= 0 {
		return
	}
	slug, _, serr := repoSlugFromURL(repoURL, provider)
	if serr != nil || slug == "" {
		return
	}
	// (1) Respect an existing semver:* label. Fail-open (proceed to stamp) on a
	// nil/failed reader - an unlabeled release is the failure being closed.
	if r.prHasSemverLabel(ctx, provider, token, repoURL, number) {
		return
	}
	// (2) verdict assignment, else (3) member implement change_significance, else (4) patch.
	level := verdictSemverLevel(v, slug, number)
	if level == "" {
		level = r.memberChangeSignificance(ctx, task, slug, number)
	}
	switch level {
	case "major", "minor", "patch":
	default:
		level = "patch"
	}
	label := semverLabel(level)
	color := managedLabelColors(proj.Spec.Scm)[label]
	r.ensureSemverLabelColor(ctx, writer, repoURL, token, provider, label, color,
		"review: ensure semver label (non-fatal)",
		"action", "scm_review_semver_label", "resource_id", task.Name, "repo", slug, "label", label)
	prRef := fmt.Sprintf("%s#%d", slug, number)
	if aerr := r.addSemverLabelToPR(ctx, writer, token, provider, prRef, label,
		"review: add semver label (non-fatal)",
		"action", "scm_review_semver_label", "resource_id", task.Name, "pr_ref", prRef, "label", label); aerr != nil {
		return
	}
	log.FromContext(ctx).Info("review: semver label stamped on approved PR",
		"action", "scm_review_semver_label", "resource_id", task.Name, "pr_ref", prRef, "level", level)
}

// prHasSemverLabel reports whether the open PR (repoURL, number) already carries a
// semver:* label. Fail-open (false) on a nil/failed reader or read error, so an
// unlabeled PR is stamped.
func (r *TaskReconciler) prHasSemverLabel(ctx context.Context, provider, token, repoURL string, number int) bool {
	if r.ReaderFor == nil {
		return false
	}
	reader, rerr := r.ReaderFor(provider, token)
	if rerr != nil {
		return false
	}
	owner, name, oerr := scm.OwnerRepo(repoURL)
	if oerr != nil {
		return false
	}
	prs, lerr := reader.ListOpenPRs(ctx, owner, name)
	if lerr != nil {
		return false
	}
	for _, pr := range prs {
		if pr.Number != number {
			continue
		}
		for _, lb := range pr.Labels {
			if strings.HasPrefix(lb, "semver:") {
				return true
			}
		}
		return false
	}
	return false
}

// verdictSemverLevel returns the review verdict's assigned level for the MR
// (slug, number), or "" when the verdict carries no matching assignment.
func verdictSemverLevel(v *tatarav1alpha1.ReviewVerdict, slug string, number int) string {
	if v == nil {
		return ""
	}
	for _, sa := range v.Semver {
		if sa.Repo == slug && sa.Number == number {
			return sa.Level
		}
	}
	return ""
}

// memberChangeSignificance resolves the implement-agent change_significance for a
// stream member PR (slug, number) by finding the sibling implement/clarify Task in
// the same project whose role:openedPR ledger entry opened it. Returns "" when no
// such Task/ChangeSummary exists (e.g. a human MR), so the caller falls back to patch.
func (r *TaskReconciler) memberChangeSignificance(ctx context.Context, task *tatarav1alpha1.Task, slug string, number int) string {
	var tasks tatarav1alpha1.TaskList
	if err := r.List(ctx, &tasks, client.InNamespace(task.Namespace)); err != nil {
		return ""
	}
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.ProjectRef != task.Spec.ProjectRef {
			continue
		}
		if t.Spec.Kind != "implement" && t.Spec.Kind != "clarify" {
			continue
		}
		for _, wi := range t.Status.WorkItems {
			if wi.Kind == tatarav1alpha1.WorkItemPR && wi.Role == tatarav1alpha1.RoleOpenedPR &&
				wi.Repo == slug && wi.Number == number && t.Status.ChangeSummary != nil {
				return t.Status.ChangeSummary.Significance
			}
		}
	}
	return ""
}

func toSCMSuggestions(in []tatarav1alpha1.Suggestion) []scm.Suggestion {
	out := make([]scm.Suggestion, 0, len(in))
	for _, s := range in {
		out = append(out, scm.Suggestion{Path: s.Path, Line: s.Line, Body: s.Body})
	}
	return out
}
