package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// refreshLedger is Tier-1 of the cron backstop. It fetches the current SCM state
// for each open WorkItemRef (issues and PRs) and updates State, HeadSHA, and
// LastRefreshedAt in place. Already-terminal entries (closed/merged) are skipped.
// Returns (changed, confirmedPRRepos): changed is true when at least one entry
// changed; confirmedPRRepos is the set of repos whose ListOpenPRs call SUCCEEDED
// this refresh. Tier-2 only acts on a PR whose repo is confirmed, so a transient
// SCM list error never lets never-confirmed seed-open state drive an action (the
// migration-safety hazard on lazily-seeded pre-deploy tasks).
func refreshLedger(ctx context.Context, reader scm.SCMReader, task *tatarav1alpha1.Task) (bool, map[string]bool) {
	// Build per-repo caches so we make one pair of SCM calls per repo, not one per
	// work-item. Repos are only queried when there is at least one non-terminal
	// entry of that kind in the ledger.
	issueCache := map[string]map[int]bool{} // repo -> set of open issue numbers
	prCache := map[string]map[int]string{}  // repo -> PR number -> current HeadSHA

	// Collect repos needing issue/PR lookups.
	issueRepos := map[string]bool{}
	prRepos := map[string]bool{}
	for _, wi := range task.Status.WorkItems {
		if wi.Repo == "" {
			continue
		}
		switch wi.Kind {
		case tatarav1alpha1.WorkItemIssue:
			if !isWITerminal(wi.State) {
				issueRepos[wi.Repo] = true
			}
		case tatarav1alpha1.WorkItemPR:
			if !isWITerminal(wi.State) {
				prRepos[wi.Repo] = true
			}
		}
	}

	// Fetch issue states.
	for repo := range issueRepos {
		owner, name, _ := strings.Cut(repo, "/")
		issues, err := reader.ListOpenIssues(ctx, owner, name)
		if err != nil {
			// Skip this repo on error; better to miss an update than block the sweep.
			continue
		}
		m := make(map[int]bool, len(issues))
		for _, iss := range issues {
			m[iss.Number] = true
		}
		issueCache[repo] = m
	}

	// Fetch PR states. Track which repos were successfully fetched: Tier-2 only
	// acts on a PR slot whose repo is confirmed open/closed here.
	confirmedPRRepos := map[string]bool{}
	for repo := range prRepos {
		owner, name, _ := strings.Cut(repo, "/")
		prs, err := reader.ListOpenPRs(ctx, owner, name)
		if err != nil {
			continue
		}
		m := make(map[int]string, len(prs))
		for _, pr := range prs {
			m[pr.Number] = pr.HeadSHA
		}
		prCache[repo] = m
		confirmedPRRepos[repo] = true
	}

	changed := false
	now := metav1.NewTime(time.Now())

	for i := range task.Status.WorkItems {
		wi := &task.Status.WorkItems[i]
		if wi.Repo == "" {
			continue
		}
		switch wi.Kind {
		case tatarav1alpha1.WorkItemIssue:
			if isWITerminal(wi.State) {
				continue
			}
			openSet, cached := issueCache[wi.Repo]
			if !cached {
				continue
			}
			if !openSet[wi.Number] {
				// Issue is no longer open in SCM.
				wi.State = tatarav1alpha1.WIClosed
				wi.LastRefreshedAt = &now
				changed = true
			}
		case tatarav1alpha1.WorkItemPR:
			if isWITerminal(wi.State) {
				continue
			}
			prMap, cached := prCache[wi.Repo]
			if !cached {
				continue
			}
			currentSHA, open := prMap[wi.Number]
			if !open {
				// PR is no longer in the open list: closed (or merged; we cannot
				// distinguish via SCMReader without GetPRState, so use WIClosed as a
				// conservative signal - backstopAction only cares open vs not-open).
				wi.State = tatarav1alpha1.WIClosed
				wi.LastRefreshedAt = &now
				changed = true
			} else if currentSHA != "" && currentSHA != wi.HeadSHA {
				// PR is still open but the head SHA advanced.
				wi.HeadSHA = currentSHA
				wi.LastRefreshedAt = &now
				changed = true
			}
		}
	}

	return changed, confirmedPRRepos
}

// isWITerminal reports whether a WorkItemRef state is already terminal and
// needs no further SCM refresh.
func isWITerminal(state string) bool {
	return state == tatarav1alpha1.WIClosed ||
		state == tatarav1alpha1.WIMerged ||
		state == tatarav1alpha1.WIDeclined ||
		state == tatarav1alpha1.WIImplemented
}

// backstopDecision is the Tier-2 action classification returned by backstopAction.
type backstopDecision int

const (
	// bsActionNone: no agent action required (pure state sync or live pod present).
	bsActionNone backstopDecision = iota
	// bsActionCloseObsolete: all source/closes issues are closed and an open MR
	// remains - close the MR with a superseded note without starting an agent.
	bsActionCloseObsolete
	// bsActionReactivate: open MR + at least one open source/closes issue + no live
	// pod - reactivate with a new MRCI task.
	bsActionReactivate
)

// backstopAction is Tier-2 of the cron backstop. It classifies what agent action
// (if any) is needed for a Task after Tier-1 state refresh. The ordering is:
// close-obsolete first, then reactivate, then none.
//
// podLive is the AUTHORITATIVE pod-liveness signal, computed by the sweep with an
// actual pod Get (Status.PodName is a deterministic name stamped once at pod
// creation and never cleared on a Done/Stopped/Parked transition, so it is a
// permanent flag, NOT a liveness signal). A genuinely stalled task - the only
// thing this backstop exists to recover - has already run a pod, so its PodName
// is non-empty forever; gating on PodName would make Tier-2 inert. We gate on
// podLive instead.
//
// The open-PR predicate uses Role==RoleOpenedPR (matching openPRCandidate) so the
// backstop never targets a human role:reviewed PR and the decision always agrees
// with the candidate the sweep will act on.
func backstopAction(task *tatarav1alpha1.Task, podLive bool) backstopDecision {
	// Find an open bot-opened-PR ledger entry (role:openedPR only - never a human
	// role:reviewed PR; this is the same predicate openPRCandidate uses).
	hasOpenPR := false
	for _, wi := range task.Status.WorkItems {
		if wi.Kind == tatarav1alpha1.WorkItemPR &&
			wi.Role == tatarav1alpha1.RoleOpenedPR &&
			wi.State == tatarav1alpha1.WIOpen {
			hasOpenPR = true
			break
		}
	}
	if !hasOpenPR {
		return bsActionNone
	}

	// A live pod means the task is already making progress; no backstop needed.
	if podLive {
		return bsActionNone
	}

	// Classify the source/closes issues.
	hasOpenSourceIssue := false
	hasAnySourceOrCloses := false
	for _, wi := range task.Status.WorkItems {
		if wi.Kind != tatarav1alpha1.WorkItemIssue {
			continue
		}
		if wi.Role != tatarav1alpha1.RoleSource && wi.Role != tatarav1alpha1.RoleCloses {
			continue
		}
		hasAnySourceOrCloses = true
		if wi.State == tatarav1alpha1.WIOpen {
			hasOpenSourceIssue = true
			break
		}
	}

	// Close-obsolete first: source/closes issues recorded and all are closed.
	if hasAnySourceOrCloses && !hasOpenSourceIssue {
		return bsActionCloseObsolete
	}

	// Reactivate only with at least one OPEN source/closes issue driving the PR
	// (spec section 4: "open MR + at least one open source/closes issue"). A bare
	// openedPR entry with no source issue (e.g. a task projected before its source,
	// or a driverless PR) must not spawn an implement agent -> None.
	if hasOpenSourceIssue {
		return bsActionReactivate
	}
	return bsActionNone
}

// backstopSweep is the two-tier cron backstop sweep. It lists all Tasks for the
// project that carry a work-item ledger, refreshes each against live SCM state
// (Tier 1), persists any changes, then applies backstopAction (Tier 2):
//   - CloseObsolete: close the stale MR with a superseded comment.
//   - Reactivate: if priorTerminalAttempts < maxRecoveryAttempts, create a new
//     MRCI QueuedEvent; otherwise closeExhaustedPR.
//   - None: nothing to do (pure state sync or live pod).
//
// Tier-1-only drift (no open MR, just ledger state updated) never creates tasks.
func (r *ProjectReconciler) backstopSweep(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository) {
	l := log.FromContext(ctx)

	// List all project Tasks with a non-empty ledger.
	var taskList tatarav1alpha1.TaskList
	if err := r.List(ctx, &taskList, client.InNamespace(proj.Namespace)); err != nil {
		l.Error(err, "backstop: list tasks for sweep", "action", "backstop_sweep_error", "resource_id", proj.Name)
		return
	}

	// Pre-fetch all existing scan tasks once for priorTerminalAttempts checks.
	existing, err := r.existingScanTasks(ctx, proj)
	if err != nil {
		l.Error(err, "backstop: list existing tasks for sweep", "action", "backstop_sweep_error", "resource_id", proj.Name)
		return
	}

	for i := range taskList.Items {
		task := &taskList.Items[i]
		if task.Spec.ProjectRef != proj.Name {
			continue
		}
		if len(task.Status.WorkItems) == 0 {
			// No ledger: nothing for the new backstop to do (old recoverOrphans handles
			// label-based orphan recovery separately).
			continue
		}

		// Tier 1: refresh ledger state from live SCM.
		changed, confirmedPRRepos := refreshLedger(ctx, reader, task)
		if changed {
			// Persist the refreshed ledger. Use RetryOnConflict so a concurrent status
			// update from the main reconcile loop does not drop our refresh.
			latest := task.DeepCopy()
			retryErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
				fresh := &tatarav1alpha1.Task{}
				if err := r.Get(ctx, client.ObjectKeyFromObject(latest), fresh); err != nil {
					return err
				}
				fresh.Status.WorkItems = latest.Status.WorkItems
				return r.Status().Update(ctx, fresh)
			})
			if retryErr != nil {
				l.Error(retryErr, "backstop: persist refreshed ledger",
					"action", "backstop_refresh_error", "resource_id", proj.Name, "task", task.Name)
				// Continue with the in-memory updated state even if persist failed;
				// the Tier-2 action is still useful to attempt.
			} else {
				l.Info("backstop: refreshed ledger",
					"action", "backstop_refreshed", "resource_id", proj.Name, "task", task.Name)
			}
		}

		// Find the open bot-PR candidate up-front: every Tier-2 action needs it, and
		// it lets us gate the action on the PR repo being SCM-confirmed this sweep.
		prCand, hasPR := openPRCandidate(task)
		// Confirmation gate: never act on a PR whose repo was NOT successfully fetched
		// this sweep. A lazily-seeded pre-deploy task defaults openedPR to WIOpen; a
		// transient ListOpenPRs error would otherwise let that never-confirmed
		// seed-open state drive a spurious Reactivate/CloseObsolete across the ~1148
		// migrated tasks. No confirmation -> skip (next sweep retries).
		if hasPR && !confirmedPRRepos[prCand.repo] {
			continue
		}

		// Tier 2: classify action. Pod liveness is the authoritative signal (actual
		// pod Get), not the permanent Status.PodName flag.
		podLive := r.podIsLive(ctx, task)
		dec := backstopAction(task, podLive)
		switch dec {
		case bsActionNone:
			// Pure state sync or live pod; nothing to do.
			continue

		case bsActionCloseObsolete:
			if !hasPR {
				continue
			}
			// Distinct from exhausted-recovery: zero attempts were made, the PR is
			// simply no longer needed. Post a superseded note and do NOT stamp the
			// recovery-exhausted label (which would corrupt priorTerminalAttempts /
			// adoption logic for this slot).
			r.closeObsoletePR(ctx, proj, repos, prCand)
			l.Info("backstop: closed obsolete MR (source issues all closed)",
				"action", "backstop_close_obsolete", "resource_id", proj.Name,
				"task", task.Name, "pr", prCand.number, "repo", prCand.repo)

		case bsActionReactivate:
			if !hasPR {
				continue
			}
			// Derive the linked-issue number from the ledger so the reactivation
			// shares mrScan's dedup identity (issueLifecycle\x00<repo>#<issueNum>),
			// not the PR-number identity. Without this, backstop and mrScan key on
			// different numbers and both fire in the same tick -> duplicate agent.
			linkedIssue := linkedIssueForPR(task, prCand.repo)

			// Exclude THIS task from its own recovery count: a stalled task is itself
			// typically Parked (terminal) and lists in `existing`, so counting it
			// would close an otherwise-reactivatable PR one attempt early.
			if priorTerminalAttemptsExcluding(existing, prCand.repo, prCand.number, task.Name) >= maxRecoveryAttempts {
				r.closeExhaustedPR(ctx, proj, repos, prCand)
				l.Info("backstop: closed exhausted MR",
					"action", "backstop_close_exhausted", "resource_id", proj.Name,
					"task", task.Name, "pr", prCand.number, "repo", prCand.repo)
				continue
			}
			// Mirror mrScan: do not spawn a second agent while a live lifecycle task
			// for the linked issue is already in-flight.
			if hasLiveLifecycleTaskForIssue(existing, prCand.repo, linkedIssue) {
				r.Metrics.ScanItem("backstop", "skipped_dedup")
				continue
			}
			repo, repoOK := r.matchRepoForSlug(repos, prCand.repo)
			if !repoOK {
				continue
			}
			goal := fmt.Sprintf("Resume stalled implementation for %s#%d (backstop reactivation)", prCand.repo, prCand.number)
			ann := map[string]string{tatarav1alpha1.LifecycleEntryAnnotation: "MRCI"}
			// labelCand carries the linked-issue identity (dedup key); srcCand carries
			// the PR identity (createScanTask sets DedupNumber when they differ).
			labelCand := candidate{repo: prCand.repo, number: linkedIssue, headSHA: prCand.headSHA, isPR: prCand.isPR}
			ok2, cerr := r.createScanTask(ctx, proj, &repo, labelCand, prCand, "backstop", "issueLifecycle", goal, ann, nil)
			if cerr != nil {
				l.Error(cerr, "backstop: create reactivation task",
					"action", "backstop_create_error", "resource_id", proj.Name, "repo", repo.Name)
				continue
			}
			if ok2 {
				l.Info("backstop: reactivated stalled open-MR task",
					"action", "backstop_reactivated", "resource_id", proj.Name,
					"task", task.Name, "pr", prCand.number, "repo", prCand.repo)
				r.Metrics.ScanItem("backstop", "reactivated")
			}
		}
	}
}

// podIsLive reports whether the Task's wrapper pod genuinely exists and is not in
// a terminal phase. It performs the same pod Get the reconciler uses, so a task
// whose pod is gone (NotFound) or Failed/Succeeded counts as not-live and is
// eligible for backstop recovery. Errors other than NotFound are treated as
// not-live (fail-open to recovery) but logged.
func (r *ProjectReconciler) podIsLive(ctx context.Context, task *tatarav1alpha1.Task) bool {
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: task.Namespace, Name: agent.PodName(task)}
	if err := r.Get(ctx, key, pod); err != nil {
		return false
	}
	switch pod.Status.Phase {
	case corev1.PodFailed, corev1.PodSucceeded:
		return false
	}
	return true
}

// linkedIssueForPR returns the number of the open role:source/role:closes issue
// in repo (the issue the PR drives), or the PR number itself when no such issue is
// recorded. This is the dedup identity mrScan uses for bot PRs (via
// scm.LinkedIssueNumber on the PR body); deriving it from the ledger here makes
// the backstop and mrScan compute the same createScanTask dedup key.
func linkedIssueForPR(task *tatarav1alpha1.Task, repo string) int {
	// Prefer an OPEN source/closes issue in the same repo (the live driver).
	for _, wi := range task.Status.WorkItems {
		if wi.Kind == tatarav1alpha1.WorkItemIssue && wi.Repo == repo &&
			(wi.Role == tatarav1alpha1.RoleSource || wi.Role == tatarav1alpha1.RoleCloses) &&
			wi.State == tatarav1alpha1.WIOpen && wi.Number > 0 {
			return wi.Number
		}
	}
	// Fall back to the PR number (matches mrScan when no Closes #N is linked).
	for _, wi := range task.Status.WorkItems {
		if wi.Kind == tatarav1alpha1.WorkItemPR && wi.Role == tatarav1alpha1.RoleOpenedPR &&
			wi.State == tatarav1alpha1.WIOpen {
			return wi.Number
		}
	}
	return 0
}

// closeObsoletePR closes a bot PR that is no longer needed (every source/closes
// issue is resolved). Unlike closeExhaustedPR it posts a superseded note with zero
// "recovery attempts" framing and does NOT stamp the recovery-exhausted label - an
// obsoleted PR is not a recovery-exhausted one, and stamping the label would
// corrupt priorTerminalAttempts/adoption for the slot.
func (r *ProjectReconciler) closeObsoletePR(ctx context.Context, proj *tatarav1alpha1.Project, repos []tatarav1alpha1.Repository, c candidate) {
	l := log.FromContext(ctx)
	repo, ok := r.matchRepoForSlug(repos, c.repo)
	if !ok {
		return
	}
	w, token, err := r.scanWriter(ctx, proj)
	if err != nil {
		l.Error(err, "backstop: scanWriter for obsolete close (leaving PR open)",
			"resource_id", proj.Name, "repo", c.repo, "pr", c.number)
		r.Metrics.ScanItem("backstop", "obsolete_close_error")
		return
	}
	body := "Closing as superseded: all linked source issues are closed or merged, " +
		"so this MR is no longer needed. The branch is preserved - reopen if the work " +
		"is still wanted."
	if cerr := w.ClosePR(ctx, repo.Spec.URL, token, c.number, body); cerr != nil {
		l.Error(cerr, "backstop: close obsolete PR failed (leaving open)",
			"resource_id", proj.Name, "repo", c.repo, "pr", c.number)
		r.Metrics.ScanItem("backstop", "obsolete_close_error")
		return
	}
	r.Metrics.ScanItem("backstop", "obsolete_closed")
}

// openPRCandidate returns a candidate built from the first open openedPR ledger
// entry. Returns ok=false when no open PR is in the ledger.
func openPRCandidate(task *tatarav1alpha1.Task) (candidate, bool) {
	for _, wi := range task.Status.WorkItems {
		if wi.Kind == tatarav1alpha1.WorkItemPR &&
			wi.Role == tatarav1alpha1.RoleOpenedPR &&
			wi.State == tatarav1alpha1.WIOpen {
			return candidate{
				repo:    wi.Repo,
				number:  wi.Number,
				headSHA: wi.HeadSHA,
				isPR:    true,
			}, true
		}
	}
	return candidate{}, false
}
