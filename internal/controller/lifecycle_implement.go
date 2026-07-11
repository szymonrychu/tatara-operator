// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// implementPrompt builds the turn-0 prompt for the Implement state.
//   - When Status.ImplementContext is set, appends a "## Re-entry context" block.
//   - When the pending-handover-resume annotation is set, prepends a
//     "## Resume from handover" block so the agent resumes with full context.
func implementPrompt(task *tatarav1alpha1.Task) string {
	base := planTurnText(task.Spec.Goal, taskBranch(task), task.Spec.ProjectRef, task.Name)
	// Hard instruction: if after investigation no PR should be opened, the agent
	// MUST call decline_implementation instead of finishing silently.
	base += "\n\n**IMPORTANT:** If after investigation you will NOT implement this " +
		"issue, you MUST call `decline_implementation` with a clear reason (what you " +
		"considered and why it should not / need not be done). A silent finish with no " +
		"PR and no `decline_implementation` call is NOT allowed and will be re-prompted."
	if len(task.Spec.ReposInScope) > 0 {
		base += "\n\n**This issue spans repos: " + strings.Join(task.Spec.ReposInScope, ", ") +
			".** Edit and push every repo you change; each repo with a change gets its own PR/MR. " +
			"If a listed repo genuinely needs no change, say so explicitly in your result summary."
	}
	if g := task.Spec.SystemicGroup; g != nil && len(g.SameRepoSiblings) > 0 {
		closes := make([]string, 0, len(g.SameRepoSiblings))
		for _, n := range g.SameRepoSiblings {
			closes = append(closes, fmt.Sprintf("Closes #%d", n))
		}
		base += "\n\n**You lead systemic improvement group " + g.SystemicID +
			".** Resolve these sibling issues in this same repo within ONE combined PR and " +
			"close them from the PR body: " + strings.Join(closes, ", ") + "."
	}
	if g := task.Spec.SystemicGroup; g != nil && len(g.CrossRepo) > 0 {
		base += "\n\nRelated work in OTHER repos (reference for context, do NOT edit them here; " +
			"each is led by its own agent): " + strings.Join(g.CrossRepo, "; ") + "."
	}
	if d := skillsDirective("issueLifecycle"); d != "" {
		base += "\n\n" + d
	}
	base += lifecyclePhaseBlock("Implement")
	if task.Status.ImplementContext != "" {
		base += "\n\n## Re-entry context\n" + task.Status.ImplementContext
	}
	// Compaction path (issue #114 decision 2): when pending-handover-resume is set
	// (LastTurnInputTokens crossed HandoverThresholdPercent), inject the compacted
	// text handover. This is mutually exclusive with full conversation replay:
	// agent.BuildPod skips CONVERSATION_SESSION_ID under the same annotation, so
	// the agent gets EITHER the full transcript (--resume, under threshold) OR the
	// handover summary (at/over threshold), never both (which would overflow the
	// context window the threshold exists to protect).
	if task.Annotations[annPendingHandoverResume] == "true" && task.Status.Handover != "" {
		base += "\n\n## Resume from handover\n" + task.Status.Handover
	}
	return base
}

// handleImplement drives the Implement agent-run state. On a finished run it
// calls writeBackOpenChange to open the MR, then transitions to MRCI.
func (r *TaskReconciler) handleImplement(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	if isTerminal(task.Status.Phase) {
		return r.finishImplement(ctx, task)
	}

	// Fresh-spawn path: Phase == "". Apply backstop + increment iterations.
	if task.Status.Phase == "" {
		maxIter := 10
		if project.Spec.Agent.MaxLifecycleIterations > 0 {
			maxIter = project.Spec.Agent.MaxLifecycleIterations
		}
		if task.Status.LifecycleIterations >= maxIter {
			// Backstop: too many attempts. Post comment and park.
			proj, repo, writer, token, provider, scmErr := r.scmContext(ctx, task)
			if scmErr == nil && task.Spec.Source != nil && task.Spec.Source.IssueRef != "" {
				msg := "max lifecycle iterations reached; leaving for a human"
				_, cerr := r.gatedComment(ctx, &proj, &repo, writer, token, provider, task.Spec.Source.Number, task.Spec.Source.IsPR, task.Spec.Source.AuthorLogin, task.Spec.Source.IssueRef, msg)
				if cerr != nil {
					log.FromContext(ctx).Error(cerr, "implement: max-iterations comment (non-fatal)", "resource_id", task.Name)
				}
			}
			if err := r.setDeployState(ctx, task, "Parked", "maxIterations"); err != nil {
				return ctrl.Result{}, err
			}
			if r.LifecycleMetrics != nil {
				r.LifecycleMetrics.RecordGiveup("maxIterations")
			}
			return ctrl.Result{}, nil
		}
		// Increment LifecycleIterations on fresh spawn. Capture the post-Update object
		// from inside the closure to propagate the new resourceVersion and status without
		// an extra Get round-trip (finding 5: the prior code discarded `fresh` and issued
		// a second Get, inconsistent with setImplementEmptyRetries/setImplementContext).
		var iterFresh tatarav1alpha1.Task
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), &iterFresh); err != nil {
				return err
			}
			iterFresh.Status.LifecycleIterations++
			return r.Status().Update(ctx, &iterFresh)
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("implement: increment iterations: %w", err)
		}
		// Copy mutable pointers back so driveAgentRun uses the updated resourceVersion.
		task.ResourceVersion = iterFresh.ResourceVersion
		task.Status = iterFresh.Status
		// Idempotent: set implementation label on fresh Implement spawn.
		if err := r.ensurePhaseLabel(ctx, project, task, "implementation"); err != nil {
			return ctrl.Result{}, err
		}
	}

	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: get repo: %w", err)
	}
	// Build the prompt using the current ImplementContext (may contain re-entry
	// instructions). Do NOT clear it here - it must persist until the pod is ready
	// and driveTurns submits the turn-0 prompt. Clearing happens in finishImplement,
	// after the run has completed and the context has been used.
	//
	// Systemic approval gate (finding #4): re-filter the SystemicGroup against the
	// CURRENT recorded maintainer approvals before prompting, so the lead is never
	// instructed to "Closes #N" a sibling a maintainer has not approved (or declined).
	// The re-filter runs on a shallow copy so only the prompt sees the narrowed group.
	promptTask := r.withApprovedSystemicGroup(ctx, task)
	planText := implementPrompt(promptTask)
	return r.driveAgentRun(ctx, project, &repo, task, planText)
}

// finishImplement opens the MR after the Implement run completes.
func (r *TaskReconciler) finishImplement(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	// Clear ImplementContext: by the time finishImplement runs the turn-0 prompt
	// has already been submitted (or the run failed), so the context is stale.
	// Each re-entry overwrites it fresh, so clearing here is safe.
	// After the write, refresh the in-memory task so subsequent status updates use
	// the current resourceVersion and do not conflict.
	if task.Status.ImplementContext != "" {
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tatarav1alpha1.Task{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
				return err
			}
			if fresh.Status.ImplementContext == "" {
				task.ResourceVersion = fresh.ResourceVersion
				task.Status = fresh.Status
				return nil
			}
			fresh.Status.ImplementContext = ""
			if err := r.Status().Update(ctx, fresh); err != nil {
				return err
			}
			task.ResourceVersion = fresh.ResourceVersion
			task.Status = fresh.Status
			return nil
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("implement: clear ImplementContext: %w", err)
		}
	}

	if task.Status.Phase == "Failed" {
		l.Info("implement agent run failed; parking task",
			"action", "lifecycle_implement_failed", "resource_id", task.Name)
		// Liveness finding #2: park with a diagnostic issue comment so the reporter
		// sees the implement run failed and is awaiting a human, not a silent Parked.
		msg := "tatara: the implementation run failed and I've paused this issue for a human to look at. " +
			"Comment here with guidance to retry."
		if _, _, writer, token, _, scmErr := r.parkSCMContext(ctx, task); scmErr == nil {
			if err := r.parkWithComment(ctx, task, writer, token, "implement-failed", msg); err != nil {
				return ctrl.Result{}, err
			}
		} else if err := r.setDeployState(ctx, task, "Parked", "implement-failed"); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("implement-failed")
		}
		return ctrl.Result{}, r.resetAgentRun(ctx, task)
	}

	// pr_outcome close signal (issueLifecycle): the agent signalled the PR is
	// superseded/obsolete/unfixable. The operator owns the close egress and
	// hard-gates on bot-authorship (the agent must never close a PR it does not
	// own). Placed before writeBackOpenChange so a close short-circuits the
	// open-a-new-PR path. Merge is owned by the Merge state, so a stray
	// pr_outcome=merge here is a no-op confirmation.
	if out := task.Status.PROutcome; out != nil && out.Action == "close" {
		number, _ := lifecyclePR(task)
		if number == 0 {
			// No PR on this task: an issue-driven Implement turn misfired
			// pr_outcome=close before any PR exists. Ignore the signal and fall
			// through to the normal open-change path - GetPRState(0) would 404 and
			// wedge the task in reconcile backoff.
			l.Info("implement: pr_outcome close ignored - no PR on task",
				"action", "lifecycle_close_noop_nopr", "resource_id", task.Name)
		} else {
			proj, repo, writer, token, provider, scErr := r.scmContext(ctx, task)
			if scErr != nil {
				return ctrl.Result{}, fmt.Errorf("implement: close outcome scm context: %w", scErr)
			}
			botLogin := ""
			if proj.Spec.Scm != nil {
				botLogin = proj.Spec.Scm.BotLogin
			}
			st, perr := writer.GetPRState(ctx, repo.Spec.URL, token, number)
			r.recordSCM(provider, "get_pr_state", perr)
			if perr != nil {
				return ctrl.Result{}, fmt.Errorf("implement: close outcome authorship gate: %w", perr)
			}
			if botLogin == "" || st.Author != botLogin {
				l.Info("implement: close outcome withheld - PR not bot-authored; parking",
					"action", "lifecycle_close_withheld", "resource_id", task.Name, "author", st.Author)
				if err := r.parkWithComment(ctx, task, writer, token, "not-bot-authored",
					fmt.Sprintf("lifecycle: PR #%d close signal withheld - not authored by the bot; parking.", number)); err != nil {
					return ctrl.Result{}, fmt.Errorf("implement: park non-bot close: %w", err)
				}
				return ctrl.Result{}, nil
			}
			if !st.Closed {
				cerr := writer.ClosePR(ctx, repo.Spec.URL, token, number, out.Reason)
				r.recordSCM(provider, "close", cerr)
				if cerr != nil {
					return ctrl.Result{}, fmt.Errorf("implement: close pr: %w", cerr)
				}
			}
			l.Info("implement: pr closed on agent signal",
				"action", "lifecycle_pr_outcome_close", "resource_id", task.Name, "pr", number, "reason", out.Reason)
			if err := r.setDeployState(ctx, task, "Stopped", "pr-closed-superseded"); err != nil {
				return ctrl.Result{}, fmt.Errorf("implement: stop after close: %w", err)
			}
			if r.LifecycleMetrics != nil {
				r.LifecycleMetrics.RecordGiveup("pr-closed-superseded")
			}
			return ctrl.Result{}, r.resetAgentRun(ctx, task)
		}
	}

	// Phase == Succeeded: open MR via the shared writeBackOpenChange path.
	// writeBackOpenChange sets task.Status.PrURL when a PR was opened.
	if _, err := r.writeBackOpenChange(ctx, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: open change: %w", err)
	}

	// Re-read task to pick up PrURL written by writeBackOpenChange.
	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: re-get task: %w", err)
	}

	if fresh.Status.PrURL == "" {
		// No PR opened. Branch on whether the agent declared a refusal.
		const emptyRetryCap = 2

		outcome := fresh.Status.ImplementOutcome
		codifiedTerminal := outcome != nil &&
			(outcome.Action == "declined" || outcome.Action == "already_done") &&
			strings.TrimSpace(outcome.Reason) != ""
		if codifiedTerminal {
			// Per-action park reason + giveup metric label.
			parkReason := "refused"
			giveupReason := "refused-declined"
			if outcome.Action == "already_done" {
				parkReason = "refused-already-done"
				giveupReason = "refused-already-done"
			}
			// CODIFIED TERMINAL: agent explicitly declared outcome via decline_implementation
			// or already_done. Post the reason as an issue comment, apply the declined label,
			// park with action-specific reason, clear ImplementOutcome, reset retries.
			l.Info("implement: agent declared codified terminal outcome; parking",
				"action", "lifecycle_implement_codified_terminal", "resource_id", task.Name,
				"impl_action", outcome.Action, "park_reason", parkReason)
			// Capture the Project from scmContext so we can pass it to ensurePhaseLabel
			// without a redundant Get (finding 15).
			if refusalProj, refusalRepo, writer, token, provider, scmErr := r.scmContext(ctx, fresh); scmErr == nil {
				if fresh.Spec.Source != nil && fresh.Spec.Source.IssueRef != "" {
					_, cerr := r.gatedComment(ctx, &refusalProj, &refusalRepo, writer, token, provider, fresh.Spec.Source.Number, fresh.Spec.Source.IsPR, fresh.Spec.Source.AuthorLogin, fresh.Spec.Source.IssueRef, outcome.Reason)
					if cerr != nil {
						l.Error(cerr, "implement: post outcome comment (non-fatal)", "resource_id", task.Name)
					}
				}
				// Use the Project returned by scmContext (no redundant Get, finding 15).
				if err := r.ensurePhaseLabel(ctx, &refusalProj, fresh, "declined"); err != nil {
					l.Error(err, "implement: apply declined label (non-fatal)", "resource_id", task.Name)
				}
			} else {
				l.Error(scmErr, "implement: scm context for outcome comment (non-fatal)", "resource_id", task.Name)
			}
			if err := r.setDeployState(ctx, fresh, "Parked", parkReason); err != nil {
				return ctrl.Result{}, err
			}
			if r.LifecycleMetrics != nil {
				r.LifecycleMetrics.RecordGiveup(giveupReason)
			}
			if err := r.clearImplementOutcome(ctx, fresh); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.setImplementEmptyRetries(ctx, fresh, 0); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, r.resetAgentRun(ctx, fresh)
		}

		// No declared decline. Re-prompt until cap, then park as refused-no-explanation.
		if fresh.Status.ImplementEmptyRetries < emptyRetryCap {
			if err := r.setImplementEmptyRetries(ctx, fresh, fresh.Status.ImplementEmptyRetries+1); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.setImplementContext(ctx, fresh, emptyImplementReentryPrompt); err != nil {
				return ctrl.Result{}, err
			}
			l.Info("implement: no commit produced; retrying with re-entry nudge",
				"action", "lifecycle_implement_empty_retry", "resource_id", task.Name,
				"attempt", fresh.Status.ImplementEmptyRetries, "cap", emptyRetryCap)
			if r.LifecycleMetrics != nil {
				r.LifecycleMetrics.ImplementEmptyRetry()
			}
			// resetAgentRun clears phase to "" and leaves DeployState=Implement,
			// so the next reconcile re-spawns the Implement run with ImplementContext.
			// NOTE: LifecycleIterations will also be incremented on the re-spawn
			// (Phase="" path). This is intentional: each empty-retry is a real
			// spawn cycle and must count against MaxLifecycleIterations so the
			// backstop can fire. See MEMORY.md: the two counters are coupled and
			// MaxLifecycleIterations must always be set > emptyRetryCap.
			return ctrl.Result{}, r.resetAgentRun(ctx, fresh)
		}
		l.Info("implement: no commit after retry cap and no explanation; commenting + parking",
			"action", "lifecycle_implement_empty_parked", "resource_id", task.Name)
		msg := "The implement agent produced no change after " +
			strconv.Itoa(emptyRetryCap) + " attempts and did not explain why via decline_implementation. " +
			"Leaving this for a human - the fix may be unclear, blocked, or already present."
		// parkWithComment posts the comment (with the IsPR ref fallback) and parks
		// atomically. If the SCM context is unavailable, still park so the task does
		// not loop, just without a comment.
		if _, _, writer, token, _, scmErr := r.scmContext(ctx, fresh); scmErr == nil {
			if err := r.parkWithComment(ctx, fresh, writer, token, "refused-no-explanation", msg); err != nil {
				return ctrl.Result{}, err
			}
		} else {
			l.Error(scmErr, "implement: scm context for empty-park comment (parking without comment)",
				"resource_id", task.Name)
			if err := r.setDeployState(ctx, fresh, "Parked", "refused-no-explanation"); err != nil {
				return ctrl.Result{}, err
			}
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("refused-no-explanation")
		}
		return ctrl.Result{}, r.resetAgentRun(ctx, fresh)
	}
	// PR opened: clear any prior empty-retry count so a later re-entry into
	// Implement starts fresh.
	if fresh.Status.ImplementEmptyRetries > 0 {
		if err := r.setImplementEmptyRetries(ctx, fresh, 0); err != nil {
			l.Error(err, "implement: reset empty-retry counter (non-fatal)", "resource_id", task.Name)
		}
	}

	// M4: open a follow-up issue when RemainingScope is set and not already done.
	if err := r.maybeOpenFollowupIssue(ctx, fresh); err != nil {
		// Non-fatal: log and continue so the lifecycle does not stall.
		l.Error(err, "implement: open follow-up issue (non-fatal)", "resource_id", task.Name)
	}

	// Record head branch, PR number, and clear the stale deadline (e.g. from a
	// prior Conversation idle deadline) in one RetryOnConflict write BEFORE the
	// state transition. The RetryOnConflict does its own Get internally so the
	// extra re-Get after maybeOpenFollowupIssue is not needed (finding 17).
	// fresh.Status.PrURL is current from the Get at the top of finishImplement;
	// maybeOpenFollowupIssue does not modify PrURL.
	prNumber := parsePRNumber(fresh.Status.PrURL)
	if prNumber == 0 && fresh.Status.PrURL != "" {
		l.Error(nil, "implement: parsePRNumber returned 0 for non-empty PrURL; unexpected URL shape",
			"resource_id", task.Name, "pr_url", fresh.Status.PrURL)
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		t2 := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), t2); err != nil {
			return err
		}
		t2.Status.HeadBranch = taskBranch(task)
		t2.Status.PRNumber = prNumber
		t2.Status.DeadlineAt = nil // clear stale Conversation/Implement deadline; MRCI sets its own via ensureDeadline
		return r.Status().Update(ctx, t2)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: record pr fields: %w", err)
	}

	// Delegate the state transition to setDeployState so the transition log,
	// metric, and wrapper teardown all live in one place.
	if err := r.setDeployState(ctx, task, "MRCI", "implement-done"); err != nil {
		return ctrl.Result{}, err
	}

	// resetAgentRun does its own Get+RetryOnConflict internally so a fresh re-Get
	// here is redundant (finding 17).
	return ctrl.Result{}, r.resetAgentRun(ctx, task)
}

// maybeOpenFollowupIssue creates a follow-up issue when ChangeSummary.RemainingScope
// is non-empty and Status.FollowupIssueURL is not yet set (idempotency guard).
// It appends the new issue URL to Status.DiscoveredIssues and records it in
// Status.FollowupIssueURL so re-entry does not open a duplicate.
// Non-fatal: if CreateIssue fails the caller logs and continues.
func (r *TaskReconciler) maybeOpenFollowupIssue(ctx context.Context, task *tatarav1alpha1.Task) error {
	cs := task.Status.ChangeSummary
	if cs == nil || cs.RemainingScope == "" {
		return nil
	}
	// Idempotency guard: already opened.
	if task.Status.FollowupIssueURL != "" {
		return nil
	}

	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return fmt.Errorf("followup: scm context: %w", err)
	}

	issueTitle := "Follow-up: " + firstLine(task.Spec.Goal) + " (remaining scope)"
	prURL := task.Status.PrURL
	// tataraProposedByMarker(kind) (FIX-7) keeps provenance spec-complete
	// alongside tataraAuthoredMarker: the follow-up is a tatara-authored
	// proposal too (kind "followup"), even though auto-approve is fail-safe
	// here (a merged PR already delivered the reviewed portion).
	issueBody := cs.RemainingScope + "\n\nOpened as a follow-up to: " + prURL + "\n\n" + tataraAuthoredMarker + "\n" + tataraProposedByMarker("followup")

	createStart := time.Now()
	created, cerr := writer.CreateIssue(ctx, repo.Spec.URL, token, scm.IssueReq{
		Title: issueTitle,
		Body:  issueBody,
	})
	r.recordSCM(provider, "create_issue", cerr)
	if cerr != nil {
		return fmt.Errorf("followup: create issue: %w", cerr)
	}

	log.FromContext(ctx).Info("lifecycle implement: follow-up issue opened",
		"action", "scm_followup_issue",
		"resource_id", task.Name,
		"issue_url", created.URL,
		"pr_url", prURL,
		"duration_ms", time.Since(createStart).Milliseconds(),
	)

	var siblings []string
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		// Append to DiscoveredIssues only if not already present.
		alreadyPresent := false
		for _, u := range fresh.Status.DiscoveredIssues {
			if u == created.URL {
				alreadyPresent = true
				break
			}
		}
		if !alreadyPresent {
			fresh.Status.DiscoveredIssues = append(fresh.Status.DiscoveredIssues, created.URL)
		}
		fresh.Status.FollowupIssueURL = created.URL
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		siblings = discoveredIssueSiblings(fresh)
		task.Status.DiscoveredIssues = fresh.Status.DiscoveredIssues
		task.Status.FollowupIssueURL = fresh.Status.FollowupIssueURL
		return nil
	}); err != nil {
		return err
	}
	// FIX-5: cross-link the follow-up against any existing sibling
	// (discoveredIssueSiblings), mirroring completeProposal - without this the
	// item-5 cross-links only ever seeded via the brainstorm path, never here.
	if len(siblings) >= 2 {
		r.syncSiblingLinks(ctx, provider, token, siblings)
	}
	return nil
}

// parsePRNumber extracts the trailing integer from a PR/MR URL
// (e.g. https://github.com/o/r/pull/42 -> 42). Returns 0 when the URL is
// empty or the trailing segment is not numeric; the caller should log a WARN
// when a non-empty URL produces 0 (indicates an unexpected URL shape).
func parsePRNumber(prURL string) int {
	if prURL == "" {
		return 0
	}
	parts := strings.Split(strings.TrimRight(prURL, "/"), "/")
	if len(parts) == 0 {
		return 0
	}
	n, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		// Non-numeric trailing segment: malformed or unexpected URL format.
		// Returns 0; the caller (finishImplement) logs at WARN.
		return 0
	}
	return n
}

// emptyImplementReentryPrompt nudges a re-spawned Implement agent that produced
// no diff on the prior turn to either deliver the change or stop and explain.
const emptyImplementReentryPrompt = "Your previous attempt finished without " +
	"committing any change, so no PR could be opened and the issue is still open. " +
	"Re-read the issue and the repository, then do EXACTLY ONE of: " +
	"(1) implement the fix and commit it; " +
	"(2) if the change is ALREADY PRESENT (e.g. another run already committed it on the shared branch), " +
	"call `already_done` with a reason naming where the fix already lives; " +
	"(3) if this issue genuinely should NOT be implemented (out of scope, wrong approach, blocked), " +
	"call `decline_implementation` with a clear reason. " +
	"A silent finish with no PR and no `already_done`/`decline_implementation` call is NOT allowed " +
	"and will be escalated to a human."

// setImplementEmptyRetries persists Status.ImplementEmptyRetries via RetryOnConflict.
func (r *TaskReconciler) setImplementEmptyRetries(ctx context.Context, task *tatarav1alpha1.Task, n int) error {
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.ImplementEmptyRetries = n
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.ImplementEmptyRetries = n
		return nil
	}); err != nil {
		return fmt.Errorf("setImplementEmptyRetries: %w", err)
	}
	return nil
}
