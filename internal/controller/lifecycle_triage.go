// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// handleTriage drives the Triage agent-run state. On a finished run it reads
// IssueOutcome and transitions: close->Done, discuss->Conversation, implement->Implement.
func (r *TaskReconciler) handleTriage(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	return r.handleFrontHalf(ctx, project, task, r.finishTriage)
}

// handleFrontHalf is the shared conversational front-half agent-run driver used
// by both issueLifecycle Triage (finish=finishTriage) and the clarify kind
// (finish=finishClarify). It ensures the brainstorming label, seeds the
// conversation fork, builds the turn-0 triage/clarify prompt, and drives the
// agent run. On a finished run it delegates to the kind-specific finish handler
// which consumes Status.IssueOutcome.
func (r *TaskReconciler) handleFrontHalf(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, finish func(context.Context, *tatarav1alpha1.Project, *tatarav1alpha1.Task) (ctrl.Result, error)) (ctrl.Result, error) {
	// Run finished -> act on the outcome.
	if isTerminal(task.Status.Phase) {
		return finish(ctx, project, task)
	}
	// Run in progress (or not yet started) -> drive another step.
	// Idempotent: ensure the brainstorming label is set (covers reactivation where
	// the task re-enters Triage without going through the case "" initializer).
	if !isTerminal(task.Status.Phase) {
		if err := r.ensurePhaseLabel(ctx, project, task, "brainstorming"); err != nil {
			return ctrl.Result{}, err
		}
	}
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return ctrl.Result{}, fmt.Errorf("triage: get repo: %w", err)
	}
	// Best-effort: on the first Triage of a brainstorm-derived issue, set the
	// fork-from-conversation annotation so the pod forks the brainstorm
	// conversation (issue #114 decision 3). Non-fatal so a fork-setup hiccup never
	// blocks triage.
	if err := r.maybeSetupConversationFork(ctx, task); err != nil {
		log.FromContext(ctx).Info("triage: conversation fork setup failed (non-fatal)",
			"resource_id", task.Name, "err", err.Error())
	}
	// Pass the already-fetched repo URL into buildTriagePromptFor so resolveTriageReader
	// inside it can reuse the URL without another Get (finding 7).
	prompt := r.buildTriagePromptFor(ctx, project, task, repo.Spec.URL)
	return r.driveAgentRun(ctx, project, &repo, task, prompt)
}

// maybeSetupConversationFork, on the first Triage of a brainstorm-derived issue,
// correlates this issueLifecycle Task to the proposal Task that opened the issue
// (matching repo + issue number) and copies the proposal's parent-conversation
// key onto this Task as the fork-from annotation, so the first pod forks the
// brainstorm conversation (issue #114 decision 3). No-op when S3 is off, when
// the Task already carries a fork pointer or its own conversation, when it has no
// issue number, or when no matching proposal with a parent key is found.
func (r *TaskReconciler) maybeSetupConversationFork(ctx context.Context, task *tatarav1alpha1.Task) error {
	if r.PodConfig.S3Bucket == "" {
		return nil
	}
	if task.Spec.Source == nil || task.Spec.Source.Number == 0 {
		return nil
	}
	if task.Annotations[annForkFromConversationKey] != "" ||
		task.Status.SessionID != "" || task.Status.ConversationObjectKey != "" {
		return nil
	}
	var tasks tatarav1alpha1.TaskList
	if err := r.List(ctx, &tasks, client.InNamespace(task.Namespace)); err != nil {
		return err
	}
	parentKey := ""
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.ProposedIssue == nil || t.Spec.Source == nil {
			continue
		}
		if t.Spec.RepositoryRef != task.Spec.RepositoryRef || t.Spec.Source.Number != task.Spec.Source.Number {
			continue
		}
		if k := t.Annotations[annParentConversationKey]; k != "" {
			parentKey = k
			break
		}
	}
	if parentKey == "" {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		if fresh.Annotations[annForkFromConversationKey] != "" {
			return nil
		}
		fresh.Annotations[annForkFromConversationKey] = parentKey
		if err := r.Update(ctx, fresh); err != nil {
			return err
		}
		task.Annotations = fresh.Annotations
		task.ResourceVersion = fresh.ResourceVersion
		return nil
	})
}

// buildTriagePromptFor fetches issue content and comments via ReaderFor (if wired) and
// builds the full triage turn-0 prompt with real title, body, and comment thread included.
// On any error it falls back gracefully with empty title/body.
// Uses resolveTriageReaderURL to share the provider/token/reader/ownerRepo resolution
// boilerplate with finishTriage, avoiding a third independent copy (finding 6).
// repoURL is the already-fetched repository URL from handleTriage (finding 7: avoids
// a second Get of the same Repository within the same reconcile).
func (r *TaskReconciler) buildTriagePromptFor(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, repoURL string) string {
	l := log.FromContext(ctx)
	// clarify (the decomposed front-half kind) gets the full operator-assembled
	// turn-0 cross-repo umbrella bundle instead of the single-issue triage text, so
	// a fresh clarify pod sees every umbrella member's body + thread + state upfront
	// and never re-crawls SCM (CROSS-REPO-CONTRACT). The retained issueLifecycle
	// bridge keeps the legacy single-issue triage prompt below.
	if task.Spec.Kind == "clarify" {
		return r.buildUmbrellaPromptFor(ctx, project, task, clarifyGoalTail(task))
	}
	if r.ReaderFor == nil || task.Spec.Source == nil {
		return lifecycleTriageText(task, "", "")
	}
	tr := r.resolveTriageReaderURL(ctx, project, task, repoURL)
	if !tr.resolved {
		l.Info("triage: reader not resolved for prompt fetch (non-fatal)", "resource_id", task.Name)
		return lifecycleTriageText(task, "", "")
	}
	content, err := tr.reader.GetIssue(ctx, tr.owner, tr.repoName, tr.issueNum)
	if err != nil {
		l.Info("triage: GetIssue failed (non-fatal)", "resource_id", task.Name, "err", err.Error())
		// Intentional fall-through: content is the zero value so Title/Body are "".
		// buildTriagePrompt handles empty fields gracefully. Unlike the token/reader/
		// OwnerRepo error branches above (which return the lifecycle fallback text),
		// a GetIssue failure keeps any ListIssueComments result that already landed
		// on the next call, so the partial fetch is used rather than thrown away
		// (finding 22).
	}
	comments, err := tr.reader.ListIssueComments(ctx, tr.owner, tr.repoName, tr.issueNum)
	if err != nil {
		l.Info("triage: ListIssueComments failed (non-fatal)", "resource_id", task.Name, "err", err.Error())
		comments = nil
	}
	return buildTriagePrompt(task, content.Title, content.Body, comments)
}

// triageReader holds the pre-resolved SCM reader context for finishTriage so the
// authorship and human-comment checks can share one token fetch, one ReaderFor
// call, and one repo-URL resolution (finding 6).
type triageReader struct {
	reader   scm.SCMReader
	owner    string
	repoName string
	botLogin string
	// approvers is the effective maintainer/approver allowlist for the task's repo
	// (issue #102). When non-empty, only a comment from one of these accounts
	// counts as the human approval go-ahead; empty preserves the historical
	// behavior (any non-bot human reply releases the self-approve hold).
	approvers []string
	issueNum  int
	resolved  bool // false = ReaderFor unavailable; callers treat as "not authored"

	// comments/commentsFetched memoize the ListIssueComments result across the
	// hasHumanReply/botHasLastWord checks within one finishTriage call, so a
	// tatara-authored issue with a human reply fetches the comment list once
	// instead of twice. Memoized ONLY on success: an error is never cached, so a
	// transient failure on the first check still lets the second check attempt
	// its own live fetch (preserving the existing fail-open retry).
	comments        []scm.IssueComment
	commentsFetched bool
}

// listComments returns the memoized comment list if a prior call on this
// triageReader already succeeded, otherwise issues a live ListIssueComments
// call. A failed attempt is never cached, so the next caller (in the same
// finishTriage invocation) gets its own live retry.
func (tr *triageReader) listComments(ctx context.Context) ([]scm.IssueComment, error) {
	if tr.commentsFetched {
		return tr.comments, nil
	}
	comments, err := tr.reader.ListIssueComments(ctx, tr.owner, tr.repoName, tr.issueNum)
	if err != nil {
		return nil, err
	}
	tr.comments = comments
	tr.commentsFetched = true
	return comments, nil
}

// resolveTriageReader resolves the SCM reader and repo coordinates once for
// finishTriage. On any error resolved is false and callers fall back safely.
// Calls repoURLForTask internally; use resolveTriageReaderURL when the caller
// already holds the repository URL (finding 7).
func (r *TaskReconciler) resolveTriageReader(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) triageReader {
	return r.resolveTriageReaderURL(ctx, project, task, r.repoURLForTask(ctx, task))
}

// resolveTriageReaderURL is the same as resolveTriageReader but accepts a
// pre-fetched repository URL so callers that already hold it avoid an extra
// Get (finding 7: handleTriage fetches the repo for driveAgentRun and passes
// its URL here rather than letting resolveTriageReader issue a second Get).
func (r *TaskReconciler) resolveTriageReaderURL(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, repoURL string) triageReader {
	if r.ReaderFor == nil || task.Spec.Source == nil {
		return triageReader{}
	}
	provider := task.Spec.Source.Provider
	if provider == "" && project.Spec.Scm != nil {
		provider = project.Spec.Scm.Provider
	}
	token, err := r.scmToken(ctx, task.Namespace, project.Spec.ScmSecretRef)
	if err != nil {
		return triageReader{}
	}
	reader, err := r.ReaderFor(provider, token)
	if err != nil {
		return triageReader{}
	}
	owner, repoName, err := scm.OwnerRepo(repoURL)
	if err != nil {
		return triageReader{}
	}
	botLogin := ""
	if project.Spec.Scm != nil {
		botLogin = project.Spec.Scm.BotLogin
	}
	// Effective approver list for the self-approve-hold release (issue #102),
	// honoring any per-repository MaintainerLogins override. Best-effort Get: on
	// failure approvers falls back to the project list (nil repo).
	var repo *tatarav1alpha1.Repository
	if task.Spec.RepositoryRef != "" {
		var repoObj tatarav1alpha1.Repository
		if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repoObj); err == nil {
			repo = &repoObj
		}
	}
	return triageReader{
		reader:    reader,
		owner:     owner,
		repoName:  repoName,
		botLogin:  botLogin,
		approvers: tatarav1alpha1.EffectiveMaintainerLogins(project, repo),
		issueNum:  task.Spec.Source.Number,
		resolved:  true,
	}
}

// isTataraAuthored uses the pre-resolved triageReader to check the tatara-authored
// marker without an additional token/reader/repo fetch (finding 6).
func (tr *triageReader) isTataraAuthored(ctx context.Context) (bool, error) {
	if !tr.resolved {
		return false, nil
	}
	content, err := tr.reader.GetIssue(ctx, tr.owner, tr.repoName, tr.issueNum)
	if err != nil {
		return false, err
	}
	return strings.Contains(content.Body, tataraAuthoredMarker), nil
}

// hasHumanReply uses the pre-resolved triageReader to check for a human comment
// that releases the self-approve hold, without an additional token/reader/repo
// fetch (finding 6). When an approver allowlist is configured (issue #102) only
// a comment from an approver counts; otherwise any non-bot comment does.
func (tr *triageReader) hasHumanReply(ctx context.Context) (bool, error) {
	if !tr.resolved {
		return false, nil
	}
	comments, err := tr.listComments(ctx)
	if err != nil {
		return false, err
	}
	for _, c := range comments {
		if c.Author == "" || c.Author == tr.botLogin {
			continue
		}
		if len(tr.approvers) > 0 && !slices.Contains(tr.approvers, c.Author) {
			continue
		}
		return true, nil
	}
	return false, nil
}

// approvingMaintainer returns the login of the most-recent thread comment whose
// author is a configured approver (EffectiveMaintainerLogins) and is not the
// bot, or "" if none. With no approver allowlist it returns "" (closed by
// default: nobody can approve). The listComments error propagates so the caller
// fails closed rather than advancing on unread evidence. Mirrors hasHumanReply's
// exact-match approver check (slices.Contains, bot excluded).
func (tr *triageReader) approvingMaintainer(ctx context.Context) (string, error) {
	if !tr.resolved || len(tr.approvers) == 0 {
		return "", nil
	}
	comments, err := tr.listComments(ctx)
	if err != nil {
		return "", err
	}
	for i := len(comments) - 1; i >= 0; i-- {
		author := comments[i].Author
		if author == "" || author == tr.botLogin {
			continue
		}
		if slices.Contains(tr.approvers, author) {
			return author, nil
		}
	}
	return "", nil
}

// botHasLastWord reports whether the newest comment on the issue is authored by
// the bot (the bot already had the last word). Newest is by CreatedAt, so it is
// robust to SCM list ordering. No comments -> false (the bot has not spoken).
// Used to suppress repeated hold comments once the bot has responded and no human
// has replied since.
func (tr *triageReader) botHasLastWord(ctx context.Context) (bool, error) {
	if !tr.resolved {
		return false, nil
	}
	comments, err := tr.listComments(ctx)
	if err != nil {
		return false, err
	}
	return botIsLastCommenter(comments, tr.botLogin), nil
}

// finishTriage consumes Status.IssueOutcome after a completed Triage agent run
// for an issueLifecycle Task: the implement outcome transitions the same Task
// into the Implement lifecycle state (the monolithic front-to-back-half path).
func (r *TaskReconciler) finishTriage(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	return r.finishFrontHalf(ctx, project, task, r.triageImplementAction)
}

// triageImplementAction is the issueLifecycle implement-outcome terminal action:
// label the issue approved and transition the SAME Task into the Implement
// lifecycle state. Returns terminal=false so finishFrontHalf runs its shared
// clearIssueOutcome+resetAgentRun tail (the Task continues into Implement).
func (r *TaskReconciler) triageImplementAction(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, bool, error) {
	_, approved, _, _ := lifecycleLabels(project.Spec.Scm)
	if err := r.setLifecycleLabel(ctx, project, task, approved); err != nil {
		return ctrl.Result{}, false, err
	}
	if err := r.setDeployState(ctx, task, "Implement", "triage-implement"); err != nil {
		return ctrl.Result{}, false, err
	}
	r.Metrics.IssueOutcome("implement")
	return ctrl.Result{}, false, nil
}

// finishFrontHalf consumes Status.IssueOutcome after a completed Triage/clarify
// agent run. The close/discuss/guard/default arms are shared; the implement arm,
// after the self-approve guard passes, delegates to onImplement (which differs
// by kind: issueLifecycle transitions into Implement; clarify flips the handoff
// label and terminates). onImplement returns terminal=true when it has fully
// handled the outcome (including any terminal transition), in which case
// finishFrontHalf returns immediately WITHOUT running the shared
// clearIssueOutcome+resetAgentRun tail (which would otherwise un-terminate a
// clarify Task by resetting Phase).
func (r *TaskReconciler) finishFrontHalf(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, onImplement func(context.Context, *tatarav1alpha1.Project, *tatarav1alpha1.Task) (ctrl.Result, bool, error)) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	if task.Status.Phase == "Failed" {
		l.Info("triage agent run failed; parking task",
			"action", "lifecycle_triage_failed", "resource_id", task.Name)
		// Liveness finding #2: park with a diagnostic issue comment so the reporter
		// sees the triage run failed and is awaiting a human, not a silent Parked.
		msg := "tatara: triage of this issue failed and I've paused it for a human to look at. " +
			"Comment here with more context to retry."
		if _, _, writer, token, _, scmErr := r.parkSCMContext(ctx, task); scmErr == nil {
			if err := r.parkWithComment(ctx, task, writer, token, "triage-failed", msg); err != nil {
				return ctrl.Result{}, err
			}
		} else if err := r.setDeployState(ctx, task, "Parked", "triage-failed"); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("triage-failed")
		}
		return ctrl.Result{}, r.resetAgentRun(ctx, task)
	}

	// Resolve the SCM reader once so the discuss-arm silence gate and the
	// implement-guard arm share one token fetch, one ReaderFor call, and one
	// repo-URL resolution rather than repeating them in each helper (finding 6).
	tr := r.resolveTriageReader(ctx, project, task)

	// Phase == Succeeded: read outcome.
	// A nil IssueOutcome means the agent ended without calling issue_outcome (turn cap,
	// NoPendingSubtasks, etc.). Defaulting to "implement" silently converts an
	// inconclusive triage into work; "discuss" is safer: it enters Conversation and
	// awaits human input. Only an explicit outcome.Action=="implement" proceeds to
	// the implement path (finding 2).
	outcome := task.Status.IssueOutcome
	action := "discuss" // default when agent did not set outcome (inconclusive run)
	comment := ""
	if outcome != nil {
		action = outcome.Action
		comment = outcome.Comment
	}

	// IssueOutcome is cleared only AFTER the action arm commits a state
	// transition (see clearIssueOutcome calls below). Clearing before acting
	// would, on any mid-arm SCM error, strand the task with a nil outcome and
	// silently default a close/discuss to implement on the next reconcile.
	// Accepted tradeoff: if the post-SCM status transition (RetryOnConflict)
	// exhausts its retries after the comment/close already landed, the next
	// reconcile re-runs the arm and may post a duplicate triage comment. That
	// is rare and cosmetic, and preferred over the wrong-implement downgrade.
	brainstorming, _, _, declined := lifecycleLabels(project.Spec.Scm)

	switch action {
	case "close":
		if hasUnmergedChange(task) {
			// Invariant: never close an issue that has an unmerged code change.
			// A human-comment re-triage of an issue whose MR is open/conflicting
			// can yield a "close" outcome; closing here would orphan the unmerged
			// change. Keep the issue open (brainstorming) and await the change being
			// merged-green or abandoned.
			l.Info("triage close withheld: unmerged change exists; keeping issue open",
				"action", "lifecycle_close_withheld", "resource_id", task.Name,
				"pr_url", task.Status.PrURL, "head_branch", task.Status.HeadBranch)
			if err := r.setLifecycleLabel(ctx, project, task, brainstorming); err != nil {
				return ctrl.Result{}, err
			}
			// Silence gate: same authored+no-human-reply check as the discuss arm
			// (finding 3). Without it, a tatara-authored issue with a persistent
			// "close" outcome re-posts the withhold note on every re-triage cycle,
			// spamming the issue. Uses the pre-resolved triageReader (finding 6).
			skipComment := false
			authored, aerr := tr.isTataraAuthored(ctx)
			if aerr != nil {
				l.Info("triage close-withheld: authorship check failed; posting comment (fail open)",
					"action", "lifecycle_close_withheld_silence_check", "resource_id", task.Name, "err", aerr.Error())
			} else if authored {
				human, herr := tr.hasHumanReply(ctx)
				if herr != nil {
					l.Info("triage close-withheld: hasHumanComment failed; posting comment (fail open)",
						"action", "lifecycle_close_withheld_silence_check", "resource_id", task.Name, "err", herr.Error())
				} else if !human {
					skipComment = true
					l.Info("triage close-withheld: tatara-authored issue with no human reply; suppressing note",
						"action", "lifecycle_close_withheld_silent_hold", "resource_id", task.Name)
				}
			}
			if !skipComment {
				lastWord, lerr := tr.botHasLastWord(ctx)
				if lerr != nil {
					l.Info("triage close-withheld: last-word check failed; posting note (fail open)",
						"action", "lifecycle_close_withheld_silence_check", "resource_id", task.Name, "err", lerr.Error())
				} else if lastWord {
					skipComment = true
					l.Info("triage close-withheld: bot already has the last word; suppressing note",
						"action", "lifecycle_close_withheld_silent_hold", "resource_id", task.Name)
				}
			}
			if !skipComment {
				note := comment
				if note != "" {
					note += "\n\n"
				}
				note += "tatara: not closing - this issue has an unmerged change that must be merged (with green main CI) or abandoned first."
				if err := r.triagePostComment(ctx, project, task, note); err != nil {
					return ctrl.Result{}, err
				}
			}
			if err := r.enterConversation(ctx, project, task, "close-withheld-unmerged"); err != nil {
				return ctrl.Result{}, err
			}
			// Record metric AFTER enterConversation commits (finding 3: path was invisible
			// to outcome metrics; now matches the discuss/implement arm discipline).
			r.Metrics.IssueOutcome("close-withheld")
			break
		}
		if err := r.setLifecycleLabel(ctx, project, task, declined); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.triageCloseIssue(ctx, project, task, comment); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setDeployState(ctx, task, "Done", "triage-close"); err != nil {
			return ctrl.Result{}, err
		}
		// Record IssueOutcome("close") AFTER setDeployState commits, so a failed
		// transition (RetryOnConflict exhausted) does not double-count on re-reconcile
		// (finding 1). triageCloseIssue is idempotent on the SCM side; the metric is not.
		r.Metrics.IssueOutcome("close")

	case "discuss":
		if err := r.setLifecycleLabel(ctx, project, task, brainstorming); err != nil {
			return ctrl.Result{}, err
		}
		// Silence gate: for tatara-authored issues with no human reply, do not
		// post a repeated "still awaiting go-ahead" comment on every triage cycle.
		// Only post when a human has actually replied since the issue was opened.
		// Human-filed issues always get the comment (authorship check returns false).
		// Uses the pre-resolved triageReader (finding 6: no repeated token/reader fetch).
		skipComment := false
		authored, aerr := tr.isTataraAuthored(ctx)
		if aerr != nil {
			l.Info("triage discuss: authorship check failed; posting comment (fail open)",
				"action", "lifecycle_discuss_silence_check", "resource_id", task.Name, "err", aerr.Error())
		} else if authored {
			human, herr := tr.hasHumanReply(ctx)
			if herr != nil {
				l.Info("triage discuss: hasHumanComment failed; posting comment (fail open)",
					"action", "lifecycle_discuss_silence_check", "resource_id", task.Name, "err", herr.Error())
			} else if !human {
				skipComment = true
				l.Info("triage discuss: tatara-authored issue with no human reply; suppressing comment",
					"action", "lifecycle_discuss_silent_hold", "resource_id", task.Name)
			}
		}
		if !skipComment {
			lastWord, lerr := tr.botHasLastWord(ctx)
			if lerr != nil {
				l.Info("triage discuss: last-word check failed; posting comment (fail open)",
					"action", "lifecycle_discuss_silence_check", "resource_id", task.Name, "err", lerr.Error())
			} else if lastWord {
				skipComment = true
				l.Info("triage discuss: bot already has the last word; suppressing comment",
					"action", "lifecycle_discuss_silent_hold", "resource_id", task.Name)
			}
		}
		if !skipComment {
			if err := r.triagePostComment(ctx, project, task, comment); err != nil {
				return ctrl.Result{}, err
			}
		}
		// Record metric AFTER enterConversation commits so a failed transition does not
		// double-count on the next reconcile (findings 1 & 5). The implement arm records
		// after setDeployState; this arm now matches that discipline.
		if err := r.enterConversation(ctx, project, task, "triage-discuss"); err != nil {
			return ctrl.Result{}, err
		}
		r.Metrics.IssueOutcome("discuss")

	case "implement":
		// A front-half issue is released into the autonomous
		// implement->review->merge->deploy chain by a VERIFIED maintainer
		// approval, recorded on the Task as Status.ApprovedByMaintainer. Two
		// paths record it, both identity-verified by the operator (never the
		// agent, never the bot):
		//   1. A MaintainerLogins member applies the approved label (webhook,
		//      recordMaintainerApproval).
		//   2. A MaintainerLogins member comments a go-ahead in the thread; the
		//      agent's implement verdict means scope is complete, and the
		//      operator confirms a maintainer actually participated
		//      (approvingMaintainer) before recording the approval here.
		// The agent cannot self-approve: a non-maintainer comment, its own
		// comment, or an empty maintainer list all fail closed. Fail CLOSED on
		// any uncertainty (no participation, or an SCM read error): park to
		// Conversation / requeue and await the maintainer.
		if task.Status.ApprovedByMaintainer == "" {
			approver, aerr := tr.approvingMaintainer(ctx)
			if aerr != nil {
				// SCM read error: fail closed by requeueing WITHOUT advancing or
				// parking - the next reconcile re-attempts the scan. Never
				// advance on unread evidence.
				l.Info("triage implement: maintainer-participation scan failed; requeueing (fail closed)",
					"action", "lifecycle_triage_participation_error", "resource_id", task.Name, "err", aerr.Error())
				return ctrl.Result{}, aerr
			}
			if approver == "" {
				l.Info("triage implement outcome withheld: no verified maintainer approval; parking (fail closed)",
					"action", "lifecycle_triage_await_approval", "resource_id", task.Name)
				if err := r.setLifecycleLabel(ctx, project, task, brainstorming); err != nil {
					return ctrl.Result{}, err
				}
				// Tear down the wrapper BEFORE Conversation so a failed resetAgentRun
				// leaves the task in Triage (still owns the pod) rather than in
				// Conversation with a leaked live pod (finding 19).
				if err := r.resetAgentRun(ctx, task); err != nil {
					return ctrl.Result{}, err
				}
				if err := r.clearIssueOutcome(ctx, task); err != nil {
					return ctrl.Result{}, err
				}
				if err := r.enterConversation(ctx, project, task, "triage-await-approval"); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, nil
			}
			// A verified maintainer participated: record the approval (attributed
			// to them) and post an audit comment, then fall through to onImplement
			// (which stamps the approved label, sets Implement, records the metric).
			if err := r.recordConversationalApproval(ctx, task, approver); err != nil {
				return ctrl.Result{}, err
			}
			l.Info("triage implement: released by verified maintainer conversational approval",
				"action", "lifecycle_triage_conversational_approval", "resource_id", task.Name, "maintainer", approver)
			if err := r.triagePostComment(ctx, project, task,
				fmt.Sprintf("tatara: approved by @%s via conversation; starting implementation.", approver)); err != nil {
				return ctrl.Result{}, err
			}
		}
		res, terminal, herr := onImplement(ctx, project, task)
		if herr != nil {
			return ctrl.Result{}, herr
		}
		if terminal {
			// onImplement fully handled the outcome (e.g. clarify handoff
			// terminated the Task): skip the shared clearIssueOutcome+resetAgentRun
			// tail so a resetAgentRun does not resurrect a terminated Task.
			return res, nil
		}

	default:
		// Unknown action: an agent returned an unrecognized action string. Route to the
		// safe discuss/Conversation hold rather than silently triggering implementation
		// (finding 17: the prior bare `default:` with comment "implement and anything else"
		// would have sent any unknown string to the implement path, which is dangerous).
		l.Info("triage: unknown action string; defaulting to discuss (safe fallback)",
			"action", "lifecycle_triage_unknown_action",
			"resource_id", task.Name,
			"unknown_action", action)
		if err := r.setLifecycleLabel(ctx, project, task, brainstorming); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.enterConversation(ctx, project, task, "triage-unknown-action"); err != nil {
			return ctrl.Result{}, err
		}
		r.Metrics.IssueOutcome("discuss")
	}

	if err := r.clearIssueOutcome(ctx, task); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.resetAgentRun(ctx, task)
}
