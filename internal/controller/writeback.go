package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/titlecheck"
)

// Writer is the SCM egress contract the reconciler uses. It is the full
// scm.SCMWriter; SCMFor returns it and tests fake it.
type Writer = scm.SCMWriter

// writebackSkip4xxCap bounds how many times a Succeeded task re-sweeps every
// project repo when OpenChange returns a permanent 4xx on all of them and opens
// no PR. After the cap the gate records a terminal WritebackFailed condition and
// stops re-attempting, instead of churning the SCM API every reconcile (issue
// #166: the un-triageable 4xx-skip loop). A 4xx is a permanent failure, so a
// couple of attempts cover a transient 403/429 then give up for good. In the
// healthy case the clear sticks after one sweep and the counter never nears the
// cap; the cap only fires when WritebackPending keeps getting re-armed.
const writebackSkip4xxCap = 3

// disarmFailureCap bounds how many times checkRemainingScopeHardFail retries an
// incomplete disarmOpenChanges sweep (F2) before giving up. Mirrors
// writebackSkip4xxCap/linksSyncFailureCap: a transient SCM error (429/5xx)
// clears within a couple of attempts; a genuinely permanent block never will,
// so retrying forever would wedge the Task in reconcile backoff without ever
// surfacing the armed-PR risk to a human. Until the disarm sweep is verified
// clean (or the cap is spent), the Task does NOT terminate - an unverified
// disarm must never be mistaken for a completed one (fail closed).
const disarmFailureCap = 3

// disarmRetryRequeue is the backoff between bounded disarm-sweep retries.
const disarmRetryRequeue = 10 * time.Second

// doWriteBack opens a PR/MR on each Project repo the task changed - repos with no
// task branch return a benign 422 (no-branch) and are skipped as a no-op - then
// comments the primary issue with all PR links and records them on the Task
// status. It is called when WritebackPending is True and prURL is not yet set.
// Permanent SCM errors (4xx) per repo are logged and skipped; transient errors
// are returned for requeue.
func (r *TaskReconciler) doWriteBack(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	// F1: hoisted ABOVE the PrURL idempotency guard below. A Task can re-enter
	// writeback with PrURL already set from an earlier turn - e.g.
	// reactivateBackHalfTask (webhook/server.go) resets Phase/DeployState on a
	// Parked back-half Task without clearing PrURL - and post a NEW
	// change_summary declaring RemainingScope on the re-run. With the guard
	// ordered first, that Task hit "AlreadyWritten" and terminated Succeeded
	// before checkRemainingScopeHardFail ever ran: the incomplete change shipped
	// silently. Checking first also makes M1's hoist-above-the-kind-switch
	// guarantee hold for every path, not only the PrURL-empty one.
	//
	// M1: hoisted ABOVE the kind switch so EVERY branch is guarded, including
	// "triageIssue" (writeBackIssue's implement action used to call
	// writeBackOpenChange directly with no check at all - the one entry point
	// F4 missed). No-ops when ChangeSummary is nil or carries no RemainingScope,
	// so kinds that never set it (review/brainstorm/...) are unaffected - the
	// check itself does no SCM I/O in that case, so this reorder costs nothing
	// on the normal AlreadyWritten path.
	if res, err, handled := r.checkRemainingScopeHardFail(ctx, task); handled {
		return res, err
	}

	// Idempotency guard: already done.
	if task.Status.PrURL != "" {
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "AlreadyWritten", "pr/mr url already set")
	}

	switch task.Spec.Kind {
	case "review":
		return r.writeBackReview(ctx, task)
	case "triageIssue":
		return r.writeBackIssue(ctx, task)
	case "brainstorm":
		// Brainstorm proposals are created via propose_issue which spawns child
		// Tasks. The brainstorm Task itself never opens a PR.
		// Only claim BrainstormProposed when at least one proposal child Task
		// exists; otherwise use BrainstormComplete so a no-yield run is visible.
		if r.brainstormHasProposal(ctx, task) {
			r.Metrics.BrainstormOutcome("proposed")
			return ctrl.Result{}, r.clearWritebackPending(ctx, task, "BrainstormProposed", "brainstorm proposals created via propose_issue; no PR to open")
		}
		reason := "brainstorm finished with no proposal filed via propose_issue"
		if o := task.Status.BrainstormOutcome; o != nil && o.Action == "none" && strings.TrimSpace(o.Reason) != "" {
			reason = "early-exit: " + o.Reason
		}
		r.Metrics.BrainstormOutcome("no_yield")
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "BrainstormComplete", reason)
	case "incident", "healthCheck":
		// Project-scoped orchestration kinds. They open no PR and file no issue
		// of their own; the agent calls propose_issue which spawns a separate
		// repo-scoped child Task that opens the issue. RepositoryRef is empty by
		// contract (projectScopedKinds), so writeBackOpenChange would resolve a
		// repo by empty name and error-loop. Clear WritebackPending as a no-op.
		if r.brainstormHasProposal(ctx, task) {
			return ctrl.Result{}, r.clearWritebackPending(ctx, task, "ProposalFiled", "project-scoped task filed via propose_issue; no PR to open")
		}
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "NoWriteback", "project-scoped task finished with no proposal (false positive or degraded)")
	default:
		// implement and other future kinds that open a change.
	}

	// Defensive fence for project-scoped kinds. The known ones (brainstorm,
	// incident, healthCheck) are handled by explicit cases above; this catches any
	// FUTURE project-scoped kind added to the enum/map but not given a case.
	// Such a kind carries an empty RepositoryRef and never opens a PR/MR, so
	// falling through to writeBackOpenChange would Get a Repository by the empty
	// RepositoryRef (`Repository "" not found`) and error-loop after the task
	// already terminated Succeeded (the incident-qe-bw5hw incident class).
	if tatarav1alpha1.IsProjectScopedKind(task.Spec.Kind) {
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "ProjectScopedComplete", task.Spec.Kind+" is project-scoped; no PR to open")
	}

	// F4/M1: full-scope-or-decline already enforced above, before the kind
	// switch, for every branch including this fallthrough (kind=implement and
	// future kinds that open a change).
	return r.writeBackOpenChange(ctx, task)
}

// checkRemainingScopeHardFail hard-fails task Failed/IncompleteImplementation
// when ChangeSummary.RemainingScope is non-empty, BEFORE writeBackOpenChange
// ever runs (Request C, full-scope-or-decline). Shared by both write-back
// entry points - the issueLifecycle bridge (finishImplement, F1) and the
// generic kind=implement path (doWriteBack, F4) - so they cannot drift: an
// incomplete change must never open a PR, get the semver label, or have
// auto-merge enabled. handled=true means the task was terminated and the
// caller must return (res, err) immediately without opening any change.
func (r *TaskReconciler) checkRemainingScopeHardFail(ctx context.Context, task *tatarav1alpha1.Task) (res ctrl.Result, err error, handled bool) {
	cs := task.Status.ChangeSummary
	if cs == nil || cs.RemainingScope == "" {
		return ctrl.Result{}, nil, false
	}
	// D3: only a kind that OPENS a change can ship an incomplete one. The REST
	// change_summary endpoint now rejects every other kind, but the guard is
	// hoisted above doWriteBack's whole kind switch, so a change_summary already
	// stored on (say) a review Task would otherwise hard-fail it before
	// writeBackReview ever ran - the verdict would never post and the PR would
	// sit unreviewed.
	if !tatarav1alpha1.IsChangeOpeningKind(task.Spec.Kind) {
		return ctrl.Result{}, nil, false
	}
	l := log.FromContext(ctx)
	l.Info("writeback: change_summary declared remaining scope; failing task before any PR opens (full-scope-or-decline)",
		"action", "lifecycle_implement_incomplete_scope", "resource_id", task.Name)
	// m9: every other terminal path (parkWithComment / the codifiedTerminal
	// declined path) posts an explanatory issue comment and swaps the phase
	// label off tatara-implementation; this hard-fail used to do neither,
	// leaving a human staring at an implementation-labelled issue with no PR
	// and no explanation. Post the same CONVERSATIONAL comment those other
	// paths use (gated, not an operator-internal status comment) and remove
	// the implementation label - best-effort, degrading to log-only when the
	// SCM context is unresolvable (e.g. an umbrella task with no single
	// RepositoryRef), matching parkWithComment's own degrade path.
	msg := "The implementation declared remaining scope (\"" + cs.RemainingScope + "\") instead of completing it in " +
		"one PR or calling decline_implementation. No PR was opened and no follow-up issue was filed " +
		"(full-scope-or-decline) - leaving this for a human."
	if proj, _, writer, token, provider, scmErr := r.scmContext(ctx, task); scmErr == nil {
		if task.Spec.Source != nil && !task.Spec.Source.IsPR && task.Spec.Source.IssueRef != "" {
			if _, cerr := r.gatedComment(ctx, &proj, nil, writer, token, provider, task.Spec.Source.Number, task.Spec.Source.IsPR, task.Spec.Source.AuthorLogin, task.Spec.Source.IssueRef, msg); cerr != nil {
				l.Error(cerr, "writeback: post remaining-scope comment (non-fatal)", "resource_id", task.Name)
			}
			if lerr := r.ensurePhaseLabel(ctx, &proj, task, "declined"); lerr != nil {
				l.Error(lerr, "writeback: remove implementation label (non-fatal)", "resource_id", task.Name)
			}
		}
	} else {
		l.Error(scmErr, "writeback: scm context for remaining-scope comment (non-fatal)", "resource_id", task.Name)
	}
	// D1: the hard-fail can also fire on a LATER turn, with a PR already open
	// from an earlier one (the lifecycle re-enters Implement with the PR still
	// open on four paths: mrci-failure, merge-conflict, mainci-failure,
	// deploy-failure). That PR carries native auto-merge, armed at open time and
	// never disarmed - so terminating the Task Failed left the forge free to
	// merge the incomplete change the moment CI went green, and push-CD to tag
	// and release it. Disarm every PR this Task opened before terminating.
	//
	// F2: disarmOpenChanges used to be void and every internal failure was
	// logged and swallowed, so the Task terminated Failed UNCONDITIONALLY even
	// when the disarm never actually happened (SCM rate-limited, token lookup
	// failed, ClosePR errored) - the PR stayed open, armed, and labelled, and
	// the forge merged it the moment CI went green. Fail CLOSED instead: an
	// unverified disarm must not let the Task terminate. Bounded retry mirrors
	// writebackSkip4xxCap/linksSyncFailureCap (Status.DisarmFailures /
	// disarmFailureCap); only once the budget is spent does the Task terminate
	// anyway, but LOUDLY (a distinct DisarmFailed condition + counter) so an
	// armed-PR-that-could-not-be-disarmed is alertable instead of silent.
	if clean := r.disarmOpenChanges(ctx, task); !clean {
		attempts := task.Status.DisarmFailures + 1
		if attempts < disarmFailureCap {
			var persisted bool
			if perr := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
				fresh.Status.DisarmFailures = attempts
				return true
			}); perr != nil {
				l.Error(perr, "writeback: persist disarm failure count (non-fatal)", "resource_id", task.Name)
			} else {
				persisted = task.Status.DisarmFailures == attempts
			}
			if persisted {
				l.Info("writeback: disarm sweep incomplete; will retry before terminating",
					"action", "writeback_disarm_incomplete", "resource_id", task.Name,
					"attempts", attempts, "cap", disarmFailureCap)
				return ctrl.Result{RequeueAfter: disarmRetryRequeue}, nil, true
			}
			// C2: the counter write appeared to succeed (no error) but did not
			// actually stick - e.g. a deployed CRD that predates
			// Status.DisarmFailures silently prunes the field server-side.
			// Requeuing blind here would livelock: every reconcile re-reads the
			// unadvanced counter, retries the full disarm sweep, and never reaches
			// disarmFailureCap. Fall through to the terminate-loudly path instead
			// so the loop stays bounded even when the counter itself cannot be
			// persisted.
			l.Error(errors.New("disarm failure counter did not persist"),
				"writeback: disarm failure counter write did not stick; terminating instead of requeuing blind to avoid a livelock",
				"action", "writeback_disarm_counter_unpersisted", "resource_id", task.Name, "attempted", attempts)
		}
		l.Error(errors.New("disarm attempt budget exhausted"),
			"writeback: disarm sweep still unverified at the attempt cap; terminating anyway - the PR may still be open and armed, human must check",
			"action", "writeback_disarm_capped", "resource_id", task.Name, "attempts", attempts, "cap", disarmFailureCap)
		r.Metrics.WritebackOutcome("disarm_failed")
		if perr := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
			// C4: do NOT reset DisarmFailures here. Resetting before terminate()
			// runs made the retry budget non-monotonic: if terminate() itself then
			// errored, the next reconcile re-read a fresh 0 and restarted the whole
			// disarmFailureCap-sweep cycle instead of re-terminating immediately.
			// The counter is only ever reset on an actual successful/clean disarm
			// (the else branch below) or a reactivation, never as a side effect of
			// giving up.
			apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
				Type:   "DisarmFailed",
				Status: metav1.ConditionTrue,
				Reason: "DisarmCapReached",
				Message: fmt.Sprintf("could not verify the open PR/MR was disarmed after %d attempts; "+
					"it may still be open with auto-merge armed and the semver label still set - check manually", attempts),
				ObservedGeneration: fresh.Generation,
			})
			return true
		}); perr != nil {
			l.Error(perr, "writeback: persist DisarmFailed condition (non-fatal)", "resource_id", task.Name)
		}
	} else if task.Status.DisarmFailures != 0 {
		// Clear a stale counter from an earlier incomplete sweep so a Task that
		// somehow re-enters this path later (e.g. reactivated) starts with a
		// fresh retry budget instead of one already primed near the cap.
		if perr := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
			fresh.Status.DisarmFailures = 0
			return true
		}); perr != nil {
			l.Error(perr, "writeback: reset disarm failure count (non-fatal)", "resource_id", task.Name)
		}
	}
	res, err = r.terminate(ctx, task, "Failed", "IncompleteImplementation",
		"change_summary declared remaining_scope; agents must implement the full scope in one PR "+
			"or call decline_implementation instead of leaving a gap - no follow-up issues are filed")
	return res, err, true
}

// disarmOpenChanges makes every PR/MR this Task already opened unmergeable, for
// a Task the operator has decided must NOT ship (the full-scope-or-decline
// hard-fail): it disables the forge's native auto-merge, strips the semver
// label that drives the release cascade, and closes the PR. Without it, an
// incomplete change opened on turn 1 (auto-merge armed by applySemverAutoMerge)
// still merged itself as soon as its checks went green, even though the Task
// terminated Failed on turn 2.
//
// Every PR on the ledger is disarmed, not only Status.PrURL: a cross-repo
// umbrella opens one PR per repo in scope and each one is armed at open time,
// so disarming the primary alone would still ship the siblings.
//
// F2/C3: returns clean=true only when every target is resolved: ClosePR
// VERIFIED closed (or already permanently gone - 404/410, e.g. a human
// already closed it), AND - when this Task's repo has a configured bot login
// (the operator itself armed auto-merge on this PR at open time via
// applySemverAutoMerge) - DisableAutoMerge did not fail with a real error.
// ClosePR alone is NOT sufficient to gate clean: GitHub does not document
// PR-close as an action that disables auto-merge, and a later reopen
// plausibly restores the prior armed state, so an unverified DisableAutoMerge
// on an operator-armed PR must keep the sweep dirty, not just closed.
// DisableAutoMerge/RemoveLabel stay best-effort and do NOT gate clean when the
// error is the documented "nothing to disable" no-op both forges use when
// auto-merge was never armed or already off (GitHub GraphQL: "auto merge is
// not enabled ..."; GitLab: 406 "no pending auto-merge") - see
// isAutoMergeAlreadyOffError. The caller (checkRemainingScopeHardFail) must
// not let the Task terminate on a dirty sweep. A target this sweep finds
// already MERGED (C1) is handled distinctly below - not silently folded into
// a "clean" outcome via ClosePR's close-of-an-already-merged no-op.
func (r *TaskReconciler) disarmOpenChanges(ctx context.Context, task *tatarav1alpha1.Task) bool {
	if task.Status.PrURL == "" {
		return true
	}
	l := log.FromContext(ctx)
	var proj tatarav1alpha1.Project
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &proj); err != nil {
		l.Error(err, "writeback: disarm open changes: get project", "resource_id", task.Name)
		return false
	}
	repos, err := r.projectRepos(ctx, &proj)
	if err != nil {
		l.Error(err, "writeback: disarm open changes: list project repos", "resource_id", task.Name)
		return false
	}
	provider := ""
	if task.Spec.Source != nil {
		provider = task.Spec.Source.Provider
	}
	if provider == "" && len(repos) > 0 {
		provider = providerForRemote(ctx, repos[0].Spec.URL)
	}
	slugToURL := map[string]string{}
	for i := range repos {
		if slug, _, serr := repoSlugFromURL(repos[i].Spec.URL, provider); serr == nil && slug != "" {
			slugToURL[slug] = repos[i].Spec.URL
		}
	}
	writer, err := r.SCMFor(provider)
	if err != nil {
		l.Error(err, "writeback: disarm open changes: scm writer", "resource_id", task.Name, "provider", provider)
		return false
	}
	token, err := r.scmToken(ctx, task.Namespace, proj.Spec.ScmSecretRef)
	if err != nil {
		l.Error(err, "writeback: disarm open changes: scm token", "resource_id", task.Name)
		return false
	}
	// botLogin is set only when the operator has a configured bot login for this
	// repo - the same condition enableNativeAutoMerge/applySemverAutoMerge gate
	// on, so its presence here means the PR was actually armed by the operator
	// at open time. C3 uses it to decide whether an unverified DisableAutoMerge
	// failure must gate clean=false.
	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}

	// Targets: every openedPR ledger entry that is not already terminal. The
	// ledger is the authoritative set of PRs this Task opened (writeBackOpenChange
	// upserts one entry per opened PR). Fall back to the primary PrURL + the
	// Task's own repo when the ledger holds none (the status write that records
	// them lands after the first OpenChange, so an interrupted writeback can leave
	// PrURL set with no entry).
	type target struct {
		repoURL string
		number  int
		prURL   string
	}
	var targets []target
	for _, wi := range task.Status.WorkItems {
		if wi.Kind != tatarav1alpha1.WorkItemPR || wi.Role != tatarav1alpha1.RoleOpenedPR ||
			wi.Number == 0 || isWITerminal(wi.State) {
			continue
		}
		if url := slugToURL[wi.Repo]; url != "" {
			targets = append(targets, target{repoURL: url, number: wi.Number, prURL: prWebURL(url, provider, wi.Repo, wi.Number)})
		}
	}
	if len(targets) == 0 {
		if n := parsePRNumber(task.Status.PrURL); n > 0 && task.Spec.RepositoryRef != "" {
			var repo tatarav1alpha1.Repository
			if gerr := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); gerr == nil {
				targets = append(targets, target{repoURL: repo.Spec.URL, number: n, prURL: task.Status.PrURL})
			}
		}
	}
	if len(targets) == 0 {
		// F2: no resolvable target means nothing was verifiably disarmed - a PR
		// may still be sitting open and armed under a ledger entry this sweep
		// could not resolve. Not clean: bounded retry, then the loud DisarmFailed
		// path, rather than silently accepting "nothing to do".
		l.Info("writeback: disarm open changes: no resolvable PR to disarm",
			"action", "writeback_disarm_no_target", "resource_id", task.Name, "pr_url", task.Status.PrURL)
		return false
	}

	closeMsg := "Closing: the implementation declared remaining scope instead of completing it in one PR " +
		"(full-scope-or-decline). This change must not merge - auto-merge disabled and the semver label removed."
	clean := true
	for _, tg := range targets {
		// C1: check whether this target already merged BEFORE attempting to
		// disarm it. A merged PR is already state=closed, so ClosePR's PATCH
		// below would be a silent 200 no-op that used to look like an ordinary
		// clean disarm - no distinct signal, and a "this change must not merge"
		// comment posted onto a PR that already merged (and may have already cut
		// a release tag). Shout distinctly instead: nothing is left to disarm on
		// an already-merged target (retrying cannot un-merge it, so it counts as
		// resolved, not dirty), but the log/metric/condition/comment all say
		// MERGED, never "disarmed".
		st, serr := writer.GetPRState(ctx, tg.repoURL, token, tg.number)
		r.recordSCM(provider, "get_pr_state", serr)
		if serr == nil && st.Merged {
			l.Error(fmt.Errorf("incomplete change already merged: %s", tg.prURL),
				"writeback: disarm sweep found an already-MERGED PR for a Task with declared remaining scope; the incomplete change shipped - human must check",
				"action", "writeback_disarm_merged", "resource_id", task.Name, "pr_url", tg.prURL)
			r.Metrics.WritebackOutcome("disarm_merged")
			if perr := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
				apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
					Type:   "IncompleteChangeMerged",
					Status: metav1.ConditionTrue,
					Reason: "DisarmTargetAlreadyMerged",
					Message: fmt.Sprintf("PR %s carried a declared-incomplete change (remaining scope) and already "+
						"merged before the disarm sweep could close it; the change has SHIPPED - check manually", tg.prURL),
					ObservedGeneration: fresh.Generation,
				})
				return true
			}); perr != nil {
				l.Error(perr, "writeback: persist IncompleteChangeMerged condition (non-fatal)", "resource_id", task.Name)
			}
			if task.Spec.Source != nil && !task.Spec.Source.IsPR && task.Spec.Source.IssueRef != "" {
				remaining := ""
				if cs := task.Status.ChangeSummary; cs != nil {
					remaining = cs.RemainingScope
				}
				mergedMsg := fmt.Sprintf("The implementation declared remaining scope (%q) but PR %s already "+
					"MERGED before the operator's disarm sweep could close it (full-scope-or-decline). The "+
					"incomplete change has ALREADY SHIPPED - this cannot be undone automatically and needs human attention.",
					remaining, tg.prURL)
				if _, cerr := r.gatedComment(ctx, &proj, nil, writer, token, provider, task.Spec.Source.Number, task.Spec.Source.IsPR, task.Spec.Source.AuthorLogin, task.Spec.Source.IssueRef, mergedMsg); cerr != nil {
					l.Error(cerr, "writeback: post already-merged comment (non-fatal)", "resource_id", task.Name)
				}
			}
			continue
		}
		// A PR whose web URL could not be reconstructed still gets closed (the
		// close verb is number-based); only the URL-keyed auto-merge call is
		// skipped, and closing already blocks the merge.
		if tg.prURL != "" {
			derr := writer.DisableAutoMerge(ctx, tg.repoURL, token, tg.prURL)
			r.recordSCM(provider, "disable_auto_merge", derr)
			if derr != nil {
				l.Error(derr, "writeback: disable auto-merge on incomplete PR (non-fatal)",
					"resource_id", task.Name, "pr_url", tg.prURL)
				// C3: a real DisableAutoMerge failure on a PR the operator itself
				// armed (botLogin configured) must not be waved through as
				// best-effort - see the doc comment above for why ClosePR alone is
				// not sufficient proof this PR can never auto-merge again after a
				// reopen. The documented "nothing was ever armed" no-op stays
				// non-fatal and does not gate clean.
				if botLogin != "" && !isAutoMergeAlreadyOffError(derr) {
					clean = false
				}
			}
		}
		// Strip the semver label so a human-merged PR cannot still cut a release
		// tag. GitHub only, mirroring applySemverAutoMerge: GitLab AddLabel routes
		// to /issues and never stamped the MR in the first place. Best-effort,
		// like DisableAutoMerge: a stuck label does not leave the PR mergeable.
		if cs := task.Status.ChangeSummary; cs != nil && cs.Significance != "" && provider == "github" {
			if slug, _, serr := repoSlugFromURL(tg.repoURL, provider); serr == nil && slug != "" {
				prRef := fmt.Sprintf("%s#%d", slug, tg.number)
				lerr := writer.RemoveLabel(ctx, token, prRef, semverLabel(cs.Significance))
				r.recordSCM(provider, "remove_label", lerr)
				if lerr != nil {
					l.Error(lerr, "writeback: strip semver label from incomplete PR (non-fatal)",
						"resource_id", task.Name, "pr_ref", prRef)
				}
			}
		}
		// ClosePR is the primary safety-critical action - a closed PR cannot be
		// merged by the forge - but per C3 it no longer gates clean alone: see
		// the doc comment above. A permanent 404/410 (already gone - e.g. a human
		// already closed it) still counts as done here; anything else (429/5xx,
		// a reachability error) leaves this sweep dirty and the caller retries.
		// C5: the "must not merge" note is content-deduped against the PR's own
		// comment thread so a dirty sweep's repeated retries do not re-post it on
		// an already-clean, already-closed target every pass.
		closeBody := closeMsg
		if r.disarmCloseCommentAlreadyPosted(ctx, provider, token, tg.repoURL, tg.number, botLogin, closeMsg) {
			closeBody = ""
		}
		cerr := writer.ClosePR(ctx, tg.repoURL, token, tg.number, closeBody)
		r.recordSCM(provider, "close", cerr)
		if cerr != nil {
			if !isPermanentTargetGone(cerr) {
				clean = false
			}
			l.Error(cerr, "writeback: close incomplete PR", "resource_id", task.Name, "pr_url", tg.prURL)
			continue
		}
		l.Info("writeback: incomplete change disarmed (auto-merge off, semver label stripped, PR closed)",
			"action", "writeback_disarm_incomplete_pr", "resource_id", task.Name, "pr_url", tg.prURL)
	}
	return clean
}

// isAutoMergeAlreadyOffError classifies a DisableAutoMerge failure as the
// documented "nothing to disable" no-op both forges use when auto-merge was
// never armed or was already off, as opposed to a real/unexpected failure
// (rate limit, auth, network) that C3 must not wave through as best-effort.
// GitLab surfaces the no-op as a 406 HTTPError (DisableAutoMerge's own doc
// comment: "the endpoint 406s when the MR has no pending auto-merge"). GitHub's
// GraphQL mutation transport succeeds even when the mutation itself reports
// nothing to do - ghGraphQL surfaces that as a plain error carrying the
// GraphQL response's message text, not an HTTPError - so it needs a message
// match on the "auto merge is not enabled" text GitHub returns.
func isAutoMergeAlreadyOffError(err error) bool {
	if err == nil {
		return false
	}
	var he *scm.HTTPError
	if errors.As(err, &he) {
		return he.Status == 406
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "auto merge is not enabled") ||
		strings.Contains(msg, "auto-merge is not enabled")
}

// disarmCloseCommentAlreadyPosted reports whether the disarm sweep's "must not
// merge" close note (closeMsg, embedded in ClosePR's body) is already present
// on the PR/MR thread, reusing the same content-dedup normalization the
// comment gate applies elsewhere (duplicateRecentBotComment). This
// deliberately bypasses the FULL comment gate (decideCommentGate): that gate
// also withholds on a CLOSED target, which this PR always is by the time the
// dedup check would matter (ClosePR itself closes it), so routing through the
// full gate would suppress the very first post too, not just repeats (C5).
// Fail-open (false, i.e. "not yet posted") on any read error or missing
// wiring, matching the gate family's fail-open contract - a lost read must
// never block the close verb itself.
func (r *TaskReconciler) disarmCloseCommentAlreadyPosted(ctx context.Context, provider, token, repoURL string, number int, botLogin, body string) bool {
	if botLogin == "" || r.ReaderFor == nil {
		return false
	}
	reader, err := r.ReaderFor(provider, token)
	if err != nil || reader == nil {
		return false
	}
	owner, name, err := scm.OwnerRepo(repoURL)
	if err != nil {
		return false
	}
	var comments []scm.IssueComment
	if pl, ok := reader.(scm.PRCommentLister); ok {
		comments, err = pl.ListPRComments(ctx, owner, name, number)
	} else {
		comments, err = reader.ListIssueComments(ctx, owner, name, number)
	}
	if err != nil {
		return false
	}
	return duplicateRecentBotComment(comments, botLogin, body)
}

// writeBackOpenChange attempts a PR/MR on every Project repo and opens one on
// each repo that has the task branch; repos the task did not change return a
// benign 422 (no task branch, classified no-branch) and are skipped without
// counting as a permanent failure. It comments the primary issue with all PR
// links and records them on the Task status. Shared by the default
// (implement/brainstorm) path and the triageIssue-implement path.
func (r *TaskReconciler) writeBackOpenChange(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	// Idempotency guard: if PrURL is already set this function ran successfully on
	// a previous reconcile. Clear WritebackPending and return without re-opening.
	if task.Status.PrURL != "" {
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "AlreadyWritten", "pr/mr url already set")
	}

	l := log.FromContext(ctx)

	// issue #166: a Succeeded task whose writeback gets a permanent 4xx on every
	// project repo must not re-sweep the SCM forever. Once the skip-4xx attempt
	// budget is spent, stop: record a terminal WritebackFailed condition and clear
	// the pending gate without opening a single SCM connection. This is the hard
	// loop-breaker that survives a lost/re-armed WritebackPending condition,
	// because the monotonic counter cannot be flipped back the way the gate flag
	// can.
	if task.Status.WritebackSkip4xxAttempts >= writebackSkip4xxCap {
		l.Info("writeback: 4xx-skip attempt cap reached; not re-sweeping repos",
			"action", "writeback_skip_4xx_capped", "task", task.Name,
			"attempts", task.Status.WritebackSkip4xxAttempts, "cap", writebackSkip4xxCap)
		r.Metrics.WritebackOutcome("skip_4xx_capped")
		return ctrl.Result{}, r.failWritebackSkip4xx(ctx, task)
	}

	var proj tatarav1alpha1.Project
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &proj); err != nil {
		return ctrl.Result{}, fmt.Errorf("writeback: get project: %w", err)
	}

	// Gather all Project repos up-front; the ordered write-back set is derived
	// from them (repo-scoped: primary first; umbrella: ledger scope, else all).
	allRepos, err := r.projectRepos(ctx, &proj)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("writeback: list project repos: %w", err)
	}

	provider := ""
	if task.Spec.Source != nil {
		provider = task.Spec.Source.Provider
	}

	// primaryRepo is the repo-scoped task's own repo (empty for an umbrella
	// implement). ordered is the write-back set: for a repo-scoped task, primary
	// first then the rest; for an umbrella (empty RepositoryRef) the ledger
	// repos-in-scope intersected with project repos, falling back to all project
	// repos. derivePRTitle is called per-repo (scope=repo.Name) so no single
	// primary is required for the umbrella case.
	var primaryRepo tatarav1alpha1.Repository
	ordered := make([]tatarav1alpha1.Repository, 0, len(allRepos))
	if task.Spec.RepositoryRef != "" {
		if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &primaryRepo); err != nil {
			return ctrl.Result{}, fmt.Errorf("writeback: get repository: %w", err)
		}
		ordered = append(ordered, primaryRepo)
		for i := range allRepos {
			if allRepos[i].Name != primaryRepo.Name {
				ordered = append(ordered, allRepos[i])
			}
		}
		if provider == "" {
			provider = providerForRemote(ctx, primaryRepo.Spec.URL)
		}
	} else {
		// Umbrella (empty RepositoryRef): scope to EffectiveReposInScope so an
		// umbrella kind (implement/review/clarify) opens a PR on EVERY enrolled
		// project repo (untouched repos return a benign 422 no-branch and skip),
		// not just the ledger/source repos (the U-B fix). allSlugs bounds the scope
		// to enrolled repos so nothing outside the project is targeted.
		allSlugs := make([]string, 0, len(allRepos))
		for i := range allRepos {
			if slug, serr := scm.RepoSlugFromURL(allRepos[i].Spec.URL); serr == nil {
				allSlugs = append(allSlugs, slug)
			}
		}
		inScope := make(map[string]bool)
		for _, slug := range tatarav1alpha1.EffectiveReposInScope(task, allSlugs) {
			inScope[slug] = true
		}
		for i := range allRepos {
			if len(inScope) == 0 {
				ordered = append(ordered, allRepos[i])
				continue
			}
			if slug, serr := scm.RepoSlugFromURL(allRepos[i].Spec.URL); serr == nil && inScope[slug] {
				ordered = append(ordered, allRepos[i])
			}
		}
		if len(ordered) == 0 {
			ordered = append(ordered, allRepos...)
		}
		if provider == "" && len(ordered) > 0 {
			provider = providerForRemote(ctx, ordered[0].Spec.URL)
		}
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

	sourceBranch := taskBranch(task)
	baseBody := writeBackBody(task)

	// M4: when the agent submitted a change_summary, use PRBody + Delivered block
	// as the MR body instead of the M1 defaults. Title is handled by derivePRTitle
	// (strong ChangeSummary.PRTitle wins; weak or absent falls back to conventional
	// derived form from Source.Title / Goal).
	if cs := task.Status.ChangeSummary; cs != nil {
		deliveredBody := cs.PRBody
		if cs.DeliveredScope != "" {
			deliveredBody += "\n\n## Delivered\n" + cs.DeliveredScope
		}
		// Preserve the tatara-authored marker so downstream merge-gate logic works.
		deliveredBody += "\n\n" + tataraAuthoredMarker
		baseBody = deliveredBody
	}

	// Systemic approval gate (finding #4): the agent-authored body may carry
	// "Closes #N" for systemic siblings. Neutralize any close directive targeting a
	// sibling that is NOT currently maintainer-approved so merging this combined PR
	// never force-closes an unapproved or declined sibling. This is the authoritative
	// net behind the prompt-level filter (agents are unreliable). Approved siblings
	// and the lead's own close stay intact.
	if unapproved := r.unapprovedSystemicSiblings(ctx, task); len(unapproved) > 0 {
		baseBody = neutralizeUnapprovedCloses(baseBody, unapproved)
	}

	var prURLs []string
	// prRepos[i] is the Repository that produced prURLs[i]; kept parallel so the
	// openedPR ledger entry derives its slug from the SAME repo as the PR number,
	// even when the primary repo is skipped (422 no-change) and prURLs[0] is a
	// secondary repo's PR.
	var prRepos []tatarav1alpha1.Repository
	var lastSkipStatus int
	// sawSkip4xx records whether any repo was skipped on a genuine permanent 4xx
	// (404/403/non-recoverable-422), as opposed to a 422 "no commits" (empty
	// implement) or "already exists" recovery. Only genuine skips arm the
	// 4xx-skip loop cap (issue #166); an empty implement must not.
	var sawSkip4xx bool
	// inScope is the declarative cross-repo scope (CR names). When a repo in this
	// set produces no branch (422 no commits) we warn on the issue instead of
	// skipping silently (Defect A1).
	inScope := make(map[string]bool, len(task.Spec.ReposInScope))
	for _, name := range task.Spec.ReposInScope {
		inScope[name] = true
	}
	var inScopeNoBranch []string
	for _, repo := range ordered {
		body := baseBody
		// Append "Closes #N" for the primary repo of an issue-linked lifecycle task
		// so the MR auto-closes the issue on merge.  Never emit this on secondary
		// repos (cross-repo leak) or for non-lifecycle / PR-entry tasks.
		//
		// push-CD: a pushCDEligible task (declared significance) rides the deploy
		// cascade, and deploy-supervision closes the issue on a CONFIRMED helmfile
		// apply with the deployed version (D9). Emitting "Closes #N" here would let
		// native auto-merge close the issue at MERGE time - before apply - and an
		// apply-failure/timeout reroll would then re-enter Implement with the issue
		// already (wrongly) closed. Suppress it and let deploy-supervision own the
		// close.
		if repo.Name == primaryRepo.Name &&
			task.Spec.Kind == "issueLifecycle" &&
			task.Spec.Source != nil &&
			!task.Spec.Source.IsPR &&
			task.Spec.Source.Number > 0 &&
			!pushCDEligible(task) {
			body = body + "\n\nCloses #" + strconv.Itoa(task.Spec.Source.Number)
		}
		// Per-repo title: the conventional scope is the repo the PR opens on, so a
		// cross-repo umbrella labels each PR with its own repo scope.
		title := derivePRTitle(task, repo.Name)
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
				if skipReason == "no-change" || skipReason == "no-branch" {
					if inScope[repo.Name] {
						l.Info("writeback: in-scope repo produced no change; will warn on issue",
							"action", "writeback_in_scope_no_branch", "repo", repo.Name, "task", task.Name, "branch", sourceBranch, "reason", skipReason)
						inScopeNoBranch = append(inScopeNoBranch, repo.Name)
						r.Metrics.WritebackOutcome("in_scope_no_branch")
					} else if skipReason == "no-branch" {
						// issue #178: the task never pushed sourceBranch to this repo
						// (GitHub 422 {field:head, code:invalid}); it simply did not change
						// this repo. This is a benign cross-repo fan-out no-op, NOT a
						// permanent failure - record it distinctly so skip_4xx stays a pure
						// permanent-failure signal and the 4xx-skip cap (issue #166) is not armed.
						l.Info("writeback: repo not touched by task (no task branch); skipping",
							"action", "writeback_no_branch", "repo", repo.Name, "task", task.Name, "branch", sourceBranch)
						r.Metrics.WritebackOutcome("no_branch")
					} else {
						l.Info("writeback: implement produced no changes (branch has no commits)",
							"action", "writeback_no_change", "repo", repo.Name, "task", task.Name, "branch", sourceBranch)
						r.Metrics.WritebackOutcome("no_change")
					}
				} else if skipReason == "already-exists" {
					if recovered, rerr := r.recoverExistingPRURL(ctx, token, provider, repo.Spec.URL, sourceBranch); rerr == nil && recovered != "" {
						l.Info("writeback: pr/mr already exists, recovered url",
							"action", "writeback_pr_recovered", "repo", repo.Name, "task", task.Name, "pr_url", recovered)
						prURLs = append(prURLs, recovered)
						prRepos = append(prRepos, repo)
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
					r.Metrics.WritebackSkip4xx(strconv.Itoa(he.Status), "already_exists")
					sawSkip4xx = true
				} else {
					l.Info("writeback: skipping repo (4xx)",
						"action", "writeback_skip_4xx", "repo", repo.Name, "task", task.Name, "status", he.Status, "path", he.Path, "body", he.Body)
					r.Metrics.WritebackOutcome("skip_4xx")
					r.Metrics.WritebackSkip4xx(strconv.Itoa(he.Status), "other")
					sawSkip4xx = true
				}
				lastSkipStatus = he.Status
				continue
			}
			return ctrl.Result{}, fmt.Errorf("writeback: open change for %s: %w", repo.Name, openErr)
		}
		l.Info("writeback: pr/mr opened", "task", task.Name, "repo", repo.Name, "pr_url", prURL)
		r.Metrics.WritebackOutcome("opened")
		// push-CD visibility: an implement/lifecycle code change SHOULD declare a
		// significance (required at the change_summary MCP tool + REST endpoint).
		// When it is absent the PR opens unlabeled and skips the cascade (legacy
		// close+Done path). That is allowed by design (pushCDEligible=false) but a
		// human-merged unlabeled bot PR has no semver:<level> label for the
		// component release workflow to read, so surface it loudly rather than let
		// the legacy path go silent.
		if (task.Spec.Kind == "implement" || task.Spec.Kind == "issueLifecycle") &&
			(task.Status.ChangeSummary == nil || task.Status.ChangeSummary.Significance == "") {
			l.Info("writeback: code-change PR opened without declared significance; no semver label / no auto-merge (legacy path)",
				"action", "writeback_no_significance", "task", task.Name, "repo", repo.Name, "pr_url", prURL)
			r.Metrics.WritebackOutcome("opened_no_significance")
		}
		// push-CD: stamp the declared significance label and enable native
		// auto-merge on the freshly-opened bot PR (D5). Best-effort, never fatal.
		// Documentation is not a versioned artifact (no release cascade / semver
		// tag): it declares no significance, so skip the label and auto-merge the
		// bot PR directly on its Build check.
		if task.Spec.Kind == "documentation" {
			r.enableNativeAutoMerge(ctx, &proj, repo, writer, token, provider, prURL)
		} else {
			r.applySemverAutoMerge(ctx, &proj, repo, writer, token, provider, prURL, task.Status.ChangeSummary)
		}
		prURLs = append(prURLs, prURL)
		prRepos = append(prRepos, repo)
		// Persist the primary PR URL immediately after the first successful OpenChange
		// so a transient failure on a later repo does not lose the already-opened URL.
		// A requeue then finds PrURL set and skips re-opening.
		if len(prURLs) == 1 {
			if perr := r.persistPrimaryPRURL(ctx, task, prURLs[0]); perr != nil {
				return ctrl.Result{}, perr
			}
		}
	}

	// Warn on the source issue for any in-scope repo that yielded no branch.
	// Best-effort and non-fatal: other repos' MRs still open (no atomicity, KISS).
	if len(inScopeNoBranch) > 0 && task.Spec.Source != nil && task.Spec.Source.IssueRef != "" {
		warnBody := "WARNING: this issue was declared to span repos that produced no change. " +
			"The following in-scope repo(s) produced no change on branch `" + sourceBranch + "` (no commits, or the branch was never pushed) and got no PR/MR: " +
			strings.Join(inScopeNoBranch, ", ") + ". " +
			"If those repos genuinely need no change this is expected; otherwise the cross-repo edit was lost - re-run or fix manually."
		_, werr := r.gatedComment(ctx, &proj, &primaryRepo, writer, token, provider, task.Spec.Source.Number, task.Spec.Source.IsPR, task.Spec.Source.AuthorLogin, task.Spec.Source.IssueRef, warnBody)
		if werr != nil {
			l.Error(werr, "writeback: in-scope no-branch warning comment (non-fatal)",
				"action", "writeback_in_scope_warn_failed", "issue_ref", task.Spec.Source.IssueRef, "repos", strings.Join(inScopeNoBranch, ","))
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
			task.Status.ResultSummary != "" && task.Status.DeployState != "Implement"
		if commented {
			_, cerr := r.gatedComment(ctx, &proj, &primaryRepo, writer, token, provider, task.Spec.Source.Number, task.Spec.Source.IsPR, task.Spec.Source.AuthorLogin, task.Spec.Source.IssueRef, task.Status.ResultSummary)
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
		// issue #166: when no PR opened because a repo returned a genuine permanent
		// 4xx (not a benign 422 "no commits"), count the attempt so the gate above
		// caps the re-sweep loop. Empty-implement (all 422 no-change) keeps the
		// plain terminal clear and never arms the cap.
		if sawSkip4xx {
			return ctrl.Result{}, r.recordSkip4xxAttempt(ctx, task, lastSkipStatus)
		}
		return ctrl.Result{}, r.clearWritebackPending(ctx, task, "WritebackSkipped", msg)
	}

	// Record primary PR URL (first in list) and all URLs in the condition message.
	// RetryOnConflict ensures this idempotency key lands even when a concurrent
	// lifecycle reconcile has bumped the resource version.
	prURLsMsg := strings.Join(prURLs, " ")
	// Derive each opened-PR ledger entry's repo slug from the SAME repo that produced
	// the matching prURLs entry; when the primary repo is skipped (422 no-change)
	// prURLs[0] belongs to a secondary repo, so using primaryRepo.Spec.URL would
	// record a corrupt {primary-slug, secondary-number} entry the backstop/dedup can
	// never match. prRepos is kept parallel to prURLs for exactly this.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); gerr != nil {
			return gerr
		}
		fresh.Status.PrURL = prURLs[0]
		// A PR opened: clear any accumulated skip-4xx attempts so a later, unrelated
		// writeback starts with a fresh budget (issue #166).
		fresh.Status.WritebackSkip4xxAttempts = 0
		// Project EVERY opened PR onto the ledger: upsert a role:openedPR entry with
		// state:open for each PR so Status.WorkItems tracks all N cross-repo PRs (the
		// U-A fix), not just the first - this is what lets review/backstop/deploy see
		// every sibling PR under the umbrella. HeadBranch is the shared task branch;
		// the real HeadSHA is filled later by the backstop refresh (Phase 3), which we
		// do not have here without an extra SCM round-trip.
		for idx := range prURLs {
			slug, _, serr := repoSlugFromURL(prRepos[idx].Spec.URL, provider)
			if serr != nil || slug == "" {
				continue
			}
			num := parsePRNumber(prURLs[idx])
			if num <= 0 {
				continue
			}
			UpsertWorkItem(fresh, tatarav1alpha1.WorkItemRef{
				Provider:   provider,
				Repo:       slug,
				Number:     num,
				Kind:       tatarav1alpha1.WorkItemPR,
				Role:       tatarav1alpha1.RoleOpenedPR,
				State:      tatarav1alpha1.WIOpen,
				HeadBranch: sourceBranch,
			})
		}
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
		_, cerr := r.gatedComment(ctx, &proj, &primaryRepo, writer, token, provider, task.Spec.Source.Number, task.Spec.Source.IsPR, task.Spec.Source.AuthorLogin, task.Spec.Source.IssueRef, commentBody)
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

// applySemverAutoMerge stamps the declared change-significance label on the
// just-opened bot PR and enables the forge's native auto-merge (push-CD, D5). It
// runs only when the agent declared a significance and the project has a bot
// login: the PR is bot-authored by construction here (the operator opened it
// with the bot token), so botLogin presence is the authorship condition. The
// PR label is applied on GitHub only - GitHub PRs share the issues label
// endpoint, while GitLab AddLabel routes to /issues and the GitLab
// "infrastructure" project is not part of the GitHub-only cd-release cascade.
// Every step is best-effort and logged; a forge that disallows auto-merge or a
// label failure never fails the writeback.
func (r *TaskReconciler) applySemverAutoMerge(ctx context.Context, proj *tatarav1alpha1.Project, repo tatarav1alpha1.Repository, writer scm.SCMWriter, token, provider, prURL string, cs *tatarav1alpha1.ChangeSummary) {
	if cs == nil || cs.Significance == "" {
		return
	}
	label := semverLabel(cs.Significance)
	color := managedLabelColors(proj.Spec.Scm)[label]
	r.ensureSemverLabelColor(ctx, writer, repo.Spec.URL, token, provider, label, color,
		"writeback: ensure semver label (non-fatal)", "repo", repo.Name, "label", label)
	if provider == "github" {
		if slug, _, serr := repoSlugFromURL(repo.Spec.URL, provider); serr == nil {
			if n := parsePRNumber(prURL); n > 0 {
				prRef := fmt.Sprintf("%s#%d", slug, n)
				_ = r.addSemverLabelToPR(ctx, writer, token, provider, prRef, label,
					"writeback: add semver label (non-fatal)",
					"repo", repo.Name, "pr_ref", prRef, "label", label)
			}
		}
	}
	r.enableNativeAutoMerge(ctx, proj, repo, writer, token, provider, prURL)
}

// enableNativeAutoMerge turns on the forge's native auto-merge for a freshly-
// opened bot PR, gated only on a configured project bot login (the PR is
// bot-authored by construction - the operator opened it moments ago with the bot
// SCM token, the same authorship condition applySemverAutoMerge relied on).
// Best-effort and logged; a forge that disallows auto-merge never fails the
// writeback. Callers that ride the semver cascade stamp the label first; the
// documentation kind (not a versioned artifact) calls this directly.
func (r *TaskReconciler) enableNativeAutoMerge(ctx context.Context, proj *tatarav1alpha1.Project, repo tatarav1alpha1.Repository, writer scm.SCMWriter, token, provider, prURL string) {
	l := log.FromContext(ctx)
	// D5 auto-merge gate (b): PR author == bot holds by construction here (this
	// code path opened the PR with the bot SCM token). The only remaining
	// condition is that a bot login is actually configured; absent it, withhold.
	if proj.Spec.Scm == nil || proj.Spec.Scm.BotLogin == "" {
		l.Info("writeback: auto-merge withheld - no project bot login",
			"action", "scm_auto_merge_withheld", "repo", repo.Name, "pr_url", prURL)
		return
	}
	merr := writer.EnableAutoMerge(ctx, repo.Spec.URL, token, prURL, "squash")
	r.recordSCM(provider, "auto_merge", merr)
	if merr != nil {
		l.Error(merr, "writeback: enable auto-merge (non-fatal)", "repo", repo.Name, "pr_url", prURL)
		return
	}
	l.Info("writeback: native auto-merge enabled", "action", "scm_auto_merge",
		"repo", repo.Name, "pr_url", prURL)
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

// recordSkip4xxAttempt increments the per-task skip-4xx attempt counter and
// clears WritebackPending. It bounds the un-triageable 4xx-skip loop (issue
// #166): the next writeback entry reads the counter and, once it reaches
// writebackSkip4xxCap, refuses to re-sweep the SCM (see failWritebackSkip4xx).
// The increment and the clear share one RetryOnConflict status write so the
// counter advances atomically with the gate clear. status is the last 4xx code
// seen, recorded in the condition message for triage.
func (r *TaskReconciler) recordSkip4xxAttempt(ctx context.Context, task *tatarav1alpha1.Task, status int) error {
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.WritebackSkip4xxAttempts++
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:   "WritebackPending",
			Status: metav1.ConditionFalse,
			Reason: "WritebackSkipped4xx",
			Message: fmt.Sprintf("no PR opened; all repos returned 4xx (last status %d); attempt %d/%d",
				status, fresh.Status.WritebackSkip4xxAttempts, writebackSkip4xxCap),
			ObservedGeneration: fresh.Generation,
		})
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		log.FromContext(ctx).Error(err, "writeback: record skip-4xx attempt", "task", task.Name)
		return err
	}
	return nil
}

// failWritebackSkip4xx records the terminal WritebackFailed condition for a task
// that exhausted its skip-4xx attempt budget and clears WritebackPending. It
// performs no SCM I/O. WritebackFailed is a distinct, sticky condition (nothing
// removes it) so the give-up stays visible in `kubectl describe` even if
// WritebackPending is later re-armed by a stray Succeeded transition.
func (r *TaskReconciler) failWritebackSkip4xx(ctx context.Context, task *tatarav1alpha1.Task) error {
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		msg := fmt.Sprintf("writeback gave up after %d skip-4xx attempts: every project repo returned a permanent 4xx and no PR was opened",
			fresh.Status.WritebackSkip4xxAttempts)
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               "WritebackFailed",
			Status:             metav1.ConditionTrue,
			Reason:             "Skip4xxCapReached",
			Message:            msg,
			ObservedGeneration: fresh.Generation,
		})
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               "WritebackPending",
			Status:             metav1.ConditionFalse,
			Reason:             "WritebackFailed",
			Message:            msg,
			ObservedGeneration: fresh.Generation,
		})
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		log.FromContext(ctx).Error(err, "writeback: record terminal WritebackFailed", "task", task.Name)
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

// derivePRTitle returns the PR/MR title for a write-back. A strong
// ChangeSummary.PRTitle wins; otherwise it derives a conventional title from the
// captured work-item title (Source.Title), falling back to the goal first line.
// It never returns a weak title.
func derivePRTitle(task *tatarav1alpha1.Task, scope string) string {
	if cs := task.Status.ChangeSummary; cs != nil && cs.PRTitle != "" {
		if weak, _ := titlecheck.Weak(cs.PRTitle); !weak {
			return cs.PRTitle
		}
	}
	subject := ""
	if task.Spec.Source != nil {
		subject = strings.TrimSpace(task.Spec.Source.Title)
	}
	if subject == "" {
		subject = firstLine(task.Spec.Goal)
	}
	kind := "feat"
	switch task.Spec.Kind {
	case "issueLifecycle", "incident":
		kind = "fix"
	case "documentation":
		kind = "docs"
	}
	return fmt.Sprintf("%s(%s): %s", kind, scope, subject)
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

// tataraProposedByMarkerPrefix delimits the kind-bearing provenance marker
// written on proposal issues only (item 4a): it extends tataraAuthoredMarker
// with WHICH tatara workflow proposed the issue, so the auto-approve release
// path can key on kind. Both markers are written together; the plain
// tataraAuthoredMarker substring check keeps working unmodified.
const tataraProposedByMarkerPrefix = "<!-- tatara-proposed-by:"

func tataraProposedByMarker(kind string) string {
	return tataraProposedByMarkerPrefix + kind + " -->"
}

// tataraProposedByKind extracts the kind from a body's tatara-proposed-by
// marker, or "" when absent.
func tataraProposedByKind(body string) string {
	i := strings.Index(body, tataraProposedByMarkerPrefix)
	if i < 0 {
		return ""
	}
	rest := body[i+len(tataraProposedByMarkerPrefix):]
	end := strings.Index(rest, " -->")
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// proposalKind derives the kind marker to stamp on a proposal issue: refine
// never creates ProposedIssue Tasks today (see MEMORY.md), so only
// brainstorm/incident are distinguished here.
func proposalKind(task *tatarav1alpha1.Task) string {
	if task.Spec.ProposedIssue == nil {
		return ""
	}
	if task.Spec.ProposedIssue.Incident {
		return "incident"
	}
	return "brainstorm"
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

// reviewTargetClosed reports whether a review Task's target PR/MR is already
// merged or closed, checked before a review turn is spawned (task_controller.go)
// and before a review verdict is posted (writeback_review.go) so a stale review
// never re-runs against, or re-comments on, a PR the deploy supervisor already
// merged (root cause of PR #295: a review verdict posted twice on an
// already-merged PR). Fail-open (false) on any resolution error so a transient
// SCM hiccup never blocks a legitimate review.
func (r *TaskReconciler) reviewTargetClosed(ctx context.Context, task *tatarav1alpha1.Task) bool {
	if task.Spec.Source == nil || !task.Spec.Source.IsPR || task.Spec.Source.Number <= 0 {
		return false
	}
	_, repo, writer, token, _, err := r.scmContext(ctx, task)
	if err != nil {
		return false
	}
	st, serr := writer.GetPRState(ctx, repo.Spec.URL, token, task.Spec.Source.Number)
	if serr != nil {
		return false
	}
	return st.Merged || st.Closed
}

func (r *TaskReconciler) recordSCM(provider, verb string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	r.Metrics.SCMWrite(provider, verb, result)
	if err != nil {
		r.Metrics.SCMRequestErrorByStatus(provider, verb, scm.ErrorStatus(err))
	}
}

// recordSCMGone records an SCM write whose target is permanently gone (404/410:
// the issue was deleted) as a distinct result="gone" outcome instead of
// result="error". A gone target is terminal, not a genuine write failure, so
// counting it as "error" inflated the SCM write-failure-ratio alert against a
// single deleted issue (issue #268). The classified HTTP status is still
// recorded on the by-status counter for visibility.
func (r *TaskReconciler) recordSCMGone(provider, verb string, err error) {
	r.Metrics.SCMWrite(provider, verb, "gone")
	r.Metrics.SCMRequestErrorByStatus(provider, verb, scm.ErrorStatus(err))
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
// "no-branch": 422 {field:head, code:invalid} - the task branch does not exist in
// this repo, i.e. the task never changed it. Benign cross-repo fan-out no-op, not
// a permanent failure (issue #178), distinct from "No commits between" where the
// branch exists but is empty.
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
	if he.Status == 422 && strings.Contains(he.Body, `"field":"head"`) && strings.Contains(he.Body, `"code":"invalid"`) {
		return "no-branch"
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
			return prWebURL(repoURL, provider, slug, pr.Number), nil
		}
	}
	return "", nil
}

// prWebURL renders the web URL of PR/MR number in the repo at repoURL, from the
// repo URL's own base host (so a self-hosted forge renders against its own host,
// never a hardcoded github.com/gitlab.com). Returns "" when the base cannot be
// parsed or slug is empty.
func prWebURL(repoURL, provider, slug string, number int) string {
	if slug == "" || number <= 0 {
		return ""
	}
	base, err := parseRepoBase(repoURL)
	if err != nil {
		return ""
	}
	if provider == "gitlab" {
		return fmt.Sprintf("%s/%s/-/merge_requests/%d", base, slug, number)
	}
	return fmt.Sprintf("%s/%s/pull/%d", base, slug, number)
}
