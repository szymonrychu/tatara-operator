package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// isPermanentTargetGone reports whether err is an SCM HTTPError meaning the
// target resource is permanently unreachable: 410 Gone (GitHub "This issue was
// deleted") or 404 Not Found. Retrying a write against such a target is futile,
// so a lifecycle reconcile must treat it as terminal (log + skip) instead of
// returning the error and letting controller-runtime requeue the same doomed
// write forever (issue #263: a deleted issue drove an unbounded add_label retry
// loop that amplified operator_scm_writes_total{result="error"} and fired the
// SCM write-failure-ratio alert). Transient 4xx (429/403 rate limits) and 5xx
// stay retryable and are NOT matched here.
func isPermanentTargetGone(err error) bool {
	var he *scm.HTTPError
	if errors.As(err, &he) {
		return he.Status == 404 || he.Status == 410
	}
	return false
}

// lifecycleLabels returns the four managed phase labels (brainstorming/approved/
// implementation/declined), applying defaults when a field is empty.
func lifecycleLabels(s *tatarav1alpha1.ScmSpec) (brainstorming, approved, implementation, declined string) {
	brainstorming, approved, implementation, declined =
		"tatara-brainstorming", "tatara-approved", "tatara-implementation", "tatara-declined"
	if s == nil {
		return
	}
	if s.BrainstormingLabel != "" {
		brainstorming = s.BrainstormingLabel
	}
	if s.ApprovedLabel != "" {
		approved = s.ApprovedLabel
	}
	if s.ImplementationLabel != "" {
		implementation = s.ImplementationLabel
	}
	if s.DeclinedLabel != "" {
		declined = s.DeclinedLabel
	}
	return
}

// legacyLabels returns the deprecated idea/rejected labels (lazy migration).
func legacyLabels(s *tatarav1alpha1.ScmSpec) (idea, rejected string) {
	idea, rejected = "tatara-idea", "tatara-rejected"
	if s == nil {
		return
	}
	if s.IdeaLabel != "" {
		idea = s.IdeaLabel
	}
	if s.RejectedLabel != "" {
		rejected = s.RejectedLabel
	}
	return
}

// incidentLabel returns the additive label for incident-originated proposals.
// It is NOT a managed phase label (never swept by setLifecycleLabel).
func incidentLabel(s *tatarav1alpha1.ScmSpec) string {
	if s != nil && s.IncidentLabel != "" {
		return s.IncidentLabel
	}
	return "tatara-incident"
}

// approverMentions returns a "cc: @a @b (for review)" line @mentioning the
// project's approvers so they are notified of a newly-opened tatara issue.
// Empty when the project has no approvers.
func approverMentions(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository) string {
	logins := tatarav1alpha1.EffectiveMaintainerLogins(proj, repo)
	if len(logins) == 0 {
		return ""
	}
	parts := make([]string, len(logins))
	for i, l := range logins {
		parts[i] = "@" + l
	}
	return "cc: " + strings.Join(parts, " ") + " (for review)"
}

// semver:* labels mark a PR's declared change significance for the push-CD
// cascade (cd-release keys the next tag off them). Additive palette, NOT phase
// labels: they MUST stay out of managedPhaseLabels/activePhaseLabels so
// setLifecycleLabel never strips them.
const (
	semverLabelMajor = "semver:major"
	semverLabelMinor = "semver:minor"
	semverLabelPatch = "semver:patch"
)

// semverLabel returns the managed label for a declared significance level.
func semverLabel(significance string) string { return "semver:" + significance }

// ensureSemverLabelColor ensures the semver:<level> label exists with its
// managed color (idempotent EnsureLabel), records the SCM call, and logs any
// failure non-fatally with the caller-supplied message and fields. Shared by
// lifecycle Merge (ensureSemverLabelBeforeMerge) and writeback
// (applySemverAutoMerge) so both drive one EnsureLabel path; neither moves the
// call relative to its own provider gate. logKV carries each caller's existing
// log fields verbatim (they intentionally differ and are not unified here).
func (r *TaskReconciler) ensureSemverLabelColor(ctx context.Context, writer scm.SCMWriter, url, token, provider, label, color, logMsg string, logKV ...any) {
	if eerr := writer.EnsureLabel(ctx, url, token, label, color); eerr != nil {
		r.recordSCM(provider, "ensure_label", eerr)
		log.FromContext(ctx).Error(eerr, logMsg, logKV...)
	}
}

// addSemverLabelToPR adds the semver:<level> label to the PR (idempotent
// AddLabel), records the SCM call, and logs a failure non-fatally with the
// caller-supplied message and fields. Returns the AddLabel error so each caller
// keeps its own post-failure behavior (lifecycle returns before its success
// info log; writeback continues). Only ever invoked from a GitHub-scoped branch.
func (r *TaskReconciler) addSemverLabelToPR(ctx context.Context, writer scm.SCMWriter, token, provider, prRef, label, logMsg string, logKV ...any) error {
	aerr := writer.AddLabel(ctx, token, prRef, label)
	r.recordSCM(provider, "add_label", aerr)
	if aerr != nil {
		log.FromContext(ctx).Error(aerr, logMsg, logKV...)
	}
	return aerr
}

// managedLabelColors maps each managed tatara label (resolving any custom names
// from ScmSpec) to its hex color (6 digits, no '#'), for EnsureLabel.
func managedLabelColors(s *tatarav1alpha1.ScmSpec) map[string]string {
	b, a, i, d := lifecycleLabels(s)
	idea, rej := legacyLabels(s)
	return map[string]string{
		b:                "1d76db", // brainstorming - blue
		a:                "0e8a16", // approved - green
		i:                "fbca04", // implementation - yellow
		d:                "9e9e9e", // declined - gray
		incidentLabel(s): "d73a4a", // incident - red
		idea:             "c5def5", // idea - light blue
		rej:              "5a5a5a", // rejected - dark gray
		semverLabelMajor: "b60205", // semver major - red
		semverLabelMinor: "d93f0b", // semver minor - orange
		semverLabelPatch: "0e8a16", // semver patch - green
	}
}

// managedPhaseLabels returns every label the operator owns (new + legacy), so
// setLifecycleLabel removes all-but-desired and dedup recognizes legacy issues.
func managedPhaseLabels(s *tatarav1alpha1.ScmSpec) []string {
	b, a, i, d := lifecycleLabels(s)
	idea, rej := legacyLabels(s)
	return []string{b, a, i, d, idea, rej}
}

// activePhaseLabels returns the labels meaning "in flight" (brainstorming,
// approved, implementation, + legacy idea). An OPEN issue bearing any of these
// with only-terminal Tasks is an orphan the backstop resumes.
func activePhaseLabels(s *tatarav1alpha1.ScmSpec) []string {
	b, a, i, _ := lifecycleLabels(s)
	idea, _ := legacyLabels(s)
	return []string{b, a, i, idea}
}

// setLifecycleLabel ensures exactly `desired` of the managed phase labels is
// present on the task's source issue: it adds `desired` if absent and removes
// all other managed labels (4 phase labels: brainstorming/approved/
// implementation/declined, plus 2 legacy labels: idea/rejected) if present.
// It never touches any non-managed label. Idempotent. AddLabel failures are
// returned (caller requeues); RemoveLabel failures are logged and tolerated.
func (r *TaskReconciler) setLifecycleLabel(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, desired string) error {
	if task.Spec.Source == nil || task.Spec.Source.IssueRef == "" {
		return nil
	}
	l := log.FromContext(ctx)
	managed := managedPhaseLabels(proj.Spec.Scm)
	brainstorming, _, implementation, _ := lifecycleLabels(proj.Spec.Scm)
	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return fmt.Errorf("set label: %w", err)
	}
	issueRef := task.Spec.Source.IssueRef

	// known reports whether we read the issue's current label set. When we could
	// not (nil reader, list error, or issue not in the open list e.g. just-closed),
	// current is empty but we must NOT skip the removals - otherwise the
	// "exactly one managed label" contract silently breaks. In that case we
	// add + remove unconditionally; AddLabel is idempotent and RemoveLabel is
	// best-effort (tolerates the label being absent).
	current, known := r.currentIssueLabels(ctx, provider, token, repo.Spec.URL, issueRef)

	// One-of-4 invariant guard: refuse to (re)set brainstorming on an issue that
	// already carries the implementation label. The issue has been handed off to an
	// implement Task (which owns the implementation phase); a stale front-half
	// (e.g. a spurious clarify Task re-entering at Triage, or a triage revert racing
	// the handoff) re-stamping brainstorming would drag the actively-implementing
	// issue back a phase and re-trigger the front-half. Only enforced when we could
	// read the current labels (fail-open to the add otherwise).
	if known && desired == brainstorming && current[implementation] {
		l.Info("set label: issue already in implementation; refusing brainstorming re-stamp",
			"action", "scm_set_label_impl_guard", "resource_id", task.Name, "issue_ref", issueRef)
		return nil
	}

	// changed tracks whether an actual add/remove API call landed, so the
	// "lifecycle label set" log only fires on a real state change. Without this
	// it logged on every reconcile (~160/h of misleading no-op lines) even when
	// the label was already correct and AddLabel was skipped.
	changed := false
	addedDesired := false
	if !known || !current[desired] {
		if aerr := writer.AddLabel(ctx, token, issueRef, desired); aerr != nil {
			// issue #263: the target issue is permanently gone (410 deleted / 404
			// not found). Requeuing this write can never succeed, so classify it as
			// terminal - record a distinct result="gone" (not "error", which
			// amplified the SCM write-error ratio and fired the alert, issue #268),
			// log it, do not add/remove any further label, and return nil so the
			// reconcile stops instead of retry-looping the doomed AddLabel.
			if isPermanentTargetGone(aerr) {
				r.recordSCMGone(provider, "add_label", aerr)
				l.Info("set label: target issue permanently gone; skipping label without requeue",
					"action", "scm_set_label_target_gone", "resource_id", task.Name,
					"issue_ref", issueRef, "label", desired, "status", scm.ErrorStatus(aerr))
				return nil
			}
			r.recordSCM(provider, "add_label", aerr)
			return fmt.Errorf("set label add %q: %w", desired, aerr)
		}
		r.recordSCM(provider, "add_label", nil)
		changed = true
		addedDesired = true
	}

	// Re-list the issue's labels IMMEDIATELY before the removes. The
	// list->add->remove sequence is three separate SCM calls with no CAS, and the
	// TaskReconciler, ProjectReconciler, and webhook can all drive it concurrently;
	// deciding the removes off the stale top-of-function read could remove a label a
	// racing controller just set (leaving zero managed labels) or skip removing one
	// it just added (leaving two). Re-reading here shrinks that window to the gap
	// between this read and each remove.
	//
	// Residual (documented; not closable without a distributed lock): two controllers
	// asserting DIFFERENT desired phase labels at the same instant remain logically
	// racy - re-listing narrows the mechanical window but cannot serialize conflicting
	// intents. In practice a given issue's phase is driven by one producer at a time,
	// and the implementation-guard above blocks the highest-impact conflict.
	fresh, freshKnown := r.currentIssueLabels(ctx, provider, token, repo.Spec.URL, issueRef)
	if freshKnown {
		// Add-then-verify: if the fresh read shows `desired` gone and we did NOT add it
		// ourselves this call (i.e. it was believed already present), a racing writer
		// stripped it - re-add once so the desired label is never lost.
		if !fresh[desired] && !addedDesired {
			if aerr := writer.AddLabel(ctx, token, issueRef, desired); aerr != nil {
				if isPermanentTargetGone(aerr) {
					r.recordSCMGone(provider, "add_label", aerr)
					return nil
				}
				r.recordSCM(provider, "add_label", aerr)
				return fmt.Errorf("set label re-add %q: %w", desired, aerr)
			}
			r.recordSCM(provider, "add_label", nil)
			fresh[desired] = true
			changed = true
		}
	}

	for _, lb := range managed {
		if lb == desired {
			continue
		}
		// Remove only labels the FRESH read still shows present (when known). Our own
		// just-added `desired` is never in this set (skipped above). When the fresh
		// read failed, fall back to unconditional best-effort removes so the
		// exactly-one-managed-label contract still holds (RemoveLabel tolerates absent).
		if freshKnown && !fresh[lb] {
			continue
		}
		if rerr := writer.RemoveLabel(ctx, token, issueRef, lb); rerr != nil {
			r.recordSCM(provider, "remove_label", rerr)
			l.Info("set label: remove other label failed (non-fatal)",
				"action", "scm_set_label", "resource_id", task.Name, "issue_ref", issueRef, "label", lb, "err", rerr.Error())
			continue
		}
		r.recordSCM(provider, "remove_label", nil)
		changed = true
	}
	if changed {
		l.Info("lifecycle label set", "action", "scm_set_label",
			"resource_id", task.Name, "issue_ref", issueRef, "label", desired)
	}

	// P4 hybrid projection. This is also the role:proposed PRODUCER: for a
	// tatara-authored proposal issue carrying a lifecycle phase label, the operator
	// mints a role:proposed ledger entry (with the real issue number) on the
	// issueLifecycle Task if none exists, then reflects the label as its State.
	// Without this producer no role:proposed entry is ever created, so the backlog
	// cap, the label readback, and this projection are all dead on real Tasks.
	// Only bot-authored proposals are ledgered: a human-reported issue that the
	// operator labels (e.g. tatara-approved on triage) is NOT a proposal and must
	// not get a spurious role:proposed entry. No-op for non-proposal labels
	// (implementation/legacy idea/rejected map to "").
	if wiState := lifecycleLabelToWIState(proj.Spec.Scm, desired); wiState != "" {
		if isBotAuthoredProposal(proj, task) {
			if seedErr := r.seedProposedEntry(ctx, task, issueRef, wiState); seedErr != nil {
				l.Info("set label: seed proposed ledger entry failed (non-fatal)",
					"action", "scm_set_label_ledger_seed", "resource_id", task.Name,
					"issue_ref", issueRef, "wi_state", wiState, "err", seedErr.Error())
			}
		}
		if upsertErr := r.upsertProposedEntryState(ctx, task, issueRef, wiState); upsertErr != nil {
			l.Info("set label: ledger entry update failed (non-fatal)",
				"action", "scm_set_label_ledger", "resource_id", task.Name,
				"issue_ref", issueRef, "wi_state", wiState, "err", upsertErr.Error())
		}
	}
	return nil
}

// currentIssueLabels reads the managed source issue's current label set from live
// SCM. It returns (labels, known): known is false when the labels could not be
// determined (nil reader, reader/parse error, list error, or the issue not present
// in the open list e.g. just-closed), in which case labels is empty and callers
// must fall back to their read-failure behaviour rather than trusting the empty set.
// setLifecycleLabel calls it twice per invocation - once before the add (for the
// add-skip decision + the implementation guard) and once immediately before the
// removes (to shrink the RMW window).
func (r *TaskReconciler) currentIssueLabels(ctx context.Context, provider, token, repoURL, issueRef string) (map[string]bool, bool) {
	labels := map[string]bool{}
	if r.ReaderFor == nil {
		return labels, false
	}
	reader, rerr := r.ReaderFor(provider, token)
	if rerr != nil {
		return labels, false
	}
	owner, name, oerr := scm.OwnerRepo(repoURL)
	if oerr != nil {
		return labels, false
	}
	issues, lerr := reader.ListOpenIssues(ctx, owner, name)
	if lerr != nil {
		return labels, false
	}
	for _, iss := range issues {
		if fmt.Sprintf("%s#%d", iss.Repo, iss.Number) == issueRef {
			for _, lb := range iss.Labels {
				labels[lb] = true
			}
			return labels, true
		}
	}
	return labels, false
}

// isBotAuthoredProposal reports whether the task's source issue is a
// tatara-authored brainstorm proposal: a non-PR issue whose AuthorLogin equals
// the configured BotLogin. Proposals filed by createProposal carry the bot
// login; human-reported issues do not. This gates the role:proposed producer so
// human bug reports never get ledgered as proposals.
func isBotAuthoredProposal(proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) bool {
	if proj.Spec.Scm == nil || proj.Spec.Scm.BotLogin == "" || task.Spec.Source == nil {
		return false
	}
	s := task.Spec.Source
	return !s.IsPR && s.IssueRef != "" && s.AuthorLogin == proj.Spec.Scm.BotLogin
}

// seedProposedEntry appends a role:proposed WorkItemRef for issueRef (parsed
// repo/number) in the given state when no role:proposed entry exists on the
// Task. Idempotent: a no-op once any role:proposed entry is present. Persisted
// under RetryOnConflict. The subsequent upsertProposedEntryState then sets the
// State precisely (this seeds at `state`, but a later projection may move it).
func (r *TaskReconciler) seedProposedEntry(ctx context.Context, task *tatarav1alpha1.Task, issueRef, state string) error {
	repo, number := parseIssueRef(issueRef)
	if repo == "" || number == 0 {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh tatarav1alpha1.Task
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), &fresh); gerr != nil {
			return gerr
		}
		for _, wi := range fresh.Status.WorkItems {
			if wi.Role == tatarav1alpha1.RoleProposed {
				return nil // already produced
			}
		}
		provider := ""
		title := ""
		if fresh.Spec.Source != nil {
			provider = fresh.Spec.Source.Provider
			title = fresh.Spec.Source.Title
		}
		UpsertWorkItem(&fresh, tatarav1alpha1.WorkItemRef{
			Provider: provider,
			Repo:     repo,
			Number:   number,
			Kind:     tatarav1alpha1.WorkItemIssue,
			Role:     tatarav1alpha1.RoleProposed,
			State:    state,
			Title:    title,
		})
		return r.Status().Update(ctx, &fresh)
	})
}

// parseIssueRef splits an "owner/repo#N" issue reference into (repo, number).
// Returns ("", 0) on parse failure.
func parseIssueRef(issueRef string) (string, int) {
	i := strings.LastIndexByte(issueRef, '#')
	if i <= 0 || i == len(issueRef)-1 {
		return "", 0
	}
	repo := issueRef[:i]
	n, err := strconv.Atoi(issueRef[i+1:])
	if err != nil || n <= 0 {
		return "", 0
	}
	return repo, n
}

// lifecycleLabelToWIState maps a managed SCM phase label to its equivalent
// work-item state for the role:proposed ledger entry. Returns "" for labels
// that have no ledger counterpart (e.g. tatara-idea, tatara-rejected).
func lifecycleLabelToWIState(s *tatarav1alpha1.ScmSpec, label string) string {
	bs, approved, impl, declined := lifecycleLabels(s)
	switch label {
	case bs:
		return tatarav1alpha1.WIProposed
	case approved:
		return tatarav1alpha1.WIApproved
	case impl:
		return tatarav1alpha1.WIImplemented
	case declined:
		return tatarav1alpha1.WIDeclined
	}
	return ""
}

// upsertProposedEntryState updates the role:proposed WorkItemRef whose
// (Repo, Number) matches the given issueRef to the given state, then persists
// the change under RetryOnConflict. No-op when no matching entry exists.
func (r *TaskReconciler) upsertProposedEntryState(ctx context.Context, task *tatarav1alpha1.Task, issueRef, newState string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh tatarav1alpha1.Task
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), &fresh); gerr != nil {
			return gerr
		}
		changed := false
		for i := range fresh.Status.WorkItems {
			wi := &fresh.Status.WorkItems[i]
			if wi.Role != tatarav1alpha1.RoleProposed {
				continue
			}
			if fmt.Sprintf("%s#%d", wi.Repo, wi.Number) != issueRef {
				continue
			}
			if wi.State == newState {
				return nil // already set, nothing to write
			}
			wi.State = newState
			changed = true
			break
		}
		if !changed {
			return nil
		}
		return r.Status().Update(ctx, &fresh)
	})
}

// hasHumanComment reports whether the task's source issue has at least one
// comment authored by a non-bot login. Used to gate self-approval of
// bot-authored issues: tatara never self-approves its own idea before a human
// has engaged. Returns the underlying error so the caller can fail closed.
func (r *TaskReconciler) hasHumanComment(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (bool, error) {
	if r.ReaderFor == nil || task.Spec.Source == nil {
		return false, nil
	}
	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	provider := task.Spec.Source.Provider
	if provider == "" && proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	token, err := r.scmToken(ctx, task.Namespace, proj.Spec.ScmSecretRef)
	if err != nil {
		return false, err
	}
	reader, err := r.ReaderFor(provider, token)
	if err != nil {
		return false, err
	}
	owner, name, err := scm.OwnerRepo(r.repoURLForTask(ctx, task))
	if err != nil {
		return false, err
	}
	comments, err := reader.ListIssueComments(ctx, owner, name, task.Spec.Source.Number)
	if err != nil {
		return false, err
	}
	for _, c := range comments {
		if c.Author != "" && c.Author != botLogin {
			return true, nil
		}
	}
	return false, nil
}

// NOTE: the former thirdPartyAuthor autoapprove tier (issue #56) was removed
// when the maintainer-approval gate landed: third-party authorship is no longer
// a release signal. Only a VERIFIED maintainer approval (Status.ApprovedByMaintainer,
// recorded by the webhook from a MaintainerLogins actor applying the approved
// label) advances a front-half issue to implement. Author-based intake gating
// still lives in IsAllowedReporter (reporter intake) and IsTrustedAuthor
// (trigger-label/reaction-scope bypass); neither releases implementation.
