package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

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
	current := map[string]bool{}
	known := false
	if r.ReaderFor != nil {
		if reader, rerr := r.ReaderFor(provider, token); rerr == nil {
			if owner, name, oerr := scm.OwnerRepo(repo.Spec.URL); oerr == nil {
				if issues, lerr := reader.ListOpenIssues(ctx, owner, name); lerr == nil {
					for _, iss := range issues {
						if fmt.Sprintf("%s#%d", iss.Repo, iss.Number) == issueRef {
							for _, lb := range iss.Labels {
								current[lb] = true
							}
							known = true
							break
						}
					}
				}
			}
		}
	}

	// changed tracks whether an actual add/remove API call landed, so the
	// "lifecycle label set" log only fires on a real state change. Without this
	// it logged on every reconcile (~160/h of misleading no-op lines) even when
	// the label was already correct and AddLabel was skipped.
	changed := false
	if !known || !current[desired] {
		if aerr := writer.AddLabel(ctx, token, issueRef, desired); aerr != nil {
			r.recordSCM(provider, "add_label", aerr)
			return fmt.Errorf("set label add %q: %w", desired, aerr)
		}
		r.recordSCM(provider, "add_label", nil)
		changed = true
	}
	for _, lb := range managed {
		if lb == desired || (known && !current[lb]) {
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

// thirdPartyAuthor reports whether the task's source issue was opened by a
// known external contributor: a non-empty Source.AuthorLogin that is neither
// the configured BotLogin nor any MaintainerLogin, and (issue #102) that is an
// allowed reporter. Third-party issues are
// trusted and autoapproved through triage without the self-approve hold
// (issue #56). AuthorLogin is authoritative here - for cron-scanned issues it
// is captured from the authenticated ListOpenIssues call, and on the webhook
// path it comes from the HMAC-verified payload. A genuine tatara-authored issue
// carries the bot login, so it never reads as third-party.
func thirdPartyAuthor(proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) bool {
	if proj.Spec.Scm == nil || task.Spec.Source == nil {
		return false
	}
	author := task.Spec.Source.AuthorLogin
	if author == "" || author == proj.Spec.Scm.BotLogin {
		return false
	}
	for _, m := range proj.Spec.Scm.MaintainerLogins {
		if author == m {
			return false
		}
	}
	// Issue #102: only autoapprove third-party authors that are allowed reporters.
	// With an empty reporter allowlist this is a no-op (every external author is
	// trusted, preserving issue #56); once an allowlist is configured a non-reporter
	// would already have been dropped at intake, so this is belt-and-braces.
	if !tatarav1alpha1.IsAllowedReporter(proj, nil, author) {
		return false
	}
	return true
}
