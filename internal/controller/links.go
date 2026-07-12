package controller

import (
	"context"
	"regexp"
	"slices"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

const (
	linksBlockStart = "<!-- tatara-links:start -->"
	linksBlockEnd   = "<!-- tatara-links:end -->"
)

var linksBlockRE = regexp.MustCompile(`(?s)` + regexp.QuoteMeta(linksBlockStart) + `.*?` + regexp.QuoteMeta(linksBlockEnd))

// linksSyncFailureCap bounds how many consecutive INCOMPLETE tatara-links
// sweeps a Task retries for one sibling URL set before giving up on it (D2).
// It mirrors writebackSkip4xxCap (issue #166): a permanent failure the
// gone-check cannot see (403 conversation-locked, a bot without issues:write, a
// 422 body over GitHub's 65536-char limit) is never clean, so without a cap the
// sweep re-reads every sibling on every reconcile forever. A couple of attempts
// cover a genuinely transient error (rate-limit) and then it stops; in the
// healthy case the first sweep is clean and the counter never leaves 0.
const linksSyncFailureCap = 3

// RenderLinksBlock builds the marker-delimited managed block cross-linking
// sibling issues opened for the same Task (item 5). urls is the OTHER
// siblings only (not the issue the block is being written into).
func RenderLinksBlock(urls []string) string {
	return linksBlockStart + "\nRelated issues (same task): " + strings.Join(urls, ", ") + "\n" + linksBlockEnd
}

// SpliceLinksBlock idempotently rewrites the managed block in body: replaces
// an existing block in place, or appends a new one when absent. The rest of
// body is preserved verbatim.
func SpliceLinksBlock(body, block string) string {
	if linksBlockRE.MatchString(body) {
		return linksBlockRE.ReplaceAllString(body, block)
	}
	if body == "" {
		return block
	}
	return body + "\n\n" + block
}

// syncSiblingLinks rewrites the managed tatara-links block in every sibling
// issue so each one lists the OTHERS. Called whenever a Task's issue-kind
// ledger gains a member (item 5). No-op (clean) when fewer than 2 issue
// siblings exist (a lone issue has nothing to cross-link). Best-effort per
// sibling: one failed edit does not block the rest, but the sweep as a whole
// reports whether it was clean via the returned bool - M5: the caller only
// stamps Status.LinksSyncedURLs (which gates the NEXT resync) when the sweep
// was clean, so a sibling hit by a transient SCM error (e.g. rate-limiting)
// gets retried on the next reconcile instead of being silently skipped
// forever. A permanent 404/410 (the issue is gone and can never be read
// again) is treated as done, not as a reason to keep retrying.
func (r *TaskReconciler) syncSiblingLinks(ctx context.Context, provider, token string, siblingURLs []string) bool {
	if len(siblingURLs) < 2 || r.ReaderFor == nil {
		return true
	}
	reader, err := r.ReaderFor(provider, token)
	if err != nil {
		return false
	}
	writer, err := r.SCMFor(provider)
	if err != nil {
		return false
	}
	clean := true
	for _, self := range siblingURLs {
		repoSlug, number, ok := parseIssueURL(self)
		if !ok {
			continue
		}
		others := make([]string, 0, len(siblingURLs)-1)
		for _, u := range siblingURLs {
			if u != self {
				others = append(others, u)
			}
		}
		owner, name, _ := strings.Cut(repoSlug, "/")
		content, gerr := reader.GetIssue(ctx, owner, name, number)
		r.recordSCM(provider, "get_issue", gerr)
		if gerr != nil {
			if !isPermanentTargetGone(gerr) {
				clean = false
			}
			continue
		}
		newBody := SpliceLinksBlock(content.Body, RenderLinksBlock(others))
		if newBody == content.Body {
			continue // idempotent: nothing changed, skip the write
		}
		if eerr := writer.EditIssue(ctx, token, repoSlug, number, scm.EditIssueReq{Body: &newBody}); eerr != nil {
			log.FromContext(ctx).Error(eerr, "syncSiblingLinks: edit issue (non-fatal, best-effort)", "issue", self)
			if !isPermanentTargetGone(eerr) {
				clean = false
			}
		}
	}
	return clean
}

// discoveredIssueSiblings returns Status.DiscoveredIssues when it holds 2+
// entries (the sibling-link threshold), else nil.
func discoveredIssueSiblings(task *tatarav1alpha1.Task) []string {
	if len(task.Status.DiscoveredIssues) < 2 {
		return nil
	}
	return task.Status.DiscoveredIssues
}

// allIssueSiblingURLs returns the deduped union of every issue URL this Task
// spans: Status.WorkItems issue-kind entries, Status.DiscoveredIssues, and
// Spec.SystemicGroup.CrossRepo refs (item Request C/b: all-links comment on
// multi-issue tasks). defaultProvider (typically the owning Project's
// Spec.Scm.Provider) is used when a WorkItemRef does not carry its own
// Provider, and for every CrossRepo ref (F6: CrossRepo refs never carry a
// provider field at all, so they used to hardcode "github" unconditionally,
// rendering github.com links for a GitLab project). Falls back to "github"
// only when defaultProvider is itself empty (the "owner/repo#N" ref format
// used throughout this codebase is GitHub-style; no self-hosted-GitLab base
// is resolvable from a bare ref string with no provider hint at all).
// repoURLFor (m8) is an optional owner/repo-slug -> Repository.Spec.URL
// lookup: when it resolves a slug, that repo's real URL threads through to
// issueURLFromRepoURL so a self-hosted GitLab project renders links against
// its own host instead of the gitlab.com fallback. A slug with no entry (or
// a nil map) still falls back to the provider-derived default host.
func allIssueSiblingURLs(task *tatarav1alpha1.Task, defaultProvider string, repoURLFor map[string]string) []string {
	fallback := defaultProvider
	if fallback == "" {
		fallback = "github"
	}
	seen := make(map[string]bool)
	var urls []string
	add := func(u string) {
		if u == "" || seen[u] {
			return
		}
		seen[u] = true
		urls = append(urls, u)
	}
	for _, wi := range task.Status.WorkItems {
		if wi.Kind != tatarav1alpha1.WorkItemIssue || wi.Number == 0 || wi.Repo == "" {
			continue
		}
		provider := wi.Provider
		if provider == "" {
			provider = fallback
		}
		add(issueURLFromRepoURL(repoURLFor[wi.Repo], provider, wi.Repo, wi.Number))
	}
	for _, u := range task.Status.DiscoveredIssues {
		add(u)
	}
	if g := task.Spec.SystemicGroup; g != nil {
		for _, ref := range g.CrossRepo {
			repo, n := parseCrossRepoRef(ref)
			if repo == "" || n == 0 {
				continue
			}
			add(issueURLFromRepoURL(repoURLFor[repo], fallback, repo, n))
		}
	}
	return urls
}

// syncAllSiblingLinksIfNeeded posts/refreshes the tatara-links cross-linking
// comment on every issue this Task spans, whenever that union holds 2+
// members (item Request C/b). Runs once per Reconcile for every Task kind,
// not only from the two prior triggers (proposal completion, the removed
// implement follow-up). Best-effort: any lookup/SCM failure is logged and
// swallowed so it never blocks the Task's real reconcile work.
//
// F5: gated on the synced sibling URL set (Status.LinksSyncedURLs) actually
// changing since the last sync. Before this gate, every reconcile of every
// multi-issue Task re-read every sibling issue (one GetIssue per sibling, no
// TTL, no change check) - unbounded read amplification on the shared bot rate
// limit for a Task that sits in Conversation/Planning for a long time with a
// stable sibling set. The writes were already idempotent; only the redundant
// reads needed bounding.
func (r *TaskReconciler) syncAllSiblingLinksIfNeeded(ctx context.Context, task *tatarav1alpha1.Task) {
	if r.SCMFor == nil || r.ReaderFor == nil {
		return
	}
	var project tatarav1alpha1.Project
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &project); err != nil {
		log.FromContext(ctx).Info("links: project lookup failed (non-fatal)",
			"action", "task_links_sync_skip", "resource_id", task.Name, "err", err.Error())
		return
	}
	if project.Spec.Scm == nil {
		return
	}
	// m8: resolve each enrolled repo's real URL so a self-hosted GitLab
	// project's sibling links render against its own host instead of the
	// gitlab.com fallback (issueURLFromRepoURL's default when repoURL is "").
	repoURLFor := map[string]string{}
	if repos, rerr := r.projectRepos(ctx, &project); rerr == nil {
		for i := range repos {
			if slug, serr := scm.RepoSlugFromURL(repos[i].Spec.URL); serr == nil {
				repoURLFor[slug] = repos[i].Spec.URL
			}
		}
	}
	urls := allIssueSiblingURLs(task, project.Spec.Scm.Provider, repoURLFor)
	if len(urls) < 2 {
		return
	}
	if slices.Equal(task.Status.LinksSyncedURLs, urls) {
		return
	}
	token, terr := r.scmToken(ctx, task.Namespace, project.Spec.ScmSecretRef)
	if terr != nil {
		log.FromContext(ctx).Info("links: scm token lookup failed (non-fatal)",
			"action", "task_links_sync_skip", "resource_id", task.Name, "err", terr.Error())
		return
	}
	if clean := r.syncSiblingLinks(ctx, project.Spec.Scm.Provider, token, urls); !clean {
		// M5: an unclean sweep (a failure on at least one sibling) must NOT stamp
		// LinksSyncedURLs, so the next reconcile retries the whole sweep instead
		// of the sibling-set-changed gate above masking it forever.
		//
		// D2: bounded, though. isPermanentTargetGone only treats 404/410 as
		// terminal, so a sibling failing permanently for any OTHER reason (403
		// conversation-locked, no issues:write, 422 body too large) stayed
		// unclean forever and re-read every sibling on every reconcile - the exact
		// read amplification this gate exists to bound. Count the attempts and,
		// at the cap, fall through to the stamp: the sweep gives up on this
		// sibling set, the reads stop, and a later CHANGE to the set re-arms a
		// fresh budget (the stamp resets the counter).
		attempts := task.Status.LinksSyncFailures + 1
		if attempts < linksSyncFailureCap {
			log.FromContext(ctx).Info("links: sibling sweep incomplete; will retry next reconcile",
				"action", "task_links_sync_incomplete", "resource_id", task.Name,
				"attempts", attempts, "cap", linksSyncFailureCap)
			if perr := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
				fresh.Status.LinksSyncFailures = attempts
				return true
			}); perr != nil {
				log.FromContext(ctx).Error(perr, "links: persist sweep failure count (non-fatal)", "resource_id", task.Name)
			}
			return
		}
		log.FromContext(ctx).Info("links: sibling sweep still incomplete at the attempt cap; giving up on this sibling set (no more re-reads)",
			"action", "task_links_sync_capped", "resource_id", task.Name,
			"attempts", attempts, "cap", linksSyncFailureCap)
		// F3: mirror writebackSkip4xxCap's self-diagnosing companion metric
		// (writeback_outcome_total{result="skip_4xx_capped"}) so this give-up is
		// alertable, not just an INFO log line a human has to go find.
		r.Metrics.WritebackOutcome("links_sync_capped")
	}
	if perr := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if slices.Equal(fresh.Status.LinksSyncedURLs, urls) && fresh.Status.LinksSyncFailures == 0 {
			return false
		}
		fresh.Status.LinksSyncedURLs = urls
		fresh.Status.LinksSyncFailures = 0
		return true
	}); perr != nil {
		log.FromContext(ctx).Error(perr, "links: persist synced url set (non-fatal)", "resource_id", task.Name)
	}
}
