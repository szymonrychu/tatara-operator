// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// umbrellaRefreshTTL is the per-member freshness window for refreshUmbrellaMembers.
// A member polled within this window is skipped so the turn-0 bundle assembly stays
// a light SCM poll (spec landmine b: keep it light).
const umbrellaRefreshTTL = 2 * time.Minute

// umbrellaRepo is one repo in a Task's cross-repo scope, used by buildUmbrellaPrompt
// to render the "Repos in scope" section with clone URL, default branch, and the
// /workspace checkout hint. Slug is the "owner/repo" SCM slug; Name is the
// Repository CR name (the /workspace/<name> mount point).
type umbrellaRepo struct {
	Slug          string
	Name          string
	URL           string
	DefaultBranch string
}

// umbrellaMergeStater is the single SCMWriter capability refreshUmbrellaMembers
// needs beyond the reader (mergeability of a PR/MR member). Optional: a nil merger
// leaves each member's Mergeable field untouched. *scm.GitHub / *scm.GitLab and the
// controller's SCMWriter satisfy it.
type umbrellaMergeStater interface {
	GetMergeState(ctx context.Context, repoURL, token string, number int) (scm.MergeState, error)
}

// umbrellaMemberRef renders the SCM ref for a work-item member: "<repo>#<n>" for
// issues (and GitHub PRs), "<repo>!<n>" for GitLab MRs. Mirrors WorkItemsContext.
func umbrellaMemberRef(wi tatarav1alpha1.WorkItemRef) string {
	if wi.Kind == tatarav1alpha1.WorkItemPR && wi.Provider == "gitlab" {
		return fmt.Sprintf("%s!%d", wi.Repo, wi.Number)
	}
	return fmt.Sprintf("%s#%d", wi.Repo, wi.Number)
}

// umbrellaLinkedIssue resolves the source/closes issue number that a PR in repo
// drives, from the Task's work-item ledger (co-membership is the linkage: a review
// or implement umbrella Task carries both the source issue and its PR members).
// Returns 0 when no source/closes issue for that repo is recorded. Used to route a
// PR-only re-review cycle back to the originating issue (Phase-6 note).
func umbrellaLinkedIssue(task *tatarav1alpha1.Task, repo string) int {
	for _, wi := range task.Status.WorkItems {
		if wi.Kind == tatarav1alpha1.WorkItemIssue && wi.Repo == repo &&
			(wi.Role == tatarav1alpha1.RoleSource || wi.Role == tatarav1alpha1.RoleCloses) &&
			wi.Number > 0 {
			return wi.Number
		}
	}
	return 0
}

// refreshUmbrellaMembers is a light SCM poll that refreshes each non-terminal
// WorkItemRef member's structured state (State, Body, Labels, HeadBranch, HeadSHA,
// CIStatus, Mergeable) in place so the turn-0 bundle reflects live cross-repo state
// without the pod re-crawling SCM. Members polled within ttl of now are skipped
// (TTL-gate) and terminal members (closed/merged/declined/implemented) are never
// re-polled. reader supplies issue/PR listings + issue bodies + commit CI; merger
// (optional, may be nil) supplies mergeability. slugToURL maps "owner/repo" to the
// repo URL for the merge-state call. Returns true when at least one member changed.
func refreshUmbrellaMembers(ctx context.Context, reader scm.SCMReader, merger umbrellaMergeStater, slugToURL map[string]string, token string, task *tatarav1alpha1.Task, ttl time.Duration, now time.Time) bool {
	// Collect repos that have at least one stale, non-terminal member so we make one
	// pair of list calls per repo, not one per member.
	issueRepos := map[string]bool{}
	prRepos := map[string]bool{}
	for i := range task.Status.WorkItems {
		wi := &task.Status.WorkItems[i]
		if wi.Repo == "" || isWITerminal(wi.State) || umbrellaMemberFresh(wi, ttl, now) {
			continue
		}
		switch wi.Kind {
		case tatarav1alpha1.WorkItemIssue:
			issueRepos[wi.Repo] = true
		case tatarav1alpha1.WorkItemPR:
			prRepos[wi.Repo] = true
		}
	}

	issueCache := map[string]map[int]scm.IssueRef{}
	for repo := range issueRepos {
		owner, name, _ := strings.Cut(repo, "/")
		issues, err := reader.ListOpenIssues(ctx, owner, name)
		if err != nil {
			continue
		}
		m := make(map[int]scm.IssueRef, len(issues))
		for _, iss := range issues {
			m[iss.Number] = iss
		}
		issueCache[repo] = m
	}
	prCache := map[string]map[int]scm.PRRef{}
	for repo := range prRepos {
		owner, name, _ := strings.Cut(repo, "/")
		prs, err := reader.ListOpenPRs(ctx, owner, name)
		if err != nil {
			continue
		}
		m := make(map[int]scm.PRRef, len(prs))
		for _, pr := range prs {
			m[pr.Number] = pr
		}
		prCache[repo] = m
	}

	nowT := metav1.NewTime(now)
	changed := false
	for i := range task.Status.WorkItems {
		wi := &task.Status.WorkItems[i]
		if wi.Repo == "" || isWITerminal(wi.State) || umbrellaMemberFresh(wi, ttl, now) {
			continue
		}
		owner, name, _ := strings.Cut(wi.Repo, "/")
		switch wi.Kind {
		case tatarav1alpha1.WorkItemIssue:
			cache, ok := issueCache[wi.Repo]
			if !ok {
				continue
			}
			ref, open := cache[wi.Number]
			if !open {
				wi.State = tatarav1alpha1.WIClosed
			} else {
				wi.State = tatarav1alpha1.WIOpen
				if len(ref.Labels) > 0 {
					wi.Labels = ref.Labels
				}
				if content, err := reader.GetIssue(ctx, owner, name, wi.Number); err == nil && content.Body != "" {
					wi.Body = content.Body
				}
			}
			wi.LastRefreshedAt = &nowT
			changed = true
		case tatarav1alpha1.WorkItemPR:
			cache, ok := prCache[wi.Repo]
			if !ok {
				continue
			}
			ref, open := cache[wi.Number]
			if !open {
				wi.State = tatarav1alpha1.WIClosed
				wi.LastRefreshedAt = &nowT
				changed = true
				continue
			}
			wi.State = tatarav1alpha1.WIOpen
			if ref.HeadSHA != "" {
				wi.HeadSHA = ref.HeadSHA
			}
			if ref.HeadBranch != "" {
				wi.HeadBranch = ref.HeadBranch
			}
			if ref.Body != "" {
				wi.Body = ref.Body
			}
			if len(ref.Labels) > 0 {
				wi.Labels = ref.Labels
			}
			if wi.HeadSHA != "" {
				if ci, err := reader.GetCommitCIStatus(ctx, owner, name, wi.HeadSHA); err == nil && ci != "" {
					wi.CIStatus = ci
				}
			}
			if merger != nil {
				if url := slugToURL[wi.Repo]; url != "" {
					if ms, err := merger.GetMergeState(ctx, url, token, wi.Number); err == nil {
						wi.Mergeable = string(ms)
					}
				}
			}
			wi.LastRefreshedAt = &nowT
			changed = true
		}
	}
	return changed
}

// umbrellaMemberFresh reports whether a member was refreshed within ttl of now.
func umbrellaMemberFresh(wi *tatarav1alpha1.WorkItemRef, ttl time.Duration, now time.Time) bool {
	if wi.LastRefreshedAt == nil {
		return false
	}
	return now.Sub(wi.LastRefreshedAt.Time) < ttl
}

// buildUmbrellaPrompt renders the operator-assembled turn-0 cross-repo context
// bundle (CROSS-REPO-CONTRACT "Turn-0 umbrella context bundle"): the goal header,
// the repos-in-scope + per-repo checkout instructions, every issue member (body +
// thread + state + labels + repo), every MR member (description + head branch + CI +
// mergeability + state + thread + repo), and the kind-specific task-goal tail.
// Pure: all SCM data (member structured state, threads) is pre-fetched by the
// caller. threads maps a member ref ("<repo>#<n>" / "<repo>!<n>") to its
// oldest-first comment thread; each thread is char-budgeted (triageCommentCharBudget)
// so a large umbrella stays bounded. goalTail is the kind directive + skills line.
func buildUmbrellaPrompt(task *tatarav1alpha1.Task, repos []umbrellaRepo, threads map[string][]scm.IssueComment, goalTail string) string {
	goal := task.Spec.Goal
	if goal == "" {
		goal = "(cross-repo umbrella)"
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "# Umbrella task: %s\n", goal)

	if len(repos) > 0 {
		sb.WriteString("\n## Repos in scope\n")
		for _, r := range repos {
			fmt.Fprintf(&sb, "- %s: clone %s, default branch `%s`. Cloned at `/workspace/%s`; "+
				"to work an existing MR run `git checkout <its head branch>` there.\n",
				r.Slug, r.URL, r.DefaultBranch, r.Name)
		}
	}

	var issues, prs []tatarav1alpha1.WorkItemRef
	for _, wi := range task.Status.WorkItems {
		if wi.Number == 0 {
			continue
		}
		switch wi.Kind {
		case tatarav1alpha1.WorkItemIssue:
			issues = append(issues, wi)
		case tatarav1alpha1.WorkItemPR:
			prs = append(prs, wi)
		}
	}

	if len(issues) > 0 {
		sb.WriteString("\n## Issues\n")
		for _, wi := range issues {
			ref := umbrellaMemberRef(wi)
			fmt.Fprintf(&sb, "### %s [%s]%s\n", ref, umbrellaState(wi.State), umbrellaLabels(wi.Labels))
			sb.WriteString(umbrellaBody(wi.Body))
			sb.WriteString(umbrellaThread(threads[ref]))
		}
	}

	if len(prs) > 0 {
		sb.WriteString("\n## Merge requests\n")
		for _, wi := range prs {
			ref := umbrellaMemberRef(wi)
			fmt.Fprintf(&sb, "### %s [%s] branch:%s CI:%s mergeable:%s%s\n",
				ref, umbrellaState(wi.State), umbrellaField(wi.HeadBranch), umbrellaField(wi.CIStatus),
				umbrellaField(wi.Mergeable), umbrellaLabels(wi.Labels))
			sb.WriteString(umbrellaBody(wi.Body))
			sb.WriteString(umbrellaThread(threads[ref]))
		}
	}

	sb.WriteString("\n## Your task\n")
	sb.WriteString(goalTail)
	return sb.String()
}

// buildUmbrellaPromptFor is the controller-side turn-0 bundle assembler for the
// project-scoped cross-repo kinds (clarify/implement/review). It resolves the SCM
// reader/merger + token from the project, runs a TTL-gated refreshUmbrellaMembers
// poll (persisting any change to Status.WorkItems), fetches each member's live
// comment thread, and renders the bundle via buildUmbrellaPrompt with goalTail as
// the "## Your task" section. No new durable store: the bundle is assembled live
// from SCM + the CR ledger at pod-build time, tatara-memory untouched. Degrades to
// returning goalTail unchanged when SCM is not wired or resolution fails, so a
// transient SCM error never blocks the run (the pod still gets its directive).
func (r *TaskReconciler) buildUmbrellaPromptFor(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, goalTail string) string {
	l := log.FromContext(ctx)
	if r.ReaderFor == nil {
		return goalTail
	}
	provider := ""
	if task.Spec.Source != nil {
		provider = task.Spec.Source.Provider
	}
	if provider == "" && project.Spec.Scm != nil {
		provider = project.Spec.Scm.Provider
	}
	token, err := r.scmToken(ctx, task.Namespace, project.Spec.ScmSecretRef)
	if err != nil {
		l.Info("umbrella: scm token unavailable; using bare directive (non-fatal)", "resource_id", task.Name, "err", err.Error())
		return goalTail
	}
	reader, err := r.ReaderFor(provider, token)
	if err != nil {
		l.Info("umbrella: reader unavailable; using bare directive (non-fatal)", "resource_id", task.Name, "err", err.Error())
		return goalTail
	}
	var merger umbrellaMergeStater
	if r.SCMFor != nil {
		if w, werr := r.SCMFor(provider); werr == nil {
			merger = w
		}
	}

	// Build the repos-in-scope list + slug->URL map from the project repos. For
	// umbrella kinds the effective scope is ALL enrolled project repos (U-B), so the
	// bundle's "Repos in scope" lists every repo with its checkout hint; for other
	// kinds it stays the ledger-derived scope. allSlugs bounds it to enrolled repos.
	repos, _ := r.projectRepos(ctx, project)
	allSlugs := make([]string, 0, len(repos))
	for i := range repos {
		if slug, serr := scm.RepoSlugFromURL(repos[i].Spec.URL); serr == nil {
			allSlugs = append(allSlugs, slug)
		}
	}
	scope := map[string]struct{}{}
	for _, s := range tatarav1alpha1.EffectiveReposInScope(task, allSlugs) {
		scope[s] = struct{}{}
	}
	var umRepos []umbrellaRepo
	slugToURL := map[string]string{}
	for i := range repos {
		slug, serr := scm.RepoSlugFromURL(repos[i].Spec.URL)
		if serr != nil {
			continue
		}
		if _, ok := scope[slug]; !ok {
			continue
		}
		slugToURL[slug] = repos[i].Spec.URL
		umRepos = append(umRepos, umbrellaRepo{
			Slug:          slug,
			Name:          repos[i].Name,
			URL:           repos[i].Spec.URL,
			DefaultBranch: repos[i].Spec.DefaultBranch,
		})
	}

	// Light poll: refresh member structured state, persist if anything changed.
	if refreshUmbrellaMembers(ctx, reader, merger, slugToURL, token, task, umbrellaRefreshTTL, time.Now()) {
		latest := task.DeepCopy()
		if perr := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
			fresh.Status.WorkItems = latest.Status.WorkItems
			return true
		}); perr != nil {
			l.Info("umbrella: persist refreshed members failed (non-fatal)", "resource_id", task.Name, "err", perr.Error())
		}
	}

	// Fetch each member's live comment thread for the bundle.
	prLister, _ := reader.(scm.PRCommentLister)
	threads := map[string][]scm.IssueComment{}
	for _, wi := range task.Status.WorkItems {
		if wi.Number == 0 || wi.Repo == "" {
			continue
		}
		owner, name, _ := strings.Cut(wi.Repo, "/")
		ref := umbrellaMemberRef(wi)
		var cmts []scm.IssueComment
		var cerr error
		if wi.Kind == tatarav1alpha1.WorkItemPR && prLister != nil {
			cmts, cerr = prLister.ListPRComments(ctx, owner, name, wi.Number)
			r.recordSCM(provider, "list_pr_comments", cerr)
		} else {
			cmts, cerr = reader.ListIssueComments(ctx, owner, name, wi.Number)
			r.recordSCM(provider, "list_issue_comments", cerr)
		}
		if cerr != nil {
			l.Info("umbrella: thread fetch failed (non-fatal)", "resource_id", task.Name, "member", ref, "err", cerr.Error())
			continue
		}
		if len(cmts) > 0 {
			threads[ref] = cmts
		}
	}

	return buildUmbrellaPrompt(task, umRepos, threads, goalTail)
}

// clarifyGoalTail is the "## Your task" directive for a clarify umbrella pod: the
// decision instructions (the issue body + thread live in the bundle's ## Issues
// section, so the agent must NOT re-crawl SCM), the Triage lifecycle-phase block,
// and the clarify skills directive.
func clarifyGoalTail(task *tatarav1alpha1.Task) string {
	issueRef := ""
	if task.Spec.Source != nil {
		issueRef = task.Spec.Source.IssueRef
	}
	dir := fmt.Sprintf(
		"You are the tatara clarify agent for issue %s. The issue body and full conversation "+
			"thread are in the ## Issues section above (and any related MRs in ## Merge requests) - "+
			"this bundle is authoritative and complete; do NOT re-crawl SCM to rebuild history.\n\n"+
			"Decide the outcome by interpreting the human's intent in the thread:\n"+
			"  - A human approval / go-ahead -> action=implement.\n"+
			"  - A human decline, or duplicate / out-of-scope / not-actionable -> action=close (reason as `comment`).\n"+
			"  - Still under discussion or needing the human -> action=discuss (questions as `comment`).\n"+
			"If THIS issue was opened by tatara itself, emit action=implement ONLY if a human has posted an "+
			"approval comment; if none yet, emit action=discuss with comment=\"\" (empty) - the operator posts nothing.\n"+
			"Call the `issue_outcome` MCP tool with your chosen action (REQUIRED before finishing). "+
			"Do not open PRs or make code changes in this turn.",
		issueRef)
	tail := dir + lifecyclePhaseGuidance("Triage")
	if d := skillsDirective("clarify"); d != "" {
		tail += "\n\n" + d
	}
	return tail
}

func umbrellaState(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

func umbrellaField(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func umbrellaLabels(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	return " labels: " + strings.Join(labels, ", ")
}

func umbrellaBody(body string) string {
	if body == "" {
		return "(no description)\n"
	}
	return body + "\n"
}

// umbrellaThread renders a member's comment thread, char-budgeted from the front
// (oldest) so the newest context survives when a thread is long. Returns "" for an
// empty thread.
func umbrellaThread(comments []scm.IssueComment) string {
	if len(comments) == 0 {
		return ""
	}
	if len(comments) > triageCommentCap {
		comments = comments[len(comments)-triageCommentCap:]
	}
	var sb strings.Builder
	sb.WriteString("#### Thread\n")
	for _, c := range comments {
		fmt.Fprintf(&sb, "**%s**: %s\n", c.Author, c.Body)
	}
	thread := sb.String()
	if len(thread) > triageCommentCharBudget {
		thread = thread[len(thread)-triageCommentCharBudget:]
	}
	return thread
}
