package controller

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// linkedIssueNumber is a package-local alias for scm.LinkedIssueNumber so
// the mrScan call sites remain readable without a long qualifier.
func linkedIssueNumber(body string) (int, bool) {
	return scm.LinkedIssueNumber(body)
}

// isLifecycleTerminal reports whether a lifecycle state counts as terminal for
// dedup purposes (Done/Stopped/Parked free the (repo,number) key on newer activity).
func isLifecycleTerminal(state string) bool {
	switch state {
	case "Done", "Stopped", "Parked":
		return true
	}
	return false
}

// activityNextFire parses a 5-field cron and returns the next fire after base.
// ok=false when the schedule is empty (disabled) or malformed (caller logs).
func activityNextFire(schedule string, base time.Time) (time.Time, bool) {
	if schedule == "" {
		return time.Time{}, false
	}
	parsed, err := cron.ParseStandard(schedule)
	if err != nil {
		return time.Time{}, false
	}
	return parsed.Next(base), true
}

// label key aliases for readability within this package.
const (
	labelSourceRepo   = tatarav1alpha1.LabelSourceRepo
	labelSourceNumber = tatarav1alpha1.LabelSourceNumber
	labelSourceKind   = tatarav1alpha1.LabelSourceKind
	labelHeadSHA      = tatarav1alpha1.LabelHeadSHA
	labelActivity     = tatarav1alpha1.LabelActivity
)

// sanitizeRepoLabel makes a repo slug DNS-label-safe by replacing '/' with '.'.
func sanitizeRepoLabel(repo string) string {
	return strings.ReplaceAll(repo, "/", ".")
}

// scanTaskLabels builds the operator-stamped dedup labels for a cron Task.
// head-sha is omitted for non-PR candidates.
func scanTaskLabels(c candidate, activity, kind string) map[string]string {
	l := map[string]string{
		labelSourceRepo:   sanitizeRepoLabel(c.repo),
		labelSourceNumber: strconv.Itoa(c.number),
		labelSourceKind:   kind,
		labelActivity:     activity,
	}
	if c.headSHA != "" {
		l[labelHeadSHA] = c.headSHA
	}
	return l
}

// findConvTaskToReactivate returns the first Conversation or Stopped lifecycle
// Task for the candidate whose LastActivityAt is strictly older than the
// candidate's updatedAt (meaning a new comment arrived that we missed). When
// such a task exists the caller should reactivate it to Triage rather than
// creating a duplicate Task. Returns nil when no reactivation is warranted.
func findConvTaskToReactivate(c candidate, existing []tatarav1alpha1.Task) *tatarav1alpha1.Task {
	if c.isPR {
		return nil
	}
	repoLabel := sanitizeRepoLabel(c.repo)
	numLabel := strconv.Itoa(c.number)
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelSourceRepo] != repoLabel || t.Labels[labelSourceNumber] != numLabel {
			continue
		}
		state := t.Status.LifecycleState
		if state != "Conversation" && state != "Stopped" {
			continue
		}
		if t.Status.LastActivityAt == nil {
			continue
		}
		if c.updatedAt.After(t.Status.LastActivityAt.Time) {
			return t
		}
	}
	return nil
}

// isDeduped reports whether a candidate already has a Task that should suppress
// a re-pick. Phase labels are the issue's state-of-truth (Option A):
//   - any non-terminal Task for (repo,number) -> skip (fast path)
//   - PR: a terminal Task at the same head-sha -> skip
//   - issue: a managed phase label present on the OPEN issue -> skip (active =>
//     handled by the live Task above; terminal+label => orphan the backstop
//     resumes; declined => no action). No managed label -> legacy/untracked, fall
//     back to activity-vs-creation so a stale terminal Task is not re-triaged
//     unless the issue saw new activity.
func isDeduped(c candidate, existing []tatarav1alpha1.Task, managed []string) bool {
	repoLabel := sanitizeRepoLabel(c.repo)
	numLabel := strconv.Itoa(c.number)
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelSourceRepo] != repoLabel || t.Labels[labelSourceNumber] != numLabel {
			continue
		}
		lifecycleTerminal := t.Status.LifecycleState != "" && isLifecycleTerminal(t.Status.LifecycleState)
		if !isTerminal(t.Status.Phase) && !lifecycleTerminal {
			return true
		}
		if c.isPR {
			if t.Labels[labelHeadSHA] == c.headSHA && c.headSHA != "" {
				return true
			}
			continue
		}
		// issue: phase label is state-of-truth.
		if hasAnyLabel(c.labels, managed) {
			return true
		}
		if !c.updatedAt.After(t.CreationTimestamp.Time) {
			return true
		}
	}
	return false
}

// candidate is one scannable work item (PR, issue, or board item) normalized
// for selection + dedup. number/repo identify it; labels drive priority;
// updatedAt drives stale-first ordering. body is used for PR "Closes #N" parsing.
type candidate struct {
	repo      string
	number    int
	author    string
	headSHA   string
	body      string
	labels    []string
	updatedAt time.Time
	isPR      bool
}

func hasLabel(labels []string, want string) bool {
	if want == "" {
		return false
	}
	for _, l := range labels {
		if l == want {
			return true
		}
	}
	return false
}

func hasAnyLabel(labels, want []string) bool {
	for _, w := range want {
		if hasLabel(labels, w) {
			return true
		}
	}
	return false
}

// selectCandidates partitions into priority-labelled and rest, sorts each
// least-recently-updated first, concatenates priority++rest, and caps at n.
// When priorityLabel is empty, selection is pure stale-first over all
// candidates (labels ignored), per design section 7.
func selectCandidates(in []candidate, priorityLabel string, n int) []candidate {
	if n < 1 {
		n = 1
	}
	staleFirst := func(s []candidate) {
		sort.SliceStable(s, func(i, j int) bool { return s[i].updatedAt.Before(s[j].updatedAt) })
	}

	if priorityLabel != "" {
		var withPriority, rest []candidate
		for _, c := range in {
			if hasLabel(c.labels, priorityLabel) {
				withPriority = append(withPriority, c)
			} else {
				rest = append(rest, c)
			}
		}
		staleFirst(withPriority)
		staleFirst(rest)
		out := append(withPriority, rest...)
		if len(out) > n {
			out = out[:n]
		}
		return out
	}

	// No priority label: pure stale-first over the whole slice (labels ignored).
	out := append([]candidate(nil), in...)
	staleFirst(out)
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// laneOccupancy counts this Project's scan Tasks for repoSlug that still occupy
// the repo's lane: Kind in kinds, phase not terminal and not AwaitingApproval
// (an awaiting-approval proposal frees the lane for the next item).
func laneOccupancy(existing []tatarav1alpha1.Task, repoSlug string, kinds ...string) int {
	label := sanitizeRepoLabel(repoSlug)
	n := 0
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelSourceRepo] != label || !slices.Contains(kinds, t.Spec.Kind) {
			continue
		}
		switch t.Status.Phase {
		case "Succeeded", "Failed", "AwaitingApproval":
			continue
		}
		n++
	}
	return n
}

// taskOpen reports whether a Task counts as non-terminal ("open") for the
// MaxOpenTasks creation budget. Terminal = Succeeded/Failed phase, or a terminal
// lifecycle state (Done/Stopped/Parked). Conversation is excluded too: a task
// blocked waiting for human input is externally gated and must not starve
// autonomous creation (brainstorm, issueScan, mrScan).
func taskOpen(t *tatarav1alpha1.Task) bool {
	if isTerminal(t.Status.Phase) {
		return false
	}
	switch t.Status.LifecycleState {
	case "Done", "Stopped", "Parked", "Conversation":
		return false
	}
	return true
}

// openTaskCount counts non-terminal Tasks in the snapshot.
func openTaskCount(existing []tatarav1alpha1.Task) int {
	n := 0
	for i := range existing {
		if taskOpen(&existing[i]) {
			n++
		}
	}
	return n
}

// maxOpenTasks returns the Project's open-task cap (default 3).
func maxOpenTasks(proj *tatarav1alpha1.Project) int {
	if proj.Spec.MaxOpenTasks > 0 {
		return proj.Spec.MaxOpenTasks
	}
	return 3
}

// selectPerRepo groups eligible candidates by repo and picks, per repo, the best
// (priority-then-stale) items up to maxPerRepo minus that repo's lane occupancy.
func selectPerRepo(eligible []candidate, priorityLabel string, maxPerRepo int, occ func(repoSlug string) int) []candidate {
	if maxPerRepo < 1 {
		maxPerRepo = 1
	}
	byRepo := map[string][]candidate{}
	var order []string
	for _, c := range eligible {
		if _, ok := byRepo[c.repo]; !ok {
			order = append(order, c.repo)
		}
		byRepo[c.repo] = append(byRepo[c.repo], c)
	}
	var out []candidate
	for _, slug := range order {
		n := maxPerRepo - occ(slug)
		if n < 1 {
			continue
		}
		out = append(out, selectCandidates(byRepo[slug], priorityLabel, n)...)
	}
	return out
}

func candidatesFromPRs(prs []scm.PRRef) []candidate {
	out := make([]candidate, 0, len(prs))
	for _, p := range prs {
		out = append(out, candidate{
			repo: p.Repo, number: p.Number, author: p.Author, headSHA: p.HeadSHA,
			body: p.Body, labels: p.Labels, updatedAt: p.UpdatedAt, isPR: true,
		})
	}
	return out
}

// candidatesFromIssues drops rows GitHub reported as PRs (IsPR) so issueScan
// never triages a PR as an issue.
func candidatesFromIssues(iss []scm.IssueRef) []candidate {
	out := make([]candidate, 0, len(iss))
	for _, i := range iss {
		if i.IsPR {
			continue
		}
		out = append(out, candidate{
			repo: i.Repo, number: i.Number, labels: i.Labels, updatedAt: i.UpdatedAt, isPR: false,
		})
	}
	return out
}

// candidatesFromBoard maps board items (issues only; Number 0 = draft, skipped)
// to candidates; deduping against per-repo issues happens in the caller via
// (repo, number).
func candidatesFromBoard(items []scm.BoardItem) []candidate {
	out := make([]candidate, 0, len(items))
	for _, b := range items {
		if b.Number == 0 {
			continue
		}
		out = append(out, candidate{repo: b.Repo, number: b.Number, updatedAt: b.UpdatedAt, isPR: false})
	}
	return out
}

const annBrainstormSources = "tatara.dev/brainstorm-sources"

// createScanTask creates one cron Task for a candidate with the dedup labels,
// a TaskSource pointing at the work item, and an owner-ref to the Project.
//
// labelCand drives the dedup labels (source-repo, source-number). srcCand
// drives the TaskSource (provider, issueRef, number, isPR, authorLogin). For
// most callers they are the same candidate. For bot-PR MRCI entries they
// differ: labelCand carries the linked-issue number (dedup key) while srcCand
// carries the PR identity (number, IsPR=true).
//
// extraAnnotations, if non-nil, is set on the Task at create time
// (e.g. tatara.dev/lifecycle-entry for issueLifecycle tasks).
func (r *ProjectReconciler) createScanTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, labelCand, srcCand candidate, activity, kind, goal string, extraAnnotations map[string]string) (*tatarav1alpha1.Task, error) {
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	// GitLab MRs use the '!' separator and a distinct notes endpoint; issues use
	// '#'. GitHub shares /issues/{n}/comments for both, so it stays on '#'. The
	// ref must be faithful here so write-back/lifecycle comments land on the MR
	// rather than an unrelated issue with the same iid.
	sep := "#"
	if srcCand.isPR && provider == "gitlab" {
		sep = "!"
	}
	src := &tatarav1alpha1.TaskSource{
		Provider: provider,
		IssueRef: fmt.Sprintf("%s%s%d", srcCand.repo, sep, srcCand.number),
		Number:   srcCand.number,
		IsPR:     srcCand.isPR,
	}
	if srcCand.author != "" {
		src.AuthorLogin = srcCand.author
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "scan-",
			Namespace:    proj.Namespace,
			Labels:       scanTaskLabels(labelCand, activity, kind),
			Annotations:  extraAnnotations,
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Goal:          goal,
			Kind:          kind,
			Source:        src,
		},
	}
	agent.StampPodName(task, proj.Name, provider, repo.Name)
	if err := controllerutil.SetControllerReference(proj, task, r.Scheme); err != nil {
		return nil, fmt.Errorf("scan: set ownerref: %w", err)
	}
	if err := r.Create(ctx, task); err != nil {
		return nil, fmt.Errorf("scan: create task: %w", err)
	}
	r.Metrics.ScanTaskCreated(activity, kind)
	return task, nil
}

// createBrainstormTask creates a Kind=brainstorm Task. sources is recorded as a
// comma-joined annotation the pod builder reads to decide the egress label.
func (r *ProjectReconciler) createBrainstormTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, goal string, sources []string) (*tatarav1alpha1.Task, error) {
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "brainstorm-",
			Namespace:    proj.Namespace,
			Labels:       map[string]string{labelActivity: "brainstorm"},
			Annotations:  map[string]string{annBrainstormSources: strings.Join(sources, ",")},
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Goal:          goal,
			Kind:          "brainstorm",
		},
	}
	agent.StampPodName(task, proj.Name, provider, repo.Name)
	if err := controllerutil.SetControllerReference(proj, task, r.Scheme); err != nil {
		return nil, fmt.Errorf("scan: set ownerref: %w", err)
	}
	if err := r.Create(ctx, task); err != nil {
		return nil, fmt.Errorf("scan: create brainstorm task: %w", err)
	}
	r.Metrics.ScanTaskCreated("brainstorm", "brainstorm")
	return task, nil
}

// scanReader resolves the token-bound SCMReader for the Project's provider.
func (r *ProjectReconciler) scanReader(ctx context.Context, proj *tatarav1alpha1.Project) (scm.SCMReader, error) {
	if r.ReaderFor == nil {
		return nil, fmt.Errorf("scan: ReaderFor not wired")
	}
	var sec corev1.Secret
	key := types.NamespacedName{Namespace: proj.Namespace, Name: proj.Spec.ScmSecretRef}
	if err := r.Get(ctx, key, &sec); err != nil {
		return nil, fmt.Errorf("scan: get scm secret: %w", err)
	}
	token := string(sec.Data["token"])
	return r.ReaderFor(proj.Spec.Scm.Provider, token)
}

// matchRepoForSlug returns the Project Repository whose URL maps to the given
// owner/name slug, or ok=false.
func (r *ProjectReconciler) matchRepoForSlug(repos []tatarav1alpha1.Repository, slug string) (tatarav1alpha1.Repository, bool) {
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		if owner+"/"+name == slug {
			return repos[i], true
		}
	}
	return tatarav1alpha1.Repository{}, false
}

// projectReposForScan returns all Repositories owned by the Project.
func (r *ProjectReconciler) projectReposForScan(ctx context.Context, proj *tatarav1alpha1.Project) ([]tatarav1alpha1.Repository, error) {
	var list tatarav1alpha1.RepositoryList
	if err := r.List(ctx, &list, client.InNamespace(proj.Namespace)); err != nil {
		return nil, fmt.Errorf("scan: list repositories: %w", err)
	}
	var out []tatarav1alpha1.Repository
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == proj.Name {
			out = append(out, list.Items[i])
		}
	}
	return out, nil
}

// existingScanTasks lists Project-owned Tasks carrying the dedup activity label.
func (r *ProjectReconciler) existingScanTasks(ctx context.Context, proj *tatarav1alpha1.Project) ([]tatarav1alpha1.Task, error) {
	var list tatarav1alpha1.TaskList
	if err := r.List(ctx, &list, client.InNamespace(proj.Namespace)); err != nil {
		return nil, fmt.Errorf("scan: list tasks: %w", err)
	}
	var out []tatarav1alpha1.Task
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == proj.Name && list.Items[i].Labels[labelActivity] != "" {
			out = append(out, list.Items[i])
		}
	}
	return out, nil
}

// activityDue computes (base, due, next, ok) for one activity. base is
// Last*Scan|creationTimestamp; ok=false on empty/bad cron.
func (r *ProjectReconciler) activityDue(proj *tatarav1alpha1.Project, activity string) (time.Time, bool, time.Time, bool) {
	schedule := ""
	var last *metav1.Time
	switch activity {
	case "mrScan":
		schedule = proj.Spec.Scm.Cron.MRScan.Schedule
		last = proj.Status.LastMRScan
	case "issueScan":
		schedule = proj.Spec.Scm.Cron.IssueScan.Schedule
		last = proj.Status.LastIssueScan
	case "brainstorm":
		schedule = proj.Spec.Scm.Cron.Brainstorm.Schedule
		last = proj.Status.LastBrainstorm
	}
	base := proj.CreationTimestamp.Time
	if last != nil {
		base = last.Time
	}
	next, ok := activityNextFire(schedule, base)
	if !ok {
		return base, false, time.Time{}, false
	}
	return base, !time.Now().Before(next), next, true
}

// stampScan records the per-activity Last*Scan and persists status.
// RetryOnConflict handles racing reconcile updates so the stamp always lands.
func (r *ProjectReconciler) stampScan(ctx context.Context, proj *tatarav1alpha1.Project, activity string) {
	now := metav1.Now()
	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Project{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}, fresh); err != nil {
			return err
		}
		switch activity {
		case "mrScan":
			fresh.Status.LastMRScan = &now
			proj.Status.LastMRScan = &now
		case "issueScan":
			fresh.Status.LastIssueScan = &now
			proj.Status.LastIssueScan = &now
		case "brainstorm":
			fresh.Status.LastBrainstorm = &now
			proj.Status.LastBrainstorm = &now
		}
		return r.Status().Update(ctx, fresh)
	})
}

// mrScan lists open PRs across repos, selects, dedups, and creates Tasks routed
// by authoritative author -> review (human) | issueLifecycle/MRCI (bot).
// budget is the shared open-task creation budget; mrScan decrements it on each
// successful create and stops creating when budget reaches zero.
func (r *ProjectReconciler) mrScan(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.CronActivity, budget *int) bool {
	l := log.FromContext(ctx)
	start := time.Now()
	bot := ""
	if proj.Spec.Scm != nil {
		bot = proj.Spec.Scm.BotLogin
	}
	var cands []candidate
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		prs, err := reader.ListOpenPRs(ctx, owner, name)
		if err != nil {
			l.Error(err, "scan: ListOpenPRs", "action", "scan_list_error", "resource_id", proj.Name, "activity", "mrScan", "repo", repos[i].Name)
			continue
		}
		cands = append(cands, candidatesFromPRs(prs)...)
	}
	for range cands {
		r.Metrics.ScanItem("mrScan", "scanned")
	}
	// Dedup BEFORE cap so a stale-but-in-flight item does not waste the cap slot.
	managed := managedPhaseLabels(proj.Spec.Scm)
	var eligible []candidate
	for _, c := range cands {
		if isDeduped(c, existing, managed) {
			r.Metrics.ScanItem("mrScan", "skipped_dedup")
		} else {
			eligible = append(eligible, c)
		}
	}
	// kind switch for bot PRs: was "selfImprove", now "issueLifecycle" (MRCI entry);
	// migration note: in-flight "selfImprove" tasks created before this deploy still complete
	// via old writeback arm.
	selected := selectPerRepo(eligible, proj.Spec.Scm.PriorityLabel, act.MaxPerRepo,
		func(slug string) int { return laneOccupancy(existing, slug, "issueLifecycle", "review") })
	for i := 0; i < len(eligible)-len(selected); i++ {
		r.Metrics.ScanItem("mrScan", "skipped_cap")
	}
	created := 0
	for _, c := range selected {
		if *budget <= 0 {
			break
		}
		repo, ok := r.matchRepoForSlug(repos, c.repo)
		if !ok {
			continue
		}
		if c.author == bot && bot != "" {
			// Bot PR -> issueLifecycle entering at MRCI. Dedup key is the linked issue
			// number from "Closes #N" if present, else the PR number.
			dedupNumber := c.number
			if issueNum, linked := linkedIssueNumber(c.body); linked {
				dedupNumber = issueNum
			}
			// labelCand carries the dedup key (issue number when present).
			labelCand := candidate{
				repo: c.repo, number: dedupNumber, headSHA: c.headSHA,
				labels: c.labels, updatedAt: c.updatedAt, isPR: c.isPR,
			}
			// srcCand carries the actual PR identity (PR number, IsPR=true).
			srcCand := candidate{
				repo: c.repo, number: c.number, author: c.author, isPR: true,
			}
			goal := fmt.Sprintf("Review issueLifecycle PR %s#%d", c.repo, c.number)
			ann := map[string]string{tatarav1alpha1.LifecycleEntryAnnotation: "MRCI"}
			if _, err := r.createScanTask(ctx, proj, &repo, labelCand, srcCand, "mrScan", "issueLifecycle", goal, ann); err != nil {
				l.Error(err, "scan: create mrScan issueLifecycle task", "resource_id", proj.Name, "repo", repo.Name)
				continue
			}
		} else {
			goal := fmt.Sprintf("Triage review PR %s#%d", c.repo, c.number)
			if _, err := r.createScanTask(ctx, proj, &repo, c, c, "mrScan", "review", goal, nil); err != nil {
				l.Error(err, "scan: create mrScan task", "resource_id", proj.Name, "repo", repo.Name)
				continue
			}
		}
		r.Metrics.ScanItem("mrScan", "picked")
		created++
		*budget--
	}
	r.Metrics.ObserveScanDuration("mrScan", time.Since(start).Seconds())
	l.Info("mrScan complete", "action", "scan_mr", "resource_id", proj.Name,
		"listed", len(cands), "picked", created, "duration_ms", time.Since(start).Milliseconds())
	return len(selected) < len(eligible)
}

// issueScan lists open issues (per-repo + board) and creates triageIssue Tasks.
// budget is the shared open-task creation budget; issueScan decrements it on each
// successful create and stops creating when budget reaches zero.
func (r *ProjectReconciler) issueScan(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.CronActivity, budget *int) bool {
	l := log.FromContext(ctx)
	start := time.Now()
	seen := map[string]bool{}
	var cands []candidate
	addUnique := func(cs []candidate) {
		for _, c := range cs {
			key := fmt.Sprintf("%s#%d", c.repo, c.number)
			if seen[key] {
				continue
			}
			seen[key] = true
			cands = append(cands, c)
		}
	}
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		iss, err := reader.ListOpenIssues(ctx, owner, name)
		if err != nil {
			l.Error(err, "scan: ListOpenIssues", "action", "scan_list_error", "resource_id", proj.Name, "activity", "issueScan", "repo", repos[i].Name)
			continue
		}
		addUnique(candidatesFromIssues(iss))
	}
	if proj.Spec.Scm.Board != nil {
		board := boardRefFromSpec(proj.Spec.Scm)
		items, err := reader.ListBoardItems(ctx, board)
		if err != nil {
			l.Error(err, "scan: ListBoardItems", "action", "scan_list_error", "resource_id", proj.Name, "activity", "issueScan")
		} else {
			addUnique(candidatesFromBoard(items))
		}
	}
	for range cands {
		r.Metrics.ScanItem("issueScan", "scanned")
	}
	// Reactivation pass: when an issue was updated after the bound lifecycle
	// Task's LastActivityAt (missed webhook), reset the Task to Triage instead
	// of creating a duplicate. This runs before dedup so the reactivated task
	// absorbs the candidate and the dedup check below skips it normally.
	for _, c := range cands {
		task := findConvTaskToReactivate(c, existing)
		if task == nil {
			continue
		}
		now := metav1.Now()
		idleMinutes := 60
		if proj.Spec.Scm != nil && proj.Spec.Scm.ConversationIdleMinutes > 0 {
			idleMinutes = proj.Spec.Scm.ConversationIdleMinutes
		}
		deadline := metav1.NewTime(now.Add(time.Duration(idleMinutes) * time.Minute))
		err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			if fetchErr := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, task); fetchErr != nil {
				return fetchErr
			}
			task.Status.LifecycleState = "Triage"
			task.Status.LastActivityAt = &now
			task.Status.DeadlineAt = &deadline
			return r.Status().Update(ctx, task)
		})
		if err != nil {
			l.Error(err, "issueScan: reactivate conversation task", "action", "reactivate_conv", "resource_id", task.Name)
			continue
		}
		l.Info("issueScan: reactivated conversation task", "action", "reactivate_conv", "resource_id", task.Name,
			"issue", fmt.Sprintf("%s#%d", c.repo, c.number))
		r.Metrics.ScanItem("issueScan", "reactivated")
	}

	// Dedup BEFORE cap so a stale-but-in-flight item does not waste the cap slot.
	managed := managedPhaseLabels(proj.Spec.Scm)
	var eligible []candidate
	for _, c := range cands {
		if isDeduped(c, existing, managed) {
			r.Metrics.ScanItem("issueScan", "skipped_dedup")
		} else {
			eligible = append(eligible, c)
		}
	}
	// kind switch: was "triageIssue", now "issueLifecycle"; migration note: in-flight
	// "triageIssue" tasks created before this deploy still complete via old writeback arm.
	selected := selectPerRepo(eligible, proj.Spec.Scm.PriorityLabel, act.MaxPerRepo,
		func(slug string) int { return laneOccupancy(existing, slug, "issueLifecycle") })
	for i := 0; i < len(eligible)-len(selected); i++ {
		r.Metrics.ScanItem("issueScan", "skipped_cap")
	}
	created := 0
	for _, c := range selected {
		if *budget <= 0 {
			break
		}
		repo, ok := r.matchRepoForSlug(repos, c.repo)
		if !ok {
			continue
		}
		goal := fmt.Sprintf("Triage issue %s#%d", c.repo, c.number)
		if _, err := r.createScanTask(ctx, proj, &repo, c, c, "issueScan", "issueLifecycle", goal, nil); err != nil {
			l.Error(err, "scan: create issueScan task", "resource_id", proj.Name, "repo", repo.Name)
			continue
		}
		r.Metrics.ScanItem("issueScan", "picked")
		created++
		*budget--
	}
	r.Metrics.ObserveScanDuration("issueScan", time.Since(start).Seconds())
	l.Info("issueScan complete", "action", "scan_issue", "resource_id", proj.Name,
		"listed", len(cands), "picked", created, "duration_ms", time.Since(start).Milliseconds())
	return len(selected) < len(eligible)
}

// brainstorm runs one brainstorm cycle: per-repo, backpressured by live open
// proposal count. Replaces the old MaxPerCycle body; MaxPerCycle field is
// retained for API compat but unused.
// budget is the shared open-task creation budget; brainstorm decrements it on
// each successful create and stops creating when budget reaches zero.
func (r *ProjectReconciler) brainstorm(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.BrainstormActivity, budget *int) {
	l := log.FromContext(ctx)
	start := time.Now()
	maxProp := act.MaxOpenProposals
	if maxProp < 1 {
		maxProp = 3
	}
	brainstormingLabel, _, _, _ := lifecycleLabels(proj.Spec.Scm)
	created := 0
	for i := range repos {
		repo := repos[i]
		slug := repoSlug(&repo)
		if slug == "" {
			continue
		}
		if brainstormInFlight(existing, repo.Name) {
			r.Metrics.ScanItem("brainstorm", "skipped_inflight")
			continue
		}
		backlog, err := r.proposalBacklog(ctx, reader, &repo, brainstormingLabel, proj.Spec.Scm, existing)
		if err != nil {
			l.Info("brainstorm: backlog count failed (non-fatal)", "resource_id", proj.Name, "repo", repo.Name, "err", err.Error())
			continue
		}
		r.Metrics.SetOpenProposals(slug, float64(backlog))
		if backlog >= maxProp {
			r.Metrics.ScanItem("brainstorm", "skipped_cap")
			continue
		}
		if *budget <= 0 {
			break
		}
		goal := brainstormGoal(slug)
		if _, err := r.createBrainstormTask(ctx, proj, &repo, goal, act.Sources); err != nil {
			l.Error(err, "scan: create brainstorm task", "resource_id", proj.Name, "repo", repo.Name)
			continue
		}
		r.Metrics.ScanItem("brainstorm", "picked")
		created++
		*budget--
	}
	r.Metrics.ObserveScanDuration("brainstorm", time.Since(start).Seconds())
	l.Info("brainstorm complete", "action", "scan_brainstorm", "resource_id", proj.Name,
		"picked", created, "duration_ms", time.Since(start).Milliseconds())
}

// brainstormGoal returns the turn-0 goal text for a brainstorm task, directing
// the agent to invoke the tatara-deep-research skill and file a single issue.
func brainstormGoal(slug string) string {
	return "Invoke the `tatara-deep-research` skill to research the platform deeply and propose a single, " +
		"well-defined discovery issue for repo " + slug + ". The skill defines how to research via the " +
		"tatara-memory graph and on-disk code, score leverage, dedup, and file exactly one issue via propose_issue."
}

// repoSlug returns "owner/name" for a Repository URL, or "" on error.
func repoSlug(repo *tatarav1alpha1.Repository) string {
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return ""
	}
	return owner + "/" + name
}

// brainstormInFlight reports whether a non-terminal brainstorm Task targets repoName.
func brainstormInFlight(existing []tatarav1alpha1.Task, repoName string) bool {
	for i := range existing {
		t := existing[i]
		if t.Labels[labelActivity] == "brainstorm" && t.Spec.RepositoryRef == repoName && !isTerminal(t.Status.Phase) {
			return true
		}
	}
	return false
}

// proposalBacklog counts open, undecided ideas for repo: open non-PR issues
// bearing the idea label (live ListOpenIssues). This subsumes tatara-originated
// proposals and any human-filed issue parked as an idea, providing conservative
// brainstorm backpressure.
func (r *ProjectReconciler) proposalBacklog(ctx context.Context, reader scm.SCMReader, repo *tatarav1alpha1.Repository, brainstormingLabel string, scmSpec *tatarav1alpha1.ScmSpec, _ []tatarav1alpha1.Task) (int, error) {
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return 0, err
	}
	issues, err := reader.ListOpenIssues(ctx, owner, name)
	if err != nil {
		return 0, err
	}
	legacyIdea, _ := legacyLabels(scmSpec)
	n := 0
	for _, iss := range issues {
		if !iss.IsPR && (hasLabel(iss.Labels, brainstormingLabel) || hasLabel(iss.Labels, legacyIdea)) {
			n++
		}
	}
	return n, nil
}

// hasNonTerminalTaskForIssue reports whether any open (non-terminal) Task exists
// for (slug, number) in the snapshot.
func hasNonTerminalTaskForIssue(existing []tatarav1alpha1.Task, slug string, number int) bool {
	repoLabel := sanitizeRepoLabel(slug)
	numLabel := strconv.Itoa(number)
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelSourceRepo] != repoLabel || t.Labels[labelSourceNumber] != numLabel {
			continue
		}
		if taskOpen(t) {
			return true
		}
	}
	return false
}

// recoverOrphans starts the correct lifecycle Task for each OPEN issue that
// carries an active phase label but has no live Task (a missed/never-started or
// stalled handler). It RE-LISTS existing Tasks so it sees Tasks mrScan/issueScan
// created earlier this cycle (an open bot MR becomes a live MRCI Task -> not an
// orphan). Bounded by the shared open-task budget.
func (r *ProjectReconciler) recoverOrphans(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, budget *int) {
	if *budget <= 0 {
		return
	}
	l := log.FromContext(ctx)
	existing, err := r.existingScanTasks(ctx, proj)
	if err != nil {
		l.Error(err, "backstop: list tasks", "action", "backstop_list_error", "resource_id", proj.Name)
		return
	}
	brainstorming, approved, implementation, _ := lifecycleLabels(proj.Spec.Scm)
	legacyIdea, _ := legacyLabels(proj.Spec.Scm)
	for i := range repos {
		owner, name, oerr := scm.OwnerRepo(repos[i].Spec.URL)
		if oerr != nil {
			continue
		}
		issues, lerr := reader.ListOpenIssues(ctx, owner, name)
		if lerr != nil {
			l.Error(lerr, "backstop: ListOpenIssues", "action", "backstop_list_error", "resource_id", proj.Name, "repo", repos[i].Name)
			continue
		}
		slug := owner + "/" + name
		for _, iss := range issues {
			if iss.IsPR {
				continue
			}
			var entry, goal string
			switch {
			case hasLabel(iss.Labels, implementation):
				entry = "Implement"
				goal = fmt.Sprintf("Resume implementation for %s#%d (phase label present, no live task)", slug, iss.Number)
			case hasLabel(iss.Labels, approved):
				entry = "Implement"
				goal = fmt.Sprintf("Implement approved issue %s#%d", slug, iss.Number)
			case hasLabel(iss.Labels, brainstorming) || hasLabel(iss.Labels, legacyIdea):
				entry = "Triage"
				goal = fmt.Sprintf("Triage issue %s#%d", slug, iss.Number)
			default:
				continue
			}
			if hasNonTerminalTaskForIssue(existing, slug, iss.Number) {
				continue
			}
			if *budget <= 0 {
				return
			}
			repo, ok := r.matchRepoForSlug(repos, slug)
			if !ok {
				continue
			}
			cand := candidate{repo: slug, number: iss.Number, labels: iss.Labels, updatedAt: iss.UpdatedAt}
			ann := map[string]string{tatarav1alpha1.LifecycleEntryAnnotation: entry}
			if _, cerr := r.createScanTask(ctx, proj, &repo, cand, cand, "backstop", "issueLifecycle", goal, ann); cerr != nil {
				l.Error(cerr, "backstop: create recovery task", "action", "backstop_create_error", "resource_id", proj.Name, "repo", repo.Name)
				continue
			}
			l.Info("backstop: recovered orphaned issue", "action", "backstop_recover",
				"resource_id", proj.Name, "issue", fmt.Sprintf("%s#%d", slug, iss.Number), "entry", entry)
			r.Metrics.ScanItem("backstop", "recovered")
			*budget--
		}
	}
}

// runScans runs each due activity and returns the soonest next-fire as a
// requeue duration. Cron parsing/SCM/create failures are logged and skipped per
// activity so one bad activity never blocks the others or crashes the reconciler.
func (r *ProjectReconciler) runScans(ctx context.Context, proj *tatarav1alpha1.Project) (time.Duration, error) {
	l := log.FromContext(ctx)
	if proj.Spec.Scm == nil || proj.Spec.Scm.Cron == nil || r.ReaderFor == nil {
		return 0, nil
	}
	cronSpec := proj.Spec.Scm.Cron
	now := time.Now()
	soonest := time.Duration(0)
	consider := func(next time.Time) {
		d := next.Sub(now)
		if d < 0 {
			d = 0
		}
		if d > maxScheduleRequeue {
			d = maxScheduleRequeue
		}
		if soonest == 0 || d < soonest {
			soonest = d
		}
	}

	reader, rerr := r.scanReader(ctx, proj)
	if rerr != nil {
		l.Error(rerr, "scan: resolve reader", "action", "scan_reader_error", "resource_id", proj.Name)
		return maxScheduleRequeue, nil
	}
	repos, err := r.projectReposForScan(ctx, proj)
	if err != nil {
		return 0, err
	}
	existing, err := r.existingScanTasks(ctx, proj)
	if err != nil {
		return 0, err
	}

	// Compute creation budget: how many more open Tasks the operator may create.
	budget := maxOpenTasks(proj) - openTaskCount(existing)
	if budget < 0 {
		budget = 0
	}
	if budget == 0 {
		l.Info("scan: at open-task cap; skipping autonomous creation",
			"action", "scan_open_cap", "resource_id", proj.Name, "cap", maxOpenTasks(proj))
	}

	// mrScan
	if _, due, next, ok := r.activityDue(proj, "mrScan"); ok {
		if due {
			backlog := r.mrScan(ctx, proj, reader, repos, existing, cronSpec.MRScan, &budget)
			r.stampScan(ctx, proj, "mrScan")
			// Recompute next-fire from now so the post-stamp schedule produces a
			// positive RequeueAfter (the pre-fire next is in the past).
			if next2, ok2 := activityNextFire(cronSpec.MRScan.Schedule, now); ok2 {
				consider(next2)
			}
			if backlog {
				consider(now.Add(backlogRequeue))
			}
		} else {
			consider(next)
		}
	} else if cronSpec.MRScan.Schedule != "" {
		l.Error(fmt.Errorf("invalid cron %q", cronSpec.MRScan.Schedule), "scan: invalid mrScan cron, disabling",
			"action", "scan_cron_invalid", "resource_id", proj.Name, "activity", "mrScan")
	}

	// issueScan
	if _, due, next, ok := r.activityDue(proj, "issueScan"); ok {
		if due {
			backlog := r.issueScan(ctx, proj, reader, repos, existing, cronSpec.IssueScan, &budget)
			r.stampScan(ctx, proj, "issueScan")
			if budget > 0 {
				r.recoverOrphans(ctx, proj, reader, repos, &budget)
			}
			if next2, ok2 := activityNextFire(cronSpec.IssueScan.Schedule, now); ok2 {
				consider(next2)
			}
			if backlog {
				consider(now.Add(backlogRequeue))
			}
		} else {
			consider(next)
		}
	} else if cronSpec.IssueScan.Schedule != "" {
		l.Error(fmt.Errorf("invalid cron %q", cronSpec.IssueScan.Schedule), "scan: invalid issueScan cron, disabling",
			"action", "scan_cron_invalid", "resource_id", proj.Name, "activity", "issueScan")
	}

	// brainstorm (opt-in)
	if cronSpec.Brainstorm.Enabled {
		if _, due, next, ok := r.activityDue(proj, "brainstorm"); ok {
			if due {
				r.brainstorm(ctx, proj, reader, repos, existing, cronSpec.Brainstorm, &budget)
				r.stampScan(ctx, proj, "brainstorm")
				if next2, ok2 := activityNextFire(cronSpec.Brainstorm.Schedule, now); ok2 {
					consider(next2)
				}
			} else {
				consider(next)
			}
		} else if cronSpec.Brainstorm.Schedule != "" {
			l.Error(fmt.Errorf("invalid cron %q", cronSpec.Brainstorm.Schedule), "scan: invalid brainstorm cron, disabling",
				"action", "scan_cron_invalid", "resource_id", proj.Name, "activity", "brainstorm")
		}
	}

	return soonest, nil
}
