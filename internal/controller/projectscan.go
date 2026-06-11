package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

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

const (
	labelSourceRepo   = "tatara.io/source-repo"
	labelSourceNumber = "tatara.io/source-number"
	labelSourceKind   = "tatara.io/source-kind"
	labelHeadSHA      = "tatara.io/head-sha"
	labelActivity     = "tatara.io/activity"
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

// isDeduped reports whether a candidate already has a Task that should suppress
// a re-pick, per the dedup rules:
//   - any non-terminal Task for (repo, number) -> skip
//   - PR: a terminal Task at the same head-sha -> skip (already handled revision)
//   - issue: a terminal Task whose creation is at/after the candidate updatedAt -> skip
func isDeduped(c candidate, existing []tatarav1alpha1.Task) bool {
	repoLabel := sanitizeRepoLabel(c.repo)
	numLabel := strconv.Itoa(c.number)
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelSourceRepo] != repoLabel || t.Labels[labelSourceNumber] != numLabel {
			continue
		}
		if !isTerminal(t.Status.Phase) {
			return true
		}
		if c.isPR {
			if t.Labels[labelHeadSHA] == c.headSHA && c.headSHA != "" {
				return true
			}
			continue
		}
		// issue: terminal Task suppresses unless the issue saw newer activity.
		if !c.updatedAt.After(t.CreationTimestamp.Time) {
			return true
		}
	}
	return false
}

// candidate is one scannable work item (PR, issue, or board item) normalized
// for selection + dedup. number/repo identify it; labels drive priority;
// updatedAt drives stale-first ordering.
type candidate struct {
	repo      string
	number    int
	author    string
	headSHA   string
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

func candidatesFromPRs(prs []scm.PRRef) []candidate {
	out := make([]candidate, 0, len(prs))
	for _, p := range prs {
		out = append(out, candidate{
			repo: p.Repo, number: p.Number, author: p.Author, headSHA: p.HeadSHA,
			labels: p.Labels, updatedAt: p.UpdatedAt, isPR: true,
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
func (r *ProjectReconciler) createScanTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, c candidate, activity, kind, goal string) (*tatarav1alpha1.Task, error) {
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	src := &tatarav1alpha1.TaskSource{
		Provider: provider,
		IssueRef: fmt.Sprintf("%s#%d", c.repo, c.number),
		Number:   c.number,
		IsPR:     c.isPR,
	}
	if c.author != "" {
		src.AuthorLogin = c.author
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "scan-",
			Namespace:    proj.Namespace,
			Labels:       scanTaskLabels(c, activity, kind),
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Goal:          goal,
			Kind:          kind,
			Source:        src,
		},
	}
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
func (r *ProjectReconciler) stampScan(ctx context.Context, proj *tatarav1alpha1.Project, activity string) {
	now := metav1.Now()
	fresh := &tatarav1alpha1.Project{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}, fresh); err != nil {
		return
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
	_ = r.Status().Update(ctx, fresh)
}

// mrScan lists open PRs across repos, selects, dedups, and creates Tasks routed
// by authoritative author -> review (human) | selfImprove (bot).
func (r *ProjectReconciler) mrScan(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.CronActivity) {
	l := log.FromContext(ctx)
	start := time.Now()
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
	var eligible []candidate
	for _, c := range cands {
		if isDeduped(c, existing) {
			r.Metrics.ScanItem("mrScan", "skipped_dedup")
		} else {
			eligible = append(eligible, c)
		}
	}
	selected := selectCandidates(eligible, proj.Spec.Scm.PriorityLabel, act.MaxPerCycle)
	for i := 0; i < len(eligible)-len(selected); i++ {
		r.Metrics.ScanItem("mrScan", "skipped_cap")
	}
	created := 0
	for _, c := range selected {
		repo, ok := r.matchRepoForSlug(repos, c.repo)
		if !ok {
			continue
		}
		kind := "review"
		if c.author == proj.Spec.Scm.BotLogin {
			kind = "selfImprove"
		}
		goal := fmt.Sprintf("Triage %s PR %s#%d", kind, c.repo, c.number)
		if _, err := r.createScanTask(ctx, proj, &repo, c, "mrScan", kind, goal); err != nil {
			l.Error(err, "scan: create mrScan task", "resource_id", proj.Name, "repo", repo.Name)
			continue
		}
		r.Metrics.ScanItem("mrScan", "picked")
		created++
	}
	r.Metrics.ObserveScanDuration("mrScan", time.Since(start).Seconds())
	l.Info("mrScan complete", "action", "scan_mr", "resource_id", proj.Name,
		"listed", len(cands), "picked", created, "duration_ms", time.Since(start).Milliseconds())
}

// issueScan lists open issues (per-repo + board) and creates triageIssue Tasks.
func (r *ProjectReconciler) issueScan(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.CronActivity) {
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
	// Dedup BEFORE cap so a stale-but-in-flight item does not waste the cap slot.
	var eligible []candidate
	for _, c := range cands {
		if isDeduped(c, existing) {
			r.Metrics.ScanItem("issueScan", "skipped_dedup")
		} else {
			eligible = append(eligible, c)
		}
	}
	selected := selectCandidates(eligible, proj.Spec.Scm.PriorityLabel, act.MaxPerCycle)
	for i := 0; i < len(eligible)-len(selected); i++ {
		r.Metrics.ScanItem("issueScan", "skipped_cap")
	}
	created := 0
	for _, c := range selected {
		repo, ok := r.matchRepoForSlug(repos, c.repo)
		if !ok {
			continue
		}
		goal := fmt.Sprintf("Triage issue %s#%d", c.repo, c.number)
		if _, err := r.createScanTask(ctx, proj, &repo, c, "issueScan", "triageIssue", goal); err != nil {
			l.Error(err, "scan: create issueScan task", "resource_id", proj.Name, "repo", repo.Name)
			continue
		}
		r.Metrics.ScanItem("issueScan", "picked")
		created++
	}
	r.Metrics.ObserveScanDuration("issueScan", time.Since(start).Seconds())
	l.Info("issueScan complete", "action", "scan_issue", "resource_id", proj.Name,
		"listed", len(cands), "picked", created, "duration_ms", time.Since(start).Milliseconds())
}

// brainstorm creates up to MaxPerCycle generative Tasks (no list).
func (r *ProjectReconciler) brainstorm(ctx context.Context, proj *tatarav1alpha1.Project, repos []tatarav1alpha1.Repository, act tatarav1alpha1.BrainstormActivity) {
	l := log.FromContext(ctx)
	start := time.Now()
	if len(repos) == 0 {
		return
	}
	n := act.MaxPerCycle
	if n < 1 {
		n = 1
	}
	created := 0
	for i := 0; i < n; i++ {
		goal := "Brainstorm new issues for project " + proj.Name
		if _, err := r.createBrainstormTask(ctx, proj, &repos[0], goal, act.Sources); err != nil {
			l.Error(err, "scan: create brainstorm task", "resource_id", proj.Name)
			continue
		}
		r.Metrics.ScanItem("brainstorm", "picked")
		created++
	}
	r.Metrics.ObserveScanDuration("brainstorm", time.Since(start).Seconds())
	l.Info("brainstorm complete", "action", "scan_brainstorm", "resource_id", proj.Name,
		"picked", created, "duration_ms", time.Since(start).Milliseconds())
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

	// mrScan
	if _, due, next, ok := r.activityDue(proj, "mrScan"); ok {
		if due {
			r.mrScan(ctx, proj, reader, repos, existing, cronSpec.MRScan)
			r.stampScan(ctx, proj, "mrScan")
			// Recompute next-fire from now so the post-stamp schedule produces a
			// positive RequeueAfter (the pre-fire next is in the past).
			if next2, ok2 := activityNextFire(cronSpec.MRScan.Schedule, now); ok2 {
				consider(next2)
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
			r.issueScan(ctx, proj, reader, repos, existing, cronSpec.IssueScan)
			r.stampScan(ctx, proj, "issueScan")
			if next2, ok2 := activityNextFire(cronSpec.IssueScan.Schedule, now); ok2 {
				consider(next2)
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
				r.brainstorm(ctx, proj, repos, cronSpec.Brainstorm)
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
