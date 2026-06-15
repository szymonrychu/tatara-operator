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

// labelRecoveryExhausted is stamped on a bot PR after closeExhaustedPR runs so
// subsequent mrScan cycles skip both re-adoption AND re-close. A human who
// removes the label resets recovery.
const labelRecoveryExhausted = "tatara-recovery-exhausted"

// isLifecycleTerminal reports whether a lifecycle state counts as terminal for
// dedup purposes (Done/Stopped/Parked free the (repo,number) key on newer activity).
func isLifecycleTerminal(state string) bool {
	switch state {
	case "Done", "Stopped", "Parked":
		return true
	}
	return false
}

// maxRecoveryAttempts bounds how many times mrScan re-adopts the same bot PR
// before giving up. A PR driven to a terminal lifecycle this many times is not
// fixable by another autonomous pass; stop re-spawning agents and leave it for
// a human (the last park comment already explains why).
const maxRecoveryAttempts = 3

// priorTerminalAttempts counts terminal (Done/Stopped/Parked) tasks that already
// targeted this exact PR, so mrScan can stop re-adopting an unfixable PR.
func priorTerminalAttempts(existing []tatarav1alpha1.Task, repoSlug string, prNumber int) int {
	want := sanitizeRepoLabel(repoSlug)
	n := 0
	for i := range existing {
		t := &existing[i]
		if t.Spec.Source == nil || !t.Spec.Source.IsPR || t.Spec.Source.Number != prNumber {
			continue
		}
		if t.Labels[labelSourceRepo] != want {
			continue
		}
		if isLifecycleTerminal(t.Status.LifecycleState) {
			n++
		}
	}
	return n
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
// the repo's lane: Kind in kinds, phase not terminal. AwaitingApproval is no
// longer a valid Phase value (approval is driven by SCM labels); the branch is
// gone. Use TaskTerminal for a single-source terminality predicate.
func laneOccupancy(existing []tatarav1alpha1.Task, repoSlug string, kinds ...string) int {
	label := sanitizeRepoLabel(repoSlug)
	n := 0
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelSourceRepo] != label || !slices.Contains(kinds, t.Spec.Kind) {
			continue
		}
		// Lifecycle tasks signal terminality via LifecycleState (Phase stays
		// empty); Conversation is human-blocked with no running pod. Such tasks
		// hold no agent slot, so they must not occupy the repo's scan lane -
		// otherwise terminal issueLifecycle tasks starve mrScan/issueScan
		// recovery forever (maxPerRepo=1).
		if isLifecycleTerminal(t.Status.LifecycleState) || t.Status.LifecycleState == "Conversation" {
			continue
		}
		switch t.Status.Phase {
		case "Succeeded", "Failed":
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
			repo: i.Repo, number: i.Number, author: i.Author, labels: i.Labels, updatedAt: i.UpdatedAt, isPR: false,
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

// createHealthCheckTask creates a project-health-check Task. It reuses Kind
// "brainstorm" (same pod egress, writeback arm, and propose_issue flow) but
// carries the distinct "healthCheck" activity label so scheduling, dedup, and
// the in-flight guard stay independent from brainstorm; the pod name is
// disambiguated by that label (see agent.podNameSuffix). sources is recorded as
// a comma-joined annotation the pod builder reads to decide the egress label.
func (r *ProjectReconciler) createHealthCheckTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, goal string, sources []string) (*tatarav1alpha1.Task, error) {
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "healthcheck-",
			Namespace:    proj.Namespace,
			Labels:       map[string]string{labelActivity: "healthCheck"},
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
		return nil, fmt.Errorf("scan: create healthCheck task: %w", err)
	}
	r.Metrics.ScanTaskCreated("healthCheck", "brainstorm")
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

// scanWriter resolves the SCMWriter + token for the Project's provider, mirroring
// scanReader. Used by mrScan to close PRs that recovery has exhausted.
func (r *ProjectReconciler) scanWriter(ctx context.Context, proj *tatarav1alpha1.Project) (scm.SCMWriter, string, error) {
	if r.SCMFor == nil {
		return nil, "", fmt.Errorf("scan: SCMFor not wired")
	}
	var sec corev1.Secret
	key := types.NamespacedName{Namespace: proj.Namespace, Name: proj.Spec.ScmSecretRef}
	if err := r.Get(ctx, key, &sec); err != nil {
		return nil, "", fmt.Errorf("scan: get scm secret: %w", err)
	}
	token := string(sec.Data["token"])
	w, err := r.SCMFor(proj.Spec.Scm.Provider)
	if err != nil {
		return nil, "", err
	}
	return w, token, nil
}

// closeExhaustedPR closes a bot PR that recovery could not land after
// maxRecoveryAttempts. The branch is preserved (ClosePR does not delete it), so
// a human can reopen to retry after removing the tatara-recovery-exhausted label.
// Errors are counted via recovery_close_error so a stuck close path is observable.
func (r *ProjectReconciler) closeExhaustedPR(ctx context.Context, proj *tatarav1alpha1.Project, repos []tatarav1alpha1.Repository, c candidate) {
	l := log.FromContext(ctx)
	repo, ok := r.matchRepoForSlug(repos, c.repo)
	if !ok {
		return
	}
	w, token, err := r.scanWriter(ctx, proj)
	if err != nil {
		l.Error(err, "mrScan: scanWriter for exhausted close (leaving PR open)",
			"resource_id", proj.Name, "repo", c.repo, "pr", c.number)
		r.Metrics.ScanItem("mrScan", "recovery_close_error")
		return
	}
	body := fmt.Sprintf("Autonomous recovery could not land this PR after %d attempts; "+
		"closing as superseded. The branch is preserved - reopen to retry or hand-fix.\n"+
		"To reset recovery, remove the `%s` label from the PR before reopening.",
		maxRecoveryAttempts, labelRecoveryExhausted)
	if cerr := w.ClosePR(ctx, repo.Spec.URL, token, c.number, body); cerr != nil {
		l.Error(cerr, "mrScan: close exhausted PR failed (leaving open)",
			"resource_id", proj.Name, "repo", c.repo, "pr", c.number)
		r.Metrics.ScanItem("mrScan", "recovery_close_error")
		return
	}
	r.Metrics.ScanItem("mrScan", "recovery_closed")
	l.Info("mrScan: closed recovery-exhausted bot PR",
		"action", "scan_recovery_closed", "resource_id", proj.Name, "repo", c.repo, "pr", c.number)
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
	case "healthCheck":
		schedule = proj.Spec.Scm.Cron.HealthCheck.Schedule
		last = proj.Status.LastHealthCheck
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
// Returns non-nil on persistent failure so the caller can log+metric the event.
func (r *ProjectReconciler) stampScan(ctx context.Context, proj *tatarav1alpha1.Project, activity string) error {
	now := metav1.Now()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
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
		case "healthCheck":
			fresh.Status.LastHealthCheck = &now
			proj.Status.LastHealthCheck = &now
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
	if diff := len(eligible) - len(selected); diff > 0 {
		for i := 0; i < diff; i++ {
			r.Metrics.ScanItem("mrScan", "skipped_cap")
		}
	}
	created := 0
	for _, c := range selected {
		if *budget <= 0 {
			break
		}
		repo, ok := r.matchRepoForSlug(repos, c.repo)
		if !ok {
			r.Metrics.ScanItem("mrScan", "skipped_norepo")
			continue
		}
		if c.author == bot && bot != "" {
			// Skip re-adoption AND re-close if the PR already carries the exhaustion label.
			if hasLabel(c.labels, labelRecoveryExhausted) {
				r.Metrics.ScanItem("mrScan", "recovery_exhausted")
				continue
			}
			if priorTerminalAttempts(existing, c.repo, c.number) >= maxRecoveryAttempts {
				r.Metrics.ScanItem("mrScan", "recovery_exhausted")
				r.closeExhaustedPR(ctx, proj, repos, c)
				continue
			}
			// Bot PR -> issueLifecycle entering at MRCI. Dedup key is the linked issue
			// number from "Closes #N" if present, else the PR number.
			dedupNumber := c.number
			if issueNum, linked := scm.LinkedIssueNumber(c.body); linked {
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
				r.Metrics.ScanItem("mrScan", "create_error")
				continue
			}
		} else {
			goal := fmt.Sprintf("Triage review PR %s#%d", c.repo, c.number)
			if _, err := r.createScanTask(ctx, proj, &repo, c, c, "mrScan", "review", goal, nil); err != nil {
				l.Error(err, "scan: create mrScan task", "resource_id", proj.Name, "repo", repo.Name)
				r.Metrics.ScanItem("mrScan", "create_error")
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
	// backlog=true if: cap removed items OR budget truncated selected (finding 3)
	return len(selected) < len(eligible) || created < len(selected)
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
			r.Metrics.ScanItem("issueScan", "reactivate_error")
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
	if diff := len(eligible) - len(selected); diff > 0 {
		for i := 0; i < diff; i++ {
			r.Metrics.ScanItem("issueScan", "skipped_cap")
		}
	}
	created := 0
	for _, c := range selected {
		if *budget <= 0 {
			break
		}
		repo, ok := r.matchRepoForSlug(repos, c.repo)
		if !ok {
			r.Metrics.ScanItem("issueScan", "skipped_norepo")
			continue
		}
		goal := fmt.Sprintf("Triage issue %s#%d", c.repo, c.number)
		if _, err := r.createScanTask(ctx, proj, &repo, c, c, "issueScan", "issueLifecycle", goal, nil); err != nil {
			l.Error(err, "scan: create issueScan task", "resource_id", proj.Name, "repo", repo.Name)
			r.Metrics.ScanItem("issueScan", "create_error")
			continue
		}
		r.Metrics.ScanItem("issueScan", "picked")
		created++
		*budget--
	}
	r.Metrics.ObserveScanDuration("issueScan", time.Since(start).Seconds())
	l.Info("issueScan complete", "action", "scan_issue", "resource_id", proj.Name,
		"listed", len(cands), "picked", created, "duration_ms", time.Since(start).Milliseconds())
	// backlog=true if: cap removed items OR budget truncated selected (finding 3)
	return len(selected) < len(eligible) || created < len(selected)
}

// brainstorm runs one brainstorm cycle at PROJECT scope: at most one brainstorm
// Task per cycle for the whole project. BrainstormActivity.MaxPerCycle is
// deprecated and ignored; the hard cap of one per cycle is enforced here.
// budget is the shared open-task creation budget; brainstorm decrements it on
// each successful create and stops creating when budget reaches zero.
func (r *ProjectReconciler) brainstorm(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.BrainstormActivity, budget *int) {
	l := log.FromContext(ctx)
	start := time.Now()
	maxProp := act.MaxOpenProposals
	if maxProp < 1 {
		maxProp = 5
	}

	// Project-scoped in-flight guard: any non-terminal brainstorm Task blocks.
	if brainstormInFlightProject(existing) {
		r.Metrics.ScanItem("brainstorm", "skipped_inflight")
		l.Info("brainstorm: in-flight project brainstorm task; skipping cycle",
			"action", "scan_brainstorm", "resource_id", proj.Name)
		r.Metrics.ObserveScanDuration("brainstorm", time.Since(start).Seconds())
		return
	}

	// Budget guard.
	if *budget <= 0 {
		r.Metrics.ObserveScanDuration("brainstorm", time.Since(start).Seconds())
		return
	}

	brainstormingLabel, _, _, _ := lifecycleLabels(proj.Spec.Scm)

	// Deterministic primary repo: sort by name, first valid slug wins.
	sortedRepos := make([]tatarav1alpha1.Repository, len(repos))
	copy(sortedRepos, repos)
	sort.Slice(sortedRepos, func(i, j int) bool {
		return sortedRepos[i].Name < sortedRepos[j].Name
	})

	// Single pass: resolve slug, set primaryRepo, accumulate backlog, collect slugs.
	// SetOpenProposals is called for every repo queried so the gauge is never stale
	// for repos processed before the cap (finding 8). Short-circuit stops querying
	// but does not skip gauge updates for already-queried repos.
	var primaryRepo *tatarav1alpha1.Repository
	var slugs []string
	total := 0
	atCap := false
	for i := range sortedRepos {
		rp := &sortedRepos[i]
		slug := repoSlug(rp)
		if slug == "" {
			continue
		}
		if primaryRepo == nil {
			primaryRepo = rp
		}
		slugs = append(slugs, slug)
		if atCap {
			// Already over cap; don't issue more SCM calls but still collect slugs.
			continue
		}
		backlog, err := r.proposalBacklog(ctx, reader, rp, brainstormingLabel, proj.Spec.Scm)
		if err != nil {
			l.Info("brainstorm: backlog count failed (non-fatal)", "resource_id", proj.Name, "repo", rp.Name, "err", err.Error())
			continue
		}
		r.Metrics.SetOpenProposals(slug, float64(backlog))
		total += backlog
		if total >= maxProp {
			atCap = true
		}
	}
	if primaryRepo == nil {
		l.Info("brainstorm: no valid repos", "resource_id", proj.Name)
		r.Metrics.ObserveScanDuration("brainstorm", time.Since(start).Seconds())
		return
	}
	if atCap {
		r.Metrics.ScanItem("brainstorm", "skipped_cap")
		l.Info("brainstorm: project backlog at cap; skipping cycle",
			"action", "scan_brainstorm", "resource_id", proj.Name, "total", total, "cap", maxProp)
		r.Metrics.ObserveScanDuration("brainstorm", time.Since(start).Seconds())
		return
	}

	// Build rich open-issues context for dedup-first reasoning.
	issuesCtx := r.buildIssuesContext(ctx, proj, reader, sortedRepos)

	goal := brainstormGoalProject(slugs, issuesCtx)
	if _, err := r.createBrainstormTask(ctx, proj, primaryRepo, goal, act.Sources); err != nil {
		l.Error(err, "scan: create brainstorm task", "resource_id", proj.Name, "repo", primaryRepo.Name)
		r.Metrics.ObserveScanDuration("brainstorm", time.Since(start).Seconds())
		return
	}
	r.Metrics.ScanItem("brainstorm", "picked")
	*budget--
	r.Metrics.ObserveScanDuration("brainstorm", time.Since(start).Seconds())
	l.Info("brainstorm complete", "action", "scan_brainstorm", "resource_id", proj.Name,
		"picked", 1, "primary_repo", primaryRepo.Name, "duration_ms", time.Since(start).Milliseconds())
}

// healthCheck runs one project-health-check cycle at PROJECT scope: at most one
// healthCheck Task per cycle for the whole project. It mirrors brainstorm (single
// in-flight guard, budget guard, shared proposal-backlog cap, project-spanning
// goal) but drives the tatara-health-check skill. budget is the shared open-task
// creation budget; healthCheck decrements it on a successful create.
func (r *ProjectReconciler) healthCheck(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.HealthCheckActivity, budget *int) {
	l := log.FromContext(ctx)
	start := time.Now()
	maxProp := act.MaxOpenProposals
	if maxProp < 1 {
		maxProp = 5
	}

	// Project-scoped in-flight guard: any non-terminal healthCheck Task blocks.
	if healthCheckInFlightProject(existing) {
		r.Metrics.ScanItem("healthCheck", "skipped_inflight")
		l.Info("healthCheck: in-flight project health-check task; skipping cycle",
			"action", "scan_healthcheck", "resource_id", proj.Name)
		r.Metrics.ObserveScanDuration("healthCheck", time.Since(start).Seconds())
		return
	}

	// Budget guard.
	if *budget <= 0 {
		r.Metrics.ObserveScanDuration("healthCheck", time.Since(start).Seconds())
		return
	}

	brainstormingLabel, _, _, _ := lifecycleLabels(proj.Spec.Scm)

	// Deterministic primary repo: sort by name, first valid slug wins.
	sortedRepos := make([]tatarav1alpha1.Repository, len(repos))
	copy(sortedRepos, repos)
	sort.Slice(sortedRepos, func(i, j int) bool {
		return sortedRepos[i].Name < sortedRepos[j].Name
	})

	var primaryRepo *tatarav1alpha1.Repository
	for i := range sortedRepos {
		if s := repoSlug(&sortedRepos[i]); s != "" {
			primaryRepo = &sortedRepos[i]
			break
		}
	}
	if primaryRepo == nil {
		l.Info("healthCheck: no valid repos", "resource_id", proj.Name)
		r.Metrics.ObserveScanDuration("healthCheck", time.Since(start).Seconds())
		return
	}

	// Aggregate proposal backlog across all repos; short-circuit once >= maxProp.
	// Shares proposalBacklog with brainstorm: both autonomous activities back off
	// against the same open-idea pressure so the project is not flooded.
	total := 0
	for i := range sortedRepos {
		rp := &sortedRepos[i]
		slug := repoSlug(rp)
		if slug == "" {
			continue
		}
		backlog, err := r.proposalBacklog(ctx, reader, rp, brainstormingLabel, proj.Spec.Scm)
		if err != nil {
			l.Info("healthCheck: backlog count failed (non-fatal)", "resource_id", proj.Name, "repo", rp.Name, "err", err.Error())
			continue
		}
		total += backlog
		if total >= maxProp {
			r.Metrics.ScanItem("healthCheck", "skipped_cap")
			l.Info("healthCheck: project backlog at cap; skipping cycle",
				"action", "scan_healthcheck", "resource_id", proj.Name, "total", total, "cap", maxProp)
			r.Metrics.ObserveScanDuration("healthCheck", time.Since(start).Seconds())
			return
		}
	}

	// Collect all valid slugs for the project-spanning goal.
	var slugs []string
	for i := range sortedRepos {
		if s := repoSlug(&sortedRepos[i]); s != "" {
			slugs = append(slugs, s)
		}
	}

	// Build rich open-issues context for dedup-first reasoning.
	issuesCtx := r.buildIssuesContext(ctx, proj, reader, sortedRepos)

	goal := healthCheckGoalProject(slugs, issuesCtx)
	if _, err := r.createHealthCheckTask(ctx, proj, primaryRepo, goal, act.Sources); err != nil {
		l.Error(err, "scan: create healthCheck task", "resource_id", proj.Name, "repo", primaryRepo.Name)
		r.Metrics.ObserveScanDuration("healthCheck", time.Since(start).Seconds())
		return
	}
	r.Metrics.ScanItem("healthCheck", "picked")
	*budget--
	r.Metrics.ObserveScanDuration("healthCheck", time.Since(start).Seconds())
	l.Info("healthCheck complete", "action", "scan_healthcheck", "resource_id", proj.Name,
		"picked", 1, "primary_repo", primaryRepo.Name, "duration_ms", time.Since(start).Milliseconds())
}

// brainstormGoalProject returns the turn-0 goal for a project-level brainstorm
// task. issuesCtx is a pre-built string of open issues across all repos
// (one line each: "repo#N [labels] title"), used to guide dedup-first reasoning.
// When issuesCtx is empty the dedup section is still present but notes no open
// issues were found.
func brainstormGoalProject(slugs []string, issuesCtx string) string {
	repoList := strings.Join(slugs, ", ")

	existingBlock := "No open issues found across the project."
	if issuesCtx != "" {
		existingBlock = "OPEN ISSUES (survey these FIRST before proposing anything new):\n" + issuesCtx
	}

	return "Invoke the `tatara-deep-research` skill to survey the ENTIRE project and identify the single highest-leverage " +
		"discovery or improvement opportunity across ALL repositories: " + repoList + ". " +
		"The skill defines how to research via the tatara-memory graph and on-disk code, score leverage, and dedup. " +
		"\n\n" + existingBlock + "\n\n" +
		"DEDUP RULE - you MUST follow exactly ONE of these three paths, in order:\n" +
		"1. If the best idea DUPLICATES an existing open issue listed above: do NOT call propose_issue. " +
		"Finish with a one-line note naming the duplicate (e.g. 'Duplicate of o/repo#N').\n" +
		"2. If the best idea is a sub-aspect or connecting improvement TO an existing issue: " +
		"call comment_on_issue(repo, number, body) on that issue. Do NOT call propose_issue.\n" +
		"3. ONLY if the idea is genuinely novel AND standalone (no existing issue covers it): " +
		"call propose_issue with exactly one proposal. " +
		"Set the `repo` argument to the specific repository that should own the issue. " +
		"The proposal must be self-contained: problem statement, proposed approach, and a single explicit decision for the human " +
		"(approve to implement or comment to refine). Do NOT produce a list of open questions or ask for input.\n\n" +
		"State which path you chose and why before executing it. Exactly one action per run - no exceptions."
}

// healthCheckGoalProject returns the turn-0 goal for a project-level health-check
// task. It mirrors brainstormGoalProject (same dedup-first contract and issuesCtx
// shape) but drives the tatara-health-check skill across all repo slugs.
func healthCheckGoalProject(slugs []string, issuesCtx string) string {
	repoList := strings.Join(slugs, ", ")

	existingBlock := "No open issues found across the project."
	if issuesCtx != "" {
		existingBlock = "OPEN ISSUES (survey these FIRST before proposing anything new):\n" + issuesCtx
	}

	return "Invoke the `tatara-health-check` skill to survey the HEALTH of the project's repositories " +
		"and identify the single highest-leverage health issue across ALL repositories: " + repoList + ". " +
		"The skill defines the five health dimensions (CI failures, code coverage gaps, code to simplify, " +
		"CI/CD pipeline steps worth adding, other tech-debt), how to gather evidence (on-disk CI config, an " +
		"actual test/lint run, and the tatara-memory code graph), score leverage, and dedup. " +
		"\n\n" + existingBlock + "\n\n" +
		"DEDUP RULE - you MUST follow exactly ONE of these three paths, in order:\n" +
		"1. If the best finding DUPLICATES an existing open issue listed above: do NOT call propose_issue. " +
		"Finish with a one-line note naming the duplicate (e.g. 'Duplicate of o/repo#N').\n" +
		"2. If the best finding is a sub-aspect or connecting improvement TO an existing issue: " +
		"call comment_on_issue(repo, number, body) on that issue. Do NOT call propose_issue.\n" +
		"3. ONLY if the finding is genuinely novel AND standalone (no existing issue covers it): " +
		"call propose_issue with exactly one proposal. " +
		"Set the `repo` argument to the specific repository that should own the issue. " +
		"The proposal must be self-contained: the concrete defect with file:line evidence, the proposed fix, " +
		"and a single explicit decision for the human (approve to implement or comment to refine). " +
		"Do NOT produce a list of open questions or ask for input.\n\n" +
		"State which path you chose and why before executing it. Exactly one action per run - no exceptions."
}

// buildIssuesContext lists all open non-PR issues across repos and builds the
// rich context string embedded in the brainstorm goal for dedup-first reasoning.
// Format per line: "repo#N [label1,label2] title - body-snippet"
// Capped at 60 issues; if more exist, appends a "(+N more omitted)" line.
// ListOpenIssues errors per repo are skipped non-fatally.
const maxIssuesContext = 60

func (r *ProjectReconciler) buildIssuesContext(ctx context.Context, _ *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository) string {
	l := log.FromContext(ctx)
	var lines []string
	total := 0
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		issues, err := reader.ListOpenIssues(ctx, owner, name)
		if err != nil {
			l.Info("brainstorm: buildIssuesContext: ListOpenIssues error (skipped)",
				"repo", repos[i].Name, "err", err.Error())
			continue
		}
		for _, iss := range issues {
			if iss.IsPR {
				continue
			}
			if len(lines) >= maxIssuesContext {
				total++
				continue
			}
			total++
			slug := owner + "/" + name
			labels := strings.Join(iss.Labels, ",")
			title := iss.Title
			// Collapse newlines in title for a single-line entry.
			title = strings.ReplaceAll(title, "\n", " ")
			title = strings.ReplaceAll(title, "\r", "")
			line := fmt.Sprintf("%s#%d [%s] %s", slug, iss.Number, labels, title)
			lines = append(lines, line)
		}
	}
	if len(lines) == 0 {
		return ""
	}
	omitted := total - len(lines)
	result := strings.Join(lines, "\n")
	if omitted > 0 {
		result += fmt.Sprintf("\n(+%d more omitted)", omitted)
		l.Info("brainstorm: buildIssuesContext: capped issues context",
			"shown", len(lines), "omitted", omitted)
	}
	return result
}

// repoSlug returns "owner/name" for a Repository URL, or "" on error.
func repoSlug(repo *tatarav1alpha1.Repository) string {
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return ""
	}
	return owner + "/" + name
}

// brainstormInFlightProject reports whether ANY non-terminal brainstorm Task
// exists in the project (project-scoped guard, replaces per-repo check).
func brainstormInFlightProject(existing []tatarav1alpha1.Task) bool {
	for i := range existing {
		t := existing[i]
		if t.Labels[labelActivity] == "brainstorm" && !isTerminal(t.Status.Phase) {
			return true
		}
	}
	return false
}

// healthCheckInFlightProject reports whether ANY non-terminal healthCheck Task
// exists in the project (project-scoped guard, mirrors brainstormInFlightProject).
func healthCheckInFlightProject(existing []tatarav1alpha1.Task) bool {
	for i := range existing {
		t := existing[i]
		if t.Labels[labelActivity] == "healthCheck" && !isTerminal(t.Status.Phase) {
			return true
		}
	}
	return false
}

// proposalBacklog counts open, undecided ideas for repo: open non-PR issues
// bearing the idea label (live ListOpenIssues). This subsumes tatara-originated
// proposals and any human-filed issue parked as an idea, providing conservative
// brainstorm backpressure.
func (r *ProjectReconciler) proposalBacklog(ctx context.Context, reader scm.SCMReader, repo *tatarav1alpha1.Repository, brainstormingLabel string, scmSpec *tatarav1alpha1.ScmSpec) (int, error) {
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

// hasLiveLifecycleTaskForIssue reports whether any non-terminal Task exists for
// (slug, number) in the snapshot, counting Conversation (human-blocked) Tasks
// too. recoverOrphans uses this rather than taskOpen-based counting: a
// Conversation lifecycle Task still owns the issue's pod name, so spawning a
// second lifecycle Task for the same issue collides on the pod and wedges the
// new Task in Planning forever. Dedup must keep at most one live lifecycle Task
// per (repo, issue) regardless of whether that Task currently holds a
// concurrency slot (which is what taskOpen answers, and why Conversation is
// excluded there).
func hasLiveLifecycleTaskForIssue(existing []tatarav1alpha1.Task, slug string, number int) bool {
	repoLabel := sanitizeRepoLabel(slug)
	numLabel := strconv.Itoa(number)
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelSourceRepo] != repoLabel || t.Labels[labelSourceNumber] != numLabel {
			continue
		}
		if isTerminal(t.Status.Phase) || isLifecycleTerminal(t.Status.LifecycleState) {
			continue
		}
		return true
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
			if hasLiveLifecycleTaskForIssue(existing, slug, iss.Number) {
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
			if serr := r.stampScan(ctx, proj, "mrScan"); serr != nil {
				l.Error(serr, "scan: persist mrScan stamp failed",
					"action", "scan_stamp_error", "resource_id", proj.Name, "activity", "mrScan")
				r.Metrics.ScanItem("mrScan", "stamp_error")
			}
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
			if serr := r.stampScan(ctx, proj, "issueScan"); serr != nil {
				l.Error(serr, "scan: persist issueScan stamp failed",
					"action", "scan_stamp_error", "resource_id", proj.Name, "activity", "issueScan")
				r.Metrics.ScanItem("issueScan", "stamp_error")
			}
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
				if serr := r.stampScan(ctx, proj, "brainstorm"); serr != nil {
					l.Error(serr, "scan: persist brainstorm stamp failed",
						"action", "scan_stamp_error", "resource_id", proj.Name, "activity", "brainstorm")
					r.Metrics.ScanItem("brainstorm", "stamp_error")
				}
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

	// healthCheck (opt-in)
	if cronSpec.HealthCheck.Enabled {
		if _, due, next, ok := r.activityDue(proj, "healthCheck"); ok {
			if due {
				r.healthCheck(ctx, proj, reader, repos, existing, cronSpec.HealthCheck, &budget)
				if serr := r.stampScan(ctx, proj, "healthCheck"); serr != nil {
					l.Error(serr, "scan: persist healthCheck stamp failed",
						"action", "scan_stamp_error", "resource_id", proj.Name, "activity", "healthCheck")
					r.Metrics.ScanItem("healthCheck", "stamp_error")
				}
				if next2, ok2 := activityNextFire(cronSpec.HealthCheck.Schedule, now); ok2 {
					consider(next2)
				}
			} else {
				consider(next)
			}
		} else if cronSpec.HealthCheck.Schedule != "" {
			l.Error(fmt.Errorf("invalid cron %q", cronSpec.HealthCheck.Schedule), "scan: invalid healthCheck cron, disabling",
				"action", "scan_cron_invalid", "resource_id", proj.Name, "activity", "healthCheck")
		}
	}

	return soonest, nil
}
