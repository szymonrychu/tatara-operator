package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// labelRecoveryExhausted is stamped on a bot PR after closeExhaustedPR runs so
// subsequent mrScan cycles skip both re-adoption AND re-close. Removing this
// label alone does NOT fully reset recovery: priorTerminalAttempts still counts
// the existing terminal Tasks for this PR, and if that count still reaches
// maxRecoveryAttempts the very next mrScan cycle will re-close and re-stamp the
// label. To fully reset, delete the terminal Tasks for the PR in addition to
// removing this label, then reopen the PR.
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
func findConvTaskToReactivate(ctx context.Context, c candidate, existing []tatarav1alpha1.Task, reader scm.SCMReader, botLogin string) *tatarav1alpha1.Task {
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
		if !c.updatedAt.After(t.Status.LastActivityAt.Time) {
			continue
		}
		// Author-aware gate: the bot's own queued comment lands after LastActivityAt
		// and would otherwise re-trigger reactivation every scan (the Conversation
		// re-comment loop). Only reactivate when a HUMAN comment is newer than our
		// last activity. Fail-open (reactivate) when we cannot read the author.
		owner, name, ok := strings.Cut(c.repo, "/")
		if !ok || reader == nil || botLogin == "" {
			return t
		}
		if humanCommentAfter(ctx, reader, owner, name, c.number, botLogin, t.Status.LastActivityAt.Time) {
			return t
		}
		continue
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
		if !tatarav1alpha1.TaskTerminal(t) {
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

// createScanTask enqueues one QueuedEvent for a candidate. Returns created=true
// when a new event was enqueued (dedupKey had no existing live work).
//
// labelCand drives the dedup labels (source-repo, source-number). srcCand
// drives the TaskSource (provider, issueRef, number, isPR, authorLogin). For
// most callers they are the same candidate. For bot-PR MRCI entries they
// differ: labelCand carries the linked-issue number (dedup key) while srcCand
// carries the PR identity (number, IsPR=true).
func (r *ProjectReconciler) createScanTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, labelCand, srcCand candidate, activity, kind, goal string, extraAnnotations map[string]string) (bool, error) {
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
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
	// Dedup key is based on labelCand (the issue/PR that determines the task's identity).
	// For bot-PR MRCI entries, labelCand.number is the linked issue (not the PR#),
	// ensuring that mrScan and issueScan share the same dedup key for the same issue.
	// Always use "#" separator: labelCand always refers to an issue (even when found
	// via a bot-PR), and issueScan always uses "#". Using isPR/provider for "!" would
	// produce a different hash and break cross-scan dedup on GitLab.
	labelIssueRef := fmt.Sprintf("%s#%d", labelCand.repo, labelCand.number)
	dedupKey := kind + "\x00" + labelIssueRef
	payload := tatarav1alpha1.QueuedEventPayload{
		Kind:          kind,
		RepositoryRef: repo.Name,
		Goal:          goal,
		Source:        src,
		Labels:        scanTaskLabels(labelCand, activity, kind),
		Annotations:   extraAnnotations,
		GenerateName:  "scan-",
		Provider:      provider,
		PodRepo:       repo.Name,
	}
	_, created, err := queue.EnqueueEvent(ctx, r.Client, r.Alloc, proj, tatarav1alpha1.QueueClassNormal, true, dedupKey, payload)
	if err != nil {
		if errors.Is(err, queue.ErrSeqNotReady) {
			log.FromContext(ctx).Info("queue not ready; skipping scan cycle", "action", "scan_skip_not_ready")
			return false, nil
		}
		return false, fmt.Errorf("scan: enqueue event: %w", err)
	}
	if created {
		r.Metrics.ScanTaskCreated(activity, kind)
		log.FromContext(ctx).Info("scan: enqueued",
			"action", "scan_task_created", "resource_id", proj.Name,
			"repo", labelCand.repo, "number", labelCand.number, "kind", kind, "activity", activity)
	}
	return created, nil
}

// createBrainstormTask enqueues a project-scoped brainstorm QueuedEvent.
// Returns created=true when a new event was enqueued.
func (r *ProjectReconciler) createBrainstormTask(ctx context.Context, proj *tatarav1alpha1.Project, goal string, sources []string) (bool, error) {
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	dedupKey := "brainstorm-" + proj.Name
	payload := tatarav1alpha1.QueuedEventPayload{
		Kind:         "brainstorm",
		Goal:         goal,
		Labels:       map[string]string{labelActivity: "brainstorm"},
		Annotations:  map[string]string{tatarav1alpha1.AnnBrainstormSources: strings.Join(sources, ",")},
		GenerateName: "brainstorm-",
		Provider:     provider,
		PodRepo:      "",
	}
	_, created, err := queue.EnqueueEvent(ctx, r.Client, r.Alloc, proj, tatarav1alpha1.QueueClassNormal, true, dedupKey, payload)
	if err != nil {
		if errors.Is(err, queue.ErrSeqNotReady) {
			log.FromContext(ctx).Info("queue not ready; skipping scan cycle", "action", "scan_skip_not_ready")
			return false, nil
		}
		return false, fmt.Errorf("scan: enqueue brainstorm event: %w", err)
	}
	if created {
		r.Metrics.ScanTaskCreated("brainstorm", "brainstorm")
	}
	return created, nil
}

// createHealthCheckTask enqueues a project-scoped healthCheck QueuedEvent.
// Returns created=true when a new event was enqueued.
func (r *ProjectReconciler) createHealthCheckTask(ctx context.Context, proj *tatarav1alpha1.Project, goal string, sources []string) (bool, error) {
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	dedupKey := "healthCheck-" + proj.Name
	payload := tatarav1alpha1.QueuedEventPayload{
		Kind:         "brainstorm",
		Goal:         goal,
		Labels:       map[string]string{labelActivity: "healthCheck"},
		Annotations:  map[string]string{tatarav1alpha1.AnnBrainstormSources: strings.Join(sources, ",")},
		GenerateName: "healthcheck-",
		Provider:     provider,
		PodRepo:      "",
	}
	_, created, err := queue.EnqueueEvent(ctx, r.Client, r.Alloc, proj, tatarav1alpha1.QueueClassNormal, true, dedupKey, payload)
	if err != nil {
		if errors.Is(err, queue.ErrSeqNotReady) {
			log.FromContext(ctx).Info("queue not ready; skipping scan cycle", "action", "scan_skip_not_ready")
			return false, nil
		}
		return false, fmt.Errorf("scan: enqueue healthCheck event: %w", err)
	}
	if created {
		r.Metrics.ScanTaskCreated("healthCheck", "brainstorm")
	}
	return created, nil
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
		"To fully reset recovery: (1) delete the existing terminal Tasks for this PR "+
		"from the cluster, (2) remove the `%s` label from the PR, then reopen. "+
		"Removing the label alone is not sufficient: prior terminal Task history "+
		"is counted independently and will re-trigger an immediate re-close.",
		maxRecoveryAttempts, labelRecoveryExhausted)
	if cerr := w.ClosePR(ctx, repo.Spec.URL, token, c.number, body); cerr != nil {
		l.Error(cerr, "mrScan: close exhausted PR failed (leaving open)",
			"resource_id", proj.Name, "repo", c.repo, "pr", c.number)
		r.Metrics.ScanItem("mrScan", "recovery_close_error")
		return
	}
	// Stamp the exhaustion label so subsequent mrScan cycles skip re-adoption AND
	// re-close. Note: removing this label alone does NOT fully reset recovery;
	// see the const comment on labelRecoveryExhausted for the full reset procedure.
	sep := "#"
	if proj.Spec.Scm != nil && proj.Spec.Scm.Provider == "gitlab" {
		sep = "!"
	}
	issueRef := fmt.Sprintf("%s%s%d", c.repo, sep, c.number)
	if lerr := w.AddLabel(ctx, token, issueRef, labelRecoveryExhausted); lerr != nil {
		l.Error(lerr, "mrScan: stamp recovery-exhausted label (non-fatal)",
			"resource_id", proj.Name, "repo", c.repo, "pr", c.number)
		// Distinct outcome: PR was closed but the exhaustion label was not stamped.
		// This is different from recovery_close_error (which means ClosePR itself failed).
		// The next cycle will re-evaluate priorTerminalAttempts and attempt to re-close.
		r.Metrics.ScanItem("mrScan", "recovery_label_error")
		r.Metrics.ScanItem("mrScan", "recovery_closed")
		l.Info("mrScan: closed recovery-exhausted bot PR (label stamp failed)",
			"action", "scan_recovery_closed", "resource_id", proj.Name, "repo", c.repo, "pr", c.number)
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

// mrScan lists open PRs across repos, dedups, and enqueues QueuedEvents routed
// by authoritative author -> review (human) | issueLifecycle/MRCI (bot).
// remaining is the shared autonomous-cap budget; mrScan decrements it on each
// successful enqueue and stops when remaining reaches zero.
func (r *ProjectReconciler) mrScan(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.CronActivity, remaining *int) bool {
	l := log.FromContext(ctx)
	start := time.Now()
	bot := ""
	if proj.Spec.Scm != nil {
		bot = proj.Spec.Scm.BotLogin
	}
	seen := map[string]bool{}
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
		for _, c := range candidatesFromPRs(prs) {
			key := fmt.Sprintf("%s#%d", c.repo, c.number)
			if seen[key] {
				continue
			}
			seen[key] = true
			cands = append(cands, c)
		}
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
	created := 0
	for i, c := range eligible {
		if *remaining <= 0 {
			for range eligible[i:] {
				r.Metrics.ScanItem("mrScan", "skipped_budget")
			}
			break
		}
		repo, ok := r.matchRepoForSlug(repos, c.repo)
		if !ok {
			r.Metrics.ScanItem("mrScan", "skipped_norepo")
			continue
		}
		if c.author == bot && bot != "" {
			if hasLabel(c.labels, labelRecoveryExhausted) {
				r.Metrics.ScanItem("mrScan", "recovery_exhausted")
				l.Info("mrScan: skipping permanently parked bot PR (recovery-exhausted label present)",
					"action", "scan_recovery_parked", "resource_id", proj.Name, "repo", c.repo, "pr", c.number)
				continue
			}
			if priorTerminalAttempts(existing, c.repo, c.number) >= maxRecoveryAttempts {
				r.Metrics.ScanItem("mrScan", "recovery_close_attempt")
				r.closeExhaustedPR(ctx, proj, repos, c)
				continue
			}
			dedupNumber := c.number
			if issueNum, linked := scm.LinkedIssueNumber(c.body); linked {
				dedupNumber = issueNum
			}
			if hasLiveLifecycleTaskForIssue(existing, c.repo, dedupNumber) {
				r.Metrics.ScanItem("mrScan", "skipped_dedup")
				continue
			}
			labelCand := candidate{
				repo: c.repo, number: dedupNumber, headSHA: c.headSHA,
				labels: c.labels, updatedAt: c.updatedAt, isPR: c.isPR,
			}
			srcCand := candidate{
				repo: c.repo, number: c.number, author: c.author, isPR: true,
			}
			goal := fmt.Sprintf("Review issueLifecycle PR %s#%d", c.repo, c.number)
			ann := map[string]string{tatarav1alpha1.LifecycleEntryAnnotation: "MRCI"}
			ok2, err := r.createScanTask(ctx, proj, &repo, labelCand, srcCand, "mrScan", "issueLifecycle", goal, ann)
			if err != nil {
				l.Error(err, "scan: enqueue mrScan issueLifecycle event", "resource_id", proj.Name, "repo", repo.Name)
				r.Metrics.ScanItem("mrScan", "create_error")
				continue
			}
			if ok2 {
				r.Metrics.ScanItem("mrScan", "picked")
				created++
				*remaining--
			}
		} else {
			goal := fmt.Sprintf("Triage review PR %s#%d", c.repo, c.number)
			ok2, err := r.createScanTask(ctx, proj, &repo, c, c, "mrScan", "review", goal, nil)
			if err != nil {
				l.Error(err, "scan: enqueue mrScan task", "resource_id", proj.Name, "repo", repo.Name)
				r.Metrics.ScanItem("mrScan", "create_error")
				continue
			}
			if ok2 {
				r.Metrics.ScanItem("mrScan", "picked")
				created++
				*remaining--
			}
		}
	}
	r.Metrics.ObserveScanDuration("mrScan", time.Since(start).Seconds())
	l.Info("mrScan complete", "action", "scan_mr", "resource_id", proj.Name,
		"listed", len(cands), "picked", created, "duration_ms", time.Since(start).Milliseconds())
	return created < len(eligible)
}

// issueScan lists open issues (per-repo + board) and enqueues QueuedEvents.
// remaining is the shared autonomous-cap budget; issueScan decrements it on each
// successful enqueue and stops when remaining reaches zero.
// Returns (backlog, issueCache): backlog=true when budget truncated the run;
// issueCache holds the per-repo slices fetched this cycle so recoverOrphans can reuse
// them without a second ListOpenIssues round-trip per repo (finding 4).
func (r *ProjectReconciler) issueScan(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.CronActivity, remaining *int) (bool, map[string][]scm.IssueRef) {
	l := log.FromContext(ctx)
	start := time.Now()
	issueCache := make(map[string][]scm.IssueRef)
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
		issueCache[owner+"/"+name] = iss
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
	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	for _, c := range cands {
		task := findConvTaskToReactivate(ctx, c, existing, reader, botLogin)
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

	// Dedup BEFORE budget so a stale-but-in-flight item does not waste a slot.
	managed := managedPhaseLabels(proj.Spec.Scm)
	var eligible []candidate
	for _, c := range cands {
		if isDeduped(c, existing, managed) {
			r.Metrics.ScanItem("issueScan", "skipped_dedup")
		} else {
			eligible = append(eligible, c)
		}
	}
	created := 0
	for i, c := range eligible {
		if *remaining <= 0 {
			for range eligible[i:] {
				r.Metrics.ScanItem("issueScan", "skipped_budget")
			}
			break
		}
		repo, ok := r.matchRepoForSlug(repos, c.repo)
		if !ok {
			r.Metrics.ScanItem("issueScan", "skipped_norepo")
			continue
		}
		goal := fmt.Sprintf("Triage issue %s#%d", c.repo, c.number)
		ok2, err := r.createScanTask(ctx, proj, &repo, c, c, "issueScan", "issueLifecycle", goal, nil)
		if err != nil {
			l.Error(err, "scan: enqueue issueScan event", "resource_id", proj.Name, "repo", repo.Name)
			r.Metrics.ScanItem("issueScan", "create_error")
			continue
		}
		if ok2 {
			r.Metrics.ScanItem("issueScan", "picked")
			created++
			*remaining--
		}
	}
	r.Metrics.ObserveScanDuration("issueScan", time.Since(start).Seconds())
	l.Info("issueScan complete", "action", "scan_issue", "resource_id", proj.Name,
		"listed", len(cands), "picked", created, "duration_ms", time.Since(start).Milliseconds())
	return created < len(eligible), issueCache
}

// brainstorm runs one brainstorm cycle at PROJECT scope: at most one brainstorm
// QueuedEvent per cycle for the whole project. BrainstormActivity.MaxPerCycle is
// deprecated and ignored; the hard cap of one per cycle is enforced here.
// remaining is the shared autonomous-cap budget; brainstorm decrements it on
// each successful enqueue and stops when remaining reaches zero.
func (r *ProjectReconciler) brainstorm(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.BrainstormActivity, remaining *int) {
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
	if *remaining <= 0 {
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

	legacyIdea, _ := legacyLabels(proj.Spec.Scm)

	// Single pass: resolve slug, set primaryRepo, accumulate backlog, collect slugs.
	// Issues are fetched once per repo (findings 4 & 5) and cached in issuesBySlug
	// for reuse by buildIssuesContext below. SetOpenProposals is called for every
	// repo queried up to the cap (best-effort: repos beyond the cap short-circuit
	// and their gauges are not refreshed this cycle).
	issuesBySlug := make(map[string][]scm.IssueRef)
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
			// Already over cap; skip SCM calls but still collect slugs for the goal.
			// SetOpenProposals is NOT called for repos past this short-circuit: their
			// per-repo open-proposal gauges retain the value from the previous cycle
			// (best-effort; documented intentional trade-off to avoid unnecessary SCM calls).
			continue
		}
		owner, name, err := scm.OwnerRepo(rp.Spec.URL)
		if err != nil {
			continue
		}
		iss, err := reader.ListOpenIssues(ctx, owner, name)
		if err != nil {
			l.Info("brainstorm: backlog count failed (non-fatal)", "resource_id", proj.Name, "repo", rp.Name, "err", err.Error())
			continue
		}
		issuesBySlug[slug] = iss
		backlog := proposalBacklogCount(iss, brainstormingLabel, legacyIdea)
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

	// Build rich open-issues context from already-fetched data (no second
	// ListOpenIssues round-trip for the repos queried above - finding 5).
	issuesCtx := r.buildIssuesContext(ctx, proj, reader, issuesBySlug, sortedRepos)

	goal := brainstormGoalProject(slugs, issuesCtx)
	created, err := r.createBrainstormTask(ctx, proj, goal, act.Sources)
	if err != nil {
		l.Error(err, "scan: enqueue brainstorm event", "resource_id", proj.Name)
		r.Metrics.ObserveScanDuration("brainstorm", time.Since(start).Seconds())
		return
	}
	if created {
		r.Metrics.ScanItem("brainstorm", "picked")
		*remaining--
	}
	r.Metrics.ObserveScanDuration("brainstorm", time.Since(start).Seconds())
	l.Info("brainstorm complete", "action", "scan_brainstorm", "resource_id", proj.Name,
		"picked", 1, "duration_ms", time.Since(start).Milliseconds())
}

// healthCheck runs one project-health-check cycle at PROJECT scope: at most one
// healthCheck QueuedEvent per cycle for the whole project. It mirrors brainstorm
// (single in-flight guard, budget guard, shared proposal-backlog cap, project-spanning
// goal) but drives the tatara-health-check skill. remaining is the shared autonomous-cap
// budget; healthCheck decrements it on a successful enqueue.
func (r *ProjectReconciler) healthCheck(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.HealthCheckActivity, remaining *int) {
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
	if *remaining <= 0 {
		r.Metrics.ObserveScanDuration("healthCheck", time.Since(start).Seconds())
		return
	}

	brainstormingLabel, _, _, _ := lifecycleLabels(proj.Spec.Scm)

	// Sort repos by name for deterministic iteration.
	sortedRepos := make([]tatarav1alpha1.Repository, len(repos))
	copy(sortedRepos, repos)
	sort.Slice(sortedRepos, func(i, j int) bool {
		return sortedRepos[i].Name < sortedRepos[j].Name
	})

	legacyIdea, _ := legacyLabels(proj.Spec.Scm)

	// Aggregate proposal backlog across all repos; short-circuit once >= maxProp.
	// Issues are fetched once per repo (findings 4 & 5) and cached in issuesBySlug
	// for reuse by buildIssuesContext below.
	issuesBySlug := make(map[string][]scm.IssueRef)
	total := 0
	var slugs []string
	for i := range sortedRepos {
		rp := &sortedRepos[i]
		slug := repoSlug(rp)
		if slug == "" {
			continue
		}
		slugs = append(slugs, slug)
		owner, name, err := scm.OwnerRepo(rp.Spec.URL)
		if err != nil {
			continue
		}
		iss, err := reader.ListOpenIssues(ctx, owner, name)
		if err != nil {
			l.Info("healthCheck: backlog count failed (non-fatal)", "resource_id", proj.Name, "repo", rp.Name, "err", err.Error())
			continue
		}
		issuesBySlug[slug] = iss
		backlog := proposalBacklogCount(iss, brainstormingLabel, legacyIdea)
		r.Metrics.SetOpenProposals(slug, float64(backlog))
		total += backlog
		if total >= maxProp {
			r.Metrics.ScanItem("healthCheck", "skipped_cap")
			l.Info("healthCheck: project backlog at cap; skipping cycle",
				"action", "scan_healthcheck", "resource_id", proj.Name, "total", total, "cap", maxProp)
			r.Metrics.ObserveScanDuration("healthCheck", time.Since(start).Seconds())
			return
		}
	}

	if len(slugs) == 0 {
		l.Info("healthCheck: no valid repos", "resource_id", proj.Name)
		r.Metrics.ObserveScanDuration("healthCheck", time.Since(start).Seconds())
		return
	}

	// Build rich open-issues context from already-fetched data (no second
	// ListOpenIssues round-trip for the repos queried above - finding 5).
	issuesCtx := r.buildIssuesContext(ctx, proj, reader, issuesBySlug, sortedRepos)

	goal := healthCheckGoalProject(slugs, issuesCtx)
	created, err := r.createHealthCheckTask(ctx, proj, goal, act.Sources)
	if err != nil {
		l.Error(err, "scan: enqueue healthCheck event", "resource_id", proj.Name)
		r.Metrics.ObserveScanDuration("healthCheck", time.Since(start).Seconds())
		return
	}
	if created {
		r.Metrics.ScanItem("healthCheck", "picked")
		*remaining--
	}
	r.Metrics.ObserveScanDuration("healthCheck", time.Since(start).Seconds())
	l.Info("healthCheck complete", "action", "scan_healthcheck", "resource_id", proj.Name,
		"picked", 1, "duration_ms", time.Since(start).Milliseconds())
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
		"2. If the best idea is a sub-aspect or connecting improvement TO an existing issue " +
		"that is NOT marked [bot-engaged]: call comment_on_issue(repo, number, body) on that issue. " +
		"Do NOT call propose_issue.\n" +
		"   An issue marked [bot-engaged] already has your comment - do NOT comment again on it. " +
		"Prefer a NEW improvement instead: a genuinely novel standalone idea (path 3, in ANY repo or " +
		"project-wide), or a comment on a DIFFERENT issue that is not [bot-engaged]. Never comment " +
		"twice on the same issue.\n" +
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
		"2. If the best finding is a sub-aspect or connecting improvement TO an existing issue " +
		"that is NOT marked [bot-engaged]: call comment_on_issue(repo, number, body) on that issue. " +
		"Do NOT call propose_issue.\n" +
		"   An issue marked [bot-engaged] already has your comment - do NOT comment again on it. " +
		"Prefer a NEW finding instead: a genuinely novel standalone issue (path 3, in ANY repo or " +
		"project-wide), or a comment on a DIFFERENT issue that is not [bot-engaged]. Never comment " +
		"twice on the same issue.\n" +
		"3. ONLY if the finding is genuinely novel AND standalone (no existing issue covers it): " +
		"call propose_issue with exactly one proposal. " +
		"Set the `repo` argument to the specific repository that should own the issue. " +
		"The proposal must be self-contained: the concrete defect with file:line evidence, the proposed fix, " +
		"and a single explicit decision for the human (approve to implement or comment to refine). " +
		"Do NOT produce a list of open questions or ask for input.\n\n" +
		"State which path you chose and why before executing it. Exactly one action per run - no exceptions."
}

// buildIssuesContext builds the rich context string embedded in the brainstorm
// goal for dedup-first reasoning from a pre-fetched per-repo issue map (the
// caller fetches once and reuses the same slice for proposalBacklog, avoiding
// duplicate ListOpenIssues calls per repo per cycle - finding 4 & 5).
// Format per line: "repo#N [label1,label2] title"
// Capped at 60 issues; if more exist, appends a "(+N more omitted)" line.
const maxIssuesContext = 60

func (r *ProjectReconciler) buildIssuesContext(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, issuesBySlug map[string][]scm.IssueRef, repos []tatarav1alpha1.Repository) string {
	l := log.FromContext(ctx)
	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	var lines []string
	total := 0
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		slug := owner + "/" + name
		issues := issuesBySlug[slug]
		for _, iss := range issues {
			if iss.IsPR {
				continue
			}
			if len(lines) >= maxIssuesContext {
				total++
				continue
			}
			total++
			labels := strings.Join(iss.Labels, ",")
			title := iss.Title
			// Collapse newlines in title for a single-line entry.
			title = strings.ReplaceAll(title, "\n", " ")
			title = strings.ReplaceAll(title, "\r", "")
			line := fmt.Sprintf("%s#%d [%s] %s", slug, iss.Number, labels, title)
			if botCommentedOnIssue(ctx, reader, owner, name, iss.Number, botLogin) {
				line += " [bot-engaged]"
			}
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

// humanCommentAfter reports whether the issue has a comment authored by a
// non-bot (a human) with CreatedAt strictly after `since`. On a read error it
// returns true (fail-open: the caller reactivates, preserving the missed-webhook
// recovery the reactivation gate exists for; the discuss/close silence gate makes
// an over-eager reactivation a silent no-op).
func humanCommentAfter(ctx context.Context, reader scm.SCMReader, owner, name string, number int, botLogin string, since time.Time) bool {
	comments, err := reader.ListIssueComments(ctx, owner, name, number)
	if err != nil {
		return true
	}
	for _, c := range comments {
		if c.Author != "" && c.Author != botLogin && c.CreatedAt.After(since) {
			return true
		}
	}
	return false
}

// botCommentedOnIssue reports whether botLogin already authored a comment on the
// issue. Empty botLogin or any SCM read error -> false (best-effort flag; the
// commentOnIssue egress gate is the authoritative backstop).
func botCommentedOnIssue(ctx context.Context, reader scm.SCMReader, owner, name string, number int, botLogin string) bool {
	if botLogin == "" {
		return false
	}
	comments, err := reader.ListIssueComments(ctx, owner, name, number)
	if err != nil {
		return false
	}
	for _, c := range comments {
		if c.Author == botLogin {
			return true
		}
	}
	return false
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

// proposalBacklogCount counts open, undecided ideas in a pre-fetched issue
// slice: non-PR issues bearing the brainstorming or legacy-idea label.
func proposalBacklogCount(issues []scm.IssueRef, brainstormingLabel, legacyIdea string) int {
	n := 0
	for _, iss := range issues {
		if !iss.IsPR && (hasLabel(iss.Labels, brainstormingLabel) || hasLabel(iss.Labels, legacyIdea)) {
			n++
		}
	}
	return n
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
	return proposalBacklogCount(issues, brainstormingLabel, legacyIdea), nil
}

// hasLiveLifecycleTaskForIssue reports whether any non-terminal Task exists for
// (slug, number) in the snapshot, counting Conversation (human-blocked) Tasks
// too. recoverOrphans uses this for dedup rather than checking only active-phase
// tasks: a Conversation lifecycle Task still owns the issue's pod name, so
// spawning a second lifecycle Task for the same issue collides on the pod and
// wedges the new Task in Planning forever. Dedup must keep at most one live
// lifecycle Task per (repo, issue) regardless of whether that Task is currently
// running an agent.
func hasLiveLifecycleTaskForIssue(existing []tatarav1alpha1.Task, slug string, number int) bool {
	repoLabel := sanitizeRepoLabel(slug)
	numLabel := strconv.Itoa(number)
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelSourceRepo] != repoLabel || t.Labels[labelSourceNumber] != numLabel {
			continue
		}
		if tatarav1alpha1.TaskTerminal(t) {
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
//
// issueCache is the per-repo slice map returned by issueScan this cycle. When a
// repo's issues were already fetched by issueScan, recoverOrphans reuses that
// slice instead of issuing a second ListOpenIssues round-trip (finding 4). A nil
// or missing key falls back to a fresh ListOpenIssues call.
func (r *ProjectReconciler) recoverOrphans(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, issueCache map[string][]scm.IssueRef, remaining *int) {
	if *remaining <= 0 {
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
		slug := owner + "/" + name
		var issues []scm.IssueRef
		if cached, ok := issueCache[slug]; ok {
			// Reuse the slice issueScan already fetched this cycle (finding 4).
			issues = cached
		} else {
			var lerr error
			issues, lerr = reader.ListOpenIssues(ctx, owner, name)
			if lerr != nil {
				l.Error(lerr, "backstop: ListOpenIssues", "action", "backstop_list_error", "resource_id", proj.Name, "repo", repos[i].Name)
				continue
			}
		}
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
			if *remaining <= 0 {
				return
			}
			repo, ok := r.matchRepoForSlug(repos, slug)
			if !ok {
				continue
			}
			cand := candidate{repo: slug, number: iss.Number, labels: iss.Labels, updatedAt: iss.UpdatedAt}
			ann := map[string]string{tatarav1alpha1.LifecycleEntryAnnotation: entry}
			ok2, cerr := r.createScanTask(ctx, proj, &repo, cand, cand, "backstop", "issueLifecycle", goal, ann)
			if cerr != nil {
				l.Error(cerr, "backstop: create recovery task", "action", "backstop_create_error", "resource_id", proj.Name, "repo", repo.Name)
				continue
			}
			if !ok2 {
				continue
			}
			l.Info("backstop: recovered orphaned issue", "action", "backstop_recover",
				"resource_id", proj.Name, "issue", fmt.Sprintf("%s#%d", slug, iss.Number), "entry", entry)
			r.Metrics.ScanItem("backstop", "recovered")
			*remaining--
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
	soonestSet := false
	consider := func(next time.Time) {
		d := next.Sub(now)
		if d < 0 {
			d = 0
		}
		if d > maxScheduleRequeue {
			d = maxScheduleRequeue
		}
		if !soonestSet || d < soonest {
			soonest = d
			soonestSet = true
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

	// Compute remaining autonomous-QE budget.
	var qel tatarav1alpha1.QueuedEventList
	if err := r.List(ctx, &qel, client.InNamespace(proj.Namespace)); err != nil {
		return 0, err
	}
	qes := filterQEsByProject(qel.Items, proj.Name)
	remaining := proj.QueuedAutonomousCap() - queuedAutonomousCount(qes)
	if remaining < 0 {
		remaining = 0
	}
	if remaining == 0 {
		l.Info("scan: at queued-autonomous cap; skipping autonomous creation",
			"action", "scan_open_cap", "resource_id", proj.Name, "cap", proj.QueuedAutonomousCap())
	}

	// mrScan
	if _, due, next, ok := r.activityDue(proj, "mrScan"); ok {
		if due {
			backlog := r.mrScan(ctx, proj, reader, repos, existing, cronSpec.MRScan, &remaining)
			// Only advance the stamp when there is no backlog. When backlog=true the
			// 60s short requeue must re-fire the activity; if we stamp now, activityDue
			// computes next-fire from the fresh stamp and returns due=false for any
			// non-sub-minute cron, making the backlog drain requeue a no-op (finding 3).
			if !backlog {
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
			} else {
				consider(now.Add(backlogRequeue))
			}
		} else {
			consider(next)
		}
	} else if cronSpec.MRScan.Schedule != "" {
		l.Error(fmt.Errorf("invalid cron %q", cronSpec.MRScan.Schedule), "scan: invalid mrScan cron, disabling",
			"action", "scan_cron_invalid", "resource_id", proj.Name, "activity", "mrScan")
	}

	// issueScan: re-list existing Tasks so Tasks created by mrScan above are visible
	// (prevents duplicate issueLifecycle tasks for bot-PR linked issues).
	// Also re-list QEs to update remaining count after mrScan enqueues.
	if fresh, ferr := r.existingScanTasks(ctx, proj); ferr == nil {
		existing = fresh
	}
	var qelFresh tatarav1alpha1.QueuedEventList
	if err := r.List(ctx, &qelFresh, client.InNamespace(proj.Namespace)); err == nil {
		qes = filterQEsByProject(qelFresh.Items, proj.Name)
		remaining = proj.QueuedAutonomousCap() - queuedAutonomousCount(qes)
		if remaining < 0 {
			remaining = 0
		}
	}
	if _, due, next, ok := r.activityDue(proj, "issueScan"); ok {
		if due {
			backlog, issueCache := r.issueScan(ctx, proj, reader, repos, existing, cronSpec.IssueScan, &remaining)
			// Only advance the stamp when there is no backlog (mirrors mrScan fix, finding 3).
			if !backlog {
				if serr := r.stampScan(ctx, proj, "issueScan"); serr != nil {
					l.Error(serr, "scan: persist issueScan stamp failed",
						"action", "scan_stamp_error", "resource_id", proj.Name, "activity", "issueScan")
					r.Metrics.ScanItem("issueScan", "stamp_error")
				}
				if next2, ok2 := activityNextFire(cronSpec.IssueScan.Schedule, now); ok2 {
					consider(next2)
				}
			} else {
				consider(now.Add(backlogRequeue))
			}
			if remaining > 0 {
				r.recoverOrphans(ctx, proj, reader, repos, issueCache, &remaining)
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
				r.brainstorm(ctx, proj, reader, repos, existing, cronSpec.Brainstorm, &remaining)
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
				r.healthCheck(ctx, proj, reader, repos, existing, cronSpec.HealthCheck, &remaining)
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
