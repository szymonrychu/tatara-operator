package controller

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/robfig/cron/v3"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/promptguidance"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/refine"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

// dueBase resolves the base time every cron computation in this file anchors
// on: the activity's persisted last-run stamp, or the Project's creation
// timestamp when it has never run - a never-run activity must read OVERDUE
// rather than invisible. Shared by activityDue, reposDueForScan,
// nextExpectedUnix and earliestIssueScanFire, which all repeated this same
// (proj, last) fallback inline (review finding: four copies is past this
// repo's "three similar lines" threshold).
func dueBase(proj *tatarav1alpha1.Project, last *metav1.Time) time.Time {
	if last != nil {
		return last.Time
	}
	return proj.CreationTimestamp.Time
}

// nextExpectedUnix returns the unix timestamp of the next expected fire for one
// activity: its cron applied via activityNextFire to dueBase(proj, last).
// ok=false for an empty (disabled) OR an unparseable schedule (activityNextFire's
// own contract); callers publish no series either way and meter invalid_cron
// only for the unparseable case, which they distinguish by re-checking
// schedule != "".
func nextExpectedUnix(proj *tatarav1alpha1.Project, schedule string, last *metav1.Time) (float64, bool) {
	next, ok := activityNextFire(schedule, dueBase(proj, last))
	if !ok {
		return 0, false
	}
	return float64(next.Unix()), true
}

// activityScheduleAndLast returns the cron schedule string and last-scan stamp
// for one activity. Callers are post-guard (Spec.Scm and Cron are non-nil).
func activityScheduleAndLast(proj *tatarav1alpha1.Project, activity string) (string, *metav1.Time) {
	c := proj.Spec.Scm.Cron
	switch activity {
	case "issueScan":
		return c.IssueScan.Schedule, proj.Status.LastIssueScan
	case "brainstorm":
		return c.Brainstorm.Schedule, proj.Status.LastBrainstorm
	case "documentation":
		return c.Documentation.Schedule, proj.Status.LastDocumentation
	}
	return "", nil
}

// scanOffset returns a deterministic offset in [0, period) for a
// (project, repo, activity) triple. Per-repo scan fires are phase-shifted by
// this offset so they spread across the cron interval instead of all firing at
// the same boundary (the synchronized hourly fan-out of issue #181). It is a
// pure hash of the identifiers, so it is stable across operator restarts and
// pods (no randomness, no wall clock).
func scanOffset(project, repo, activity string, period time.Duration) time.Duration {
	if period <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(project + "\x00" + repo + "\x00" + activity))
	return time.Duration(uint64(h.Sum32()) % uint64(period))
}

// cronPeriod returns the nominal interval between two consecutive fires of a
// parsed cron, used to bound per-repo scan offsets. base anchors the
// computation so it is deterministic.
func cronPeriod(sched cron.Schedule, base time.Time) time.Duration {
	f1 := sched.Next(base)
	return sched.Next(f1).Sub(f1)
}

// repoNextFire returns a repo's next phase-shifted fire strictly after `after`,
// given the base cron schedule and the repo's deterministic offset.
func repoNextFire(sched cron.Schedule, offset time.Duration, after time.Time) time.Time {
	return sched.Next(after.Add(-offset)).Add(offset)
}

// label key aliases for readability within this package.
const (
	labelSourceKind = tatarav1alpha1.LabelSourceKind
	labelActivity   = tatarav1alpha1.LabelActivity
	// labelIncident is stamped on issueLifecycle Tasks whose source issue carries
	// the incident SCM label, so tatara_issue_state can distinguish
	// incident-derived issues from regular improvements without SCM round-trips.
	labelIncident = "tatara.io/incident"
)

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

// createBrainstormTask enqueues a project-scoped brainstorm QueuedEvent.
// Returns created=true when a new event was enqueued. quota is the resolved
// per-session proposal allowance; it rides as AnnBrainstormQuota, which
// internal/restapi/outcome.go truncates submit_outcome against.
func (r *ProjectReconciler) createBrainstormTask(ctx context.Context, proj *tatarav1alpha1.Project, goal string, sources []string, quota int) (bool, error) {
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	dedupKey := "brainstorm-" + proj.Name
	payload := tatarav1alpha1.QueuedEventPayload{
		Kind:   "brainstorm",
		Goal:   goal,
		Labels: map[string]string{labelActivity: "brainstorm"},
		Annotations: map[string]string{
			tatarav1alpha1.AnnBrainstormSources: strings.Join(sources, ","),
			tatarav1alpha1.AnnBrainstormQuota:   strconv.Itoa(quota),
		},
		GenerateName: "brainstorm-",
		Provider:     provider,
		PodRepo:      "",
	}
	_, created, err := queue.EnqueueEvent(ctx, r.Client, r.Seq, proj, tatarav1alpha1.QueueClassNormal, true, dedupKey, payload)
	if err != nil {
		log.FromContext(ctx).Error(err, "scan: enqueue brainstorm event failed; skipping item", "action", "scan_enqueue_failed", "project", proj.Name)
		// Intentional: project-scoped tasks stamp unconditionally; no backlog/fast-refire coupling,
		// unlike createScanTask which propagates errors for per-issue deferral.
		return false, nil
	}
	return created, nil
}

// documentationScan is the scheduled documentation-sync tick. For each enrolled
// component repo (excluding the docs repo itself) that advanced since
// Status.LastDocumentation, it enqueues a documentation Task scoped to the docs
// repo carrying the source diff window as annotations. The push webhook trigger
// is retired; this is the sole documentation producer. The agent decides doc
// relevance (no-ops on trivial change); the operator only spawns when the
// source default branch has commits in the since-last-doc window.
func (r *ProjectReconciler) documentationScan(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository) {
	l := log.FromContext(ctx)
	doc := proj.Spec.Documentation
	if doc == nil || !doc.Enabled || doc.Repo == "" {
		return
	}
	var docsRepo *tatarav1alpha1.Repository
	for i := range repos {
		if scm.SameRemote(doc.Repo, repos[i].Spec.URL) {
			docsRepo = &repos[i]
			break
		}
	}
	if docsRepo == nil {
		// Docs repo not enrolled as a Repository CR: no push access, nowhere to
		// write. Mirrors the retired push path's guard.
		l.Info("documentation: docs repo not enrolled; skipping cycle",
			"action", "scan_documentation_no_docs_repo", "resource_id", proj.Name, "docs_repo", doc.Repo)
		return
	}
	// Liveness finding #7: overlap/orphan guard. The per-head dedup key means two
	// doc Tasks for DIFFERENT source heads never dedup and could run concurrently.
	// Re-sweep dropped/Parked doc cycles so they retry (bounded), then an in-flight
	// guard (mirroring brainstormInFlightProject) suppresses starting a new doc Task
	// while one is already live. Fail-open on a list error (keep prior behavior).
	if existing, lerr := r.existingScanTasks(ctx, proj); lerr == nil {
		if documentationInFlightProject(existing) {
			l.Info("documentation: a doc cycle is already in-flight; skipping new doc Task this tick",
				"action", "scan_documentation_inflight", "resource_id", proj.Name)
			return
		}
	} else {
		l.Error(lerr, "documentation: list tasks for in-flight guard failed; proceeding",
			"action", "scan_documentation_guard_error", "resource_id", proj.Name)
	}

	var since time.Time
	if proj.Status.LastDocumentation != nil {
		since = proj.Status.LastDocumentation.Time
	}
	for i := range repos {
		src := &repos[i]
		if src.Name == docsRepo.Name || scm.SameRemote(doc.Repo, src.Spec.URL) {
			continue // self-trigger guard
		}
		owner, name, err := scm.OwnerRepo(src.Spec.URL)
		if err != nil {
			continue
		}
		commits, err := reader.ListCommits(ctx, owner, name, since)
		if err != nil {
			l.Error(err, "documentation: ListCommits", "action", "scan_list_error", "resource_id", proj.Name, "activity", "documentation", "repo", src.Name)
			continue
		}
		if len(commits) == 0 {
			continue // no change since last doc run
		}
		head, err := reader.GetDefaultBranchHeadSHA(ctx, owner, name)
		if err != nil || head == "" {
			// Fall back to the newest commit in the window as head.
			head = latestCommitSHA(commits)
		}
		base := oldestCommitSHA(commits)
		if _, cerr := r.createDocumentationTask(ctx, proj, docsRepo, src, base, head); cerr != nil {
			l.Error(cerr, "documentation: enqueue", "action", "scan_enqueue_failed", "resource_id", proj.Name, "repo", src.Name)
		}
	}
}

// oldestCommitSHA / latestCommitSHA pick the window boundary SHAs by commit date
// without assuming the reader's ordering.
func oldestCommitSHA(commits []scm.CommitRef) string {
	oldest := commits[0]
	for _, c := range commits[1:] {
		if c.Date.Before(oldest.Date) {
			oldest = c
		}
	}
	return oldest.SHA
}

func latestCommitSHA(commits []scm.CommitRef) string {
	latest := commits[0]
	for _, c := range commits[1:] {
		if c.Date.After(latest.Date) {
			latest = c
		}
	}
	return latest.SHA
}

// createDocumentationTask enqueues a documentation QueuedEvent repo-scoped to the
// docs repo (documentation is the one repo-scoped agent kind). The source repo +
// its diff window ride as annotations, matching the retired push path's shape so
// the skill contract is unchanged. Model tier (sonnet) comes from the Phase-2
// kindDefaultModel map. dedupKey keys on the source head SHA so a head that has
// not advanced re-collapses to the same event (no duplicate work per window).
func (r *ProjectReconciler) createDocumentationTask(ctx context.Context, proj *tatarav1alpha1.Project, docsRepo, sourceRepo *tatarav1alpha1.Repository, baseSHA, headSHA string) (bool, error) {
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	dedupKey := fmt.Sprintf("doc-%s-%s", sourceRepo.Name, headSHA)
	payload := tatarav1alpha1.QueuedEventPayload{
		Kind: "documentation",
		Goal: fmt.Sprintf("Scheduled documentation sync: %s advanced to %s since the last doc "+
			"update. Review the diff and update the documentation repo if it is doc-relevant; "+
			"no-op otherwise.", sourceRepo.Spec.URL, headSHA),
		RepositoryRef: docsRepo.Name,
		GenerateName:  "documentation-",
		Provider:      provider,
		PodRepo:       docsRepo.Name,
		Labels:        map[string]string{labelActivity: "documentation"},
		Annotations: map[string]string{
			tatarav1alpha1.AnnSourceRepo:    sourceRepo.Spec.URL,
			tatarav1alpha1.AnnSourceBaseSHA: baseSHA,
			tatarav1alpha1.AnnSourceHeadSHA: headSHA,
		},
	}
	_, created, err := queue.EnqueueEvent(ctx, r.Client, r.Seq, proj, tatarav1alpha1.QueueClassNormal, true, dedupKey, payload)
	if err != nil {
		log.FromContext(ctx).Error(err, "scan: enqueue documentation event failed; skipping item", "action", "scan_enqueue_failed", "project", proj.Name)
		return false, nil
	}
	if created {
		log.FromContext(ctx).Info("scan: enqueued documentation",
			"action", "scan_task_created", "resource_id", proj.Name,
			"source_repo", sourceRepo.Name, "docs_repo", docsRepo.Name, "head_sha", headSHA)
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

// labelsColoredAnnotation marks a Project whose managed labels have been colored,
// so the one-shot ensure does not re-issue SCM calls every reconcile.
const labelsColoredAnnotation = "tatara.dev/labels-colored"

// ensureLabelColors best-effort creates/updates the managed tatara labels with
// their colors across the project's repos, once per project (gated by the
// annotation). Failures are logged and tolerated; it never blocks reconcile.
func (r *ProjectReconciler) ensureLabelColors(ctx context.Context, proj *tatarav1alpha1.Project) {
	if proj.Spec.Scm == nil || proj.Annotations[labelsColoredAnnotation] == "true" {
		return
	}
	l := log.FromContext(ctx)
	writer, token, err := r.scanWriter(ctx, proj)
	if err != nil {
		l.Info("ensure label colors: scm writer unavailable (retry next reconcile)",
			"action", "ensure_label_colors", "resource_id", proj.Name, "err", err.Error())
		return
	}
	repos, err := r.projectReposForScan(ctx, proj)
	if err != nil {
		return
	}
	provider := proj.Spec.Scm.Provider
	colors := managedLabelColors(proj.Spec.Scm)
	allOK := true
	for i := range repos {
		for name, color := range colors {
			e := writer.EnsureLabel(ctx, repos[i].Spec.URL, token, name, color)
			RecordSCM(r.Metrics, provider, "ensure_label", e)
			if e != nil {
				allOK = false
				l.Info("ensure label colors: EnsureLabel failed (non-fatal)",
					"action", "ensure_label_colors", "resource_id", proj.Name,
					"repo", repos[i].Name, "label", name, "err", e.Error())
			}
		}
	}
	if !allOK {
		return // retry next reconcile
	}
	patch := client.MergeFrom(proj.DeepCopy())
	if proj.Annotations == nil {
		proj.Annotations = map[string]string{}
	}
	proj.Annotations[labelsColoredAnnotation] = "true"
	if e := r.Patch(ctx, proj, patch); e != nil {
		l.Info("ensure label colors: annotation patch failed (non-fatal)",
			"action", "ensure_label_colors", "resource_id", proj.Name, "err", e.Error())
	}
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
	schedule, last := activityScheduleAndLast(proj, activity)
	base := dueBase(proj, last)
	next, ok := activityNextFire(schedule, base)
	if !ok {
		return base, false, time.Time{}, false
	}
	return base, !time.Now().Before(next), next, true
}

// reposDueForScan returns the repos whose deterministic phase-shifted fire for
// `activity` has occurred since the last project-level scan stamp, plus the
// soonest upcoming per-repo fire (for requeue). ok=false when the schedule is
// empty or malformed. Spreading per-repo fires across the cron interval is the
// fix for the synchronized top-of-hour fan-out that backs up the queue
// (issue #181): the shared project-level stamp still advances on each fire, so
// the (stamp, now] window covers every repo's slot exactly once per period.
func (r *ProjectReconciler) reposDueForScan(proj *tatarav1alpha1.Project, activity string, repos []tatarav1alpha1.Repository, now time.Time) ([]tatarav1alpha1.Repository, time.Time, bool) {
	schedule, last := activityScheduleAndLast(proj, activity)
	if schedule == "" {
		return nil, time.Time{}, false
	}
	sched, err := cron.ParseStandard(schedule)
	if err != nil {
		return nil, time.Time{}, false
	}
	base := dueBase(proj, last)
	period := cronPeriod(sched, base)
	var due []tatarav1alpha1.Repository
	var soonest time.Time
	for i := range repos {
		off := scanOffset(proj.Name, repos[i].Name, activity, period)
		if fire := repoNextFire(sched, off, base); !now.Before(fire) {
			due = append(due, repos[i])
		}
		if nf := repoNextFire(sched, off, now); soonest.IsZero() || nf.Before(soonest) {
			soonest = nf
		}
	}
	// No repos (or all offsets coincided): fall back to the unshifted next fire
	// so an empty project still requeues to the next period instead of busy-looping.
	if soonest.IsZero() {
		soonest = sched.Next(now)
	}
	return due, soonest, true
}

// stampScan records the per-activity Last*Scan and persists status.
// RetryOnConflict handles racing reconcile updates so the stamp always lands.
// Returns non-nil on persistent failure so the caller can log+metric the event.
//
// On success it also sets obs.SweepLastSuccessTimestamp{activity} - the same
// heartbeat gauge sweep.go's B.4 pass sets for sweep/nightlySweep, extended
// to the brainstorm/documentation/issueScan crons. This is the successor for
// tatara_scan_items_total, which the metric-wiring audit (issue #370) pruned
// as dead-per-redesign; TataraLoopStalled's deadman alert and the
// tatara-loop dashboard panel are repointed onto this gauge in the same
// change so a stalled scan cron is still caught. The gauge is process-local
// and resets on every redeploy, so runScans also rehydrates it from the
// persisted Status.Last* stamps at the top of every reconcile (fix #386) -
// this stamp-on-success path is what advances it between rehydrates, not
// the only way it ever gets set.
func (r *ProjectReconciler) stampScan(ctx context.Context, proj *tatarav1alpha1.Project, activity string) error {
	now := metav1.Now()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Project{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}, fresh); err != nil {
			return err
		}
		switch activity {
		case "issueScan":
			fresh.Status.LastIssueScan = &now
			proj.Status.LastIssueScan = &now
		case "brainstorm":
			fresh.Status.LastBrainstorm = &now
			proj.Status.LastBrainstorm = &now
		case "documentation":
			fresh.Status.LastDocumentation = &now
			proj.Status.LastDocumentation = &now
		}
		return r.Status().Update(ctx, fresh)
	})
	if err != nil {
		return err
	}
	obs.SweepLastSuccessTimestamp.WithLabelValues(proj.Name, activity).Set(float64(now.Unix()))
	return nil
}

// brainstorm runs one brainstorm refill decision at PROJECT scope. trigger is
// TriggerCron (the schedule backstop, which also resets the skip breaker) or
// TriggerEvent (a maintainer verdict landed on a proposal Issue). It returns
// whether a brainstorm QueuedEvent was created.
//
// The backlog level is read from Issue CRs in etcd, never from the forge: see
// proposalPending. Concurrency is unchanged at one brainstorm Task per project -
// only the TRIGGER and the QUOTA changed. BrainstormActivity.MaxPerCycle stays
// deprecated and ignored.
func (r *ProjectReconciler) brainstorm(ctx context.Context, proj *tatarav1alpha1.Project,
	reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task,
	act tatarav1alpha1.BrainstormActivity, trigger string) bool {

	l := log.FromContext(ctx)
	start := time.Now()

	issues, err := r.projectProposalIssues(ctx, proj)
	if err != nil {
		l.Error(err, "brainstorm: list proposal issues",
			"action", "scan_brainstorm_error", "resource_id", proj.Name, "trigger", trigger)
		return false
	}
	// The bot login is the authorship anchor the forgeable body-marker fallback
	// in proposalPending requires. Resolved ONCE per pass, from the Project we
	// already hold: no extra read, no SCM call.
	botLogin := botLoginOf(proj)
	pending := pendingProposalCount(issues, botLogin)
	// operator_open_proposals is labelled by the owner/name SLUG, which is what
	// the tatara-observability dashboard joins on - NOT by the Repository CR name
	// pendingProposalCountByRepo keys on (DNS-1123, so it can never contain "/").
	// Translating here keeps the series identity stable across this change.
	//
	// Every enrolled repo is written every pass, zeros included: the map only
	// carries nonzero counts, so a repo whose proposals were all approved would
	// otherwise drop out and latch its last nonzero value forever.
	byRepoRef := pendingProposalCountByRepo(issues, botLogin)
	for i := range repos {
		if slug := repoSlug(&repos[i]); slug != "" {
			r.Metrics.SetOpenProposals(slug, float64(byRepoRef[repos[i].Name]))
		}
	}

	// The in-flight guard doubles as the read-your-writes ledger: a reconcile
	// storm between "Task created" and "issues filed" sees inflight == 1 and
	// cannot double-spawn.
	inflight := 0
	if brainstormInFlightProject(existing) {
		inflight = 1
	}

	target := act.ResolveTarget()
	deficit := brainstormDeficit(target, pending, inflight)
	quota, refill, reason := brainstormRefillDecision(act, pending, inflight,
		proj.Status.BrainstormConsecutiveSkips, trigger)

	if !refill {
		// O6 review Important 2: the EVENT trigger runs this decision on every
		// reconcile of a brainstorm-enabled project (tens-of-seconds cadence),
		// not just on a due cron tick like the pre-O6 code - so a healthy
		// at-target project logging this "no refill" line at INFO would emit it
		// continuously, the exact anti-pattern O4's review already deleted from
		// projectProposalIssues. The CRON path still fires at most once per
		// schedule period, so it keeps the INFO level; the EVENT path drops to
		// V(1) (debug).
		logSkip := l.Info
		if trigger == TriggerEvent {
			logSkip = l.V(1).Info
		}
		logSkip("brainstorm: no refill this pass",
			"action", "scan_brainstorm_skipped", "resource_id", proj.Name,
			"target", target, "pending", pending, "inflight", inflight,
			"deficit", deficit, "trigger", trigger, "reason", reason,
			"consecutive_skips", proj.Status.BrainstormConsecutiveSkips)
		return false
	}

	// Deterministic repo order: sort by name, first valid slug wins.
	sortedRepos := make([]tatarav1alpha1.Repository, len(repos))
	copy(sortedRepos, repos)
	sort.Slice(sortedRepos, func(i, j int) bool { return sortedRepos[i].Name < sortedRepos[j].Name })

	created := r.runProjectScopedProposalCycle(ctx, proj, reader, sortedRepos,
		"brainstorm", "scan_brainstorm", act.Sources, quota)
	if !created {
		// Decided to refill, nothing enqueued: no valid repos, an enqueue error,
		// or the dedup key already holds a queued event. All three are logged in
		// place; this line is what ties them back to the decision that led here.
		l.Info("brainstorm: refill decided but no event enqueued",
			"action", "scan_brainstorm_not_enqueued", "resource_id", proj.Name,
			"target", target, "pending", pending, "inflight", inflight,
			"deficit", deficit, "quota", quota, "trigger", trigger)
		return false
	}
	l.Info("brainstorm: refill dispatched",
		"action", "scan_brainstorm", "resource_id", proj.Name,
		"target", target, "pending", pending, "inflight", inflight,
		"deficit", deficit, "quota", quota, "trigger", trigger,
		"duration_ms", time.Since(start).Milliseconds())
	return true
}

// runProjectScopedProposalCycle resolves per-repo slugs, gathers CI/MR state,
// builds the rich repo-state context, builds the goal, and enqueues the
// brainstorm event. It no longer counts a backlog: the caller owns the control
// law (brainstormRefillDecision) and passes the resolved quota. The former
// shared-with-healthCheck shape (goalBuilder/taskCreator function params plus a
// checkCapFirst order flag) is gone with healthCheck and the cap - one caller
// does not justify two function-valued parameters and an order flag.
//
// The at-cap guard whose ordering against no-valid-repos was caller-specific
// (2026-06-13 flooding incident) no longer exists here: the caller decides
// whether to refill at all BEFORE any repo work, and no-valid-repos is now the
// only post-loop guard, so there is no ordering left to get wrong.
func (r *ProjectReconciler) runProjectScopedProposalCycle(
	ctx context.Context,
	proj *tatarav1alpha1.Project,
	reader scm.SCMReader,
	sortedRepos []tatarav1alpha1.Repository,
	activityLabel, scanAction string,
	sources []string,
	quota int,
) bool {
	l := log.FromContext(ctx)
	issuesBySlug := make(map[string][]scm.IssueRef)
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
			l.Info(activityLabel+": open-issue read failed (non-fatal)",
				"resource_id", proj.Name, "repo", rp.Name, "err", err.Error())
			continue
		}
		issuesBySlug[slug] = iss
	}
	if len(slugs) == 0 {
		l.Info(activityLabel+": no valid repos", "action", scanAction, "resource_id", proj.Name)
		return false
	}

	// Build PR / main-CI data (bounded + non-fatal) for the rich repo-state context.
	prsBySlug, prCIBySlug, mainCIBySlug := r.gatherRepoCIState(ctx, proj, reader, sortedRepos, activityLabel)

	// Build rich context from already-fetched data + bounded MR/main reads.
	issuesCtx := r.buildRepoStateContext(ctx, proj, reader, issuesBySlug, prsBySlug, prCIBySlug, mainCIBySlug, sortedRepos)

	goal := brainstormGoalProject(slugs, issuesCtx, scmGuidance(proj), quota)
	created, err := r.createBrainstormTask(ctx, proj, goal, sources, quota)
	if err != nil {
		l.Error(err, "scan: enqueue "+activityLabel+" event", "resource_id", proj.Name)
		return false
	}
	return created
}

// appendGuidance appends a PROJECT CHARTER block when guidance is non-empty.
func appendGuidance(goal, guidance string) string {
	if strings.TrimSpace(guidance) == "" {
		return goal
	}
	return goal + "\n\nPROJECT CHARTER: " + guidance
}

// scmGuidance returns the Guidance field from a Project's Scm spec, nil-safe.
func scmGuidance(proj *tatarav1alpha1.Project) string {
	if proj.Spec.Scm == nil {
		return ""
	}
	return proj.Spec.Scm.Guidance
}

// brainstormGoalProject returns the turn-0 goal for a project-level brainstorm
// task. repoStateCtx is the rich three-block string built by buildRepoStateContext
// (ISSUES / OPEN MRs / MAIN HEALTH). When empty a fallback note is substituted.
// quota is the resolved per-session proposal allowance; the PROPOSAL QUOTA line
// is the agent-visible half of AnnBrainstormQuota and is a cross-repo interface
// contract: the brainstorm skills quote the phrase "PROPOSAL QUOTA" to find it.
func brainstormGoalProject(slugs []string, repoStateCtx string, guidance string, quota int) string {
	repoList := strings.Join(slugs, ", ")
	stateBlock := "No live repo state available."
	if repoStateCtx != "" {
		stateBlock = repoStateCtx
	}
	goal := fmt.Sprintf("PROPOSAL QUOTA: file AT MOST %d proposal(s) in this session. "+
		"The operator truncates anything beyond %d.\n\n", quota, quota) +
		"Invoke the `tatara-council-brainstorm` skill FIRST and follow its seven-lens phases in " +
		"order; it owns the whole turn and emits the single terminal action itself (`propose_issue`, or " +
		"`skip_research` when nothing clears the bar or the idea duplicates an open issue), grounded per " +
		"the `tatara-code-quality-proposal` skill.\n\n" +
		"HANDOFF CONTINUATION (do this FIRST): call `list_handoffs` for this project. For each open handoff that " +
		"still describes live, unfinished work, call `get_handoff` and propose continuing it (a `propose_issue` framed " +
		"as resuming that work) before generating fresh ideas. Skip stale/superseded/delivered handoffs. Continuation " +
		"proposals count against the same quota as fresh ideas.\n\n" +
		"MANDATE: propose the highest-leverage code-quality, simplification, or robustness improvement across ALL " +
		"repositories: " + repoList + ". Ground every claim in REAL code.\n\n" +
		"READ REAL CODE (two signals, use both): (1) every listed repo is shallow-cloned read-only into " +
		"`workspace/<owner>/<repo>` - open the actual source, configs, and tests; (2) the code-graph MCP tools " +
		"(`code_search`, `code_explain`, `code_related`, `code_important`, `code_cross_repo`, `code_bridges`, " +
		"`code_communities`) index every enrolled repo - use them for the whole-project map, then open the on-disk " +
		"files they point at to confirm before proposing. See the `tatara-code-quality-proposal` skill.\n\n" +
		stateBlock + "\n\n" +
		"EARLY EXIT (do this FIRST, cheaply): scan the ISSUES / OPEN MRs / MAIN HEALTH state above. If nothing clears " +
		"the bar for a genuinely novel, high-leverage proposal this cycle, call `skip_research(reason)` and STOP. " +
		"Silence over noise.\n\n" +
		"NEW-IDEAS-ONLY CONTRACT - follow exactly ONE path:\n" +
		"1. If the best idea DUPLICATES an existing open issue above: do NOT propose. Finish with a one-line note " +
		"naming the duplicate. Do NOT comment on it.\n" +
		"2. If genuinely novel: call `propose_issue`. Set `repo` to the owning repository. Required " +
		"body shape: (a) a one-paragraph problem statement citing the concrete file/symbol you read; (b) a " +
		"DECOMPOSITION into sub-problems; (c) for EACH sub-problem, 2-3 concrete OPTIONS with one-line tradeoffs and " +
		"your recommended pick; (d) the maintainer's decision framed as choosing one option per sub-problem. No flat " +
		"list of open questions.\n\n" +
		"ACTION RULE: emit exactly ONE `submit_outcome`, carrying between 1 and your quota of proposals. Each " +
		"proposal becomes its own issue and its own clarify Task; there is no umbrella and no systemic group. " +
		"State which scope you chose before executing. You are a read-only proposer: never implement, never push, " +
		"never open a PR."
	return appendGuidance(goal+promptguidance.PlatformProblemGuidance+promptguidance.ToolingNoteGuidance, guidance)
}

// gatherRepoCIState fetches open PRs, per-PR CI (bounded to the first 20 PRs),
// and main-branch CI for each repo in sortedRepos. activity is a log prefix
// ("brainstorm" or "healthCheck"). For GitLab repos the CI owner is the full
// project path (URL-encoded by the gitlab client), matching the pattern already
// used by lifecycle.go for main-CI and GetCommitCIStatus. All errors are
// non-fatal; missing data degrades to empty/unknown in the returned maps.
func (r *ProjectReconciler) gatherRepoCIState(
	ctx context.Context,
	proj *tatarav1alpha1.Project,
	reader scm.SCMReader,
	sortedRepos []tatarav1alpha1.Repository,
	activity string,
) (prsBySlug map[string][]scm.PRRef, prCIBySlug map[string]map[int]string, mainCIBySlug map[string]string) {
	l := log.FromContext(ctx)
	prsBySlug = map[string][]scm.PRRef{}
	prCIBySlug = map[string]map[int]string{}
	mainCIBySlug = map[string]string{}
	isGitLab := proj.Spec.Scm != nil && proj.Spec.Scm.Provider == "gitlab"
	for i := range sortedRepos {
		rp := &sortedRepos[i]
		slug := repoSlug(rp)
		if slug == "" {
			continue
		}
		owner, name, err := scm.OwnerRepo(rp.Spec.URL)
		if err != nil {
			continue
		}
		// Resolve provider-correct owner/repo for CI lookups.
		ciOwner, ciRepo := owner, name
		if isGitLab {
			if pp, perr := scm.GitLabProjectPath(rp.Spec.URL); perr == nil {
				ciOwner = pp
				ciRepo = ""
			}
		}
		if prs, perr := reader.ListOpenPRs(ctx, owner, name); perr == nil {
			prsBySlug[slug] = prs
			ci := map[int]string{}
			const prCILimit = 20
			for j, pr := range prs {
				if j >= prCILimit {
					break
				}
				if pr.HeadSHA != "" {
					if st, serr := reader.GetCommitCIStatus(ctx, ciOwner, ciRepo, pr.HeadSHA); serr == nil {
						ci[pr.Number] = st
					}
				}
			}
			prCIBySlug[slug] = ci
		} else {
			l.Info(activity+": list open PRs failed (non-fatal)", "resource_id", proj.Name, "repo", rp.Name, "err", perr.Error())
		}
		if sha, serr := reader.GetDefaultBranchHeadSHA(ctx, ciOwner, ciRepo); serr == nil && sha != "" {
			if st, cerr := reader.GetCommitCIStatus(ctx, ciOwner, ciRepo, sha); cerr == nil {
				mainCIBySlug[slug] = st
			}
		} else if serr != nil {
			l.Info(activity+": main head sha failed (non-fatal)", "resource_id", proj.Name, "repo", rp.Name, "err", serr.Error())
		}
	}
	return
}

// buildRepoStateContext builds the rich context string embedded in the brainstorm
// / healthCheck goal. It emits three blocks: ISSUES (pre-fetched, cap 60),
// OPEN MRs (from prsBySlug, cap 40, per-PR CI from prCIBySlug), and MAIN HEALTH
// (one line per repo from mainCIBySlug). All maps are caller-built and may be nil
// (degrade gracefully).
const maxIssuesContext = 60
const maxMRsContext = 40

func (r *ProjectReconciler) buildRepoStateContext(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, issuesBySlug map[string][]scm.IssueRef, prsBySlug map[string][]scm.PRRef, prCIBySlug map[string]map[int]string, mainCIBySlug map[string]string, repos []tatarav1alpha1.Repository) string {
	l := log.FromContext(ctx)
	botLogin := ""
	provider := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
		provider = proj.Spec.Scm.Provider
	}

	// ISSUES block.
	var issueLines []string
	issueTotal := 0
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
			if len(issueLines) >= maxIssuesContext {
				issueTotal++
				continue
			}
			issueTotal++
			labels := strings.Join(iss.Labels, ",")
			title := strings.ReplaceAll(strings.ReplaceAll(iss.Title, "\n", " "), "\r", "")
			line := fmt.Sprintf("%s#%d [%s] %s", slug, iss.Number, labels, title)
			if botCommentedOnIssue(ctx, reader, owner, name, iss.Number, botLogin) {
				line += " [bot-engaged]"
			}
			issueLines = append(issueLines, line)
		}
	}
	omitted := issueTotal - len(issueLines)
	issuesBlock := strings.Join(issueLines, "\n")
	if omitted > 0 {
		issuesBlock += fmt.Sprintf("\n(+%d more omitted)", omitted)
		l.Info("brainstorm: buildRepoStateContext: capped issues context",
			"shown", len(issueLines), "omitted", omitted)
	}

	// OPEN MRs block: provider-correct separator (! for gitlab, # for github).
	mrSep := "#"
	if provider == "gitlab" {
		mrSep = "!"
	}
	var mrLines []string
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		slug := owner + "/" + name
		prs := prsBySlug[slug]
		ciMap := prCIBySlug[slug]
		for _, pr := range prs {
			if len(mrLines) >= maxMRsContext {
				break
			}
			ciStatus := "unknown"
			if ciMap != nil {
				if st, ok := ciMap[pr.Number]; ok && st != "" {
					ciStatus = st
				}
			}
			title := ""
			if pr.Body != "" {
				title = firstLine(pr.Body)
			}
			mrLines = append(mrLines, fmt.Sprintf("%s%s%d [ci:%s] %s", slug, mrSep, pr.Number, ciStatus, title))
		}
	}

	// MAIN HEALTH block: one line per repo.
	var healthLines []string
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		slug := owner + "/" + name
		status := "unknown"
		if mainCIBySlug != nil {
			if st, ok := mainCIBySlug[slug]; ok && st != "" {
				status = st
			}
		}
		healthLines = append(healthLines, fmt.Sprintf("%s main CI: %s", slug, status))
	}

	var sb strings.Builder
	sb.WriteString("ISSUES:\n")
	if issuesBlock != "" {
		sb.WriteString(issuesBlock)
	}
	sb.WriteString("\n\nOPEN MRs:\n")
	sb.WriteString(strings.Join(mrLines, "\n"))
	sb.WriteString("\n\nMAIN HEALTH:\n")
	sb.WriteString(strings.Join(healthLines, "\n"))
	return sb.String()
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
		t := &existing[i]
		if t.Labels[labelActivity] == "brainstorm" && !tatarav1alpha1.TaskDone(t) {
			return true
		}
	}
	return false
}

// documentationInFlightProject reports whether ANY live documentation Task
// exists in the project. The overlap guard for the doc-sync cron: a parked or
// failed doc Task counts as finished (the reaper collects it; the next tick
// mints a fresh one).
func documentationInFlightProject(existing []tatarav1alpha1.Task) bool {
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelActivity] == "documentation" && !tatarav1alpha1.TaskDone(t) {
			return true
		}
	}
	return false
}

// createRefineTask enqueues a project-scoped refine QueuedEvent.
// Returns created=true when a new event was enqueued.
func (r *ProjectReconciler) createRefineTask(ctx context.Context, proj *tatarav1alpha1.Project, goal string) (bool, error) {
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	dedupKey := "refine-" + proj.Name
	payload := tatarav1alpha1.QueuedEventPayload{
		Kind:         "refine",
		Goal:         goal,
		Labels:       map[string]string{labelActivity: "refine"},
		GenerateName: "refine-",
		Provider:     provider,
		PodRepo:      "",
	}
	_, created, err := queue.EnqueueEvent(ctx, r.Client, r.Seq, proj, tatarav1alpha1.QueueClassNormal, true, dedupKey, payload)
	if err != nil {
		log.FromContext(ctx).Error(err, "scan: enqueue refine event failed; skipping", "action", "scan_enqueue_failed", "project", proj.Name)
		return false, nil
	}
	return created, nil
}

// inflightRefineTask returns the first non-terminal refine Task for the project,
// or nil when no such task exists.
func (r *ProjectReconciler) inflightRefineTask(ctx context.Context, proj *tatarav1alpha1.Project) (*tatarav1alpha1.Task, error) {
	var list tatarav1alpha1.TaskList
	if err := r.List(ctx, &list, client.InNamespace(proj.Namespace)); err != nil {
		return nil, err
	}
	for i := range list.Items {
		t := &list.Items[i]
		if t.Spec.ProjectRef != proj.Name || t.Spec.Kind != "refine" {
			continue
		}
		if !tatarav1alpha1.TaskDone(t) {
			return t, nil
		}
	}
	return nil, nil
}

// latestTerminalRefineTask returns the most recently created terminal refine
// Task for the project that was created at/after since (the current cycle's
// due-base), or nil if none exist. Scoping to since prevents a terminal
// refine Task from a past cycle (still around because TaskRetention has not
// GC'd it yet) from satisfying the barrier for every cycle until it expires;
// each brainstorm cycle must be satisfied by a refine Task from that cycle.
func (r *ProjectReconciler) latestTerminalRefineTask(ctx context.Context, proj *tatarav1alpha1.Project, since time.Time) (*tatarav1alpha1.Task, error) {
	var list tatarav1alpha1.TaskList
	if err := r.List(ctx, &list, client.InNamespace(proj.Namespace)); err != nil {
		return nil, err
	}
	var latest *tatarav1alpha1.Task
	for i := range list.Items {
		t := &list.Items[i]
		if t.Spec.ProjectRef != proj.Name || t.Spec.Kind != "refine" {
			continue
		}
		if !tatarav1alpha1.TaskDone(t) {
			continue
		}
		if t.CreationTimestamp.Time.Before(since) {
			continue
		}
		if latest == nil || t.CreationTimestamp.After(latest.CreationTimestamp.Time) {
			latest = t
		}
	}
	return latest, nil
}

// refineNeededThisCycle reports whether the project needs a refine run this
// cycle. Returns true when LastRefine is nil or was set before the earliest
// due-activity base time (meaning the refine stamp predates the current cycle).
func (r *ProjectReconciler) refineNeededThisCycle(proj *tatarav1alpha1.Project, earliestDueBase time.Time) bool {
	if proj.Status.LastRefine == nil {
		return true
	}
	return proj.Status.LastRefine.Before(&metav1.Time{Time: earliestDueBase})
}

// stampRefine records LastRefine on the project status.
func (r *ProjectReconciler) stampRefine(ctx context.Context, proj *tatarav1alpha1.Project) error {
	now := metav1.Now()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Project{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}, fresh); err != nil {
			return err
		}
		fresh.Status.LastRefine = &now
		proj.Status.LastRefine = &now
		return r.Status().Update(ctx, fresh)
	})
}

// projectRepoSlugs returns owner/repo slugs for all repositories in the project.
func (r *ProjectReconciler) projectRepoSlugs(ctx context.Context, proj *tatarav1alpha1.Project, repos []tatarav1alpha1.Repository) []string {
	var slugs []string
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		slugs = append(slugs, owner+"/"+name)
	}
	return slugs
}

// earliestIssueScanFire computes the TRUE next-expected issueScan/sweep fire:
// the earliest, across every enrolled repo, of that repo's phase-shifted fire -
// the exact scanOffset/repoNextFire arithmetic reposDueForScan itself drives
// issueScan/sweep through (issue #181's fan-out spread: each repo fires at
// sched.Next(base-offset)+offset, offset = a deterministic hash in [0, period)
// of (project, repo, activity), not at the raw unshifted cron boundary).
// nextExpectedUnix's plain sched.Next(base) is correct for brainstorm and
// documentation, which run through activityDue with no per-repo offset, but it
// is NOT when issueScan/sweep runs - a project whose minimum hashed offset
// exceeds the consumer's grace period would otherwise false-page every cycle
// (review finding, tatara-observability#65 follow-up).
//
// ok=false, no series to publish, in the same three cases nextExpectedUnix
// covers plus one: empty (disabled) schedule; unparseable schedule (cronErr
// non-nil, so the caller can meter invalid_cron - the only ok=false case that
// is an error); and zero enrolled repos, where a valid cron simply has no
// repo to ever fire for. That third case is new here (nextExpectedUnix has no
// repo-shaped input to be missing) and is deliberately NOT metered as
// invalid_cron: an empty repo list is a configuration state, not a broken one.
func earliestIssueScanFire(proj *tatarav1alpha1.Project, schedule string, repos []tatarav1alpha1.Repository, last *metav1.Time) (value float64, ok bool, cronErr error) {
	if schedule == "" {
		return 0, false, nil
	}
	sched, err := cron.ParseStandard(schedule)
	if err != nil {
		return 0, false, err
	}
	if len(repos) == 0 {
		return 0, false, nil
	}
	base := dueBase(proj, last)
	period := cronPeriod(sched, base)
	var earliest time.Time
	for i := range repos {
		off := scanOffset(proj.Name, repos[i].Name, "issueScan", period)
		if fire := repoNextFire(sched, off, base); earliest.IsZero() || fire.Before(earliest) {
			earliest = fire
		}
	}
	return float64(earliest.Unix()), true, nil
}

// publishNextExpected republishes obs.SweepNextExpectedTimestamp for every
// ENABLED activity of proj, and meters invalid_cron for an unparseable one. It is
// the single site that meters invalid_cron for a cron activity: the three
// per-activity branches in runScans keep their "invalid cron, disabling" logs but
// no longer double-count.
//
// Enablement is re-checked exactly as runScans checks it below. An activity that
// is configured but switched off must publish NO series - a next-expected-run for
// a run that is never going to happen is the false page this metric exists to
// remove. That covers the never-enabled case; it is not sufficient on its own
// for the enabled-then-disabled transition, because a GaugeVec child, once
// created, stays exported at its last value for the life of the process -
// disabling brainstorm, clearing the sweep-disabled annotation on, or clearing
// Documentation.Repo would otherwise leave a frozen timestamp that reads as
// ever-more-overdue and false-pages forever (review finding). Every branch below
// therefore has an explicit else that actively retracts its series via
// DeleteLabelValues, the same idiom used at
// internal/obs/task_metrics.go:DeleteTaskSeries and
// internal/obs/merge_metrics.go:ClearMergeCursorStalled.
//
// reposLoaded distinguishes "runScans successfully listed the enrolled repos
// this tick" from "it never got that far" (scanReader/projectReposForScan
// failure): the two look identical as an empty repos slice, but they must NOT
// be treated the same (review finding, round 2). A genuinely repo-less
// Project is a configuration state that should retract the issueScan/sweep
// series; a Project whose reader/repo-list call is broken (e.g. a deleted
// scmSecretRef) is exactly the case this gauge exists to keep paging on -
// retracting there would silence the alert on every tick the sweep cannot
// even start, which is a regression from "pages correctly on a stale
// boundary" to "never pages again" for the one scenario that matters most.
// When reposLoaded is false the issueScan/sweep branch is skipped entirely:
// neither published nor retracted, so the last real value (or lack of one)
// stands and ages normally toward the alert's threshold.
//
// runScans defers this so it runs on every exit path AND reads the Status.Last*
// stamps stampScan has already advanced in place this tick. Publishing at the top
// instead would leave the gauge stale for up to maxScheduleRequeue (6h), longer
// than the alert's 3h grace.
func (r *ProjectReconciler) publishNextExpected(proj *tatarav1alpha1.Project, repos []tatarav1alpha1.Repository, reposLoaded bool) {
	c := proj.Spec.Scm.Cron

	switch {
	case !SweepEnabled(proj):
		obs.SweepNextExpectedTimestamp.DeleteLabelValues(proj.Name, "issueScan")
		obs.SweepNextExpectedTimestamp.DeleteLabelValues(proj.Name, SweepActivity)
	case !reposLoaded:
		// Repo list unknown this tick: leave whatever was already published.
	default:
		if next, ok, cronErr := earliestIssueScanFire(proj, c.IssueScan.Schedule, repos, proj.Status.LastIssueScan); ok {
			obs.SweepNextExpectedTimestamp.WithLabelValues(proj.Name, "issueScan").Set(next)
			obs.SweepNextExpectedTimestamp.WithLabelValues(proj.Name, SweepActivity).Set(next)
		} else {
			obs.SweepNextExpectedTimestamp.DeleteLabelValues(proj.Name, "issueScan")
			obs.SweepNextExpectedTimestamp.DeleteLabelValues(proj.Name, SweepActivity)
			if cronErr != nil {
				obs.SweepErrorsTotal.WithLabelValues(proj.Name, "issueScan", "invalid_cron").Inc()
			}
		}
	}

	if c.Brainstorm.Enabled {
		if next, ok := nextExpectedUnix(proj, c.Brainstorm.Schedule, proj.Status.LastBrainstorm); ok {
			obs.SweepNextExpectedTimestamp.WithLabelValues(proj.Name, "brainstorm").Set(next)
		} else {
			obs.SweepNextExpectedTimestamp.DeleteLabelValues(proj.Name, "brainstorm")
			if c.Brainstorm.Schedule != "" {
				obs.SweepErrorsTotal.WithLabelValues(proj.Name, "brainstorm", "invalid_cron").Inc()
			}
		}
	} else {
		obs.SweepNextExpectedTimestamp.DeleteLabelValues(proj.Name, "brainstorm")
	}

	if doc := proj.Spec.Documentation; doc != nil && doc.Enabled && doc.Repo != "" {
		if next, ok := nextExpectedUnix(proj, c.Documentation.Schedule, proj.Status.LastDocumentation); ok {
			obs.SweepNextExpectedTimestamp.WithLabelValues(proj.Name, "documentation").Set(next)
		} else {
			obs.SweepNextExpectedTimestamp.DeleteLabelValues(proj.Name, "documentation")
			if c.Documentation.Schedule != "" {
				obs.SweepErrorsTotal.WithLabelValues(proj.Name, "documentation", "invalid_cron").Inc()
			}
		}
	} else {
		obs.SweepNextExpectedTimestamp.DeleteLabelValues(proj.Name, "documentation")
	}
}

// runScans runs each due activity and returns the soonest next-fire as a
// requeue duration, plus the repos/existing-tasks/reader it fetched along the
// way (nil on any early-return path that never reached the fetch). Cron
// parsing/SCM/create failures are logged and skipped per activity so one bad
// activity never blocks the others or crashes the reconciler. A caller that
// needs the SAME scan-scoped data this pass - the O6 event-driven refill pass
// in Reconcile - reuses these return values instead of re-listing/
// re-resolving them a second time this reconcile (O6 review Minor 3).
func (r *ProjectReconciler) runScans(ctx context.Context, proj *tatarav1alpha1.Project) (time.Duration, []tatarav1alpha1.Repository, []tatarav1alpha1.Task, scm.SCMReader, error) {
	l := log.FromContext(ctx)
	if proj.Spec.Scm == nil || proj.Spec.Scm.Cron == nil || r.ReaderFor == nil {
		// spec.scm and spec.scm.cron are +optional pointers: a Project that had
		// published next-expected series while cron was configured and then has
		// either cleared can no longer compute a next fire for any activity, but
		// this guard returns BEFORE the publishNextExpected defer below is
		// registered, so without an explicit retraction here the series freeze
		// at their last value forever - the consumer is a time()-minus-gauge
		// alert, so a frozen series ages toward "ever more overdue" and never
		// stops paging. Same failure mode already closed for the
		// disable-via-annotation transition (publishNextExpected's own else
		// branches) and for Project deletion (project_controller.go's
		// IsNotFound branch); this mirrors that precedent's DeletePartialMatch
		// rather than restructuring publishNextExpected (which dereferences
		// proj.Spec.Scm.Cron and cannot run before this guard without its own
		// nil-guard rewrite, review finding).
		obs.SweepNextExpectedTimestamp.DeletePartialMatch(prometheus.Labels{"project": proj.Name})
		return 0, nil, nil, nil, nil
	}
	// repos is populated below (after the reader/list-repos calls) but must be
	// in scope for the defer now: publishNextExpected's issueScan/sweep branch
	// needs the enrolled repo list to compute the true phase-shifted next fire
	// (review finding: the raw unshifted cron boundary is not when issueScan
	// actually runs). Captured by the closure, not passed by value, so the
	// defer sees whatever repos/reposLoaded hold at return time.
	//
	// reposLoaded is set true ONLY after projectReposForScan actually succeeds
	// (below). It stays false on the two pre-list early returns - a
	// scanReader failure or a projectReposForScan failure - so
	// publishNextExpected can tell "genuinely zero repos" (retract) apart from
	// "repos never fetched this tick" (leave the existing series alone; round-2
	// review finding: retracting there silenced the alert on exactly the
	// tick the sweep is broken, e.g. a deleted scmSecretRef, since
	// scanReader's failure path returns a requeue with a NIL error - no other
	// metric or reconcile-error signal fires).
	var repos []tatarav1alpha1.Repository
	var reposLoaded bool
	defer func() { r.publishNextExpected(proj, repos, reposLoaded) }()

	// Rehydrate the sweep heartbeat gauge from the persisted Status.Last* stamps
	// on every reconcile (fix #386): obs.SweepLastSuccessTimestamp is process-
	// local and resets to unset on every pod redeploy, while these stamps are
	// etcd-backed and survive it. Without this, a redeploy that lands faster
	// than the next due activity leaves the gauge at NoData even though the
	// project is scanning fine, and TataraLoopStalled's alertOnNoData fires a
	// false positive. A never-scanned project (nil stamp) is correctly left
	// unset - that IS true NoData.
	//
	// Every write here is per-PROJECT (issue #441). This block runs on EVERY
	// Project's reconcile, so before `project` joined the label set the three
	// Projects overwrote one series in turn.
	if proj.Status.LastIssueScan != nil {
		ts := float64(proj.Status.LastIssueScan.Unix())
		obs.SweepLastSuccessTimestamp.WithLabelValues(proj.Name, "issueScan").Set(ts)
		obs.SweepLastSuccessTimestamp.WithLabelValues(proj.Name, SweepActivity).Set(ts)
	}
	if proj.Status.LastBrainstorm != nil {
		obs.SweepLastSuccessTimestamp.WithLabelValues(proj.Name, "brainstorm").Set(float64(proj.Status.LastBrainstorm.Unix()))
	}
	if proj.Status.LastDocumentation != nil {
		obs.SweepLastSuccessTimestamp.WithLabelValues(proj.Name, "documentation").Set(float64(proj.Status.LastDocumentation.Unix()))
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
		return maxScheduleRequeue, nil, nil, nil, nil
	}
	var err error
	repos, err = r.projectReposForScan(ctx, proj)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	reposLoaded = true
	existing, err := r.existingScanTasks(ctx, proj)
	if err != nil {
		return 0, nil, nil, nil, err
	}

	// THE B.4 SWEEP. Since the Task 20 cutover it is the ONLY intake: issueScan,
	// mrScan and the backstop are gone, and with them every issueLifecycle
	// producer. It runs on the issueScan cadence (the old intake cadence) so the
	// forge-request rate is unchanged. Its errors are logged and metered by
	// SweepProject itself.
	if SweepEnabled(proj) {
		if dueRepos, soonestSweep, ok := r.reposDueForScan(proj, "issueScan", repos, now); ok {
			if len(dueRepos) > 0 {
				if serr := r.SweepProject(ctx, proj, reader, dueRepos, nil, SweepActivity); serr != nil {
					l.Error(serr, "scan: sweep failed",
						"action", "scan_sweep_error", "resource_id", proj.Name, "activity", SweepActivity)
				}
				if serr := r.stampScan(ctx, proj, "issueScan"); serr != nil {
					l.Error(serr, "scan: persist sweep stamp failed",
						"action", "scan_stamp_error", "resource_id", proj.Name, "activity", SweepActivity)
					obs.SweepErrorsTotal.WithLabelValues(proj.Name, "issueScan", "stamp_failed").Inc()
				}
				if _, next2, ok2 := r.reposDueForScan(proj, "issueScan", repos, now); ok2 {
					consider(next2)
				}
			} else {
				consider(soonestSweep)
			}
		} else if cronSpec.IssueScan.Schedule != "" {
			l.Error(fmt.Errorf("invalid cron %q", cronSpec.IssueScan.Schedule), "scan: invalid issueScan cron, disabling",
				"action", "scan_cron_invalid", "resource_id", proj.Name, "activity", "issueScan")
			// invalid_cron is metered once per tick by publishNextExpected (deferred above).
		}
	}

	// brainstorm (opt-in), gated by the refine pre-scan barrier: a due
	// brainstorm tick first ensures the project refiner has run this cycle
	// (grooming the backlog + handoffs) before brainstorm executes. "This
	// cycle" = LastRefine is nil or precedes the brainstorm due-base time. The
	// barrier defers ONLY brainstorm - mrScan/issueScan/healthCheck run on
	// their own schedules regardless. Both Succeeded and Failed refine release
	// the gate so a broken refine never wedges brainstorm.
	//
	// O6 REVIEW, Important 1: this refine barrier runs UNCONDITIONALLY of the
	// eventual refill decision - it fires (and, when needed, mints a refine
	// Task) the moment the brainstorm schedule is due, BEFORE brainstorm() is
	// even called below, so it never learns nor cares whether the backlog
	// still has a deficit. That independence is load-bearing now that O6's
	// event-driven refill path exists: the event path deliberately BYPASSES
	// this barrier entirely (re-running refine per maintainer verdict was
	// rejected as too expensive - approved design, not an oversight), which
	// means the CRON tick above is refine's ONLY trigger. If this barrier were
	// ever made conditional on brainstorm's own deficit (e.g. skipped
	// whenever the event path has already kept the project at target),
	// refine would silently stop running for any healthy, well-served
	// project - see TestRefineBarrierRunsEvenWhenBrainstormBacklogIsAtTarget,
	// which pins exactly this: a refine Task still gets created on a due cron
	// tick even with TargetOpenProposals=0, where brainstorm() can never
	// decide to refill.
	if cronSpec.Brainstorm.Enabled {
		if base, due, next, ok := r.activityDue(proj, "brainstorm"); ok {
			if due {
				proceed := true
				if r.refineNeededThisCycle(proj, base) {
					terminal, terr := r.latestTerminalRefineTask(ctx, proj, base)
					if terr != nil {
						l.Error(terr, "scan: check terminal refine task", "action", "scan_refine_error", "resource_id", proj.Name)
						obs.SweepErrorsTotal.WithLabelValues(proj.Name, "brainstorm", "refine_check_failed").Inc()
					}
					if terminal != nil {
						// Stamp LastRefine and fall through to brainstorm.
						if serr := r.stampRefine(ctx, proj); serr != nil {
							l.Error(serr, "scan: stamp LastRefine failed", "action", "scan_stamp_error", "resource_id", proj.Name, "activity", "refine")
						}
					} else {
						// Check or create an in-flight refine task.
						inflight, ierr := r.inflightRefineTask(ctx, proj)
						if ierr != nil {
							l.Error(ierr, "scan: check inflight refine task", "action", "scan_refine_error", "resource_id", proj.Name)
							obs.SweepErrorsTotal.WithLabelValues(proj.Name, "brainstorm", "refine_inflight_check_failed").Inc()
						}
						if inflight == nil {
							slugs := r.projectRepoSlugs(ctx, proj, repos)
							lookback := cronSpec.Refine.ClosedLookbackDays
							if lookback <= 0 {
								lookback = 30
							}
							goal := refine.GoalProject(slugs, lookback)
							_, _ = r.createRefineTask(ctx, proj, goal)
						}
						// Release valve (issue #401): a refine Task that never reaches a
						// terminal stage must not wedge brainstorm - and the LastBrainstorm
						// heartbeat behind it - forever. Past the max hold, proceed anyway;
						// the stuck refine Task is left for the reaper/on-call, surfaced by
						// the metric+log below.
						held := now.Sub(base)
						if held > requeueRefineBarrierMaxHold {
							l.Info("scan: brainstorm refine barrier max-hold exceeded; releasing",
								"action", "scan_refine_barrier_timeout", "resource_id", proj.Name,
								"held_duration", held.String())
							obs.SweepErrorsTotal.WithLabelValues(proj.Name, "brainstorm", "refine_barrier_timeout").Inc()
						} else {
							// Defer brainstorm until refine is terminal; poll at the barrier cadence.
							l.Info("scan: brainstorm deferred by refine barrier",
								"action", "scan_brainstorm_refine_barrier_held", "resource_id", proj.Name,
								"held_duration", held.String())
							obs.SweepErrorsTotal.WithLabelValues(proj.Name, "brainstorm", "refine_barrier_held").Inc()
							proceed = false
							consider(now.Add(requeueRefineBarrier))
						}
					}
				}
				if proceed {
					r.brainstorm(ctx, proj, reader, repos, existing, cronSpec.Brainstorm, TriggerCron)
					// The cron tick is the ONLY thing that re-enables the event-driven
					// refill path after the skip breaker trips. Reset unconditionally,
					// refill or not: a dry spell must cost one session per cron period,
					// never wedge brainstorm permanently.
					if rerr := r.resetBrainstormSkips(ctx, proj); rerr != nil {
						l.Error(rerr, "scan: reset brainstorm skip breaker",
							"action", "scan_stamp_error", "resource_id", proj.Name, "activity", "brainstorm")
						obs.SweepErrorsTotal.WithLabelValues(proj.Name, "brainstorm", "skip_reset_failed").Inc()
					}
					if serr := r.stampScan(ctx, proj, "brainstorm"); serr != nil {
						l.Error(serr, "scan: persist brainstorm stamp failed",
							"action", "scan_stamp_error", "resource_id", proj.Name, "activity", "brainstorm")
						obs.SweepErrorsTotal.WithLabelValues(proj.Name, "brainstorm", "stamp_failed").Inc()
					}
					if next2, ok2 := activityNextFire(cronSpec.Brainstorm.Schedule, now); ok2 {
						consider(next2)
					}
				}
			} else {
				consider(next)
			}
		} else if cronSpec.Brainstorm.Schedule != "" {
			l.Error(fmt.Errorf("invalid cron %q", cronSpec.Brainstorm.Schedule), "scan: invalid brainstorm cron, disabling",
				"action", "scan_cron_invalid", "resource_id", proj.Name, "activity", "brainstorm")
			// invalid_cron is metered once per tick by publishNextExpected (deferred above).
		}
	}

	// documentation (opt-in cron): the scheduled doc-sync tick. Replaces the
	// retired per-merge push trigger. Gated on Spec.Documentation being enabled
	// with a docs repo; each due tick spawns a doc Task per changed source repo
	// and stamps LastDocumentation (advancing the diff window) even when nothing
	// changed, so it does not busy-refire.
	doc := proj.Spec.Documentation
	if cronSpec.Documentation.Schedule != "" && doc != nil && doc.Enabled && doc.Repo != "" {
		if _, due, next, ok := r.activityDue(proj, "documentation"); ok {
			if due {
				// THE B.6 NIGHTLY DOC BATCH (fix W2/F2, USER DECISION). ONE doc Task
				// per project per night covering every delivered Task that still needs
				// documenting - the mechanism that finally stamps status.documentedBy
				// and un-pins delivered Tasks from the reaper. It SUPERSEDES the
				// per-changed-repo documentationScan (design section 9/12, contract
				// CONFLICT 5): MintDocBatch had ZERO production callers, so delivered
				// Tasks were never documented and never collected.
				if derr := r.MintDocBatch(ctx, proj); derr != nil {
					l.Error(derr, "scan: nightly doc batch failed",
						"action", "scan_doc_batch_error", "resource_id", proj.Name, "activity", "documentation")
				}
				if serr := r.stampScan(ctx, proj, "documentation"); serr != nil {
					l.Error(serr, "scan: persist documentation stamp failed",
						"action", "scan_stamp_error", "resource_id", proj.Name, "activity", "documentation")
					obs.SweepErrorsTotal.WithLabelValues(proj.Name, "documentation", "stamp_failed").Inc()
				}
				if next2, ok2 := activityNextFire(cronSpec.Documentation.Schedule, now); ok2 {
					consider(next2)
				}
			} else {
				consider(next)
			}
		} else {
			l.Error(fmt.Errorf("invalid cron %q", cronSpec.Documentation.Schedule), "scan: invalid documentation cron, disabling",
				"action", "scan_cron_invalid", "resource_id", proj.Name, "activity", "documentation")
			// invalid_cron is metered once per tick by publishNextExpected (deferred above).
		}
	}

	return soonest, repos, existing, reader, nil
}
