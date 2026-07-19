package controller

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

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
	base := proj.CreationTimestamp.Time
	if last != nil {
		base = last.Time
	}
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
// change so a stalled scan cron is still caught.
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
	obs.SweepLastSuccessTimestamp.WithLabelValues(activity).Set(float64(now.Unix()))
	return nil
}

// brainstorm runs one brainstorm cycle at PROJECT scope: at most one brainstorm
// QueuedEvent per cycle for the whole project. BrainstormActivity.MaxPerCycle is
// deprecated and ignored; the hard cap of one per cycle is enforced here.
// Concurrency is bounded solely by the dispatcher's QueueCapacity.
func (r *ProjectReconciler) brainstorm(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.BrainstormActivity) {
	l := log.FromContext(ctx)
	start := time.Now()
	maxProp := act.MaxOpenProposals
	if maxProp < 1 {
		maxProp = 10
	}

	// Project-scoped in-flight guard: any non-terminal brainstorm Task blocks.
	if brainstormInFlightProject(existing) {
		l.Info("brainstorm: in-flight project brainstorm task; skipping cycle",
			"action", "scan_brainstorm", "resource_id", proj.Name)
		return
	}

	brainstormingLabel, _, _, _ := lifecycleLabels(proj.Spec.Scm)

	// Deterministic primary repo: sort by name, first valid slug wins.
	sortedRepos := make([]tatarav1alpha1.Repository, len(repos))
	copy(sortedRepos, repos)
	sort.Slice(sortedRepos, func(i, j int) bool {
		return sortedRepos[i].Name < sortedRepos[j].Name
	})

	// no-valid-repos is checked before at-cap here (2026-06-13 flooding-incident
	// ordering).
	r.runProjectScopedProposalCycle(ctx, proj, reader, sortedRepos, existing,
		brainstormingLabel, maxProp, "brainstorm", "scan_brainstorm", start, act.Sources,
		false, brainstormGoalProject, r.createBrainstormTask)
}

// runProjectScopedProposalCycle runs the shared 90%-identical middle of
// brainstorm() and healthCheck(): resolve per-repo slugs, accumulate the
// proposal backlog (SCM issue count),
// gather CI state, build the rich repo-state context, build the activity goal
// text, and create the scan task - emitting the same log fields and metric
// calls both callers previously duplicated.
//
// The two post-loop early-return guards (no-valid-repos / at-cap) are checked
// in checkCapFirst order: brainstorm checks no-valid-repos first, healthCheck
// checks at-cap first. This order is preserved verbatim per caller (do not let
// this helper pick a single order - it touches the 2026-06-13 flooding-
// incident path).
func (r *ProjectReconciler) runProjectScopedProposalCycle(
	ctx context.Context,
	proj *tatarav1alpha1.Project,
	reader scm.SCMReader,
	sortedRepos []tatarav1alpha1.Repository,
	existing []tatarav1alpha1.Task,
	brainstormingLabel string,
	maxProp int,
	activityLabel, scanAction string,
	start time.Time,
	sources []string,
	checkCapFirst bool,
	goalBuilder func(slugs []string, repoStateCtx, guidance string) string,
	taskCreator func(ctx context.Context, proj *tatarav1alpha1.Project, goal string, sources []string) (bool, error),
) {
	l := log.FromContext(ctx)
	issuesBySlug := make(map[string][]scm.IssueRef)
	scmTotal := 0
	scmAtCap := false
	var slugs []string
	for i := range sortedRepos {
		rp := &sortedRepos[i]
		slug := repoSlug(rp)
		if slug == "" {
			continue
		}
		slugs = append(slugs, slug)
		if scmAtCap {
			// SCM backlog already at cap; skip the issue list for remaining repos
			// (best-effort: their per-repo gauge keeps last cycle's value). Still
			// collect slugs for the goal text.
			continue
		}
		owner, name, err := scm.OwnerRepo(rp.Spec.URL)
		if err != nil {
			continue
		}
		iss, err := reader.ListOpenIssues(ctx, owner, name)
		if err != nil {
			l.Info(activityLabel+": backlog count failed (non-fatal)", "resource_id", proj.Name, "repo", rp.Name, "err", err.Error())
			continue
		}
		issuesBySlug[slug] = iss
		backlog := proposalBacklogCount(iss, brainstormingLabel)
		r.Metrics.SetOpenProposals(slug, float64(backlog))
		scmTotal += backlog
		if scmTotal >= maxProp {
			scmAtCap = true
		}
	}
	total := scmTotal
	atCap := total >= maxProp
	noValidRepos := len(slugs) == 0

	noValidReposGuard := func() bool {
		if !noValidRepos {
			return false
		}
		l.Info(activityLabel+": no valid repos", "resource_id", proj.Name)
		return true
	}
	atCapGuard := func() bool {
		if !atCap {
			return false
		}
		l.Info(activityLabel+": project backlog at cap; skipping cycle",
			"action", scanAction, "resource_id", proj.Name, "total", total, "cap", maxProp)
		return true
	}

	if checkCapFirst {
		if atCapGuard() {
			return
		}
		if noValidReposGuard() {
			return
		}
	} else {
		if noValidReposGuard() {
			return
		}
		if atCapGuard() {
			return
		}
	}

	// Build PR / main-CI data (bounded + non-fatal) for the rich repo-state context.
	prsBySlug, prCIBySlug, mainCIBySlug := r.gatherRepoCIState(ctx, proj, reader, sortedRepos, activityLabel)

	// Build rich context from already-fetched data + bounded MR/main reads.
	issuesCtx := r.buildRepoStateContext(ctx, proj, reader, issuesBySlug, prsBySlug, prCIBySlug, mainCIBySlug, sortedRepos)

	goal := goalBuilder(slugs, issuesCtx, scmGuidance(proj))
	_, err := taskCreator(ctx, proj, goal, sources)
	if err != nil {
		l.Error(err, "scan: enqueue "+activityLabel+" event", "resource_id", proj.Name)
		return
	}
	l.Info(activityLabel+" complete", "action", scanAction, "resource_id", proj.Name,
		"picked", 1, "duration_ms", time.Since(start).Milliseconds())
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
func brainstormGoalProject(slugs []string, repoStateCtx string, guidance string) string {
	repoList := strings.Join(slugs, ", ")
	stateBlock := "No live repo state available."
	if repoStateCtx != "" {
		stateBlock = repoStateCtx
	}
	goal := "Invoke the `tatara-council-brainstorm` skill FIRST and follow its seven-lens phases in " +
		"order; it owns the whole turn and emits the single terminal action itself (`propose_issue`, or " +
		"`skip_research` when nothing clears the bar or the idea duplicates an open issue), grounded per " +
		"the `tatara-code-quality-proposal` skill.\n\n" +
		"HANDOFF CONTINUATION (do this FIRST): call `list_handoffs` for this project. For each open handoff that " +
		"still describes live, unfinished work, call `get_handoff` and propose continuing it (a `propose_issue` framed " +
		"as resuming that work) before generating fresh ideas. Skip stale/superseded/delivered handoffs. Continuation " +
		"proposals count against the same MaxOpenProposals cap as fresh ideas.\n\n" +
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
		"SYSTEMIC MANDATE: prefer a single systemic improvement (a pattern spanning >=2 repositories, a platform-wide " +
		"gap, or recurring debt) over a one-repo tweak. Decompose: dispatch one parallel subagent per repository, then " +
		"synthesize one systemic conclusion.\n\n" +
		"NEW-IDEAS-ONLY CONTRACT - follow exactly ONE path:\n" +
		"1. If the best idea DUPLICATES an existing open issue above: do NOT propose. Finish with a one-line note " +
		"naming the duplicate. Do NOT comment on it.\n" +
		"2. If genuinely novel AND standalone: call `propose_issue`. Set `repo` to the owning repository. Required " +
		"body shape: (a) a one-paragraph problem statement citing the concrete file/symbol you read; (b) a " +
		"DECOMPOSITION into sub-problems; (c) for EACH sub-problem, 2-3 concrete OPTIONS with one-line tradeoffs and " +
		"your recommended pick; (d) the maintainer's decision framed as choosing one option per sub-problem. No flat " +
		"list of open questions.\n\n" +
		"ACTION RULE: a one-repo improvement emits exactly ONE propose_issue. A genuinely systemic improvement MAY " +
		"emit one propose_issue per affected repository (bounded: at most 6), all sharing a single `systemicId` you " +
		"generate. State which path and scope you chose before executing. You are a read-only proposer: never " +
		"implement, never push, never open a PR."
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

// proposalBacklogCount counts open, undecided ideas in a pre-fetched issue
// slice: non-PR issues bearing the brainstorming label.
// Issues sharing a tatara/systemic-<id> label count as a single entry so that
// a multi-repo systemic improvement does not inflate the backlog cap.
func proposalBacklogCount(issues []scm.IssueRef, brainstormingLabel string) int {
	const systemicPrefix = "tatara/systemic-"
	groups := map[string]bool{}
	standalone := 0
	for _, iss := range issues {
		if iss.IsPR {
			continue
		}
		if !hasLabel(iss.Labels, brainstormingLabel) {
			continue
		}
		sid := ""
		for _, l := range iss.Labels {
			if strings.HasPrefix(l, systemicPrefix) {
				sid = l
				break
			}
		}
		if sid != "" {
			groups[sid] = true
		} else {
			standalone++
		}
	}
	return standalone + len(groups)
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
		}
	}

	// brainstorm (opt-in), gated by the refine pre-scan barrier: a due
	// brainstorm tick first ensures the project refiner has run this cycle
	// (grooming the backlog + handoffs) before brainstorm executes. "This
	// cycle" = LastRefine is nil or precedes the brainstorm due-base time. The
	// barrier defers ONLY brainstorm - mrScan/issueScan/healthCheck run on
	// their own schedules regardless. Both Succeeded and Failed refine release
	// the gate so a broken refine never wedges brainstorm.
	if cronSpec.Brainstorm.Enabled {
		if base, due, next, ok := r.activityDue(proj, "brainstorm"); ok {
			if due {
				proceed := true
				if r.refineNeededThisCycle(proj, base) {
					terminal, terr := r.latestTerminalRefineTask(ctx, proj, base)
					if terr != nil {
						l.Error(terr, "scan: check terminal refine task", "action", "scan_refine_error", "resource_id", proj.Name)
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
						// Defer brainstorm until refine is terminal; poll at the barrier cadence.
						proceed = false
						consider(now.Add(requeueRefineBarrier))
					}
				}
				if proceed {
					r.brainstorm(ctx, proj, reader, repos, existing, cronSpec.Brainstorm)
					if serr := r.stampScan(ctx, proj, "brainstorm"); serr != nil {
						l.Error(serr, "scan: persist brainstorm stamp failed",
							"action", "scan_stamp_error", "resource_id", proj.Name, "activity", "brainstorm")
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
		}
	}

	return soonest, nil
}
