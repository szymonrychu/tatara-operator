package controller

// Target-backlog brainstorm tests. They define the behaviour the control law
// gives the cycle: the operator refills toward a TARGET number of open
// proposals counted from Issue CRs, stamps the resolved quota on the Task, and
// never reconciles downward by closing anything.

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// emptyReader is a reader whose repos have no open forge issues. The backlog
// level no longer comes from the forge, so the brainstorm tests only need the
// reader for the repo-state context.
func emptyReader(slugs ...string) *perRepoFakeReader {
	byRepo := map[string][]scm.IssueRef{}
	for _, s := range slugs {
		byRepo[s] = nil
	}
	return &perRepoFakeReader{issuesByRepo: byRepo}
}

// seedProposalIssue creates a brainstorm-provenance Issue CR in the project's
// namespace with the given mirrored SCM state and platform decision status.
func seedProposalIssue(t *testing.T, r *ProjectReconciler, proj *tatarav1alpha1.Project,
	repoRef string, number int, kind, state, status string) *tatarav1alpha1.Issue {
	t.Helper()
	ctx := context.Background()
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.IssueName(repoRef, number),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repoRef,
			Number:        number,
			URL:           "https://github.com/o/" + repoRef + "/issues/" + strconv.Itoa(number),
			ProjectRef:    proj.Name,
			ProposalKind:  kind,
		},
	}
	if err := r.Create(ctx, iss); err != nil {
		t.Fatalf("create issue CR: %v", err)
	}
	iss.Status.State, iss.Status.Status = state, status
	if err := r.Status().Update(ctx, iss); err != nil {
		t.Fatalf("seed issue status: %v", err)
	}
	return iss
}

// liveBrainstormTask is a non-terminal brainstorm Task for the in-flight guard.
// It is never persisted: brainstormInFlightProject only inspects the slice.
func liveBrainstormTask(proj *tatarav1alpha1.Project) tatarav1alpha1.Task {
	tk := tatarav1alpha1.Task{}
	tk.Name = "live-brainstorm"
	tk.Namespace = proj.Namespace
	tk.Labels = map[string]string{labelActivity: "brainstorm"}
	tk.Spec = tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Goal: "g", Kind: "brainstorm"}
	tk.Status.State = tatarav1alpha1.StateRefined
	return tk
}

func TestBrainstormGoalCarriesTheQuotaLine(t *testing.T) {
	goal := brainstormGoalProject([]string{"o/r1"}, "STATE", "", 2)
	want := "PROPOSAL QUOTA: file AT MOST 2 proposal(s) in this session."
	if !strings.Contains(goal, want) {
		t.Fatalf("goal does not carry the quota line %q:\n%s", want, goal)
	}
	if strings.Contains(goal, "systemicId") {
		t.Fatal("goal still names systemicId, which submit_outcome no longer accepts")
	}
	if strings.Contains(goal, "MaxOpenProposals") {
		t.Fatal("goal still names the deprecated MaxOpenProposals cap")
	}
}

// The quota rides on the Task in TWO places: the annotation the operator
// truncates against, and the goal line the agent reads.
func TestBrainstormStampsTheQuotaAnnotation(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-tgt-ann", []string{"o/r1"}, ptrInt(3))
	r := newScanReconciler(emptyReader("o/r1"))

	created := r.brainstorm(ctx, proj, emptyReader("o/r1"), repos, nil,
		proj.Spec.Scm.Cron.Brainstorm)
	if !created {
		t.Fatal("want a brainstorm event created with an empty backlog")
	}
	qes := listBrainstormQEs(t, proj.Name)
	if len(qes) != 1 {
		t.Fatalf("want 1 brainstorm QueuedEvent, got %d", len(qes))
	}
	got := qes[0].Spec.Payload.Annotations[tatarav1alpha1.AnnBrainstormQuota]
	if got != strconv.Itoa(3) {
		t.Fatalf("quota annotation = %q, want %q", got, "3")
	}
	if !strings.Contains(qes[0].Spec.Payload.Goal, "AT MOST 3 proposal(s)") {
		t.Fatalf("goal does not carry quota 3:\n%s", qes[0].Spec.Payload.Goal)
	}
}

func TestBrainstormRefillsOnlyTheDeficit(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-tgt-deficit", []string{"o/r1"}, ptrInt(3))
	r := newScanReconciler(emptyReader("o/r1"))
	// Two pending proposal Issue CRs -> deficit 1.
	seedProposalIssue(t, r, proj, "bs-tgt-deficit-r1", 1, "brainstorm", "open", "new")
	seedProposalIssue(t, r, proj, "bs-tgt-deficit-r1", 2, "brainstorm", "open", "new")

	if created := r.brainstorm(ctx, proj, emptyReader("o/r1"), repos, nil,
		proj.Spec.Scm.Cron.Brainstorm); !created {
		t.Fatal("want a brainstorm event created with pending=2 target=3")
	}
	qes := listBrainstormQEs(t, proj.Name)
	if len(qes) != 1 {
		t.Fatalf("want 1 brainstorm QueuedEvent, got %d", len(qes))
	}
	if got := qes[0].Spec.Payload.Annotations[tatarav1alpha1.AnnBrainstormQuota]; got != "1" {
		t.Fatalf("quota annotation = %q, want %q", got, "1")
	}
}

func TestBrainstormApprovedProposalFreesItsSlot(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-tgt-approved", []string{"o/r1"}, ptrInt(1))
	r := newScanReconciler(emptyReader("o/r1"))
	// One approved proposal, forge issue still OPEN. It must NOT count.
	seedProposalIssue(t, r, proj, "bs-tgt-approved-r1", 1, "brainstorm", "open", "approved")

	if created := r.brainstorm(ctx, proj, emptyReader("o/r1"), repos, nil,
		proj.Spec.Scm.Cron.Brainstorm); !created {
		t.Fatal("an approved proposal must free its slot and allow a refill")
	}
}

func TestBrainstormOverTargetCreatesNothingAndClosesNothing(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-tgt-over", []string{"o/r1"}, ptrInt(1))
	r := newScanReconciler(emptyReader("o/r1"))
	for _, num := range []int{1, 2, 3} {
		seedProposalIssue(t, r, proj, "bs-tgt-over-r1", num, "brainstorm", "open", "new")
	}

	if created := r.brainstorm(ctx, proj, emptyReader("o/r1"), repos, nil,
		proj.Spec.Scm.Cron.Brainstorm); created {
		t.Fatal("pending 3 over target 1 must yield deficit 0 and no Task")
	}
	if n := len(listBrainstormQEs(t, proj.Name)); n != 0 {
		t.Fatalf("want 0 brainstorm QueuedEvents, got %d", n)
	}
	// And nothing was closed: all three Issue CRs still read open/new.
	for _, num := range []int{1, 2, 3} {
		got := getIssueCR(t, r.Client, tatarav1alpha1.IssueName("bs-tgt-over-r1", num))
		if got.Status.State != "open" || got.Status.Status != "new" {
			t.Fatalf("issue %d was mutated to state=%q status=%q; the controller must never close a proposal",
				num, got.Status.State, got.Status.Status)
		}
	}
}

func TestBrainstormInFlightSessionCountsTowardTheTarget(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-tgt-inflight", []string{"o/r1"}, ptrInt(1))
	r := newScanReconciler(emptyReader("o/r1"))
	existing := []tatarav1alpha1.Task{liveBrainstormTask(proj)}

	if created := r.brainstorm(ctx, proj, emptyReader("o/r1"), repos, existing,
		proj.Spec.Scm.Cron.Brainstorm); created {
		t.Fatal("an in-flight brainstorm Task must consume the only slot")
	}
}

// forgeReadCalls is the total SCM fan-out cost of one brainstorm() pass:
// ListOpenIssues per repo plus gatherRepoCIState's ListOpenPRs per repo, all of
// which runProjectScopedProposalCycle pays BEFORE createBrainstormTask's dedup
// key gets a chance to throw the result away.
func forgeReadCalls(rd *perRepoFakeReader) int { return rd.issueCalls + rd.prCalls }

// TestBrainstormInFlightSkipsTheForgeFanOut pins a COST property, not a
// correctness one, and it is the property the target-law refactor silently
// dropped. Pre-target-backlog, brainstorm() opened with a hard early return on
// the in-flight guard; the target law demoted that to an arithmetic term
// (inflight = 1 inside the deficit), which still leaves deficit > 0 for any
// target above pending+1 - here 3 - 0 - 1 = 2. The event trigger then made this
// decision run on every Project reconcile (30s floor), so the whole per-repo
// fan-out re-ran every 30s for the entire duration of every session and every
// one of its results was discarded by the dedup key at the very END.
//
// Asserting "no second QueuedEvent" would prove nothing: the dedup key already
// guaranteed that before and after. The assertion has to be on the READER.
func TestBrainstormInFlightSkipsTheForgeFanOut(t *testing.T) {
	tests := []struct {
		name        string
		slug        string
		inflight    bool
		wantCreated bool
		wantReads   bool
	}{
		{"a live session suppresses the cycle before any forge read", "bs-fanout-live", true, false, false},
		{"an idle project still pays for the fan-out and refills", "bs-fanout-idle", false, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			proj, repos := seedBrainstormProject(t, tc.slug, []string{"o/" + tc.slug}, ptrInt(3))
			rd := emptyReader("o/" + tc.slug)
			r := newScanReconciler(rd)
			var existing []tatarav1alpha1.Task
			if tc.inflight {
				existing = []tatarav1alpha1.Task{liveBrainstormTask(proj)}
			}

			created := r.brainstorm(ctx, proj, rd, repos, existing,
				proj.Spec.Scm.Cron.Brainstorm)

			if created != tc.wantCreated {
				t.Fatalf("brainstorm created = %v, want %v", created, tc.wantCreated)
			}
			if got := forgeReadCalls(rd); (got > 0) != tc.wantReads {
				t.Fatalf("forge read calls = %d, want %s", got,
					map[bool]string{true: "at least one", false: "exactly zero"}[tc.wantReads])
			}
		})
	}
}

// TestBrainstormQueuedEventSkipsTheForgeFanOut covers the other half of the
// short-circuit: the queued-but-not-admitted window. brainstormInFlightProject
// only sees a minted TASK, so between EnqueueEvent and admission inflight reads
// 0 and the fan-out would run on every pass for as long as the event sits
// queued. The second pass here must cost exactly nothing.
func TestBrainstormQueuedEventSkipsTheForgeFanOut(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-fanout-queued", []string{"o/q1"}, ptrInt(3))
	rd := emptyReader("o/q1")
	r := newScanReconciler(rd)

	if created := r.brainstorm(ctx, proj, rd, repos, nil,
		proj.Spec.Scm.Cron.Brainstorm); !created {
		t.Fatal("the first pass must enqueue a brainstorm event")
	}
	first := forgeReadCalls(rd)
	if first == 0 {
		t.Fatal("the first pass must actually reach the forge, otherwise the second-pass assertion proves nothing")
	}

	// No Task exists yet - only the Queued QueuedEvent - so the in-flight guard
	// above is still 0 here and the queued check is the only thing holding the
	// line.
	if created := r.brainstorm(ctx, proj, rd, repos, nil,
		proj.Spec.Scm.Cron.Brainstorm); created {
		t.Fatal("a still-queued brainstorm event must suppress a second refill")
	}
	if got := forgeReadCalls(rd); got != first {
		t.Fatalf("the second pass made %d extra forge reads while the event was still queued; want 0", got-first)
	}
	if n := len(listBrainstormQEs(t, proj.Name)); n != 1 {
		t.Fatalf("want exactly 1 brainstorm QueuedEvent, got %d", n)
	}
}

// A paused project files nothing however large its deficit. This is the
// scheduling half of the exhausted contract; the API half is
// TestBrainstormExhaustedStampsThePause (internal/restapi).
//
// The pause is set directly on the in-memory proj passed to r.brainstorm(),
// which is the only copy the function ever reads (no internal re-Get) - a
// round-trip Status().Update here would prove nothing about r.brainstorm()'s
// own read and was dropped (review finding I6: it used to write via
// k8sClient.Status().Update and then pass this SAME already-mutated proj,
// so the write was unexercised theatre).
func TestBrainstormPausedProjectRefillsNothing(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-tgt-paused", []string{"o/r1"}, ptrInt(3))
	now := metav1.Now()
	proj.Status.BrainstormPausedAt = &now
	proj.Status.BrainstormPauseReason = "every lane is blocked on a human"
	r := newScanReconciler(emptyReader("o/r1"))

	if created := r.brainstorm(ctx, proj, emptyReader("o/r1"), repos, nil,
		proj.Spec.Scm.Cron.Brainstorm); created {
		t.Fatal("a paused project must not dispatch a refill")
	}
}

// TestBrainstormCooldownGateSuppressesASecondSessionThenReleases is C2's
// integration proof through the real wiring (not just the pure
// brainstormRefillDecision table): a project whose backlog stays in deficit
// (deficit=3, nothing pending) gets its FIRST session through unthrottled,
// but a session dispatched moments ago - the durable Status.LastBrainstorm
// stamp, round-tripped through etcd exactly as the event-driven Reconcile
// path (project_controller.go) now writes it after a real dispatch - blocks
// a second one until the configured floor elapses.
func TestBrainstormCooldownGateSuppressesASecondSessionThenReleases(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-tgt-cooldown", []string{"o/r1"}, ptrInt(3))
	proj.Spec.Scm.Cron.Brainstorm.MinSessionIntervalMinutes = 10
	if err := k8sClient.Update(ctx, proj); err != nil {
		t.Fatalf("set MinSessionIntervalMinutes: %v", err)
	}
	r := newScanReconciler(emptyReader("o/r1"))

	// First session: no prior stamp anywhere, so the floor must not block it.
	if created := r.brainstorm(ctx, proj, emptyReader("o/r1"), repos, nil,
		proj.Spec.Scm.Cron.Brainstorm); !created {
		t.Fatal("want the first session dispatched; the floor must never block session one")
	}

	// Delete the QueuedEvent the first session enqueued: this test isolates the
	// cooldown gate, not the separate already-queued dedup guard that a real
	// Task admission would otherwise clear (out of scope here).
	for _, qe := range listBrainstormQEs(t, proj.Name) {
		if err := k8sClient.Delete(ctx, &qe); err != nil {
			t.Fatalf("delete brainstorm QE: %v", err)
		}
	}

	// Durably stamp LastBrainstorm one minute ago (well inside the 10-minute
	// floor) via a real Status().Update round-trip, then re-Get so the next
	// call reads it exactly like a fresh reconcile would - this is the part
	// that must be durable across operator replicas/restarts, not in-memory.
	recent := metav1.NewTime(time.Now().Add(-1 * time.Minute))
	proj.Status.LastBrainstorm = &recent
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("stamp LastBrainstorm: %v", err)
	}
	fresh := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: proj.Name}, fresh); err != nil {
		t.Fatalf("re-get project: %v", err)
	}

	if created := r.brainstorm(ctx, fresh, emptyReader("o/r1"), repos, nil,
		fresh.Spec.Scm.Cron.Brainstorm); created {
		t.Fatal("a session one minute after the last one must be gated by the 10-minute floor")
	}

	// Past the floor: a stamp from 11 minutes ago must release the gate again -
	// this is the "delays, never suppresses permanently" property.
	past := metav1.NewTime(time.Now().Add(-11 * time.Minute))
	fresh.Status.LastBrainstorm = &past
	if err := k8sClient.Status().Update(ctx, fresh); err != nil {
		t.Fatalf("stamp older LastBrainstorm: %v", err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: proj.Name}, fresh); err != nil {
		t.Fatalf("re-get project: %v", err)
	}
	if created := r.brainstorm(ctx, fresh, emptyReader("o/r1"), repos, nil,
		fresh.Spec.Scm.Cron.Brainstorm); !created {
		t.Fatal("a session past the 10-minute floor must be allowed again")
	}
}

// A refill DECISION with no repo to act on ("no valid repos", the third exit
// of brainstorm()) must still set the target/pending gauges from this pass's
// fresh values. O10 review Minor 1: this exit used to skip the setters the
// other two exits call, so a stretch of enqueue failures left a stale
// reading instead of the current one.
func TestBrainstormNotEnqueuedStillSetsGauges(t *testing.T) {
	ctx := context.Background()
	proj, _ := seedBrainstormProject(t, "bs-tgt-notenq", nil, ptrInt(3))
	reg := prometheus.NewRegistry()
	r := newScanReconciler(emptyReader())
	r.Metrics = obs.NewOperatorMetrics(reg)

	if created := r.brainstorm(ctx, proj, emptyReader(), nil, nil,
		proj.Spec.Scm.Cron.Brainstorm); created {
		t.Fatal("no repos with a valid slug: nothing can be enqueued")
	}
	if got, ok := brainstormTargetSeries(t, reg, proj.Name); !ok || got != 3 {
		t.Fatalf("operator_brainstorm_target_proposals{project=%s} = %v (present=%v), want 3",
			proj.Name, got, ok)
	}
	if got, ok := brainstormPendingSeries(t, reg, proj.Name); !ok || got != 0 {
		t.Fatalf("operator_brainstorm_pending_proposals{project=%s} = %v (present=%v), want an explicit 0",
			proj.Name, got, ok)
	}
}

// openProposalsSeries reads operator_open_proposals{repo}. It reports PRESENCE
// separately from value: gaugeValue (task_controller_test.go) folds "absent"
// into 0, which is exactly the distinction this gauge's regression is about.
func openProposalsSeries(t *testing.T, reg *prometheus.Registry, repo string) (float64, bool) {
	t.Helper()
	return projectGaugeSeries(t, reg, "operator_open_proposals", "repo", repo)
}

// brainstormTargetSeries reads operator_brainstorm_target_proposals{project}.
func brainstormTargetSeries(t *testing.T, reg *prometheus.Registry, project string) (float64, bool) {
	t.Helper()
	return projectGaugeSeries(t, reg, "operator_brainstorm_target_proposals", "project", project)
}

// brainstormPendingSeries reads operator_brainstorm_pending_proposals{project}.
func brainstormPendingSeries(t *testing.T, reg *prometheus.Registry, project string) (float64, bool) {
	t.Helper()
	return projectGaugeSeries(t, reg, "operator_brainstorm_pending_proposals", "project", project)
}

// brainstormPausedSeries reads operator_brainstorm_paused{project}.
func brainstormPausedSeries(t *testing.T, reg *prometheus.Registry, project string) (float64, bool) {
	t.Helper()
	return projectGaugeSeries(t, reg, "operator_brainstorm_paused", "project", project)
}

// TestBrainstormPausedProjectSetsPausedGauge is I1's fix round proof: the
// design spec calls for metric-level observability of a project paused and
// never resuming, and operator_brainstorm_paused did not exist at all before
// this fix - the observability alert citing it was referencing a series that
// was never published. A paused project's refill pass must publish it as 1.
func TestBrainstormPausedProjectSetsPausedGauge(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-tgt-paused-gauge", []string{"o/r1"}, ptrInt(3))
	now := metav1.Now()
	proj.Status.BrainstormPausedAt = &now
	proj.Status.BrainstormPauseReason = "every lane is blocked on a human"
	reg := prometheus.NewRegistry()
	r := newScanReconciler(emptyReader("o/r1"))
	r.Metrics = obs.NewOperatorMetrics(reg)

	if created := r.brainstorm(ctx, proj, emptyReader("o/r1"), repos, nil,
		proj.Spec.Scm.Cron.Brainstorm); created {
		t.Fatal("a paused project must not dispatch a refill")
	}
	if got, ok := brainstormPausedSeries(t, reg, proj.Name); !ok || got != 1 {
		t.Fatalf("operator_brainstorm_paused{project=%s} = %v (present=%v), want 1", proj.Name, got, ok)
	}
}

// TestBrainstormUnpausedProjectClearsPausedGauge proves the gauge is not a
// one-way latch: an at-target (unpaused) project must read an EXPLICIT 0, not
// absent - a dashboard cannot otherwise tell "never paused" from "paused, then
// the pod restarted and the process-local series was lost".
func TestBrainstormUnpausedProjectClearsPausedGauge(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-tgt-unpaused-gauge", []string{"o/r1"}, ptrInt(0))
	reg := prometheus.NewRegistry()
	r := newScanReconciler(emptyReader("o/r1"))
	r.Metrics = obs.NewOperatorMetrics(reg)

	if created := r.brainstorm(ctx, proj, emptyReader("o/r1"), repos, nil,
		proj.Spec.Scm.Cron.Brainstorm); created {
		t.Fatal("target 0: nothing should be dispatched")
	}
	if got, ok := brainstormPausedSeries(t, reg, proj.Name); !ok || got != 0 {
		t.Fatalf("operator_brainstorm_paused{project=%s} = %v (present=%v), want an explicit 0",
			proj.Name, got, ok)
	}
}

// projectGaugeSeries reads a single-label gauge vec's value for one label
// value, reporting PRESENCE separately from value: gaugeValue
// (task_controller_test.go) folds "absent" into 0, which is exactly the
// distinction the "reaches zero, never latches" regressions above are about.
func projectGaugeSeries(t *testing.T, reg *prometheus.Registry, metric, label, value string) (float64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metric {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m.GetLabel(), map[string]string{label: value}) {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// The {repo} label is the owner/name SLUG (what the tatara-observability
// dashboard joins on), never the DNS-1123 Repository CR name that the Issue CRs
// carry, and every enrolled repo is written every pass so an emptied repo falls
// to 0 instead of latching its last nonzero value.
func TestBrainstormOpenProposalsGaugeIsSluggedAndReachesZero(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-tgt-gauge", []string{"o/g1", "o/g2"}, ptrInt(5))
	reg := prometheus.NewRegistry()
	r := newScanReconciler(emptyReader("o/g1", "o/g2"))
	r.Metrics = obs.NewOperatorMetrics(reg)
	// Two pending on g1, none at all on g2.
	seedProposalIssue(t, r, proj, repos[0].Name, 1, "brainstorm", "open", "new")
	iss2 := seedProposalIssue(t, r, proj, repos[0].Name, 2, "brainstorm", "open", "new")

	r.brainstorm(ctx, proj, emptyReader("o/g1", "o/g2"), repos, nil,
		proj.Spec.Scm.Cron.Brainstorm)

	if _, ok := openProposalsSeries(t, reg, repos[0].Name); ok {
		t.Fatalf("gauge is labelled with the Repository CR name %q; it must be the owner/name slug", repos[0].Name)
	}
	if got, ok := openProposalsSeries(t, reg, "o/g1"); !ok || got != 2 {
		t.Fatalf("operator_open_proposals{repo=o/g1} = %v (present=%v), want 2", got, ok)
	}
	// g2 has no proposals at all: the series must EXIST at 0, not be absent.
	if got, ok := openProposalsSeries(t, reg, "o/g2"); !ok || got != 0 {
		t.Fatalf("operator_open_proposals{repo=o/g2} = %v (present=%v), want an explicit 0", got, ok)
	}

	// Approving one proposal must move the gauge DOWN, and emptying g1 entirely
	// must take it to 0 rather than latching at its last nonzero value.
	iss2.Status.Status = "approved"
	if err := r.Status().Update(ctx, iss2); err != nil {
		t.Fatalf("approve issue: %v", err)
	}
	r.brainstorm(ctx, proj, emptyReader("o/g1", "o/g2"), repos, nil,
		proj.Spec.Scm.Cron.Brainstorm)
	if got, ok := openProposalsSeries(t, reg, "o/g1"); !ok || got != 1 {
		t.Fatalf("after one approval operator_open_proposals{repo=o/g1} = %v (present=%v), want 1", got, ok)
	}

	iss1 := getIssueCR(t, r.Client, tatarav1alpha1.IssueName(repos[0].Name, 1))
	iss1.Status.Status = "approved"
	if err := r.Status().Update(ctx, iss1); err != nil {
		t.Fatalf("approve issue: %v", err)
	}
	r.brainstorm(ctx, proj, emptyReader("o/g1", "o/g2"), repos, nil,
		proj.Spec.Scm.Cron.Brainstorm)
	if got, ok := openProposalsSeries(t, reg, "o/g1"); !ok || got != 0 {
		t.Fatalf("an emptied repo latched at %v (present=%v); want an explicit 0", got, ok)
	}
}
