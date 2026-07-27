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
	tk.Status.Stage = tatarav1alpha1.StageBrainstorming
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
		proj.Spec.Scm.Cron.Brainstorm, TriggerCron)
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
		proj.Spec.Scm.Cron.Brainstorm, TriggerCron); !created {
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
		proj.Spec.Scm.Cron.Brainstorm, TriggerCron); !created {
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
		proj.Spec.Scm.Cron.Brainstorm, TriggerCron); created {
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
		proj.Spec.Scm.Cron.Brainstorm, TriggerCron); created {
		t.Fatal("an in-flight brainstorm Task must consume the only slot")
	}
}

func TestBrainstormBreakerSuppressesTheEventPathOnly(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-tgt-breaker", []string{"o/r1"}, ptrInt(3))
	proj.Status.BrainstormConsecutiveSkips = 3
	r := newScanReconciler(emptyReader("o/r1"))

	if created := r.brainstorm(ctx, proj, emptyReader("o/r1"), repos, nil,
		proj.Spec.Scm.Cron.Brainstorm, TriggerEvent); created {
		t.Fatal("a tripped breaker must suppress the event-driven refill")
	}
	if created := r.brainstorm(ctx, proj, emptyReader("o/r1"), repos, nil,
		proj.Spec.Scm.Cron.Brainstorm, TriggerCron); !created {
		t.Fatal("the cron backstop must refill regardless of the breaker")
	}
}

// openProposalsSeries reads operator_open_proposals{repo}. It reports PRESENCE
// separately from value: gaugeValue (task_controller_test.go) folds "absent"
// into 0, which is exactly the distinction this gauge's regression is about.
func openProposalsSeries(t *testing.T, reg *prometheus.Registry, repo string) (float64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_open_proposals" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m.GetLabel(), map[string]string{"repo": repo}) {
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
		proj.Spec.Scm.Cron.Brainstorm, TriggerCron)

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
		proj.Spec.Scm.Cron.Brainstorm, TriggerCron)
	if got, ok := openProposalsSeries(t, reg, "o/g1"); !ok || got != 1 {
		t.Fatalf("after one approval operator_open_proposals{repo=o/g1} = %v (present=%v), want 1", got, ok)
	}

	iss1 := getIssueCR(t, r.Client, tatarav1alpha1.IssueName(repos[0].Name, 1))
	iss1.Status.Status = "approved"
	if err := r.Status().Update(ctx, iss1); err != nil {
		t.Fatalf("approve issue: %v", err)
	}
	r.brainstorm(ctx, proj, emptyReader("o/g1", "o/g2"), repos, nil,
		proj.Spec.Scm.Cron.Brainstorm, TriggerCron)
	if got, ok := openProposalsSeries(t, reg, "o/g1"); !ok || got != 0 {
		t.Fatalf("an emptied repo latched at %v (present=%v); want an explicit 0", got, ok)
	}
}

// resetBrainstormSkips is what makes the cron tick the breaker's only reset. It
// must be a no-op (no write, no error) when the counter is already zero.
func TestResetBrainstormSkips(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		start int
	}{
		{"tripped breaker resets to zero", 4},
		{"already zero is a no-op", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proj, _ := seedBrainstormProject(t, "bs-tgt-reset-"+strconv.Itoa(tc.start), []string{"o/r1"}, ptrInt(3))
			r := newScanReconciler(emptyReader("o/r1"))
			if tc.start > 0 {
				proj.Status.BrainstormConsecutiveSkips = tc.start
				if err := r.Status().Update(ctx, proj); err != nil {
					t.Fatalf("seed skips: %v", err)
				}
			}
			if err := r.resetBrainstormSkips(ctx, proj); err != nil {
				t.Fatalf("resetBrainstormSkips: %v", err)
			}
			if proj.Status.BrainstormConsecutiveSkips != 0 {
				t.Fatalf("in-memory skips = %d, want 0", proj.Status.BrainstormConsecutiveSkips)
			}
			var fresh tatarav1alpha1.Project
			if err := r.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}, &fresh); err != nil {
				t.Fatalf("get project: %v", err)
			}
			if fresh.Status.BrainstormConsecutiveSkips != 0 {
				t.Fatalf("persisted skips = %d, want 0", fresh.Status.BrainstormConsecutiveSkips)
			}
		})
	}
}
