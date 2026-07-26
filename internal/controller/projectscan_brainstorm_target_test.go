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

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
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

func intPtr(v int) *int { return &v }

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
	proj, repos := seedBrainstormProject(t, "bs-tgt-ann", []string{"o/r1"}, intPtr(3))
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
	proj, repos := seedBrainstormProject(t, "bs-tgt-deficit", []string{"o/r1"}, intPtr(3))
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
	proj, repos := seedBrainstormProject(t, "bs-tgt-approved", []string{"o/r1"}, intPtr(1))
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
	proj, repos := seedBrainstormProject(t, "bs-tgt-over", []string{"o/r1"}, intPtr(1))
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
	proj, repos := seedBrainstormProject(t, "bs-tgt-inflight", []string{"o/r1"}, intPtr(1))
	r := newScanReconciler(emptyReader("o/r1"))
	existing := []tatarav1alpha1.Task{liveBrainstormTask(proj)}

	if created := r.brainstorm(ctx, proj, emptyReader("o/r1"), repos, existing,
		proj.Spec.Scm.Cron.Brainstorm, TriggerCron); created {
		t.Fatal("an in-flight brainstorm Task must consume the only slot")
	}
}

func TestBrainstormBreakerSuppressesTheEventPathOnly(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-tgt-breaker", []string{"o/r1"}, intPtr(3))
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
			proj, _ := seedBrainstormProject(t, "bs-tgt-reset-"+strconv.Itoa(tc.start), []string{"o/r1"}, intPtr(3))
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
