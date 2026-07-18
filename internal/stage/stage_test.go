package stage_test

import (
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ---------------------------------------------------------------------------
// TEST DOUBLES
//
// internal/stage is a PURE package: it never talks to the API server, the
// forge, or a Kubelet. These doubles exist to prove that THE TABLE never
// yields an answer that a caller could turn into an implement pod (or a merge)
// on a kind=review Task. They are driven by AgentKindFor + LegalFor + Enter +
// Unpark, which is every way a stage is reachable.
// ---------------------------------------------------------------------------

// podFactory is the double that a reconciler would be. It PANICS if the stage
// machine ever hands it an implement pod for a kind=review Task. Any path that
// reaches one crashes the test rather than passing quietly.
type podFactory struct {
	t       *testing.T
	spawned map[string]int // agent kind -> spawn count
}

func newPodFactory(t *testing.T) *podFactory {
	t.Helper()
	return &podFactory{t: t, spawned: map[string]int{}}
}

// spawn is what the reconciler does on entering a stage: it asks the F.2 table
// which agent kind runs here, and creates that pod.
func (f *podFactory) spawn(task *v1alpha1.Task) {
	f.t.Helper()
	kind := stage.AgentKindFor(task.Status.Stage)
	if kind == "" {
		return // pod-less stage
	}
	if task.Spec.Kind == "review" && kind == stage.AgentImplement {
		f.t.Fatalf("PRIMARY-PATH VIOLATION: implement pod spawned for a kind=review Task at stage %q (reason %q)",
			task.Status.Stage, task.Status.StageReason)
		panic("implement pod on a review Task")
	}
	f.spawned[kind]++
}

// forge is the double for the SCM writer. Merge PANICS for a kind=review Task:
// merging a human's PR is a human action, and the machine must never produce
// the merging stage for one.
type forge struct {
	t      *testing.T
	merges int
}

func (fg *forge) merge(task *v1alpha1.Task) {
	fg.t.Helper()
	if task.Spec.Kind == "review" {
		fg.t.Fatalf("MERGE VIOLATION: merging stage reached on a kind=review Task (reason %q)", task.Status.StageReason)
		panic("merge on a review Task")
	}
	fg.merges++
}

// harness drives a Task through the machine exactly the way the reconciler
// will: every stage entry goes through stage.Enter, and every entry then asks
// the pod factory (and, on merging, the forge) to act.
type harness struct {
	t    *testing.T
	pods *podFactory
	fg   *forge
	task *v1alpha1.Task
	mrs  []v1alpha1.MergeRequest
	now  time.Time
}

func newHarness(t *testing.T, task *v1alpha1.Task) *harness {
	t.Helper()
	return &harness{
		t:    t,
		pods: newPodFactory(t),
		fg:   &forge{t: t},
		task: task,
		now:  time.Date(2026, 7, 12, 9, 0, 0, 0, time.UTC),
	}
}

// enter is the ONE way a stage is entered in the harness. It mirrors the
// reconciler: Enter (which enforces LegalFor), then act on the new stage.
func (h *harness) enter(to, reason string) error {
	h.t.Helper()
	if err := stage.Enter(h.task, h.mrs, to, reason, h.now); err != nil {
		return err
	}
	h.act()
	return nil
}

// act is the side effect of BEING in a stage.
func (h *harness) act() {
	h.t.Helper()
	h.pods.spawn(h.task)
	if h.task.Status.Stage == v1alpha1.StageMerging {
		h.fg.merge(h.task)
	}
}

// unpark runs F.6 and, when it promotes, performs the side effects of the
// stage it promoted into.
func (h *harness) unpark(in stage.UnparkInput) (string, bool) {
	h.t.Helper()
	in.Task = h.task
	in.MRs = h.mrs
	if in.Now.IsZero() {
		in.Now = h.now
	}
	target, ok := stage.Unpark(in)
	if ok {
		h.act()
	}
	return target, ok
}

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

func newTask(kind, stg, reason string) *v1alpha1.Task {
	at := metav1.NewTime(time.Date(2026, 7, 12, 8, 0, 0, 0, time.UTC))
	return &v1alpha1.Task{
		Spec: v1alpha1.TaskSpec{Kind: kind},
		Status: v1alpha1.TaskStatus{
			Stage:          stg,
			StageReason:    reason,
			StageEnteredAt: &at,
		},
	}
}

func humanEvent(t *v1alpha1.Task) {
	t.Status.PendingEvents = append(t.Status.PendingEvents, v1alpha1.TaskEvent{
		At:     metav1.Now(),
		Kind:   "issue_comment",
		Author: "maintainer",
		Body:   "go ahead",
	})
}

func botEvent(t *v1alpha1.Task) {
	t.Status.PendingEvents = append(t.Status.PendingEvents, v1alpha1.TaskEvent{
		At:     metav1.Now(),
		Kind:   "issue_comment",
		Author: "tatara-bot",
		Body:   "parking this task",
	})
}

func openIssue(status string) v1alpha1.Issue {
	return v1alpha1.Issue{Status: v1alpha1.IssueStatus{State: "open", Status: status}}
}

func openMR() v1alpha1.MergeRequest {
	return v1alpha1.MergeRequest{Status: v1alpha1.MergeRequestStatus{State: "open"}}
}

func mergedMR() v1alpha1.MergeRequest {
	now := metav1.Now()
	return v1alpha1.MergeRequest{Status: v1alpha1.MergeRequestStatus{State: "merged", MergedAt: &now}}
}

func ptime(t time.Time) *metav1.Time {
	mt := metav1.NewTime(t)
	return &mt
}

const botLogin = "tatara-bot"

// ===========================================================================
// 1. THE PRIMARY-PATH TEST (fix V7-1). THIS ONE FIRST.
//
// A kind=review Task in `reviewing`, given submit_outcome(request_changes) -
// the review agent's NORMAL verdict on a bad human PR - must go to
// parked(awaiting-human). It must NOT go to implementing.
//
// v6 guarded reviewing -> merging on kind and MISSED THE IMPLEMENTING EDGE.
// ===========================================================================

func TestPrimaryPath_ReviewKindRequestChangesParksAwaitingHuman(t *testing.T) {
	task := newTask("review", v1alpha1.StageReviewing, "")
	h := newHarness(t, task)
	h.mrs = []v1alpha1.MergeRequest{openMR()} // human-authored PR, no pendingReview

	// The edge the review agent's NORMAL verdict wants: it does not exist.
	if stage.LegalFor(task, h.mrs, v1alpha1.StageReviewing, v1alpha1.StageImplementing) {
		t.Fatal("reviewing -> implementing is LEGAL on a kind=review Task: the PRIMARY path is open")
	}
	if stage.LegalFor(task, h.mrs, v1alpha1.StageReviewing, v1alpha1.StageMerging) {
		t.Fatal("reviewing -> merging is LEGAL on a kind=review Task")
	}

	// Enter must REFUSE it, so no caller can bypass the guard.
	err := h.enter(v1alpha1.StageImplementing, "")
	var ill *stage.IllegalTransitionError
	if !errors.As(err, &ill) {
		t.Fatalf("Enter(reviewing -> implementing) on a review Task: got err %v, want IllegalTransitionError", err)
	}
	if ill.From != v1alpha1.StageReviewing || ill.To != v1alpha1.StageImplementing {
		t.Fatalf("IllegalTransitionError carries from=%q to=%q, want reviewing/implementing (the metric is labelled on them)", ill.From, ill.To)
	}
	if task.Status.Stage != v1alpha1.StageReviewing {
		t.Fatalf("a REFUSED Enter mutated the Task: stage is now %q", task.Status.Stage)
	}

	// The verdict that DOES exist: request_changes -> parked(awaiting-human).
	if err := h.enter(v1alpha1.StageParked, stage.ReasonAwaitingHuman); err != nil {
		t.Fatalf("reviewing -> parked(awaiting-human) on request_changes: %v", err)
	}
	if task.Status.Stage != v1alpha1.StageParked || task.Status.StageReason != stage.ReasonAwaitingHuman {
		t.Fatalf("got %s(%s), want parked(awaiting-human)", task.Status.Stage, task.Status.StageReason)
	}
	if h.pods.spawned[stage.AgentImplement] != 0 {
		t.Fatal("an implement pod was spawned for a kind=review Task")
	}
	if h.fg.merges != 0 {
		t.Fatal("the forge Merge was called for a kind=review Task")
	}
}

// The approve verdict lands in the SAME place. Both verdicts, one destination.
func TestPrimaryPath_ReviewKindApproveParksAwaitingHuman(t *testing.T) {
	task := newTask("review", v1alpha1.StageReviewing, "")
	h := newHarness(t, task)
	h.mrs = []v1alpha1.MergeRequest{openMR()}

	if err := h.enter(v1alpha1.StageParked, stage.ReasonAwaitingHuman); err != nil {
		t.Fatalf("reviewing -> parked(awaiting-human) on approve: %v", err)
	}
	if h.fg.merges != 0 {
		t.Fatal("a kind=review Task reached merging")
	}
}

// A NON-review Task keeps BOTH edges. The guard discriminates on kind, and on
// nothing else.
func TestPrimaryPath_NonReviewKindKeepsBothEdges(t *testing.T) {
	for _, kind := range []string{"implement", "clarify", "documentation"} {
		t.Run(kind, func(t *testing.T) {
			task := newTask(kind, v1alpha1.StageReviewing, "")
			mrs := []v1alpha1.MergeRequest{openMR()}
			if !stage.LegalFor(task, mrs, v1alpha1.StageReviewing, v1alpha1.StageImplementing) {
				t.Error("reviewing -> implementing must exist for a non-review Task")
			}
			if !stage.LegalFor(task, mrs, v1alpha1.StageReviewing, v1alpha1.StageMerging) {
				t.Error("reviewing -> merging must exist for a non-review Task")
			}
		})
	}
}

// NO implement pod is EVER created for a kind=review Task, at ANY stage, by
// ANY path. Exhaustive over the whole transition table.
func TestPrimaryPath_NoImplementPodForReviewKindFromAnyStage(t *testing.T) {
	for _, from := range append(stage.AllStages(), stage.Create) {
		for _, to := range append(stage.AllStages(), stage.Create) {
			task := newTask("review", from, "")
			mrs := []v1alpha1.MergeRequest{openMR()}
			if stage.LegalFor(task, mrs, from, to) &&
				(to == v1alpha1.StageImplementing || to == v1alpha1.StageMerging) {
				t.Errorf("LegalFor(review, %s -> %s) = true: a review Task can reach %s", from, to, to)
			}
		}
	}
}

// ===========================================================================
// 2. THE STEADY-STATE TEST (fixes V6-1, V7-7).
// ===========================================================================

func TestSteadyState_QueuedBehindThreeAgentsDoesNotTerminate(t *testing.T) {
	// maxConcurrentAgents = 3, three agents live, a fourth Task queues.
	// It sits in `approved` (the admission gate) then in `implementing` with
	// no pod yet. It must NOT terminate.
	task := newTask("implement", v1alpha1.StageApproved, "")
	h := newHarness(t, task)
	entered := time.Date(2026, 7, 12, 9, 0, 0, 0, time.UTC)
	task.Status.StageEnteredAt = ptime(entered)

	// 40 minutes in the admission queue.
	at40 := entered.Add(40 * time.Minute)
	if edge, fired := stage.Elapsed(task, false, at40); fired {
		t.Fatalf("a Task queueing 40m in `approved` fired %v: the normal steady state must not terminate", edge)
	}

	// Admitted. approved -> implementing.
	h.now = at40
	if err := h.enter(v1alpha1.StageImplementing, ""); err != nil {
		t.Fatalf("approved -> implementing on admission: %v", err)
	}
	if task.Status.Stage != v1alpha1.StageImplementing {
		t.Fatalf("stage = %q, want implementing", task.Status.Stage)
	}

	// The pod is not created instantly either. Another 40m of queueing in a POD
	// stage with podStartedAt == nil: clock 1, 24h. Still alive.
	at80 := at40.Add(40 * time.Minute)
	clock, since, budget, _ := stage.ArmedClock(task, false)
	if clock != stage.ClockAdmission {
		t.Fatalf("queued pod stage armed clock %q, want %q", clock, stage.ClockAdmission)
	}
	if !since.Equal(at40) {
		t.Fatalf("clock 1 measures from %v, want stageEnteredAt %v", since, at40)
	}
	if budget != v1alpha1.AdmissionStarvedBudget {
		t.Fatalf("clock 1 budget %v, want 24h", budget)
	}
	if edge, fired := stage.Elapsed(task, false, at80); fired {
		t.Fatalf("a Task queueing 80m in `implementing` fired %v", edge)
	}

	// Pod created, becomes Ready. Now the WORK clock runs, from work start.
	task.Status.PodStartedAt = ptime(at80)
	task.Status.StageWorkStartedAt = ptime(at80.Add(30 * time.Second))
	if edge, fired := stage.Elapsed(task, false, at80.Add(5*time.Hour)); fired {
		t.Fatalf("implementing at 5h of WORK fired %v: the budget is 6h", edge)
	}
	if h.pods.spawned[stage.AgentImplement] != 1 {
		t.Fatalf("implement pods spawned = %d, want 1", h.pods.spawned[stage.AgentImplement])
	}
}

func TestSteadyState_NeverReadyPodRespawnsThenExhausts(t *testing.T) {
	const maxPodRecreations = 3
	task := newTask("implement", v1alpha1.StageImplementing, "")
	entered := time.Date(2026, 7, 12, 9, 0, 0, 0, time.UTC)
	task.Status.StageEnteredAt = ptime(entered)

	created := entered.Add(2 * time.Hour) // 2h in the admission queue first
	respawns := 0
	for i := range 10 {
		task.Status.PodStartedAt = ptime(created)
		task.Status.StageWorkStartedAt = nil // ImagePullBackOff: never Ready

		clock, since, budget, _ := stage.ArmedClock(task, false)
		if clock != stage.ClockReadiness {
			t.Fatalf("lap %d: armed clock %q, want %q", i, clock, stage.ClockReadiness)
		}
		if !since.Equal(created) {
			t.Fatalf("lap %d: the READINESS clock measures from %v, want podStartedAt %v; "+
				"measuring from stageEnteredAt includes the admission queue", i, since, created)
		}
		if budget != v1alpha1.PodReadyTimeout {
			t.Fatalf("lap %d: readiness budget %v, want 5m", i, budget)
		}

		// 4m in: not yet.
		if _, fired := stage.Elapsed(task, false, created.Add(4*time.Minute)); fired {
			t.Fatalf("lap %d: the readiness clock fired at 4m", i)
		}

		// Past 5m: it breaches.
		edge, fired := stage.Elapsed(task, false, created.Add(5*time.Minute+time.Second))
		if !fired {
			t.Fatalf("lap %d: the readiness clock did not fire past 5m", i)
		}
		if edge.To != stage.Respawn {
			t.Fatalf("lap %d: readiness breach -> %v, want a RESPAWN (it is not a terminal)", i, edge)
		}

		term, terminal := stage.RecordRespawn(task, maxPodRecreations)
		if !terminal {
			respawns++
			if task.Status.Stats.PodRecreations != respawns {
				t.Fatalf("lap %d: podRecreations = %d, want %d", i, task.Status.Stats.PodRecreations, respawns)
			}
			created = created.Add(6 * time.Minute)
			continue
		}
		// Budget spent.
		if respawns != maxPodRecreations {
			t.Fatalf("terminated after %d respawns, want %d", respawns, maxPodRecreations)
		}
		if term.To != v1alpha1.StageFailed || term.Reason != stage.ReasonPodRecreationExhausted {
			t.Fatalf("terminal = %s(%s), want failed(pod-recreation-exhausted)", term.To, term.Reason)
		}
		if term.Reason == "pod-not-ready" {
			t.Fatal("pod-not-ready is not a stage reason: it does not exist")
		}
		return
	}
	t.Fatal("the never-Ready pod never terminated: the respawn budget is unbounded")
}

func TestSteadyState_PausedProjectDoesNotStarvePark(t *testing.T) {
	// The pause kill switch (maxConcurrentAgents == 0) must not be a backlog
	// shredder. Clock 1 is SKIPPED on EVERY pod stage, and on `approved`, whose
	// pod-less budget elapses to the same admission-starved reason.
	for _, stg := range []string{
		v1alpha1.StageApproved,
		v1alpha1.StageImplementing,
		v1alpha1.StageClarifying,
		v1alpha1.StageReviewing,
		v1alpha1.StageBrainstorming,
		v1alpha1.StageInvestigating,
		v1alpha1.StageRefining,
		v1alpha1.StageDocumenting,
	} {
		t.Run(stg, func(t *testing.T) {
			task := newTask("implement", stg, "")
			entered := time.Date(2026, 7, 12, 9, 0, 0, 0, time.UTC)
			task.Status.StageEnteredAt = ptime(entered)

			clock, _, _, _ := stage.ArmedClock(task, true)
			if clock != stage.ClockNone {
				t.Fatalf("PAUSED project: %s armed clock %q, want none", stg, clock)
			}
			if edge, fired := stage.Elapsed(task, true, entered.Add(30*time.Hour)); fired {
				t.Fatalf("PAUSED project: %s fired %v after 30h", stg, edge)
			}
			// Unpaused, the same 30h DOES starve-park it.
			if _, fired := stage.Elapsed(task, false, entered.Add(30*time.Hour)); !fired {
				t.Fatalf("UNPAUSED: %s did not starve-park after 30h", stg)
			}
		})
	}
}

// ===========================================================================
// 3. THE EMPTY SET IS NOT A LICENCE (fix V6-3).
// ===========================================================================

func TestEmptySetIsNotALicence_ReviewTaskParkedAwaitingHuman(t *testing.T) {
	// A review Task owns ZERO Issues. all([]) == true. v5's rule promoted it
	// straight into implementing on ANY human comment. And it looped.
	task := newTask("review", v1alpha1.StageParked, stage.ReasonAwaitingHuman)
	humanEvent(task)
	h := newHarness(t, task)
	h.mrs = []v1alpha1.MergeRequest{openMR()}

	target, ok := h.unpark(stage.UnparkInput{
		Issues:          nil, // ZERO owned Issues. This is the whole point.
		ActiveTasks:     1,
		MaxOpenTasks:    6,
		BotLogin:        botLogin,
		MaxTurnsPerTask: 300,
	})
	if !ok {
		t.Fatal("a human comment on a parked review Task must re-enter reviewing")
	}
	if target != v1alpha1.StageReviewing {
		t.Fatalf("target = %q, want reviewing. It must NEVER be implementing", target)
	}
	if h.pods.spawned[stage.AgentImplement] != 0 {
		t.Fatal("an implement pod was spawned for a kind=review Task from an EMPTY owned-Issue set")
	}
}

// The len()>0 guard, on a NON-review Task: an empty open-Issue set does not
// satisfy the universal quantifier either.
func TestEmptySetIsNotALicence_NonReviewTaskWithNoOpenIssuesStaysParked(t *testing.T) {
	task := newTask("implement", v1alpha1.StageParked, stage.ReasonAwaitingHuman)
	humanEvent(task)
	h := newHarness(t, task)

	target, ok := h.unpark(stage.UnparkInput{
		Issues:          nil,
		ActiveTasks:     1,
		MaxOpenTasks:    6,
		BotLogin:        botLogin,
		MaxTurnsPerTask: 300,
	})
	if ok {
		t.Fatalf("an EMPTY owned-Issue set promoted a parked Task to %q. all([]) == true is not a licence", target)
	}

	// A CLOSED-only owned set is the same empty set once filtered.
	closed := v1alpha1.Issue{Status: v1alpha1.IssueStatus{State: "closed", Status: "approved"}}
	if target, ok := h.unpark(stage.UnparkInput{
		Issues: []v1alpha1.Issue{closed}, ActiveTasks: 1, MaxOpenTasks: 6,
		BotLogin: botLogin, MaxTurnsPerTask: 300,
	}); ok {
		t.Fatalf("a Task whose only owned Issue is CLOSED promoted to %q", target)
	}
}

// ===========================================================================
// 4. THE THREE CLOCKS (F.4).
// ===========================================================================

func TestArmedClock(t *testing.T) {
	base := time.Date(2026, 7, 12, 9, 0, 0, 0, time.UTC)
	entered := base
	podAt := base.Add(2 * time.Hour)
	workAt := base.Add(2*time.Hour + 30*time.Second)

	cases := []struct {
		name       string
		stg        string
		reason     string
		podStarted *time.Time
		workStart  *time.Time
		paused     bool
		wantClock  string
		wantSince  time.Time
		wantBudget time.Duration
		wantTo     string
		wantReason string
	}{
		{
			// THE NAMED CASE. podStartedAt == nil AND stageWorkStartedAt == nil.
			// Not an inference: a row.
			name: "pod stage, podStartedAt nil AND stageWorkStartedAt nil -> CLOCK 1",
			stg:  v1alpha1.StageImplementing, podStarted: nil, workStart: nil,
			wantClock: stage.ClockAdmission, wantSince: entered,
			wantBudget: v1alpha1.AdmissionStarvedBudget,
			wantTo:     v1alpha1.StageParked, wantReason: stage.ReasonAdmissionStarved,
		},
		{
			name: "pod stage, podStartedAt set, workStartedAt nil -> CLOCK 2 from podStartedAt",
			stg:  v1alpha1.StageImplementing, podStarted: &podAt, workStart: nil,
			wantClock: stage.ClockReadiness, wantSince: podAt,
			wantBudget: v1alpha1.PodReadyTimeout,
			wantTo:     stage.Respawn, wantReason: "",
		},
		{
			name: "pod stage, workStartedAt set -> CLOCK 3 from stageWorkStartedAt",
			stg:  v1alpha1.StageImplementing, podStarted: &podAt, workStart: &workAt,
			wantClock: stage.ClockWork, wantSince: workAt, wantBudget: 6 * time.Hour,
			wantTo: v1alpha1.StageParked, wantReason: stage.ReasonStageDeadline,
		},
		{
			name: "POD-LESS merging -> CLOCK 3 ONLY, from stageEnteredAt, against ITS OWN 4h budget",
			stg:  v1alpha1.StageMerging,
			// merging is pod-less: it must NOT get a 24h admission-starved clock.
			wantClock: stage.ClockWork, wantSince: entered, wantBudget: 4 * time.Hour,
			wantTo: v1alpha1.StageParked, wantReason: stage.ReasonMergeTimeout,
		},
		{
			name:      "POD-LESS deploying -> CLOCK 3, 2h, deploy-timeout",
			stg:       v1alpha1.StageDeploying,
			wantClock: stage.ClockWork, wantSince: entered, wantBudget: 2 * time.Hour,
			wantTo: v1alpha1.StageParked, wantReason: stage.ReasonDeployTimeout,
		},
		{
			name:      "POD-LESS triaging -> CLOCK 3, 5m, failed(triage-stalled)",
			stg:       v1alpha1.StageTriaging,
			wantClock: stage.ClockWork, wantSince: entered, wantBudget: 5 * time.Minute,
			wantTo: v1alpha1.StageFailed, wantReason: stage.ReasonTriageStalled,
		},
		{
			name:      "POD-LESS approved -> CLOCK 3, 24h, admission-starved",
			stg:       v1alpha1.StageApproved,
			wantClock: stage.ClockWork, wantSince: entered, wantBudget: 24 * time.Hour,
			wantTo: v1alpha1.StageParked, wantReason: stage.ReasonAdmissionStarved,
		},
		{
			name: "documenting -> CLOCK 3, 2h, delivered(doc-timeout)",
			stg:  v1alpha1.StageDocumenting, podStarted: &podAt, workStart: &workAt,
			wantClock: stage.ClockWork, wantSince: workAt, wantBudget: v1alpha1.DocStageBudget,
			wantTo: v1alpha1.StageDelivered, wantReason: stage.ReasonDocTimeout,
		},
		{
			name: "PAUSED project disarms clock 1 on a pod stage",
			stg:  v1alpha1.StageClarifying, paused: true,
			wantClock: stage.ClockNone,
		},
		{
			name: "PAUSED project disarms `approved` (its elapse reason IS admission-starved)",
			stg:  v1alpha1.StageApproved, paused: true,
			wantClock: stage.ClockNone,
		},
		{
			name: "PAUSED project does NOT disarm clock 2: the pod already exists",
			stg:  v1alpha1.StageImplementing, podStarted: &podAt, paused: true,
			wantClock: stage.ClockReadiness, wantSince: podAt, wantBudget: v1alpha1.PodReadyTimeout,
			wantTo: stage.Respawn,
		},
		{
			name: "PAUSED project does NOT disarm clock 3: it is already running",
			stg:  v1alpha1.StageImplementing, podStarted: &podAt, workStart: &workAt, paused: true,
			wantClock: stage.ClockWork, wantSince: workAt, wantBudget: 6 * time.Hour,
			wantTo: v1alpha1.StageParked, wantReason: stage.ReasonStageDeadline,
		},
		{
			// THE ONE EXEMPTION, NAMED.
			name: "parked(backlog-sweep) is EXEMPT: no clock at all",
			stg:  v1alpha1.StageParked, reason: stage.ReasonBacklogSweep,
			wantClock: stage.ClockNone,
		},
		{
			name: "parked(any other reason) ages out at parkRetention",
			stg:  v1alpha1.StageParked, reason: stage.ReasonStageDeadline,
			wantClock: stage.ClockWork, wantSince: entered, wantBudget: v1alpha1.ParkRetention,
			wantTo: stage.Reap,
		},
		{
			name:      "delivered is reaped at 48h",
			stg:       v1alpha1.StageDelivered,
			wantClock: stage.ClockWork, wantSince: entered, wantBudget: v1alpha1.DeliveredRetention,
			wantTo: stage.Reap,
		},
		{
			name: "failed is reaped at 7d",
			stg:  v1alpha1.StageFailed, reason: stage.ReasonOperatorError,
			wantClock: stage.ClockWork, wantSince: entered, wantBudget: v1alpha1.FailedRetention,
			wantTo: stage.Reap,
		},
		{
			name: "rejected is reaped at 24h",
			stg:  v1alpha1.StageRejected, reason: stage.ReasonTriageStalled,
			wantClock: stage.ClockWork, wantSince: entered, wantBudget: v1alpha1.RejectedRetention,
			wantTo: stage.Reap,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := newTask("implement", tc.stg, tc.reason)
			task.Status.StageEnteredAt = ptime(entered)
			if tc.podStarted != nil {
				task.Status.PodStartedAt = ptime(*tc.podStarted)
			}
			if tc.workStart != nil {
				task.Status.StageWorkStartedAt = ptime(*tc.workStart)
			}
			clock, since, budget, onElapse := stage.ArmedClock(task, tc.paused)
			if clock != tc.wantClock {
				t.Fatalf("clock = %q, want %q", clock, tc.wantClock)
			}
			if tc.wantClock == stage.ClockNone {
				return
			}
			if !since.Equal(tc.wantSince) {
				t.Errorf("since = %v, want %v", since, tc.wantSince)
			}
			if budget != tc.wantBudget {
				t.Errorf("budget = %v, want %v", budget, tc.wantBudget)
			}
			if onElapse.To != tc.wantTo {
				t.Errorf("onElapse.To = %q, want %q", onElapse.To, tc.wantTo)
			}
			if onElapse.Reason != tc.wantReason {
				t.Errorf("onElapse.Reason = %q, want %q", onElapse.Reason, tc.wantReason)
			}
		})
	}
}

// merging is POD-LESS. Read literally, v6 gave it a 24h admission-starved
// clock instead of its 4h merge-timeout budget - so merging could never reach
// merge-timeout and the bounded merge re-entry cycle never engaged AT ALL.
func TestMergingStallsIntoMergeTimeoutNotAdmissionStarved(t *testing.T) {
	task := newTask("implement", v1alpha1.StageMerging, "")
	entered := time.Date(2026, 7, 12, 9, 0, 0, 0, time.UTC)
	task.Status.StageEnteredAt = ptime(entered)

	if _, fired := stage.Elapsed(task, false, entered.Add(3*time.Hour+59*time.Minute)); fired {
		t.Fatal("merging fired before its 4h budget")
	}
	edge, fired := stage.Elapsed(task, false, entered.Add(4*time.Hour+time.Second))
	if !fired {
		t.Fatal("merging did NOT elapse at 4h: the merge re-entry cycle can never engage")
	}
	if edge.To != v1alpha1.StageParked || edge.Reason != stage.ReasonMergeTimeout {
		t.Fatalf("merging elapsed to %s(%s), want parked(merge-timeout)", edge.To, edge.Reason)
	}
	if edge.Reason == stage.ReasonAdmissionStarved {
		t.Fatal("merging got a 24h admission-starved clock: it is POD-LESS and runs clock 3 ONLY")
	}
}

// The WORK clock measures WORK, not queue wait.
func TestWorkClockMeasuresWorkNotQueueWait(t *testing.T) {
	task := newTask("implement", v1alpha1.StageImplementing, "")
	entered := time.Date(2026, 7, 12, 9, 0, 0, 0, time.UTC)
	task.Status.StageEnteredAt = ptime(entered)

	// 3h in the admission queue, then the pod runs.
	podAt := entered.Add(3 * time.Hour)
	task.Status.PodStartedAt = ptime(podAt)
	task.Status.StageWorkStartedAt = ptime(podAt.Add(time.Minute))

	// 5h of WORK, 8h since stageEnteredAt. The 6h budget is NOT spent.
	if edge, fired := stage.Elapsed(task, false, podAt.Add(5*time.Hour)); fired {
		t.Fatalf("a Task that queued 3h then worked 5h fired %v: the work budget is measured from stageWorkStartedAt", edge)
	}
	// 6h of WORK: spent.
	edge, fired := stage.Elapsed(task, false, podAt.Add(time.Minute).Add(6*time.Hour+time.Second))
	if !fired {
		t.Fatal("implementing did not park at 6h of work")
	}
	if edge.To != v1alpha1.StageParked || edge.Reason != stage.ReasonStageDeadline {
		t.Fatalf("got %s(%s), want parked(stage-deadline)", edge.To, edge.Reason)
	}
}

// ===========================================================================
// 5. THE podStartedAt LIFECYCLE (fix V7-4).
// ===========================================================================

func TestEveryTransitionClearsBothTimestampsAndPodRecreations(t *testing.T) {
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	stale := time.Date(2026, 7, 12, 8, 0, 0, 0, time.UTC)

	for from, edges := range stage.Transitions {
		for _, e := range edges {
			if from == stage.Create {
				continue
			}
			t.Run(from+"->"+e.To+"("+e.Reason+")", func(t *testing.T) {
				// A non-review kind so no kind guard interferes; MRs with no
				// pendingReview so the C.5.3 gate is open.
				task := newTask("implement", from, "")
				task.Status.PodStartedAt = ptime(stale)
				task.Status.StageWorkStartedAt = ptime(stale)
				task.Status.Stats.PodRecreations = 2
				mrs := []v1alpha1.MergeRequest{openMR()}

				if err := stage.Enter(task, mrs, e.To, e.Reason, now); err != nil {
					t.Fatalf("Enter: %v", err)
				}
				if task.Status.PodStartedAt != nil {
					t.Error("podStartedAt was NOT cleared. It is load-bearing: a stale value leaves the Task covered by NO clock while it queues, and TTL-stops its next pod before that pod's first turn")
				}
				if task.Status.StageWorkStartedAt != nil {
					t.Error("stageWorkStartedAt was NOT cleared")
				}
				if task.Status.Stats.PodRecreations != 0 {
					t.Error("stats.podRecreations was NOT reset")
				}
				if task.Status.StageEnteredAt == nil || !task.Status.StageEnteredAt.Time.Equal(now) {
					t.Error("stageEnteredAt was NOT re-stamped")
				}
				if task.Status.Stage != e.To {
					t.Errorf("stage = %q, want %q", task.Status.Stage, e.To)
				}
			})
		}
	}
}

// Failure (a): a re-entry edge with a stale podStartedAt is covered by NO clock
// while it queues.
func TestReentryEdgeIsCoveredByAClockWhileItQueues(t *testing.T) {
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	task := newTask("implement", v1alpha1.StageReviewing, "")
	task.Status.PodStartedAt = ptime(now.Add(-4 * time.Hour)) // the review pod
	task.Status.StageWorkStartedAt = ptime(now.Add(-3 * time.Hour))
	mrs := []v1alpha1.MergeRequest{openMR()}

	if err := stage.Enter(task, mrs, v1alpha1.StageImplementing, "", now); err != nil {
		t.Fatalf("reviewing -> implementing: %v", err)
	}
	clock, since, _, _ := stage.ArmedClock(task, false)
	if clock == stage.ClockNone {
		t.Fatal("a Task on the reviewing -> implementing re-entry edge is covered by NO CLOCK while it queues")
	}
	if clock != stage.ClockAdmission {
		t.Fatalf("armed clock = %q, want %q", clock, stage.ClockAdmission)
	}
	if !since.Equal(now) {
		t.Fatalf("clock 1 since = %v, want the FRESH stageEnteredAt %v", since, now)
	}
}

// Failure (b): a stale podStartedAt puts the G.7 TTL base in the past, so the
// operator TTL-stops the fresh pod before its first turn.
func TestReentryEdgeDoesNotTTLStopTheFreshPod(t *testing.T) {
	const agentPodTTLSeconds = 3600
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	task := newTask("implement", v1alpha1.StageReviewing, "")
	task.Status.PodStartedAt = ptime(now.Add(-4 * time.Hour))
	task.Status.StageWorkStartedAt = ptime(now.Add(-3 * time.Hour))
	mrs := []v1alpha1.MergeRequest{openMR()}

	if err := stage.Enter(task, mrs, v1alpha1.StageImplementing, "", now); err != nil {
		t.Fatalf("Enter: %v", err)
	}
	if task.Status.PodStartedAt != nil {
		t.Fatal("podStartedAt survived the transition: t0 = podStartedAt + agentPodTTLSeconds is ALREADY IN THE PAST for the fresh pod")
	}
	// The reconciler now stamps podStartedAt at pod CREATE.
	task.Status.PodStartedAt = ptime(now)
	t0 := task.Status.PodStartedAt.Add(agentPodTTLSeconds * time.Second)
	if !t0.After(now) {
		t.Fatalf("TTL t0 = %v is not in the future of %v", t0, now)
	}
}

// ===========================================================================
// 6. THE INVARIANT: every stage enum member has a row in the deadline table.
// ===========================================================================

func TestEveryStageHasABudget(t *testing.T) {
	if len(stage.AllStages()) != 15 {
		t.Errorf("AllStages() has %d members, want the 15 of F.1", len(stage.AllStages()))
	}
	for _, s := range stage.AllStages() {
		if _, ok := stage.Budget(s); !ok {
			t.Errorf("stage %q has NO budget row: no stage may be entered without a deadline that leaves it", s)
		}
		if _, ok := stage.OnElapse(s); !ok {
			t.Errorf("stage %q has no OnElapse row", s)
		}
	}
}

// parked(backlog-sweep) is the ONE exemption, and it is an explicit NAMED entry
// here, not an omission.
func TestBacklogSweepIsTheOneExemption(t *testing.T) {
	task := newTask("implement", v1alpha1.StageParked, stage.ReasonBacklogSweep)
	entered := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	task.Status.StageEnteredAt = ptime(entered)

	if clock, _, _, _ := stage.ArmedClock(task, false); clock != stage.ClockNone {
		t.Fatalf("parked(backlog-sweep) armed clock %q, want none: it is not stalled work, it is the durable owner of an Issue CR", clock)
	}
	if edge, fired := stage.Elapsed(task, false, entered.Add(365*24*time.Hour)); fired {
		t.Fatalf("parked(backlog-sweep) aged out after a year into %v. It NEVER ages out", edge)
	}
	// The stage `parked` still HAS a row in the budget table.
	if _, ok := stage.Budget(v1alpha1.StageParked); !ok {
		t.Fatal("the `parked` STAGE must still have a budget row (parkRetention)")
	}
}

func TestBudgetTableIsVerbatimF4(t *testing.T) {
	want := map[string]struct {
		budget time.Duration
		to     string
		reason string
	}{
		v1alpha1.StageTriaging:      {5 * time.Minute, v1alpha1.StageFailed, stage.ReasonTriageStalled},
		v1alpha1.StageBrainstorming: {2 * time.Hour, v1alpha1.StageParked, stage.ReasonStageDeadline},
		v1alpha1.StageClarifying:    {24 * time.Hour, v1alpha1.StageParked, stage.ReasonAwaitingHuman},
		v1alpha1.StageInvestigating: {2 * time.Hour, v1alpha1.StageParked, stage.ReasonStageDeadline},
		v1alpha1.StageRefining:      {2 * time.Hour, v1alpha1.StageParked, stage.ReasonStageDeadline},
		v1alpha1.StageApproved:      {24 * time.Hour, v1alpha1.StageParked, stage.ReasonAdmissionStarved},
		v1alpha1.StageImplementing:  {6 * time.Hour, v1alpha1.StageParked, stage.ReasonStageDeadline},
		v1alpha1.StageReviewing:     {4 * time.Hour, v1alpha1.StageParked, stage.ReasonStageDeadline},
		v1alpha1.StageMerging:       {4 * time.Hour, v1alpha1.StageParked, stage.ReasonMergeTimeout},
		v1alpha1.StageDeploying:     {2 * time.Hour, v1alpha1.StageParked, stage.ReasonDeployTimeout},
		v1alpha1.StageDocumenting:   {2 * time.Hour, v1alpha1.StageDelivered, stage.ReasonDocTimeout},
		v1alpha1.StageDelivered:     {48 * time.Hour, stage.Reap, ""},
		v1alpha1.StageRejected:      {24 * time.Hour, stage.Reap, ""},
		v1alpha1.StageFailed:        {7 * 24 * time.Hour, stage.Reap, ""},
		v1alpha1.StageParked:        {7 * 24 * time.Hour, stage.Reap, ""},
	}
	for s, w := range want {
		got, ok := stage.Budget(s)
		if !ok {
			t.Errorf("%s: no budget", s)
			continue
		}
		if got != w.budget {
			t.Errorf("%s budget = %v, want %v", s, got, w.budget)
		}
		edge, ok := stage.OnElapse(s)
		if !ok {
			t.Errorf("%s: no OnElapse", s)
			continue
		}
		if edge.To != w.to || edge.Reason != w.reason {
			t.Errorf("%s onElapse = %s(%s), want %s(%s)", s, edge.To, edge.Reason, w.to, w.reason)
		}
	}
}

// ===========================================================================
// 7. THE F.3 TRANSITION TABLE, ROW BY ROW.
// ===========================================================================

func TestTransitionTable(t *testing.T) {
	legal := [][2]string{
		{stage.Create, v1alpha1.StageTriaging},
		{stage.Create, v1alpha1.StageParked},      // parked(backlog-sweep)
		{stage.Create, v1alpha1.StageDocumenting}, // the nightly batch mint
		{v1alpha1.StageParked, v1alpha1.StageTriaging},
		{v1alpha1.StageTriaging, v1alpha1.StageBrainstorming},
		{v1alpha1.StageTriaging, v1alpha1.StageClarifying},
		{v1alpha1.StageTriaging, v1alpha1.StageInvestigating},
		{v1alpha1.StageTriaging, v1alpha1.StageRefining},
		{v1alpha1.StageTriaging, v1alpha1.StageReviewing},
		{v1alpha1.StageTriaging, v1alpha1.StageDocumenting},
		{v1alpha1.StageTriaging, v1alpha1.StageFailed},
		{v1alpha1.StageBrainstorming, v1alpha1.StageDelivered},
		{v1alpha1.StageClarifying, v1alpha1.StageApproved},
		{v1alpha1.StageClarifying, v1alpha1.StageParked},
		{v1alpha1.StageClarifying, v1alpha1.StageRejected},
		{v1alpha1.StageApproved, v1alpha1.StageClarifying},
		{v1alpha1.StageApproved, v1alpha1.StageImplementing},
		{v1alpha1.StageInvestigating, v1alpha1.StageClarifying},
		{v1alpha1.StageInvestigating, v1alpha1.StageRejected},
		{v1alpha1.StageRefining, v1alpha1.StageDelivered},
		{v1alpha1.StageRefining, v1alpha1.StageFailed},
		{v1alpha1.StageImplementing, v1alpha1.StageReviewing},
		{v1alpha1.StageImplementing, v1alpha1.StageParked},
		{v1alpha1.StageReviewing, v1alpha1.StageImplementing},
		{v1alpha1.StageReviewing, v1alpha1.StageMerging},
		{v1alpha1.StageReviewing, v1alpha1.StageParked},
		{v1alpha1.StageMerging, v1alpha1.StageReviewing},
		{v1alpha1.StageMerging, v1alpha1.StageDeploying},
		{v1alpha1.StageMerging, v1alpha1.StageFailed},
		{v1alpha1.StageMerging, v1alpha1.StageParked},
		{v1alpha1.StageMerging, v1alpha1.StageImplementing}, // maintainer changes_requested on the still-open MR (Task 4b)
		{v1alpha1.StageDeploying, v1alpha1.StageDelivered},
		{v1alpha1.StageDeploying, v1alpha1.StageFailed},
		{v1alpha1.StageDeploying, v1alpha1.StageParked},
		{v1alpha1.StageDocumenting, v1alpha1.StageReviewing},
		{v1alpha1.StageDocumenting, v1alpha1.StageDelivered},
		{v1alpha1.StageParked, v1alpha1.StageReviewing},
		{v1alpha1.StageParked, v1alpha1.StageImplementing},
		{v1alpha1.StageParked, v1alpha1.StageClarifying},
		{v1alpha1.StageParked, v1alpha1.StageMerging},
		{v1alpha1.StageParked, v1alpha1.StageDeploying},
		{v1alpha1.StageParked, v1alpha1.StageFailed},
	}
	for _, e := range legal {
		if !stage.Legal(e[0], e[1]) {
			t.Errorf("Legal(%s -> %s) = false, want true", e[0], e[1])
		}
	}

	// Every non-terminal stage can fail on operator-error.
	for _, s := range stage.AllStages() {
		if s == v1alpha1.StageFailed || s == v1alpha1.StageRejected {
			continue
		}
		if s == v1alpha1.StageParked {
			continue // parked -> failed exists only for merge-blocked/deploy-blocked
		}
		if !stage.Legal(s, v1alpha1.StageFailed) {
			t.Errorf("Legal(%s -> failed) = false: any non-terminal must be able to fail(operator-error)", s)
		}
	}

	illegal := [][2]string{
		// The autonomous loops.
		{v1alpha1.StageTriaging, v1alpha1.StageImplementing}, // no approval gate
		{v1alpha1.StageTriaging, v1alpha1.StageMerging},
		{v1alpha1.StageBrainstorming, v1alpha1.StageImplementing},
		{v1alpha1.StageClarifying, v1alpha1.StageImplementing}, // must pass through approved
		{v1alpha1.StageClarifying, v1alpha1.StageMerging},
		{v1alpha1.StageApproved, v1alpha1.StageMerging},
		{v1alpha1.StageApproved, v1alpha1.StageReviewing},
		{v1alpha1.StageImplementing, v1alpha1.StageMerging}, // never skip review
		{v1alpha1.StageImplementing, v1alpha1.StageDeploying},
		{v1alpha1.StageImplementing, v1alpha1.StageDelivered},
		{v1alpha1.StageReviewing, v1alpha1.StageDeploying}, // never skip merge
		{v1alpha1.StageReviewing, v1alpha1.StageDelivered},
		{v1alpha1.StageMerging, v1alpha1.StageDelivered},
		{v1alpha1.StageDeploying, v1alpha1.StageImplementing},
		{v1alpha1.StageDeploying, v1alpha1.StageMerging},
		{v1alpha1.StageDeploying, v1alpha1.StageReviewing},
		// Terminals are terminal.
		{v1alpha1.StageFailed, v1alpha1.StageImplementing},
		{v1alpha1.StageFailed, v1alpha1.StageTriaging},
		{v1alpha1.StageRejected, v1alpha1.StageClarifying},
		{v1alpha1.StageRejected, v1alpha1.StageTriaging},
		{v1alpha1.StageDelivered, v1alpha1.StageImplementing},
		{v1alpha1.StageDelivered, v1alpha1.StageDocumenting},
		// parked never re-enters brainstorming/investigating/refining/documenting.
		{v1alpha1.StageParked, v1alpha1.StageBrainstorming},
		{v1alpha1.StageParked, v1alpha1.StageInvestigating},
		{v1alpha1.StageParked, v1alpha1.StageRefining},
		{v1alpha1.StageParked, v1alpha1.StageDocumenting},
		{v1alpha1.StageParked, v1alpha1.StageApproved},
		{v1alpha1.StageParked, v1alpha1.StageDelivered},
		{v1alpha1.StageParked, v1alpha1.StageRejected},
		// Self-loops are not transitions.
		{v1alpha1.StageImplementing, v1alpha1.StageImplementing},
		{v1alpha1.StageMerging, v1alpha1.StageMerging},
		{v1alpha1.StageParked, v1alpha1.StageParked},
		// The create pseudo-stage is not a destination.
		{v1alpha1.StageTriaging, stage.Create},
	}
	for _, e := range illegal {
		if stage.Legal(e[0], e[1]) {
			t.Errorf("Legal(%s -> %s) = true, want false", e[0], e[1])
		}
		// The reconciler emits operator_illegal_stage_transition_total{from,to}
		// off exactly these labels.
		task := newTask("implement", e[0], "")
		err := stage.Enter(task, nil, e[1], "", time.Now())
		var ill *stage.IllegalTransitionError
		if !errors.As(err, &ill) {
			t.Errorf("Enter(%s -> %s): err = %v, want IllegalTransitionError", e[0], e[1], err)
			continue
		}
		if ill.From != e[0] || ill.To != e[1] {
			t.Errorf("IllegalTransitionError{from=%q,to=%q}, want {%q,%q}", ill.From, ill.To, e[0], e[1])
		}
	}
}

// Step 4: the machine is GATED on pendingReview == nil.
func TestReviewingExitsAreGatedOnPendingReviewNil(t *testing.T) {
	pending := &v1alpha1.MergeRequest{Status: v1alpha1.MergeRequestStatus{
		State:         "open",
		PendingReview: &v1alpha1.PendingReview{},
	}}
	for _, to := range []string{v1alpha1.StageImplementing, v1alpha1.StageMerging} {
		t.Run(to, func(t *testing.T) {
			task := newTask("implement", v1alpha1.StageReviewing, "")
			mrs := []v1alpha1.MergeRequest{openMR(), *pending}
			if stage.LegalFor(task, mrs, v1alpha1.StageReviewing, to) {
				t.Fatalf("reviewing -> %s with a non-nil pendingReview is LEGAL: a pod must never be spawned to fix findings that have not been recorded", to)
			}
			if err := stage.Enter(task, mrs, to, "", time.Now()); err == nil {
				t.Fatalf("Enter(reviewing -> %s) with a pending review succeeded", to)
			}
			// Drained: the gate opens.
			mrs[1].Status.PendingReview = nil
			if !stage.LegalFor(task, mrs, v1alpha1.StageReviewing, to) {
				t.Fatalf("reviewing -> %s with every pendingReview drained is still refused", to)
			}
		})
	}
	// An EMPTY owned-MR set does not open the gate either.
	task := newTask("implement", v1alpha1.StageReviewing, "")
	if stage.LegalFor(task, nil, v1alpha1.StageReviewing, v1alpha1.StageMerging) {
		t.Fatal("reviewing -> merging with ZERO owned MRs is legal: the empty set is not a licence")
	}
}

// ===========================================================================
// 8. THE F.2 AGENT-KIND TABLE.
// ===========================================================================

func TestAgentKindFor(t *testing.T) {
	want := map[string]string{
		v1alpha1.StageTriaging:      "",
		v1alpha1.StageBrainstorming: "brainstorm",
		v1alpha1.StageClarifying:    "clarify",
		v1alpha1.StageInvestigating: "incident",
		v1alpha1.StageRefining:      "refine",
		v1alpha1.StageApproved:      "",
		v1alpha1.StageImplementing:  "implement",
		v1alpha1.StageReviewing:     "review",
		v1alpha1.StageMerging:       "",
		v1alpha1.StageDeploying:     "",
		v1alpha1.StageDocumenting:   "documentation",
		v1alpha1.StageDelivered:     "",
		v1alpha1.StageRejected:      "",
		v1alpha1.StageFailed:        "",
		v1alpha1.StageParked:        "",
	}
	for _, s := range stage.AllStages() {
		got := stage.AgentKindFor(s)
		if got != want[s] {
			t.Errorf("AgentKindFor(%s) = %q, want %q", s, got, want[s])
		}
		// F.2 and v1alpha1.StagePodless must agree, mechanically.
		if (got == "") != v1alpha1.StagePodless(s) {
			t.Errorf("AgentKindFor(%s)=%q contradicts StagePodless(%s)=%v", s, got, s, v1alpha1.StagePodless(s))
		}
	}
}

// ===========================================================================
// 9. THE FOUR CYCLE CAPS. THERE ARE FOUR, NOT THREE.
// ===========================================================================

// Cycle 2: merging <-> parked(merge-timeout). A permanently red MR cannot spin
// forever.
func TestCycleCap_MergeReentriesBounded(t *testing.T) {
	task := newTask("implement", v1alpha1.StageMerging, "")
	h := newHarness(t, task)
	h.mrs = []v1alpha1.MergeRequest{openMR()}

	laps := 0
	for i := range 20 {
		h.now = h.now.Add(4 * time.Hour)
		// The 4h merge budget elapses.
		if err := h.enter(v1alpha1.StageParked, stage.ReasonMergeTimeout); err != nil {
			t.Fatalf("lap %d: merging -> parked(merge-timeout): %v", i, err)
		}
		target, ok := h.unpark(stage.UnparkInput{MaxOpenTasks: 6, BotLogin: botLogin, MaxTurnsPerTask: 300})
		if !ok {
			t.Fatalf("lap %d: unpark(merge-timeout) refused without a terminal", i)
		}
		if target == v1alpha1.StageFailed {
			if task.Status.StageReason != stage.ReasonMergeBlocked {
				t.Fatalf("terminal reason = %q, want merge-blocked", task.Status.StageReason)
			}
			if laps != v1alpha1.MaxMergeReentries {
				t.Fatalf("failed(merge-blocked) after %d re-entries, want %d", laps, v1alpha1.MaxMergeReentries)
			}
			if task.Status.MergeReentries != v1alpha1.MaxMergeReentries {
				t.Fatalf("mergeReentries = %d, want %d", task.Status.MergeReentries, v1alpha1.MaxMergeReentries)
			}
			return
		}
		if target != v1alpha1.StageMerging {
			t.Fatalf("lap %d: merge-timeout re-entered %q. It re-enters merging, NEVER implementing", i, target)
		}
		laps++
	}
	t.Fatal("a permanently red MR cycled merging <-> parked(merge-timeout) indefinitely")
}

// Cycle 3: deploying <-> parked(deploy-timeout).
func TestCycleCap_DeployReentriesBounded(t *testing.T) {
	task := newTask("implement", v1alpha1.StageDeploying, "")
	h := newHarness(t, task)
	h.mrs = []v1alpha1.MergeRequest{mergedMR()}

	laps := 0
	for i := range 20 {
		h.now = h.now.Add(2 * time.Hour)
		if err := h.enter(v1alpha1.StageParked, stage.ReasonDeployTimeout); err != nil {
			t.Fatalf("lap %d: %v", i, err)
		}
		target, ok := h.unpark(stage.UnparkInput{MaxOpenTasks: 6, BotLogin: botLogin, MaxTurnsPerTask: 300})
		if !ok {
			t.Fatalf("lap %d: unpark(deploy-timeout) refused without a terminal", i)
		}
		if target == v1alpha1.StageFailed {
			if task.Status.StageReason != stage.ReasonDeployBlocked {
				t.Fatalf("terminal reason = %q, want deploy-blocked", task.Status.StageReason)
			}
			if laps != v1alpha1.MaxDeployReentries {
				t.Fatalf("failed(deploy-blocked) after %d re-entries, want %d", laps, v1alpha1.MaxDeployReentries)
			}
			return
		}
		if target != v1alpha1.StageDeploying {
			t.Fatalf("lap %d: deploy-timeout re-entered %q. NEVER implementing", i, target)
		}
		laps++
	}
	t.Fatal("deploying <-> parked(deploy-timeout) cycled indefinitely")
}

// Cycle 4: reviewing <-> merging on a MOVED HEAD. This is the FOURTH cycle,
// the ONLY one that SPAWNS A POD every lap, and it had no counter anywhere.
func TestCycleCap_HeadMoveReentriesBoundedAndPodSpawnsCapped(t *testing.T) {
	task := newTask("implement", v1alpha1.StageMerging, "")
	h := newHarness(t, task)
	h.mrs = []v1alpha1.MergeRequest{openMR()}

	for i := range 20 {
		h.now = h.now.Add(time.Minute)
		// The head moved (or Merge 409'd "head moved"). The operator re-reviews.
		edge, ok := stage.HeadMoved(task, v1alpha1.MaxHeadMoveReentries)
		if !ok {
			t.Fatalf("lap %d: HeadMoved returned no edge", i)
		}
		if edge.To == v1alpha1.StageFailed {
			if edge.Reason != stage.ReasonHeadMoving {
				t.Fatalf("terminal reason = %q, want head-moving", edge.Reason)
			}
			if err := h.enter(edge.To, edge.Reason); err != nil {
				t.Fatalf("merging -> failed(head-moving): %v", err)
			}
			// THE POINT: a PR whose head moves every lap spawns a BOUNDED number
			// of review pods.
			if got := h.pods.spawned[stage.AgentReview]; got != v1alpha1.MaxHeadMoveReentries {
				t.Fatalf("review pods spawned on head moves = %d, want at most %d", got, v1alpha1.MaxHeadMoveReentries)
			}
			if task.Status.HeadMoveReentries != v1alpha1.MaxHeadMoveReentries {
				t.Fatalf("headMoveReentries = %d, want %d", task.Status.HeadMoveReentries, v1alpha1.MaxHeadMoveReentries)
			}
			return
		}
		if edge.To != v1alpha1.StageReviewing {
			t.Fatalf("lap %d: head-moved edge -> %q, want reviewing", i, edge.To)
		}
		if err := h.enter(edge.To, edge.Reason); err != nil {
			t.Fatalf("lap %d: merging -> reviewing: %v", i, err)
		}
		// The review completes and we go back to merging.
		if err := h.enter(v1alpha1.StageMerging, ""); err != nil {
			t.Fatalf("lap %d: reviewing -> merging: %v", i, err)
		}
	}
	t.Fatal("reviewing <-> merging on a moved head cycled indefinitely, spawning a review pod every lap")
}

// Cycle 1: reviewing <-> implementing, bounded by mr.status.reviewRounds. It is
// the ONE cycle v3 got right; assert it still is.
func TestCycleCap_ReviewRoundsBounded(t *testing.T) {
	const maxReviewRounds = 3
	task := newTask("implement", v1alpha1.StageReviewing, "")
	mr := openMR()
	mr.Status.ReviewRounds = maxReviewRounds
	edge, ok := stage.RequestChanges(task, []v1alpha1.MergeRequest{mr}, maxReviewRounds)
	if !ok {
		t.Fatal("RequestChanges produced no edge")
	}
	if edge.To != v1alpha1.StageParked || edge.Reason != stage.ReasonReviewLoopExhausted {
		t.Fatalf("request_changes at maxReviewRounds = %s(%s), want parked(review-loop-exhausted)", edge.To, edge.Reason)
	}
	mr.Status.ReviewRounds = maxReviewRounds - 1
	edge, _ = stage.RequestChanges(task, []v1alpha1.MergeRequest{mr}, maxReviewRounds)
	if edge.To != v1alpha1.StageImplementing {
		t.Fatalf("request_changes under the cap = %q, want implementing", edge.To)
	}
	// And on a review-kind Task the SAME verdict parks awaiting-human. This is
	// the V7-1 edge, checked here too because RequestChanges is what /outcome
	// calls.
	rev := newTask("review", v1alpha1.StageReviewing, "")
	edge, _ = stage.RequestChanges(rev, []v1alpha1.MergeRequest{openMR()}, maxReviewRounds)
	if edge.To != v1alpha1.StageParked || edge.Reason != stage.ReasonAwaitingHuman {
		t.Fatalf("request_changes on a kind=review Task = %s(%s), want parked(awaiting-human)", edge.To, edge.Reason)
	}
}

// ===========================================================================
// 10. UNPARK: one test per stageReason in F.6.
// ===========================================================================

func TestUnpark_BacklogSweepPromotesOnNonBotEventUnderCap(t *testing.T) {
	task := newTask("clarify", v1alpha1.StageParked, stage.ReasonBacklogSweep)
	humanEvent(task)
	h := newHarness(t, task)
	target, ok := h.unpark(stage.UnparkInput{ActiveTasks: 5, MaxOpenTasks: 6, BotLogin: botLogin})
	if !ok || target != v1alpha1.StageTriaging {
		t.Fatalf("backlog-sweep + human comment under cap = (%q,%v), want (triaging,true)", target, ok)
	}
}

func TestUnpark_BacklogSweepIgnoresBotEvent(t *testing.T) {
	task := newTask("clarify", v1alpha1.StageParked, stage.ReasonBacklogSweep)
	botEvent(task)
	h := newHarness(t, task)
	if target, ok := h.unpark(stage.UnparkInput{ActiveTasks: 0, MaxOpenTasks: 6, BotLogin: botLogin}); ok {
		t.Fatalf("a BOT event promoted a parked Task to %q: the operator's own park comment can never un-park anything", target)
	}
	// And with no event at all.
	task2 := newTask("clarify", v1alpha1.StageParked, stage.ReasonBacklogSweep)
	h2 := newHarness(t, task2)
	if _, ok := h2.unpark(stage.UnparkInput{ActiveTasks: 0, MaxOpenTasks: 6, BotLogin: botLogin}); ok {
		t.Fatal("backlog-sweep promoted with NO pendingEvent")
	}
}

// fix H8: a maintainer's bulk comment pass must not promote 40 Tasks past a cap
// of 6. Exactly 6 promote; 34 stay parked with their events INTACT.
func TestUnpark_BacklogSweepDefersOverCapAndRetainsTheEvent(t *testing.T) {
	const maxOpen = 6
	active := 0 // no ACTIVE Tasks to start

	tasks := make([]*v1alpha1.Task, 40)
	for i := range tasks {
		tasks[i] = newTask("clarify", v1alpha1.StageParked, stage.ReasonBacklogSweep)
		humanEvent(tasks[i])
	}

	promoted, deferred := 0, 0
	for _, task := range tasks {
		h := newHarness(t, task)
		target, ok := h.unpark(stage.UnparkInput{ActiveTasks: active, MaxOpenTasks: maxOpen, BotLogin: botLogin})
		if ok {
			if target != v1alpha1.StageTriaging {
				t.Fatalf("promoted to %q, want triaging", target)
			}
			promoted++
			active++ // it is now an ACTIVE Task and occupies a slot
			continue
		}
		deferred++
		if task.Status.Stage != v1alpha1.StageParked || task.Status.StageReason != stage.ReasonBacklogSweep {
			t.Fatalf("a DEFERRED promotion mutated the Task: %s(%s)", task.Status.Stage, task.Status.StageReason)
		}
		if len(task.Status.PendingEvents) != 1 {
			t.Fatalf("the deferred Task's pendingEvent was DROPPED (%d events left). It is RETAINED, never dropped", len(task.Status.PendingEvents))
		}
	}
	if promoted != maxOpen {
		t.Fatalf("promoted %d Tasks past a cap of %d", promoted, maxOpen)
	}
	if deferred != 40-maxOpen {
		t.Fatalf("deferred %d, want %d", deferred, 40-maxOpen)
	}
}

func TestUnpark_BacklogSweepNeverAgesOut(t *testing.T) {
	// Covered by TestBacklogSweepIsTheOneExemption for the clock; here for the
	// reap decision.
	task := newTask("clarify", v1alpha1.StageParked, stage.ReasonBacklogSweep)
	entered := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	task.Status.StageEnteredAt = ptime(entered)
	if _, fired := stage.Elapsed(task, false, entered.Add(6*365*24*time.Hour)); fired {
		t.Fatal("parked(backlog-sweep) aged out. It NEVER ages out: it is reaped only when its Issues close")
	}
}

func TestUnpark_AwaitingHumanReviewKindBoundedByHumanReviewRounds(t *testing.T) {
	// 20 human comments on a parked review Task spawn AT MOST 5 review pods.
	// humanReviewRounds is a NEW counter and it is NOT mr.status.reviewRounds
	// (which increments only on request_changes, so on the approve path v6's
	// claimed bound did not exist).
	task := newTask("review", v1alpha1.StageParked, stage.ReasonAwaitingHuman)
	h := newHarness(t, task)
	h.mrs = []v1alpha1.MergeRequest{openMR()}

	for i := range 20 {
		h.now = h.now.Add(time.Hour)
		task.Status.PendingEvents = nil
		humanEvent(task)
		if task.Status.Stage == v1alpha1.StageReviewing {
			// The review pod ran and re-parked (both verdicts land here).
			if err := h.enter(v1alpha1.StageParked, stage.ReasonAwaitingHuman); err != nil {
				t.Fatalf("lap %d: reviewing -> parked(awaiting-human): %v", i, err)
			}
		}
		target, ok := h.unpark(stage.UnparkInput{ActiveTasks: 1, MaxOpenTasks: 6, BotLogin: botLogin, MaxTurnsPerTask: 300})
		if ok && target != v1alpha1.StageReviewing {
			t.Fatalf("lap %d: a review Task un-parked into %q", i, target)
		}
	}
	if got := h.pods.spawned[stage.AgentReview]; got != v1alpha1.MaxHumanReviewRounds {
		t.Fatalf("20 human comments spawned %d review pods, want at most %d", got, v1alpha1.MaxHumanReviewRounds)
	}
	if h.pods.spawned[stage.AgentImplement] != 0 {
		t.Fatal("an implement pod was spawned")
	}
	if task.Status.Stage != v1alpha1.StageParked {
		t.Fatalf("at the cap the Task must STAY PARKED, got %q", task.Status.Stage)
	}
}

func TestUnpark_AwaitingHumanTargetIsRederivedFromState(t *testing.T) {
	cases := []struct {
		name   string
		issues []v1alpha1.Issue
		want   string
		ok     bool
	}{
		{"every open Issue approved -> implementing",
			[]v1alpha1.Issue{openIssue("approved"), openIssue("approved")},
			v1alpha1.StageImplementing, true},
		{"one open Issue NOT approved -> clarifying",
			[]v1alpha1.Issue{openIssue("approved"), openIssue("new")},
			v1alpha1.StageClarifying, true},
		{"no open Issue at all -> STAY PARKED",
			nil, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := newTask("clarify", v1alpha1.StageParked, stage.ReasonAwaitingHuman)
			// parkedFromStage is OBSERVABILITY ONLY. Set it to a LIE and assert
			// the target is re-derived from state anyway.
			task.Status.ParkedFromStage = v1alpha1.StageMerging
			humanEvent(task)
			h := newHarness(t, task)
			target, ok := h.unpark(stage.UnparkInput{Issues: tc.issues, ActiveTasks: 1, MaxOpenTasks: 6, BotLogin: botLogin, MaxTurnsPerTask: 300})
			if ok != tc.ok || target != tc.want {
				t.Fatalf("got (%q,%v), want (%q,%v)", target, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestUnpark_AwaitingHumanRequiresANonBotEvent(t *testing.T) {
	task := newTask("clarify", v1alpha1.StageParked, stage.ReasonAwaitingHuman)
	botEvent(task)
	h := newHarness(t, task)
	if _, ok := h.unpark(stage.UnparkInput{
		Issues: []v1alpha1.Issue{openIssue("approved")}, ActiveTasks: 1, MaxOpenTasks: 6, BotLogin: botLogin,
	}); ok {
		t.Fatal("a BOT comment un-parked an awaiting-human Task")
	}
}

func TestUnpark_IdentityUnverified(t *testing.T) {
	t.Run("a BOT comment changes NOTHING", func(t *testing.T) {
		task := newTask("clarify", v1alpha1.StageParked, stage.ReasonIdentityUnverified)
		botEvent(task)
		h := newHarness(t, task)
		if _, ok := h.unpark(stage.UnparkInput{
			Issues: []v1alpha1.Issue{openIssue("approved")}, GrammarPassed: true,
			ActiveTasks: 1, MaxOpenTasks: 6, BotLogin: botLogin, MaxTurnsPerTask: 300,
		}); ok {
			t.Fatal("a BOT comment un-parked an identity-unverified Task")
		}
	})

	t.Run("a maintainer comment that PASSES the grammar reaches implementing in ONE comment", func(t *testing.T) {
		task := newTask("clarify", v1alpha1.StageParked, stage.ReasonIdentityUnverified)
		humanEvent(task)
		h := newHarness(t, task)
		// The caller has ALREADY synced the thread from the forge and re-run the
		// C.6 grammar; it passed, and it stamped the Issue approved.
		target, ok := h.unpark(stage.UnparkInput{
			Issues: []v1alpha1.Issue{openIssue("approved")}, GrammarPassed: true,
			ActiveTasks: 1, MaxOpenTasks: 6, BotLogin: botLogin, MaxTurnsPerTask: 300,
		})
		if !ok || target != v1alpha1.StageImplementing {
			t.Fatalf("got (%q,%v), want (implementing,true): human approval must not take 7 days and two comments", target, ok)
		}
		if h.pods.spawned[stage.AgentImplement] != 1 {
			t.Fatal("no implement pod spawned")
		}
	})

	t.Run("a maintainer comment that FAILS the grammar stays parked", func(t *testing.T) {
		task := newTask("clarify", v1alpha1.StageParked, stage.ReasonIdentityUnverified)
		humanEvent(task)
		h := newHarness(t, task)
		if target, ok := h.unpark(stage.UnparkInput{
			Issues: []v1alpha1.Issue{openIssue("new")}, GrammarPassed: false,
			ActiveTasks: 1, MaxOpenTasks: 6, BotLogin: botLogin, MaxTurnsPerTask: 300,
		}); ok {
			t.Fatalf("a comment that FAILED the grammar un-parked the Task to %q. A comment ALONE cannot un-park it", target)
		}
		if task.Status.Stage != v1alpha1.StageParked {
			t.Fatal("the Task left parked")
		}
	})

	t.Run("H9: EVERY owned Issue must be approved", func(t *testing.T) {
		task := newTask("clarify", v1alpha1.StageParked, stage.ReasonIdentityUnverified)
		humanEvent(task)
		h := newHarness(t, task)
		if _, ok := h.unpark(stage.UnparkInput{
			Issues: []v1alpha1.Issue{openIssue("approved"), openIssue("new")}, GrammarPassed: true,
			ActiveTasks: 1, MaxOpenTasks: 6, BotLogin: botLogin, MaxTurnsPerTask: 300,
		}); ok {
			t.Fatal("un-parked to implementing with an UNAPPROVED owned Issue")
		}
	})

	t.Run("the EMPTY owned-Issue set is not a licence here either", func(t *testing.T) {
		task := newTask("clarify", v1alpha1.StageParked, stage.ReasonIdentityUnverified)
		humanEvent(task)
		h := newHarness(t, task)
		if _, ok := h.unpark(stage.UnparkInput{
			Issues: nil, GrammarPassed: true,
			ActiveTasks: 1, MaxOpenTasks: 6, BotLogin: botLogin, MaxTurnsPerTask: 300,
		}); ok {
			t.Fatal("an EMPTY owned-Issue set satisfied the universal quantifier and spawned an implement pod")
		}
	})
}

func TestUnpark_NoOutcome(t *testing.T) {
	t.Run("zero merged MRs and turns under budget -> implementing", func(t *testing.T) {
		task := newTask("implement", v1alpha1.StageParked, stage.ReasonNoOutcome)
		task.Status.Stats.Turns = 10
		h := newHarness(t, task)
		h.mrs = []v1alpha1.MergeRequest{openMR()}
		target, ok := h.unpark(stage.UnparkInput{MaxTurnsPerTask: 300, MaxOpenTasks: 6, BotLogin: botLogin})
		if !ok || target != v1alpha1.StageImplementing {
			t.Fatalf("got (%q,%v), want (implementing,true)", target, ok)
		}
	})

	t.Run("ONE merged MR does NOT re-enter", func(t *testing.T) {
		task := newTask("implement", v1alpha1.StageParked, stage.ReasonNoOutcome)
		task.Status.Stats.Turns = 10
		h := newHarness(t, task)
		h.mrs = []v1alpha1.MergeRequest{mergedMR(), openMR()}
		if target, ok := h.unpark(stage.UnparkInput{MaxTurnsPerTask: 300, MaxOpenTasks: 6, BotLogin: botLogin}); ok {
			t.Fatalf("a no-outcome park with a MERGED MR re-entered %q: a re-implement duplicates an already-merged change", target)
		}
	})

	t.Run("turn budget spent does NOT re-enter", func(t *testing.T) {
		task := newTask("implement", v1alpha1.StageParked, stage.ReasonNoOutcome)
		task.Status.Stats.Turns = 300
		h := newHarness(t, task)
		if _, ok := h.unpark(stage.UnparkInput{MaxTurnsPerTask: 300, MaxOpenTasks: 6, BotLogin: botLogin}); ok {
			t.Fatal("re-entered implementing with the turn budget spent")
		}
	})

	t.Run("a kind=review Task NEVER re-enters implementing on no-outcome", func(t *testing.T) {
		// F.6's no-outcome case has NO kind guard of its own. LegalFor closes it.
		task := newTask("review", v1alpha1.StageParked, stage.ReasonNoOutcome)
		task.Status.Stats.Turns = 10
		h := newHarness(t, task)
		h.mrs = []v1alpha1.MergeRequest{openMR()}
		if target, ok := h.unpark(stage.UnparkInput{MaxTurnsPerTask: 300, MaxOpenTasks: 6, BotLogin: botLogin}); ok {
			t.Fatalf("a kind=review Task un-parked from no-outcome into %q", target)
		}
	})
}

func TestUnpark_DefaultReasonsHaveNoReentry(t *testing.T) {
	for _, reason := range []string{
		stage.ReasonReviewLoopExhausted,
		stage.ReasonImplementDeclined,
		stage.ReasonStageDeadline,
		stage.ReasonAdmissionStarved,
		stage.ReasonTurnBudgetExhausted,
		stage.ReasonPodRecreationExhausted,
		stage.ReasonFoldAdoptionUnverified,
		stage.ReasonDocTimeout,
		stage.ReasonOperatorError,
		stage.ReasonTriageStalled,
		stage.ReasonNameTooLong,
		stage.ReasonReviewPostRefused,
		stage.ReasonObjectTooLarge,
		stage.ReasonMergeOrderMissing,
		stage.ReasonAgentContractMismatch,
	} {
		t.Run(reason, func(t *testing.T) {
			task := newTask("implement", v1alpha1.StageParked, reason)
			humanEvent(task) // even WITH a human comment
			h := newHarness(t, task)
			if target, ok := h.unpark(stage.UnparkInput{
				Issues: []v1alpha1.Issue{openIssue("approved")}, GrammarPassed: true,
				ActiveTasks: 0, MaxOpenTasks: 6, BotLogin: botLogin, MaxTurnsPerTask: 300,
			}); ok {
				t.Fatalf("parked(%s) re-entered %q. It has NO re-entry: it ages out and is reaped", reason, target)
			}
		})
	}
}

// ===========================================================================
// 11. THE F.5 REASON SET IS CLOSED.
// ===========================================================================

func TestReasonsIsTheClosedF5Set(t *testing.T) {
	want := []string{
		"backlog-sweep", "triage-stalled", "name-too-long", "stage-deadline",
		"awaiting-human", "identity-unverified", "implement-declined",
		"review-loop-exhausted", "review-post-refused", "merge-timeout",
		"merge-blocked", "merge-order-missing", "deploy-timeout", "deploy-blocked",
		"no-outcome", "turn-budget-exhausted", "pod-recreation-exhausted",
		"object-too-large", "fold-adoption-unverified", "admission-starved",
		"agent-contract-mismatch", "doc-timeout", "operator-error",
		// head-moving IS in the set (fix M3-9).
		"head-moving",
		// F.5's prose list omits these two, but F.3's transition table
		// emits both (clarifying->rejected on decision=close; investigating
		// ->rejected on submit_outcome(false_positive)), and stage.Enter
		// validates against this set. Per contract M.3, F.3's table wins.
		// The set has 27 members, not 24.
		"declined", "false-positive",
		// handoff-stalled: reviewing -> parked when the outcome COMMITTED but
		// the phase-2 drain never advanced the Task within HandoffDeadline.
		"handoff-stalled",
		// issue-closed: WS3-I3 rejected(issue-closed), the human-closed-the-driving
		// -issue stop edge from the nine live stages.
		"issue-closed",
		// tracked-elsewhere: investigating->rejected on submit_outcome(comment_issue),
		// the incident agent appended evidence to an existing tracker rather than
		// filing a new issue.
		"tracked-elsewhere",
	}
	for _, r := range want {
		if !stage.ValidReason(r) {
			t.Errorf("%q is not in Reasons, but F.5 says it is", r)
		}
	}
	if len(stage.Reasons) != len(want) {
		t.Errorf("Reasons has %d members, F.5 has %d", len(stage.Reasons), len(want))
	}
	// The set is CLOSED.
	for _, r := range []string{"pod-not-ready", "", "whatever", "Parked", "brainstormed"} {
		if stage.ValidReason(r) {
			t.Errorf("%q was accepted: the reason set is CLOSED", r)
		}
	}
}

func TestPodNotReadyIsNotAReason(t *testing.T) {
	for _, r := range stage.Reasons {
		if r == "pod-not-ready" {
			t.Fatal("pod-not-ready is a member of Reasons. It does not exist: a never-Ready pod RESPAWNS, and the terminal is pod-recreation-exhausted")
		}
	}
	// And Enter refuses it.
	task := newTask("implement", v1alpha1.StageImplementing, "")
	err := stage.Enter(task, nil, v1alpha1.StageFailed, "pod-not-ready", time.Now())
	if err == nil {
		t.Fatal("Enter accepted the reason pod-not-ready")
	}
	var bad *stage.UnknownReasonError
	if !errors.As(err, &bad) {
		t.Fatalf("err = %v, want UnknownReasonError", err)
	}
}

func TestEnterRequiresAReasonOnTheTerminals(t *testing.T) {
	for _, to := range []string{v1alpha1.StageParked, v1alpha1.StageFailed, v1alpha1.StageRejected} {
		task := newTask("implement", v1alpha1.StageClarifying, "")
		err := stage.Enter(task, nil, to, "", time.Now())
		if err == nil {
			t.Errorf("Enter(-> %s) with an EMPTY reason succeeded: the reason is MANDATORY on parked/failed/rejected", to)
		}
	}
	// And a non-terminal takes no reason.
	task := newTask("implement", v1alpha1.StageClarifying, "")
	if err := stage.Enter(task, nil, v1alpha1.StageApproved, "", time.Now()); err != nil {
		t.Errorf("Enter(clarifying -> approved) with no reason: %v", err)
	}
	if task.Status.StageReason != "" {
		t.Errorf("stageReason = %q on a non-terminal stage, want empty", task.Status.StageReason)
	}
}

func TestEnterStampsParkedFromStageAndAgentKind(t *testing.T) {
	task := newTask("implement", v1alpha1.StageImplementing, "")
	if err := stage.Enter(task, nil, v1alpha1.StageParked, stage.ReasonImplementDeclined, time.Now()); err != nil {
		t.Fatal(err)
	}
	if task.Status.ParkedFromStage != v1alpha1.StageImplementing {
		t.Errorf("parkedFromStage = %q, want implementing (OBSERVABILITY ONLY)", task.Status.ParkedFromStage)
	}
	if task.Status.AgentKind != "" {
		t.Errorf("agentKind = %q on a pod-less stage, want empty", task.Status.AgentKind)
	}

	task2 := newTask("clarify", v1alpha1.StageTriaging, "")
	if err := stage.Enter(task2, nil, v1alpha1.StageClarifying, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	if task2.Status.AgentKind != stage.AgentClarify {
		t.Errorf("agentKind = %q, want clarify", task2.Status.AgentKind)
	}
}

// ===========================================================================
// 12. THE POD-STAGE BUDGET EXITS.
// ===========================================================================

func TestBudgetExit(t *testing.T) {
	const maxTurns, maxRecreations = 300, 3

	t.Run("turns >= maxTurnsPerTask -> failed(turn-budget-exhausted)", func(t *testing.T) {
		task := newTask("implement", v1alpha1.StageImplementing, "")
		task.Status.Stats.Turns = maxTurns
		edge, ok := stage.BudgetExit(task, maxTurns, maxRecreations, false)
		if !ok || edge.To != v1alpha1.StageFailed || edge.Reason != stage.ReasonTurnBudgetExhausted {
			t.Fatalf("got (%v,%v), want failed(turn-budget-exhausted)", edge, ok)
		}
	})

	t.Run("podRecreations > maxPodRecreations -> failed(pod-recreation-exhausted)", func(t *testing.T) {
		task := newTask("implement", v1alpha1.StageImplementing, "")
		task.Status.Stats.PodRecreations = maxRecreations + 1
		edge, ok := stage.BudgetExit(task, maxTurns, maxRecreations, false)
		if !ok || edge.To != v1alpha1.StageFailed || edge.Reason != stage.ReasonPodRecreationExhausted {
			t.Fatalf("got (%v,%v), want failed(pod-recreation-exhausted)", edge, ok)
		}
	})

	t.Run("pod stopped with no outcome and the recreation budget is spent -> parked(no-outcome)", func(t *testing.T) {
		task := newTask("implement", v1alpha1.StageImplementing, "")
		task.Status.Stats.PodRecreations = maxRecreations
		edge, ok := stage.BudgetExit(task, maxTurns, maxRecreations, true)
		if !ok || edge.To != v1alpha1.StageParked || edge.Reason != stage.ReasonNoOutcome {
			t.Fatalf("got (%v,%v), want parked(no-outcome)", edge, ok)
		}
		// With budget LEFT it respawns instead of parking.
		task.Status.Stats.PodRecreations = 0
		if _, ok := stage.BudgetExit(task, maxTurns, maxRecreations, true); ok {
			t.Fatal("parked(no-outcome) fired while the recreation budget still had room")
		}
	})

	t.Run("POD-LESS stages take NONE of these exits", func(t *testing.T) {
		for _, s := range stage.AllStages() {
			if !v1alpha1.StagePodless(s) {
				continue
			}
			task := newTask("implement", s, "")
			task.Status.Stats.Turns = maxTurns + 100
			task.Status.Stats.PodRecreations = maxRecreations + 10
			if edge, ok := stage.BudgetExit(task, maxTurns, maxRecreations, true); ok {
				t.Errorf("pod-less stage %s took a pod budget exit -> %v", s, edge)
			}
		}
	})
}

// maxTurnsPerPod does NOT terminate the Task, and the implement agent kind is
// EXEMPT from it entirely.
func TestMaxTurnsPerPodIsAPodStopNotATaskTerminal(t *testing.T) {
	if stage.EnforcesMaxTurnsPerPod(stage.AgentImplement) {
		t.Fatal("the implement agent kind is EXEMPT from maxTurnsPerPod: a long healthy coding run must not be cut off")
	}
	for _, k := range []string{stage.AgentReview, stage.AgentClarify, stage.AgentBrainstorm, stage.AgentIncident, stage.AgentRefine, stage.AgentDocumentation} {
		if !stage.EnforcesMaxTurnsPerPod(k) {
			t.Errorf("agent kind %q must be bounded by maxTurnsPerPod", k)
		}
	}
}

// TestRejectedEdgesCarryTheirOwnReason pins contract gap F.5-GAP: the closed
// reason set had NO member for `rejected`, yet Enter() requires a reason on
// every terminal. Both rejected edges were therefore labelled
// `implement-declined`, which is false on both of them:
//
//	clarifying   -> rejected  is decision=close       (nobody declined an implementation)
//	investigating-> rejected  is false_positive       (an alert that was not real)
//
// `implement-declined` is correct in exactly ONE place - parked(implement-declined),
// where an implement agent declines full-scope work - and conflating the three
// makes operator_task_terminal_total{stageReason} unreadable: a false-positive
// alert and a declined implementation would be the same series.
func TestRejectedEdgesCarryTheirOwnReason(t *testing.T) {
	for _, tc := range []struct {
		from, wantReason string
	}{
		{v1alpha1.StageClarifying, stage.ReasonDeclined},
		{v1alpha1.StageInvestigating, stage.ReasonFalsePositive},
	} {
		t.Run(tc.from, func(t *testing.T) {
			var got string
			var found bool
			for _, e := range stage.Transitions[tc.from] {
				// Skip the shared WS3-I3 rejected(issue-closed) stop edge; this test
				// is about each stage's OWN decline reason.
				if e.To == v1alpha1.StageRejected && e.Reason != stage.ReasonIssueClosed {
					got, found = e.Reason, true
				}
			}
			if !found {
				t.Fatalf("%s has no edge to rejected", tc.from)
			}
			if got == stage.ReasonImplementDeclined {
				t.Fatalf("%s -> rejected still borrows implement-declined; it needs its own reason", tc.from)
			}
			if got != tc.wantReason {
				t.Errorf("%s -> rejected reason = %q, want %q", tc.from, got, tc.wantReason)
			}
			if !stage.ValidReason(got) {
				t.Errorf("reason %q is not in the closed set", got)
			}
		})
	}
}

func TestHandoffStalledIsInTheF5ClosedSet(t *testing.T) {
	if !stage.ValidReason(stage.ReasonHandoffStalled) {
		t.Fatal("handoff-stalled must be a member of the F.5 closed set, else Enter rejects it")
	}
}

// Only reviewing needs the edge: every other kind's commit calls stage.Enter in
// the SAME write as its outcome condition, so no other stage can be
// committed-but-not-advanced.
func TestReviewingHasTheHandoffStalledEdgeAndNothingElseDoes(t *testing.T) {
	found := false
	for _, e := range stage.Transitions[v1alpha1.StageReviewing] {
		if e.To == v1alpha1.StageParked && e.Reason == stage.ReasonHandoffStalled {
			found = true
		}
	}
	if !found {
		t.Fatal("reviewing -> parked[handoff-stalled] is missing from the F.3 table")
	}
	for from, edges := range stage.Transitions {
		if from == v1alpha1.StageReviewing {
			continue
		}
		for _, e := range edges {
			if e.Reason == stage.ReasonHandoffStalled {
				t.Fatalf("%s must not carry a handoff-stalled edge: its commit advances the stage in the same write", from)
			}
		}
	}
}

func TestEnterReviewingToParkedHandoffStalled(t *testing.T) {
	task := &v1alpha1.Task{
		Spec: v1alpha1.TaskSpec{Kind: "review"},
		Status: v1alpha1.TaskStatus{
			Stage:              v1alpha1.StageReviewing,
			PodStartedAt:       &metav1.Time{Time: time.Unix(100, 0)},
			StageWorkStartedAt: &metav1.Time{Time: time.Unix(100, 0)},
			Stats:              v1alpha1.TaskStats{PodRecreations: 2},
		},
	}
	now := time.Unix(500, 0)
	if err := stage.Enter(task, nil, v1alpha1.StageParked, stage.ReasonHandoffStalled, now); err != nil {
		t.Fatalf("Enter: %v", err)
	}
	if task.Status.Stage != v1alpha1.StageParked || task.Status.StageReason != stage.ReasonHandoffStalled {
		t.Fatalf("stage=%q reason=%q", task.Status.Stage, task.Status.StageReason)
	}
	if task.Status.ParkedFromStage != v1alpha1.StageReviewing {
		t.Fatalf("parkedFromStage = %q", task.Status.ParkedFromStage)
	}
}

// handoff-stalled has NO F.6 re-entry: it ages out at parkRetention and the
// reaper collects it. It must not fall into any Unpark rule by accident.
func TestUnparkDoesNotReEnterHandoffStalled(t *testing.T) {
	task := &v1alpha1.Task{
		Spec: v1alpha1.TaskSpec{Kind: "review"},
		Status: v1alpha1.TaskStatus{
			Stage:         v1alpha1.StageParked,
			StageReason:   stage.ReasonHandoffStalled,
			PendingEvents: []v1alpha1.TaskEvent{{Author: "a-human"}},
		},
	}
	if _, ok := stage.Unpark(stage.UnparkInput{Task: task, BotLogin: "tatara-bot", Now: time.Unix(1, 0)}); ok {
		t.Fatal("handoff-stalled must have no F.6 re-entry")
	}
}

func TestReenterOnReviewChangesRequested(t *testing.T) {
	now := time.Now()
	const maxTurns = 300
	cases := []struct {
		name       string
		kind       string
		from       string
		reason     string
		wantOK     bool
		wantTarget string
	}{
		{"from reviewing", "clarify", v1alpha1.StageReviewing, "", true, v1alpha1.StageImplementing},
		{"from merging", "clarify", v1alpha1.StageMerging, "", true, v1alpha1.StageImplementing},
		{"from implementing is redundant", "clarify", v1alpha1.StageImplementing, "", false, ""},
		{"kind=review never re-enters", "review", v1alpha1.StageReviewing, "", false, ""},
		{"terminal failed not resurrected", "clarify", v1alpha1.StageFailed, stage.ReasonTurnBudgetExhausted, false, ""},
		{"delivered not resurrected", "clarify", v1alpha1.StageDelivered, "", false, ""},
		{"earlier stage clarifying has no re-entry edge", "clarify", v1alpha1.StageClarifying, "", false, ""},
		// Parked-origin: routed by StageReason, mirroring Unpark.
		{"parked merge-timeout -> merging", "clarify", v1alpha1.StageParked, stage.ReasonMergeTimeout, true, v1alpha1.StageMerging},
		{"parked no-outcome -> implementing", "clarify", v1alpha1.StageParked, stage.ReasonNoOutcome, true, v1alpha1.StageImplementing},
		{"parked review-loop-exhausted folds", "clarify", v1alpha1.StageParked, stage.ReasonReviewLoopExhausted, false, ""},
		{"parked stage-deadline folds", "clarify", v1alpha1.StageParked, stage.ReasonStageDeadline, false, ""},
		{"parked awaiting-human folds (pending-event path handles it)", "clarify", v1alpha1.StageParked, stage.ReasonAwaitingHuman, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: tc.kind}}
			task.Status.Stage = tc.from
			task.Status.StageReason = tc.reason
			ent := metav1.NewTime(now.Add(-time.Hour))
			task.Status.StageEnteredAt = &ent
			pod := metav1.NewTime(now.Add(-time.Minute))
			task.Status.PodStartedAt = &pod
			var mrs []v1alpha1.MergeRequest
			if tc.from == v1alpha1.StageReviewing {
				// PendingReview nil -> reviewGateOpen; a still-owed bot review would
				// block the reviewing edge and fold (Task 4d).
				mrs = []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{}}}
			}
			ok := stage.ReenterOnReviewChangesRequested(task, mrs, maxTurns, now)
			require.Equal(t, tc.wantOK, ok)
			if ok {
				require.Equal(t, tc.wantTarget, task.Status.Stage)
				require.Nil(t, task.Status.PodStartedAt) // Enter's re-arm ran
			} else {
				require.Equal(t, tc.from, task.Status.Stage) // untouched on fold
			}
		})
	}
}

// F3: a maintainer-driven re-entry into implementing zeroes the merge and
// head-move budgets, so a merging -> implementing -> merging round trip does not
// carry a spent head-move budget into a premature failed(head-moving).
func TestReenterOnReviewChangesRequested_ResetsMergeAndHeadBudgets(t *testing.T) {
	now := time.Now()
	task := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "clarify"}}
	task.Status.Stage = v1alpha1.StageMerging
	ent := metav1.NewTime(now.Add(-time.Hour))
	task.Status.StageEnteredAt = &ent
	task.Status.HeadMoveReentries = v1alpha1.MaxHeadMoveReentries
	task.Status.MergeReentries = v1alpha1.MaxMergeReentries

	require.True(t, stage.ReenterOnReviewChangesRequested(task, nil, 300, now))
	require.Equal(t, v1alpha1.StageImplementing, task.Status.Stage)
	require.Zero(t, task.Status.HeadMoveReentries, "fresh implementation gets a fresh head-move budget")
	require.Zero(t, task.Status.MergeReentries, "fresh implementation gets a fresh merge budget")
}

// F1: merge-timeout re-entry accounts MergeReentries exactly like Unpark, and
// folds (does not terminate) once the budget is spent.
func TestReenterOnReviewChangesRequested_MergeTimeoutAccounting(t *testing.T) {
	now := time.Now()
	mk := func(reentries int) *v1alpha1.Task {
		task := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "clarify"}}
		task.Status.Stage = v1alpha1.StageParked
		task.Status.StageReason = stage.ReasonMergeTimeout
		ent := metav1.NewTime(now.Add(-time.Hour))
		task.Status.StageEnteredAt = &ent
		task.Status.MergeReentries = reentries
		return task
	}
	under := mk(v1alpha1.MaxMergeReentries - 1)
	require.True(t, stage.ReenterOnReviewChangesRequested(under, nil, 300, now))
	require.Equal(t, v1alpha1.StageMerging, under.Status.Stage)
	require.Equal(t, v1alpha1.MaxMergeReentries, under.Status.MergeReentries, "one merge re-entry consumed")

	atCap := mk(v1alpha1.MaxMergeReentries)
	require.False(t, stage.ReenterOnReviewChangesRequested(atCap, nil, 300, now), "budget spent: fold, do not terminate")
	require.Equal(t, v1alpha1.StageParked, atCap.Status.Stage)
	require.Equal(t, v1alpha1.MaxMergeReentries, atCap.Status.MergeReentries, "counter not touched on the fold")
}

// F4: no-outcome re-entry honors Unpark's guards - declines on any merged MR or
// at the lifetime turn cap instead of bouncing into failed(turn-budget-exhausted).
func TestReenterOnReviewChangesRequested_NoOutcomeGuards(t *testing.T) {
	now := time.Now()
	mk := func(turns int, merged bool) (*v1alpha1.Task, []v1alpha1.MergeRequest) {
		task := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "clarify"}}
		task.Status.Stage = v1alpha1.StageParked
		task.Status.StageReason = stage.ReasonNoOutcome
		ent := metav1.NewTime(now.Add(-time.Hour))
		task.Status.StageEnteredAt = &ent
		task.Status.Stats.Turns = turns
		var mrs []v1alpha1.MergeRequest
		if merged {
			mr := v1alpha1.MergeRequest{}
			mr.Status.State = "merged"
			mrs = append(mrs, mr)
		}
		return task, mrs
	}
	ok, _ := mk(0, false)
	require.True(t, stage.ReenterOnReviewChangesRequested(ok, nil, 300, now))
	require.Equal(t, v1alpha1.StageImplementing, ok.Status.Stage)

	atCap, _ := mk(300, false)
	require.False(t, stage.ReenterOnReviewChangesRequested(atCap, nil, 300, now), "at turn cap: fold")
	require.Equal(t, v1alpha1.StageParked, atCap.Status.Stage)

	mergedTask, mergedMRs := mk(0, true)
	require.False(t, stage.ReenterOnReviewChangesRequested(mergedTask, mergedMRs, 300, now), "any merged MR: fold")
	require.Equal(t, v1alpha1.StageParked, mergedTask.Status.Stage)
}

// ---------------------------------------------------------------------------
// WS3-I3: the rejected(issue-closed) stop edge.
// ---------------------------------------------------------------------------

// TestIssueClosedStopEdge asserts the nine LIVE source stages can enter
// rejected(issue-closed) and that deploying/documenting/terminals cannot - the
// F.3 table half of the WS3-I3 stop edge.
func TestIssueClosedStopEdge(t *testing.T) {
	live := []string{
		v1alpha1.StageTriaging, v1alpha1.StageBrainstorming, v1alpha1.StageClarifying,
		v1alpha1.StageInvestigating, v1alpha1.StageRefining, v1alpha1.StageApproved,
		v1alpha1.StageImplementing, v1alpha1.StageReviewing, v1alpha1.StageMerging,
	}
	for _, from := range live {
		if !stage.Legal(from, v1alpha1.StageRejected) {
			t.Errorf("Legal(%s -> rejected) = false, want true (issue-closed stop edge)", from)
		}
		if !stage.AllowsIssueClosedStop(from) {
			t.Errorf("AllowsIssueClosedStop(%s) = false, want true", from)
		}
		task := newTask("implement", from, "")
		if err := stage.Enter(task, nil, v1alpha1.StageRejected, stage.ReasonIssueClosed, time.Now()); err != nil {
			t.Errorf("Enter(%s -> rejected(issue-closed)): %v", from, err)
			continue
		}
		require.Equal(t, v1alpha1.StageRejected, task.Status.Stage)
		require.Equal(t, stage.ReasonIssueClosed, task.Status.StageReason)
	}

	// deploying is EXCLUDED (merged work is not rewound); documenting has no
	// driving issue; terminals are terminal.
	excluded := []string{
		v1alpha1.StageDeploying, v1alpha1.StageDocumenting, v1alpha1.StageDelivered,
		v1alpha1.StageRejected, v1alpha1.StageFailed, v1alpha1.StageParked,
	}
	for _, from := range excluded {
		if stage.AllowsIssueClosedStop(from) {
			t.Errorf("AllowsIssueClosedStop(%s) = true, want false", from)
		}
		if from == v1alpha1.StageDeploying && stage.Legal(from, v1alpha1.StageRejected) {
			t.Errorf("Legal(deploying -> rejected) = true: a late issue close must not rewind merged work")
		}
	}
}

// TestIssueClosedIsValidReasonWithNoReentry asserts issue-closed is a member of
// the F.5 closed set and that a parked... no: rejected(issue-closed) is terminal
// and Unpark never exits it.
func TestIssueClosedIsValidReasonWithNoReentry(t *testing.T) {
	require.True(t, stage.ValidReason(stage.ReasonIssueClosed))
	// rejected has no exits; a Task terminal at rejected(issue-closed) never
	// re-enters. Unpark only runs on parked Tasks, but assert the reason itself
	// is not a resumable park reason by construction: it is a rejected reason.
	task := newTask("clarify", v1alpha1.StageRejected, stage.ReasonIssueClosed)
	_, ok := stage.Unpark(stage.UnparkInput{Task: task, BotLogin: botLogin, Now: time.Now()})
	require.False(t, ok, "rejected(issue-closed) never re-enters")
}
