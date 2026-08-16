package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/budget"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// adoptionEngineBot is the login the dependency engine authors as, which is what
// makes an adopted merge request safe to merge (AdoptUpgradeMR clause (c)).
const adoptionEngineBot = "szymonrychu-bot"

// adoptionNamespace creates a namespace of this test's own and returns its name.
//
// THE DISPATCHER BELOW RUNS FOR REAL, so it must not be able to see any other
// test's Project: one reconcile admits the WHOLE project pool, and a shared
// namespace would let it mint Tasks for fixtures that belong to someone else.
// The manager's cache is scoped to this namespace for the same reason.
// It is NOT deleted on cleanup: envtest runs no namespace controller, so a
// deleted namespace never leaves Terminating and every later create in it fails.
func adoptionNamespace(t *testing.T, ctx context.Context, name string) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := k8sClient.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create namespace %s: %v", name, err)
	}
	return name
}

// adoptionProject is a Project armed for adoption with room for exactly ONE
// agent: with spec.queue unset, QueueCapacity() IS MaxConcurrentAgents, so the
// normal pool saturates on the first admission and everything else must wait.
func adoptionProject(name, ns string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef:        "scm-secret",
			TriggerLabel:        "tatara",
			MaxConcurrentAgents: 1,
			MaxNewTasksPerSweep: 5,
			MaxOpenTasks:        6,
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github", BotLogin: adoptionEngineBot, PRReactionScope: "labeledOrMentioned",
			},
			UpgradePolicy: &tatarav1alpha1.UpgradePolicySpec{AdoptBranchPrefix: "renovate/"},
		},
	}
}

func adoptionRepository(proj *tatarav1alpha1.Project, name string) *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: proj.Namespace},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       proj.Name,
			URL:              "https://github.com/szymonrychu/" + name + ".git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
}

// adoptionQE creates the QUEUED adoption event the webhook (Task 6) and the
// sweep (Task 8) both produce: class normal, priority 2, one per merge request,
// named off the shared dedup key.
func adoptionQE(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, seq int64) *tatarav1alpha1.QueuedEvent {

	t.Helper()
	p := 2
	key := queue.AdoptUpgradeDedupKey(repo.Name, number)
	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: queue.QueuedEventName(proj.Name, key), Namespace: proj.Namespace},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: seq, Class: tatarav1alpha1.QueueClassNormal, Kind: "upgrade",
			ProjectRef: proj.Name, RepositoryRef: repo.Name, DedupKey: key, Priority: &p,
			Payload: tatarav1alpha1.QueuedEventPayload{
				Kind: "upgrade", RepositoryRef: repo.Name,
				AdoptedUpgrade: &tatarav1alpha1.AdoptedUpgradeRef{
					Number: number, Author: adoptionEngineBot,
					Title:      "chore(deps): update cilium",
					HeadSHA:    "sha-" + repo.Name,
					HeadBranch: "renovate/cilium",
					Repo:       repo.Name, HeadRepo: repo.Name,
				},
			},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q)
	return q
}

// startAdoptionDispatcher runs the REAL DispatcherReconciler against the envtest
// control plane, wired by the REAL SetupWithManager - not a hand-copied builder -
// so deleting its Watches(&Task{}, mapTaskToQE) edge turns this file RED.
//
// IT SCHEDULES NO TIMER OF ITS OWN, which is what makes the causal claim below
// provable rather than merely plausible. BackstopEvents is non-nil (nothing is
// ever pushed onto it), so blockedPoll() returns 0 and a pool_full hold requeues
// nothing; the manager's resync period is controller-runtime's 10h default; and
// no other controller is registered. The only thing that can drive a reconcile
// here is a watch event on a QueuedEvent or a Task.
func startAdoptionDispatcher(t *testing.T, ctx context.Context, ns string) *DispatcherReconciler {
	t.Helper()
	skipNameValidation := true
	mgr := newTestManager(t, func(o *ctrl.Options) {
		o.Cache = cache.Options{DefaultNamespaces: map[string]cache.Config{ns: {}}}
		// The controller-name registry is process-wide and this binary already
		// registers a "queuedevent" controller elsewhere; skipping the check is
		// what lets the PRODUCTION SetupWithManager run here unmodified.
		o.Controller = config.Controller{SkipNameValidation: &skipNameValidation}
	})
	r := &DispatcherReconciler{
		Client: mgr.GetClient(), Scheme: mgr.GetScheme(),
		BackstopEvents: make(chan event.GenericEvent),
	}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}
	if d := r.blockedPoll(); d != 0 {
		t.Fatalf("blockedPoll = %v, want 0: a self-poll would make the admissions below untraceable to any trigger", d)
	}
	runCtx, cancel := context.WithCancel(ctx)
	started := make(chan error, 1)
	go func() { started <- mgr.Start(runCtx) }()
	t.Cleanup(func() {
		cancel()
		<-started
	})
	if !mgr.GetCache().WaitForCacheSync(runCtx) {
		t.Fatal("manager cache did not sync")
	}
	return r
}

// awaitAdmitted polls the API SERVER (never the manager's cache) until the named
// event is Admitted, and returns it.
func awaitAdmitted(t *testing.T, ctx context.Context, ns, name string, d time.Duration) *tatarav1alpha1.QueuedEvent {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		var q tatarav1alpha1.QueuedEvent
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &q)
		if err == nil && q.Status.State == tatarav1alpha1.QueueStateAdmitted {
			return &q
		}
		if time.Now().After(deadline) {
			t.Fatalf("event %s was not admitted within %s (err %v, state %q)", name, d, err, q.Status.State)
		}
		time.Sleep(interval)
	}
}

// mustBeQueued fails unless the named event is Queued right now.
func mustBeQueued(t *testing.T, ctx context.Context, ns, name, why string) {
	t.Helper()
	var q tatarav1alpha1.QueuedEvent
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &q); err != nil {
		t.Fatalf("get event %s: %v", name, err)
	}
	if !isQueued(q.Status.State) {
		t.Fatalf("event %s state = %q, want Queued: %s", name, q.Status.State, why)
	}
}

// mustStayQueued holds for d and fails if the named event ever leaves Queued.
// This is the QUIESCENCE check: it establishes that the system is at rest with
// the pool full, so the admission that follows can only be attributed to the one
// write the test makes next.
func mustStayQueued(t *testing.T, ctx context.Context, ns, name string, d time.Duration, why string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		mustBeQueued(t, ctx, ns, name, why)
		time.Sleep(interval)
	}
}

// queuedDepth counts the still-Queued events of a project.
func queuedDepth(t *testing.T, ctx context.Context, ns, project string) int {
	t.Helper()
	var qel tatarav1alpha1.QueuedEventList
	if err := k8sClient.List(ctx, &qel, client.InNamespace(ns)); err != nil {
		t.Fatalf("list events: %v", err)
	}
	n := 0
	for i := range qel.Items {
		if qel.Items[i].Spec.ProjectRef == project && isQueued(qel.Items[i].Status.State) {
			n++
		}
	}
	return n
}

// adoptedTask fetches a Task by name and fails if it is missing.
func adoptedTask(t *testing.T, ctx context.Context, ns, name, why string) *tatarav1alpha1.Task {
	t.Helper()
	var task tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &task); err != nil {
		t.Fatalf("task %s: %v (%s)", name, err, why)
	}
	return &task
}

// stampTaskState writes a Task's status.state, the way the Task controller does.
// Every call is ALSO the watch trigger the dispatcher reacts to, which is the
// whole mechanism under test.
func stampTaskState(t *testing.T, ctx context.Context, task *tatarav1alpha1.Task, state string) {
	t.Helper()
	fresh := adoptedTask(t, ctx, task.Namespace, task.Name, "stamp "+state)
	fresh.Status.State = state
	mustStatusUpdate(t, ctx, fresh)
}

// stageTicket cuts the per-stage ADMISSION TICKET a Task asks for when it
// reaches a pod stage - TaskReconciler.ensureTicket's write, made by hand
// because this suite registers only the dispatcher.
//
// IT IS NOT DECORATION, and the first draft of this test that left it out was
// WRONG about production. A mint event is GC'd the moment its Task enters the
// state machine (ticketSpent's no-agentKind arm), which takes the Task's
// queue.LabelQueuedEvent with it - so a live Task carrying a mint label names a
// DELETED event, and mapTaskToQE's request for it reconciles nothing. What
// actually holds a live Task's lane in production, and what its terminal write
// actually maps back to, is this ticket.
//
// Cut AFTER the Task's live-state write, which is the order production runs in
// (reconcileTriaging stamps the state; the reconcile that follows calls
// ensureTicket) and which keeps this SETUP step off the mechanism under test:
// the ticket's own create event wakes the dispatcher through For(&QueuedEvent{}),
// so it is admitted whether or not the Task watch works at all.
func stageTicket(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, seq int64) *tatarav1alpha1.QueuedEvent {

	t.Helper()
	p := 2
	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "ticket-" + task.Name, Namespace: proj.Namespace},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: seq, Class: tatarav1alpha1.QueueClassNormal, Kind: "upgrade",
			ProjectRef: proj.Name, DedupKey: "ticket-" + task.Name, Priority: &p,
			Payload: tatarav1alpha1.QueuedEventPayload{
				Kind: "upgrade", AgentKind: "review", TaskRef: task.Name,
			},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q)
	return q
}

// occupyTheLane walks a freshly minted adopted Task through the two writes that
// put it on a pod and give it the project's only lane: the live state its own
// triage pass reaches (`new` -> awaiting-review, because review goes first on an
// adopted merge request), then its stage ticket. It returns once the dispatcher
// has ADMITTED that ticket, which is the moment the lane is genuinely occupied.
//
// The gap between the two writes cannot be stolen by a waiting adoption: the
// Task is live and the project's live-pod ceiling is 1 (MaxLivePods clamps to
// MaxConcurrentAgents-1, floored at 1), so every queued mint is held on
// live_ceiling until this Task is done.
func occupyTheLane(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, seq int64) {

	t.Helper()
	stampTaskState(t, ctx, task, tatarav1alpha1.StateAwaitingReview)
	ticket := stageTicket(t, ctx, proj, task, seq)
	admitted := awaitAdmitted(t, ctx, proj.Namespace, ticket.Name, timeout)
	if admitted.Status.TaskRef != task.Name {
		t.Fatalf("ticket %s admitted for %q, want %q", ticket.Name, admitted.Status.TaskRef, task.Name)
	}
}

// THE ADMISSION HALF OF THE MEASURED DEFECT, AS A TEST - and only that half.
// Both tests in this file hand-create their adoption QueuedEvents, so NOTHING
// here exercises the producers: re-instating an enqueue-side headroom bound,
// which is the actual production cause, would slip past this file entirely. The
// enqueue half is pinned by internal/webhook/upgrade_adoption_test.go (Task 6)
// and internal/controller/sweep_adopt_upgrade_test.go (Task 8). What this file
// owns is what happens once the work IS in the queue.
//
// 2026-08-16 on szymonrychu/charts: three Renovate merge requests arrived within
// 30 seconds, the pass that ran adopted !1024 and !1025 and skipped !1026
// upgrade_headroom_bound, and the annotation that pulled the pass forward was
// cleared by that same pass. Six minutes later 1024 was done and the lanes were
// empty; twenty minutes after that 1024 and 1025 were MERGED and !1026 was still
// untouched, waiting for a 0 */4 * * * cron.
//
// The immediacy needs NO new trigger. DispatcherReconciler already watches Task
// (mapTaskToQE) and every doReconcile runs a full admit pass over the whole
// queue, so a Task's terminal write already re-evaluates admission. What was
// missing was never the trigger - it was the work being in the queue at all.
//
// WHY THIS RUNS A REAL MANAGER. The property is a CAUSAL one - "the next queued
// adoption is admitted off the terminal transition" - and no hand-driven
// Reconcile call can establish it: driving Reconcile myself would prove only
// that admission works when someone reconciles, which is exactly what the
// production bug did not do for 79 minutes. So the dispatcher here is the real
// one, registered by the real SetupWithManager, with no self-poll and no
// backstop (see startAdoptionDispatcher), and every admission below is preceded
// by a quiescence window proving nothing was going to happen on its own.
func TestDispatcher_ThirdQueuedAdoptionAdmitsOnTheFirstTaskGoingTerminal(t *testing.T) {
	ctx := context.Background()
	ns := adoptionNamespace(t, ctx, "queue-adopt-terminal")
	proj := adoptionProject("adopt-terminal-proj", ns)
	mustCreate(t, ctx, proj)
	repo := adoptionRepository(proj, "charts")
	mustCreate(t, ctx, repo)

	// The three merge requests, queued before the dispatcher starts - the shape of
	// a Renovate fan-out landing in one burst. The seq numbers are spaced so that
	// each in-flight Task's stage ticket (11, 21) sorts AHEAD of the adoptions
	// still waiting, which is the ordinary case: a Task already underway asks for
	// its next pod before newer work is admitted (see the D3 test below).
	first := adoptionQE(t, ctx, proj, repo, 41, 10)
	second := adoptionQE(t, ctx, proj, repo, 42, 20)
	third := adoptionQE(t, ctx, proj, repo, 43, 30)

	startAdoptionDispatcher(t, ctx, ns)

	// ROUND 1: one lane, so exactly ONE adoption is admitted and the other two
	// wait. Identity matters: it must be the LOWEST seq, and the Task it minted
	// must be the one named for merge request 41.
	admitted := awaitAdmitted(t, ctx, ns, first.Name, timeout)
	taskName41 := AdoptedUpgradeTaskName(proj.Name, repo.Name, 41)
	if admitted.Status.TaskRef != taskName41 {
		t.Fatalf("first admission minted %q, want %q", admitted.Status.TaskRef, taskName41)
	}
	task41 := adoptedTask(t, ctx, ns, taskName41, "the first adoption's Task")
	if task41.Labels[tatarav1alpha1.LabelUpgradeOrigin] != tatarav1alpha1.UpgradeOriginAdopted {
		t.Fatalf("task %s upgrade origin = %q, want adopted", taskName41, task41.Labels[tatarav1alpha1.LabelUpgradeOrigin])
	}
	mustBeQueued(t, ctx, ns, second.Name, "a one-lane pool admits one adoption, not two")
	mustBeQueued(t, ctx, ns, third.Name, "a one-lane pool admits one adoption, not three")

	// The lane is now genuinely occupied, by an admitted stage ticket for a live
	// Task - the state !1024's Task was in while !1026 waited.
	occupyTheLane(t, ctx, proj, task41, 11)
	mustStayQueued(t, ctx, ns, second.Name, 2*time.Second,
		"the pool is full and nothing has finished; only a freed lane may admit it")
	if got := queuedDepth(t, ctx, ns, proj.Name); got != 2 {
		t.Fatalf("queue depth = %d, want 2 (merge requests 42 and 43 waiting)", got)
	}

	// ACT. The ONLY write between the quiescence check above and the assertion
	// below - and the exact write that took 79 minutes to matter in production.
	// It reaches the dispatcher through Watches(&Task{}, mapTaskToQE), which maps
	// the Task back to the stage ticket named on its queue.LabelQueuedEvent; that
	// reconcile GCs the spent ticket and runs a full admit pass in one go.
	stampTaskState(t, ctx, task41, tatarav1alpha1.StateDone)

	// ROUND 2: the SECOND adoption is admitted off that terminal write. No sweep
	// ran, no cron fired, no SweepRequestedAnnotation was involved at any point.
	admitted = awaitAdmitted(t, ctx, ns, second.Name, timeout)
	taskName42 := AdoptedUpgradeTaskName(proj.Name, repo.Name, 42)
	if admitted.Status.TaskRef != taskName42 {
		t.Fatalf("the freed lane went to %q, want %q (merge request 42, the next in seq order)",
			admitted.Status.TaskRef, taskName42)
	}
	task42 := adoptedTask(t, ctx, ns, taskName42, "the second adoption's Task")
	mustBeQueued(t, ctx, ns, third.Name, "one lane freed admits exactly one adoption")

	occupyTheLane(t, ctx, proj, task42, 21)
	mustStayQueued(t, ctx, ns, third.Name, 2*time.Second,
		"the pool is full again; the third adoption waits for the next terminal transition")

	// ACT again.
	stampTaskState(t, ctx, task42, tatarav1alpha1.StateDone)

	// ROUND 3: THE ASSERTION THE OLD SHAPE COULD NOT SATISFY AT ANY SPEED. Under
	// the retired adoptHeadroom the third merge request was never queued at all,
	// so no transition could ever reach it and it sat until the 4-hourly scan.
	admitted = awaitAdmitted(t, ctx, ns, third.Name, timeout)
	taskName43 := AdoptedUpgradeTaskName(proj.Name, repo.Name, 43)
	if admitted.Status.TaskRef != taskName43 {
		t.Fatalf("the second freed lane went to %q, want %q (merge request 43)",
			admitted.Status.TaskRef, taskName43)
	}
	adoptedTask(t, ctx, ns, taskName43, "the third adoption's Task")

	// All three merge requests got a Task, one each, and no admission ever
	// re-minted an earlier one: the identity assertions above walked 41, 42, 43
	// in seq order, which is what admitPool's (priority, seq) sort promises for
	// three events sharing priority 2.
	var tl tatarav1alpha1.TaskList
	if err := k8sClient.List(ctx, &tl, client.InNamespace(ns)); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tl.Items) != 3 {
		t.Fatalf("minted %d tasks, want exactly 3 (one per merge request)", len(tl.Items))
	}
	if got := queuedDepth(t, ctx, ns, proj.Name); got != 0 {
		t.Fatalf("queue depth = %d, want 0: every adoption has been admitted", got)
	}
}

// PRIORITY 2 DOES NOT LEAPFROG WORK ALREADY UNDERWAY (design D3). A twelve-MR
// Renovate run at priority 1 would drain ahead of the next stage of every task
// in flight, and the one-hour starvation guard would reserve exactly one slot
// after an hour of that.
//
// The enqueue side is pinned in internal/webhook (WithPriority(2) on the
// adoption enqueue); this pins the CONSEQUENCE inside the dispatcher: with equal
// priority, admitPool falls back to seq, so the older admission ticket of a
// half-done Task takes the slot its next pod needs and the newer adoption waits.
//
// THE POOL MUST BE THE ONLY THING THAT BINDS, which is why maxConcurrentAgents
// is 3 while spec.queue.capacity stays 1. At maxConcurrentAgents 1 this test was
// VACUOUS: MaxLivePods clamps to maxConcurrentAgents-1 (floored at 1), the live
// Task below consumes it, and a priority-1 adoption - one that DID sort first -
// was then skipped on the live-mint ceiling with its slot unburned, leaving the
// ticket admitted next and all three assertions below green under the very
// design they exist to refuse. Raising the ceiling to 2 takes it out of the
// answer, so the ONLY reason the ticket wins is the (priority, seq) order.
func TestDispatcher_QueuedAdoptionsDoNotOutrankAnInFlightTaskTicket(t *testing.T) {
	ctx := context.Background()
	proj := adoptionProject("adopt-priority-proj", testNS)
	proj.Spec.MaxConcurrentAgents = 3
	proj.Spec.Queue = &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}
	mustCreate(t, ctx, proj)
	repo := adoptionRepository(proj, "charts-priority")
	mustCreate(t, ctx, repo)

	// The Task already underway: live, mid-review, and about to ask for its next
	// pod through an admission ticket.
	live := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "adopt-priority-live", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: repo.Name, Kind: "review",
			Goal: "review the maintainer's merge request",
		},
	}
	mustCreate(t, ctx, live)
	live.Status.State = tatarav1alpha1.StateAwaitingReview
	mustStatusUpdate(t, ctx, live)

	p := 2
	ticket := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "adopt-priority-ticket", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 10, Class: tatarav1alpha1.QueueClassNormal, Kind: "review",
			ProjectRef: proj.Name, DedupKey: "ticket-adopt-priority", Priority: &p,
			Payload: tatarav1alpha1.QueuedEventPayload{
				Kind: "review", AgentKind: "review", TaskRef: live.Name,
			},
		},
	}
	mustCreate(t, ctx, ticket)
	ticket.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, ticket)

	adoption := adoptionQE(t, ctx, proj, repo, 61, 11)

	// The premise, spelled out: the two are the SAME priority, so seq decides.
	if a, b := tatarav1alpha1.EffectiveQueuePriority(ticket.Spec), tatarav1alpha1.EffectiveQueuePriority(adoption.Spec); a != 2 || b != 2 {
		t.Fatalf("priorities = ticket %d, adoption %d, want 2 and 2", a, b)
	}

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, _, err := r.admit(ctx, proj, qes, tasks,
		budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatalf("admit: %v", err)
	}

	got := refreshQE(t, ctx, ticket)
	if got.Status.State != tatarav1alpha1.QueueStateAdmitted || got.Status.TaskRef != live.Name {
		t.Fatalf("the in-flight Task's ticket was not admitted first: %+v", got.Status)
	}
	mustBeQueued(t, ctx, testNS, adoption.Name,
		"a queued adoption must not take the slot a half-done task's next pod needs")

	adoptedName := AdoptedUpgradeTaskName(proj.Name, repo.Name, 61)
	var minted tatarav1alpha1.Task
	err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: adoptedName}, &minted)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("a losing adoption minted %s anyway (err %v)", adoptedName, err)
	}
}
