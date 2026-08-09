package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/budget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE MINT PATH IS THE HALF OF THE CEILING THAT WAS NEVER ENFORCED (#570).
//
// LiveHasRoom had callers on unpark.go, reaper.go, ownership_redeliver.go and
// webhook/pending_events.go, and NONE on the mint/queue-admission path. Measured
// on project-mtg 2026-08-09 against an effective ceiling of ONE:
// mt-i-mtg-decks-24 entered a live state at 11:13:35 while -23 was already live,
// and mt-i-mtg-decks-22/-24 - parked awaiting-human with the human's answers
// already queued as non-bot pendingEvents - were refused re-entry 21 times each
// with decline=no-live-room, replies undelivered for ~55 minutes.
//
// These tests are the two halves of that asymmetry, stated as behaviour: a mint
// may not walk past the ceiling, and an owed resumption reserves the slot a mint
// would otherwise take.

// mintGateProject creates a Project whose live-pod ceiling is
// MaxConcurrentAgents-1 (the clamp in v1alpha1.MaxLivePods), so
// maxConcurrentAgents=2 reproduces project-mtg's effective ceiling of 1 while
// leaving the normal pool (QueueCapacity == MaxConcurrentAgents == 2) room to
// admit, so the LIVE ceiling is the only binding constraint in the test.
func mintGateProject(t *testing.T, ctx context.Context, name string, maxConcurrentAgents int) *tatarav1alpha1.Project {
	t.Helper()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			MaxConcurrentAgents: maxConcurrentAgents,
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github", Owner: "szymonrychu", BotLogin: "tatara-bot",
			},
		},
	}
	mustCreate(t, ctx, proj)
	return proj
}

// mustCreateTaskWithStatus persists a Task fixture AND its status: Create drops
// the status subresource, so a fixture that only calls mustCreate lands with an
// empty state and counts as nothing.
func mustCreateTaskWithStatus(t *testing.T, ctx context.Context, task *tatarav1alpha1.Task) *tatarav1alpha1.Task {
	t.Helper()
	status := task.Status
	mustCreate(t, ctx, task)
	task.Status = status
	mustStatusUpdate(t, ctx, task)
	return task
}

// parkedAwaitingHuman is the shape the ceiling kept locking out: an
// awaiting-human park sitting IN a live state (park is orthogonal to state,
// #521, so the un-park drops it straight back where it was talking), owning one
// open Issue, and - when reply is true - carrying the human's answer as a
// non-bot pendingEvent.
//
// The open Issue is not decoration. Without it stage.Unpark declines
// no-open-issues ("the empty set is not a licence", fix V6-3), which is a park
// blocked on something OTHER than the ceiling and so reserves nothing - the
// fixture would pass the reservation tests for entirely the wrong reason. With
// it, reply is the ONLY difference between a park that reserves a live slot and
// one that does not.
func parkedAwaitingHuman(t *testing.T, ctx context.Context, name, project string, now time.Time, reply bool) *tatarav1alpha1.Task {
	t.Helper()
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-issue", Namespace: testNS},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: "r", Number: 1, ProjectRef: project,
			URL: "https://example.invalid/" + name,
		},
	}
	mustCreate(t, ctx, iss)
	iss.Status.State = "open"
	mustStatusUpdate(t, ctx, iss)

	task := liveStateTask(name, project, now.Add(-30*time.Minute))
	task.Status.ParkReason = stage.ReasonAwaitingHuman
	task.Status.ParkedAt = &metav1.Time{Time: now.Add(-25 * time.Minute)}
	task.Status.ParkedFromState = tatarav1alpha1.StateRefined
	task.Status.IssueRefs = []string{iss.Name}
	if reply {
		task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
			At: metav1.Time{Time: now.Add(-20 * time.Minute)}, Kind: "issue_comment",
			Repo: "r", Number: 1, Author: "szymonrychu", Body: "yes, go ahead",
		}}
	}
	return mustCreateTaskWithStatus(t, ctx, task)
}

// freshMintQE builds a QueuedEvent that MINTS (payload carries no taskRef), which is
// the branch of admit() the ceiling never gated.
func freshMintQE(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project, generateName string, seq int64) *tatarav1alpha1.QueuedEvent {
	t.Helper()
	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: generateName, Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: seq, Class: tatarav1alpha1.QueueClassNormal, Kind: "implement", ProjectRef: proj.Name,
			Payload: tatarav1alpha1.QueuedEventPayload{
				Kind: "implement", GenerateName: "mint-", Goal: "fresh work",
			},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q)
	return q
}

// TestAdmit_MintDoesNotWalkPastTheLivePodCeiling is #570's headline case,
// transcribed from the production measurement: ceiling 1, one live Task holding
// the slot, one parked Task holding the human's answer. A fresh mint must NOT be
// admitted ahead of the resumption.
func TestAdmit_MintDoesNotWalkPastTheLivePodCeiling(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	proj := mintGateProject(t, ctx, "p-mint-ceiling", 2) // ceiling clamps to 1

	// -23: the live conversation already holding the project's one live slot.
	mustCreateTaskWithStatus(t, ctx, liveStateTask("mt-i-mtg-decks-23", proj.Name, now.Add(-10*time.Minute)))
	// -22: parked awaiting-human, the human's reply queued, refused every pass.
	parkedAwaitingHuman(t, ctx, "mt-i-mtg-decks-22", proj.Name, now, true)

	fresh := freshMintQE(t, ctx, proj, "qe-mint-", 1)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: metrics}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, now); err != nil {
		t.Fatal(err)
	}

	if got := refreshQE(t, ctx, fresh); got.Status.State != tatarav1alpha1.QueueStateQueued || got.Status.TaskRef != "" {
		t.Fatalf("fresh mint admitted past the live-pod ceiling: state=%q taskRef=%q (want Queued with no Task); "+
			"the ceiling bounds resumption but not creation", got.Status.State, got.Status.TaskRef)
	}
	// Nothing was created either: a mint held must mint nothing at all.
	_, after := listQEsTasks(t, ctx, proj.Name)
	if len(after) != 2 {
		t.Fatalf("task count = %d, want 2 (the live one and the parked one); a Task was minted past the ceiling", len(after))
	}
	if got := testutil.ToFloat64(metrics.AdmissionBlockedCounter(proj.Name, tatarav1alpha1.QueueClassNormal, "implement", "live_ceiling")); got != 1 {
		t.Fatalf("admission_blocked{reason=live_ceiling} = %v, want 1", got)
	}
}

// TestAdmit_MintProceedsWhenTheCeilingHasRoom is the other side of the gate: it
// must not become a blanket refusal. Same project, same ceiling, NO live Task -
// the mint admits exactly as before.
func TestAdmit_MintProceedsWhenTheCeilingHasRoom(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := mintGateProject(t, ctx, "p-mint-room", 2)

	fresh := freshMintQE(t, ctx, proj, "qe-room-", 1)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, now); err != nil {
		t.Fatal(err)
	}
	if got := refreshQE(t, ctx, fresh); got.Status.State != tatarav1alpha1.QueueStateAdmitted || got.Status.TaskRef == "" {
		t.Fatalf("mint into an empty project was refused: state=%q taskRef=%q", got.Status.State, got.Status.TaskRef)
	}
}

// TestAdmit_MintYieldsTheLastSlotToAnOwedResumption is the FAIRNESS half. The
// project is UNDER its ceiling (2, one live Task), so a pure "consult
// LiveHasRoom before minting" gate would let the mint take the free slot and the
// parked human reply would be declined no-live-room on the very next un-park
// pass - the production shape again, one slot along. An owed resumption reserves
// the slot instead.
func TestAdmit_MintYieldsTheLastSlotToAnOwedResumption(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := mintGateProject(t, ctx, "p-mint-reserve", 3) // ceiling clamps to 2

	mustCreateTaskWithStatus(t, ctx, liveStateTask("reserve-live", proj.Name, now.Add(-10*time.Minute)))
	parkedAwaitingHuman(t, ctx, "reserve-parked", proj.Name, now, true)

	fresh := freshMintQE(t, ctx, proj, "qe-reserve-", 1)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, now); err != nil {
		t.Fatal(err)
	}
	if got := refreshQE(t, ctx, fresh); got.Status.State != tatarav1alpha1.QueueStateQueued {
		t.Fatalf("fresh mint took the slot an owed resumption was waiting for: state=%q", got.Status.State)
	}
}

// A park with NO human reply queued owes nothing and must reserve nothing: it
// is not waiting on the ceiling, it is waiting on a human. Otherwise every
// parked Task in a project would silently become a permanent mint hold.
func TestAdmit_ParkWithNoHumanReplyReservesNothing(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := mintGateProject(t, ctx, "p-mint-noreply", 3) // ceiling clamps to 2

	mustCreateTaskWithStatus(t, ctx, liveStateTask("noreply-live", proj.Name, now.Add(-10*time.Minute)))
	parkedAwaitingHuman(t, ctx, "noreply-parked", proj.Name, now, false)

	fresh := freshMintQE(t, ctx, proj, "qe-noreply-", 1)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, now); err != nil {
		t.Fatal(err)
	}
	if got := refreshQE(t, ctx, fresh); got.Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("a park with no queued human reply blocked a mint: state=%q", got.Status.State)
	}
}

// THE STARVATION TRAP, ASSERTED SO IT CANNOT BE RE-INTRODUCED. admitTicket
// admits an EXISTING Task that is ALREADY in a live state, so gating it on
// countLive would make the Task count ITSELF and starve forever. The ticket path
// stays ungated even when the project is at - or over - its ceiling.
func TestAdmitTicket_IsNotGatedByTheLiveCeiling(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := mintGateProject(t, ctx, "p-ticket-ceiling", 2) // ceiling clamps to 1

	live := mustCreateTaskWithStatus(t, ctx, liveStateTask("ticket-live", proj.Name, now.Add(-time.Minute)))

	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-ticket-", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 1, Class: tatarav1alpha1.QueueClassNormal, Kind: "implement", ProjectRef: proj.Name,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "implement", TaskRef: live.Name},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, now); err != nil {
		t.Fatal(err)
	}
	if got := refreshQE(t, ctx, q); got.Status.State != tatarav1alpha1.QueueStateAdmitted || got.Status.TaskRef != live.Name {
		t.Fatalf("a live Task's own admission ticket was refused by the ceiling it already occupies: state=%q taskRef=%q",
			got.Status.State, got.Status.TaskRef)
	}
}

// A Task minted but not yet triaged into a live state is PENDING live work and
// must count: without it, two admit passes seconds apart both see live==0 and
// both mint, and the project lands two over a ceiling of one.
func TestLiveMintBudget_CountsMintedButNotYetLiveTasks(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := mintGateProject(t, ctx, "p-mint-pending", 2) // ceiling clamps to 1

	minted := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "pending-new", Namespace: testNS,
			Labels: map[string]string{queue.LabelQueuedEvent: "qe-x"}},
		Spec: tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "implement"},
	}
	mustCreate(t, ctx, minted)

	got, err := liveMintBudget(ctx, k8sClient, k8sClient, proj, now)
	if err != nil {
		t.Fatalf("liveMintBudget: %v", err)
	}
	if got > 0 {
		t.Fatalf("liveMintBudget = %d, want <= 0: a minted Task still at `new` is live work the ceiling has already promised", got)
	}
}
