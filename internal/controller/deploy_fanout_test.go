package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// mdFanoutFixture builds a Task in deploying whose merged MR cut tag v1.4.0
// while tatara-helmfile's main is still pinned at 1.3.0 and no apply has carried
// it - the exact 2026-07-29 shape: the tag exists, the bump PR does not, and
// nothing will ever produce one.
func mdFanoutFixture(t *testing.T, mergedAt metav1.Time) (*tatarav1alpha1.Task, *StageDriver) {
	t.Helper()
	task := mdTask("t1", "implement", tatarav1alpha1.StateDeployed)
	mr := mdDeployingMR(task, "tatara-operator", 7)
	mr.Status.MergedAt = &mergedAt
	iss := mdIssue(task, "tatara-operator", 41)
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), mdHelmfileRepo(), task, mr, iss)

	f := newFakeForge(t)
	rd := mdNewReader(f)
	rd.tags["tatara-operator"] = "v1.4.0"
	// The helmfile pins this artifact - so the fail-open "not deployed here at
	// all" escape does not apply - but its main is BEHIND our tag, and there is no
	// green apply carrying it either.
	rd.pin["main"] = mdPin("tatara-operator", "1.3.0")
	return task, mdNewDriverWithReader(t, f, c, rd)
}

// TestDeployingParksOnceTheCDPinFanOutIsProvablyStalled is the #512 regression.
//
// deploying is a pod-less POLL: it re-checks the tatara-helmfile pin every 60s
// and has no way to conclude that the pin is never coming. On 2026-07-29 the CD
// fan-out stopped emitting bump PRs at 12:24:53Z; task
// mt-c-tatara-operator-506 entered deploying at 18:57:21Z and polled in total
// silence for 2h06m59s against a pin that could not materialise, then burned all
// three deploy re-entries in 95 seconds and discarded merged PR #509 at
// deploy-blocked.
//
// The wait must be BOUNDED with a NAMED cause: once our tag has provably never
// reached the helmfile's main, waiting longer cannot help.
func TestDeployingParksOnceTheCDPinFanOutIsProvablyStalled(t *testing.T) {
	// Merged well over deployPinFanoutDeadline before the driver's clock.
	task, d := mdFanoutFixture(t, metav1.NewTime(time.Date(2026, 7, 12, 9, 0, 0, 0, time.UTC)))

	if _, err := d.ReconcileDeploying(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileDeploying: %v", err)
	}

	got := mdGetTask(t, d.Client, "t1")
	if !tatarav1alpha1.Parked(got) {
		t.Fatal("the task is still polling a pin that will never materialise: the deploy wait is unbounded")
	}
	if got.Status.ParkReason != stage.ReasonDeployTimeout {
		t.Fatalf("park reason = %q, want %q", got.Status.ParkReason, stage.ReasonDeployTimeout)
	}
	if got.Status.State != tatarav1alpha1.StateDeployed {
		t.Fatalf("state = %q, want unchanged: a park never moves state", got.Status.State)
	}
	// deploy-timeout is deliberately the retryable park (UnparkTimer, bounded by
	// MaxDeployReentries), not a terminal: a fan-out somebody repairs must still
	// be able to deliver this merge.
	if _, ok := stage.UnparkClassFor(got.Status.ParkReason); !ok {
		t.Fatalf("%q is not a park reason", got.Status.ParkReason)
	}
	if cls, _ := stage.UnparkClassFor(got.Status.ParkReason); cls != stage.UnparkTimer {
		t.Fatalf("unpark class = %v, want UnparkTimer: a repaired fan-out must still deliver this merge", cls)
	}
}

// TestDeployingWaitsInsideTheFanOutWindow: the bound must not turn an ordinary
// CD lap into a failure. A pin that simply has not landed YET is still a poll.
func TestDeployingWaitsInsideTheFanOutWindow(t *testing.T) {
	// Merged ten minutes ago against the driver's 12:00 clock - well inside the
	// window a normal tag -> bump PR -> merge lap needs.
	task, d := mdFanoutFixture(t, metav1.NewTime(time.Date(2026, 7, 12, 11, 50, 0, 0, time.UTC)))

	res, err := d.ReconcileDeploying(context.Background(), mdProject(), task)
	if err != nil {
		t.Fatalf("ReconcileDeploying: %v", err)
	}
	if res.RequeueAfter != deployStageRequeue {
		t.Fatalf("requeueAfter = %v, want %v: a pin still inside its window is a poll", res.RequeueAfter, deployStageRequeue)
	}
	got := mdGetTask(t, d.Client, "t1")
	if tatarav1alpha1.Parked(got) {
		t.Fatalf("parked(%s) ten minutes into a normal CD lap", got.Status.ParkReason)
	}
}
