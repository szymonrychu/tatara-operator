package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/budget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// adoptAdmitProject is projectWithAdoptPrefix with an agent concurrency the
// dispatcher will actually admit against: maxConcurrentAgents is the FULL-PROJECT
// PAUSE kill switch at 0, and a fake client applies no CRD default, so the shared
// sweep fixture would short-circuit admission before any of this is reached.
func adoptAdmitProject() *tatarav1alpha1.Project {
	p := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	p.Spec.MaxConcurrentAgents = 3
	return p
}

// adoptionEvent is the QueuedEvent the webhook and the sweep both produce.
func adoptionEvent(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) *tatarav1alpha1.QueuedEvent {
	p := 2
	key := queue.AdoptUpgradeDedupKey(repo.Name, number)
	return &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{
			Name: queue.QueuedEventName(proj.Name, key), Namespace: proj.Namespace,
			UID: types.UID("uid-" + key),
		},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: int64(number), Class: tatarav1alpha1.QueueClassNormal, Kind: "upgrade",
			ProjectRef: proj.Name, RepositoryRef: repo.Name, DedupKey: key, Priority: &p,
			Payload: tatarav1alpha1.QueuedEventPayload{
				Kind: "upgrade", RepositoryRef: repo.Name,
				AdoptedUpgrade: &tatarav1alpha1.AdoptedUpgradeRef{
					Number: number, Author: "szymonrychu-bot", Title: "chore(deps): bump",
					HeadSHA: "sha", HeadBranch: "renovate/cilium",
					Repo: "charts", HeadRepo: "charts",
				},
			},
		},
		Status: tatarav1alpha1.QueuedEventStatus{State: tatarav1alpha1.QueueStateQueued},
	}
}

// THE HEADLINE. An adoption payload mints through MintAdoptedUpgradeTask, NOT
// through BuildTaskFromQueuedEvent: only that path binds the merge-request
// mirror, stamps AnnTakeoverHeadBranch, sets MergeOrder and seeds the adopted
// significance floor. A generic mint would produce a Task the merge corridor
// parks operator-error on.
func TestAdmit_AdoptedUpgradePayloadMintsThroughTheAdoptionFunnel(t *testing.T) {
	ctx := context.Background()
	proj := adoptAdmitProject()
	repo := adoptRepo()
	qe := adoptionEvent(proj, repo, 41)
	c := newMirrorClient(t, proj, repo, qe)
	r := &DispatcherReconciler{Client: c, Scheme: c.Scheme()}

	if _, _, _, err := r.admit(ctx, proj, []tatarav1alpha1.QueuedEvent{*qe}, nil,
		budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatalf("admit: %v", err)
	}

	var task tatarav1alpha1.Task
	name := AdoptedUpgradeTaskName(proj.Name, repo.Name, 41)
	if err := c.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: name}, &task); err != nil {
		t.Fatalf("adopted task %s not minted: %v", name, err)
	}
	if task.Spec.Kind != "upgrade" {
		t.Errorf("kind = %q, want upgrade", task.Spec.Kind)
	}
	if task.Annotations[tatarav1alpha1.AnnTakeoverHeadBranch] != "renovate/cilium" {
		t.Errorf("head branch annotation = %q", task.Annotations[tatarav1alpha1.AnnTakeoverHeadBranch])
	}
	if task.Labels[tatarav1alpha1.LabelUpgradeOrigin] != tatarav1alpha1.UpgradeOriginAdopted {
		t.Errorf("upgrade origin = %q, want adopted", task.Labels[tatarav1alpha1.LabelUpgradeOrigin])
	}
	if task.Labels[queue.LabelQueuedEvent] != qe.Name {
		t.Errorf("queued-event label = %q, want %q", task.Labels[queue.LabelQueuedEvent], qe.Name)
	}
	if task.Labels[queue.LabelMintedBy] != string(qe.UID) {
		t.Errorf("minted-by label = %q", task.Labels[queue.LabelMintedBy])
	}

	var fresh tatarav1alpha1.QueuedEvent
	if err := c.Get(ctx, types.NamespacedName{Namespace: qe.Namespace, Name: qe.Name}, &fresh); err != nil {
		t.Fatalf("get event: %v", err)
	}
	if fresh.Status.State != tatarav1alpha1.QueueStateAdmitted || fresh.Status.TaskRef != name {
		t.Fatalf("event status = %+v, want Admitted -> %s", fresh.Status, name)
	}
}

// THE PREDICATES RUN AT ADMIT, NOT ONLY AT ENQUEUE. A queued adoption may wait,
// and MintAdoptedUpgradeTask does not check AdoptUpgradeMR itself - so without
// this the dispatcher would adopt a FORK merge request whose head repo the
// webhook could not report. The guard fails CLOSED on an empty head repo.
func TestAdmit_AdoptedUpgradeRefusedByThePredicateIsDropped(t *testing.T) {
	ctx := context.Background()
	proj := adoptAdmitProject()
	repo := adoptRepo()
	qe := adoptionEvent(proj, repo, 42)
	qe.Spec.Payload.AdoptedUpgrade.HeadRepo = "" // the forge did not say; fail closed
	c := newMirrorClient(t, proj, repo, qe)
	r := &DispatcherReconciler{Client: c, Scheme: c.Scheme()}

	before := testutil.ToFloat64(obs.AdoptionEventDroppedTotal.WithLabelValues(proj.Name, "not_adoptable"))
	if _, _, _, err := r.admit(ctx, proj, []tatarav1alpha1.QueuedEvent{*qe}, nil,
		budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatalf("admit: %v", err)
	}
	var task tatarav1alpha1.Task
	err := c.Get(ctx, types.NamespacedName{
		Namespace: proj.Namespace, Name: AdoptedUpgradeTaskName(proj.Name, repo.Name, 42)}, &task)
	if err == nil {
		t.Fatal("a fork-guard refusal must mint nothing")
	}
	var gone tatarav1alpha1.QueuedEvent
	if err := c.Get(ctx, types.NamespacedName{Namespace: qe.Namespace, Name: qe.Name}, &gone); err == nil {
		t.Fatal("a refused adoption event must be deleted, not left Queued forever")
	}
	after := testutil.ToFloat64(obs.AdoptionEventDroppedTotal.WithLabelValues(proj.Name, "not_adoptable"))
	if after-before != 1 {
		t.Errorf("not_adoptable drops counted = %v, want 1", after-before)
	}
}

// A DELETED REPOSITORY LEAVES NOTHING TO ADOPT INTO. The event is dropped rather
// than retried forever: the merge request is unreachable and the Project's owner
// reference will not collect the event on its own.
func TestAdmit_AdoptedUpgradeWithNoRepositoryIsDropped(t *testing.T) {
	ctx := context.Background()
	proj := adoptAdmitProject()
	repo := adoptRepo()
	qe := adoptionEvent(proj, repo, 43)
	c := newMirrorClient(t, proj, qe) // repo deliberately NOT seeded
	r := &DispatcherReconciler{Client: c, Scheme: c.Scheme()}

	before := testutil.ToFloat64(obs.AdoptionEventDroppedTotal.WithLabelValues(proj.Name, "repository_gone"))
	if _, _, _, err := r.admit(ctx, proj, []tatarav1alpha1.QueuedEvent{*qe}, nil,
		budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatalf("admit: %v", err)
	}
	var gone tatarav1alpha1.QueuedEvent
	if err := c.Get(ctx, types.NamespacedName{Namespace: qe.Namespace, Name: qe.Name}, &gone); err == nil {
		t.Fatal("an adoption event whose repository is gone must be dropped")
	}
	var task tatarav1alpha1.Task
	if err := c.Get(ctx, types.NamespacedName{
		Namespace: proj.Namespace, Name: AdoptedUpgradeTaskName(proj.Name, repo.Name, 43)}, &task); err == nil {
		t.Fatal("a repository-gone drop must mint nothing")
	}
	after := testutil.ToFloat64(obs.AdoptionEventDroppedTotal.WithLabelValues(proj.Name, "repository_gone"))
	if after-before != 1 {
		t.Errorf("repository_gone drops counted = %v, want 1", after-before)
	}
}

// THE ADOPTED MINT'S ONLY CONVERGENCE PATH ONCE THE DISPATCHER OWNS IT. The mint
// is four writes and only the Task create carries the mint stamp atomically, so a
// mint that died at bindMRToTask or seedAdoptedSemverFloor leaves a stamped Task
// behind - and mintedTask then finds it on every later pass, skipping the mint
// branch and MintAdoptedUpgradeTask's own MintExistingLive converge arm forever.
// The mr-binding backstop re-binds the mirror but never re-seeds the floor, so
// without this the merge request merges with an empty significance, CI cuts no
// tag, and the Task sits in `deploying`.
func TestAdmit_AdoptedUpgradeConvergesAnInterruptedMint(t *testing.T) {
	ctx := context.Background()
	proj := adoptAdmitProject()
	repo := adoptRepo()
	qe := adoptionEvent(proj, repo, 45)
	// The Task the interrupted mint left behind: stamped, so mintedTask finds it,
	// but with no mirror bound and no significance floor seeded.
	half := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      AdoptedUpgradeTaskName(proj.Name, repo.Name, 45),
			Namespace: proj.Namespace,
			Labels: map[string]string{
				queue.LabelQueuedEvent:            qe.Name,
				queue.LabelMintedBy:               string(qe.UID),
				tatarav1alpha1.LabelUpgradeOrigin: tatarav1alpha1.UpgradeOriginAdopted,
			},
		},
		Spec: tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, RepositoryRef: repo.Name, Kind: "upgrade"},
	}
	c := newMirrorClient(t, proj, repo, qe, half)
	r := &DispatcherReconciler{Client: c, Scheme: c.Scheme()}

	if _, _, _, err := r.admit(ctx, proj, []tatarav1alpha1.QueuedEvent{*qe}, nil,
		budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatalf("admit: %v", err)
	}

	var mr tatarav1alpha1.MergeRequest
	mrName := tatarav1alpha1.MergeRequestName(repo.Name, 45)
	if err := c.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: mrName}, &mr); err != nil {
		t.Fatalf("the interrupted bind was not converged: %v", err)
	}
	if mr.Status.Significance != AdoptedSignificanceFloor {
		t.Errorf("significance = %q, want the adopted floor %q", mr.Status.Significance, AdoptedSignificanceFloor)
	}
	var tl tatarav1alpha1.TaskList
	if err := c.List(ctx, &tl); err != nil || len(tl.Items) != 1 {
		t.Fatalf("converging minted a SECOND task: %d tasks (err %v)", len(tl.Items), err)
	}
	var fresh tatarav1alpha1.QueuedEvent
	if err := c.Get(ctx, types.NamespacedName{Namespace: qe.Namespace, Name: qe.Name}, &fresh); err != nil {
		t.Fatalf("get event: %v", err)
	}
	if fresh.Status.State != tatarav1alpha1.QueueStateAdmitted || fresh.Status.TaskRef != half.Name {
		t.Fatalf("event status = %+v, want Admitted -> %s", fresh.Status, half.Name)
	}
}

// BACKWARD COMPATIBILITY. An event already Queued when this ships carries no
// adoptedUpgrade and must still take BuildTaskFromQueuedEvent unchanged.
func TestAdmit_LegacyMintPayloadIsUnaffected(t *testing.T) {
	ctx := context.Background()
	proj := adoptAdmitProject()
	qe := adoptionEvent(proj, adoptRepo(), 44)
	qe.Spec.Payload.AdoptedUpgrade = nil
	qe.Spec.Payload.Goal = "a plain upgrade"
	qe.Spec.Payload.GenerateName = "upgrade-"
	qe.Spec.RepositoryRef, qe.Spec.Payload.RepositoryRef = "", ""
	c := newMirrorClient(t, proj, qe)
	r := &DispatcherReconciler{Client: c, Scheme: c.Scheme()}

	if _, _, _, err := r.admit(ctx, proj, []tatarav1alpha1.QueuedEvent{*qe}, nil,
		budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatalf("admit: %v", err)
	}
	var tl tatarav1alpha1.TaskList
	if err := c.List(ctx, &tl); err != nil || len(tl.Items) != 1 {
		t.Fatalf("legacy mint produced %d tasks (err %v), want 1", len(tl.Items), err)
	}
	if _, adopted := tl.Items[0].Labels[tatarav1alpha1.LabelUpgradeOrigin]; adopted {
		t.Fatal("a legacy mint must not be marked adopted")
	}
	if tl.Items[0].Spec.Goal != "a plain upgrade" {
		t.Errorf("goal = %q, want the payload's own", tl.Items[0].Spec.Goal)
	}
}
