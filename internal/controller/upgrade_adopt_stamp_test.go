package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// EVERY ADOPTED TASK SAYS SO, or the upgrade cron reads a draining Renovate
// backlog as "lanes full" and stops proposing bumps for as long as the backlog
// lasts, with no error and no log (design D2).
func TestMintAdoptedUpgradeTask_StampsTheAdoptedOrigin(t *testing.T) {
	ctx := context.Background()
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	repo := adoptRepo()
	c := newMirrorClient(t, proj, repo)
	m := &Minter{Client: c, Scheme: c.Scheme()}

	task, outcome, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, renovatePR(), nil, nil)
	if err != nil || outcome != MintCreated {
		t.Fatalf("mint = (%v, %v)", outcome, err)
	}
	if got := task.Labels[tatarav1alpha1.LabelUpgradeOrigin]; got != tatarav1alpha1.UpgradeOriginAdopted {
		t.Fatalf("upgrade-origin = %q, want %q", got, tatarav1alpha1.UpgradeOriginAdopted)
	}
}

// THE MINT-ACCOUNTABILITY LABELS COME FROM THE CALLER, and without them the
// dispatcher's event is admitted and NEVER reaped: reconcileDone matches the
// Task by LabelQueuedEvent, mapTaskToQE re-triggers admission through it, and
// mintedTask resolves #443 idempotency through LabelMintedBy.
func TestMintAdoptedUpgradeTask_CarriesTheCallersMintStamp(t *testing.T) {
	ctx := context.Background()
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	repo := adoptRepo()
	c := newMirrorClient(t, proj, repo)
	m := &Minter{Client: c, Scheme: c.Scheme()}

	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "qe-1", Namespace: proj.Namespace, UID: types.UID("u-9")},
		Spec:       tatarav1alpha1.QueuedEventSpec{DedupKey: queue.AdoptUpgradeDedupKey(repo.Name, 41)},
	}
	task, _, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, renovatePR(), nil, queue.MintStamp(qe))
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	for k, want := range queue.MintStamp(qe) {
		if task.Labels[k] != want {
			t.Errorf("task label %s = %q, want %q", k, task.Labels[k], want)
		}
	}
}

// A NIL STAMP IS LEGAL and stamps only the origin: the mint has one caller
// today, but a nil map must never panic on a range.
func TestMintAdoptedUpgradeTask_NilStampIsSafe(t *testing.T) {
	ctx := context.Background()
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	repo := adoptRepo()
	c := newMirrorClient(t, proj, repo)
	m := &Minter{Client: c, Scheme: c.Scheme()}
	if _, _, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, renovatePR(), nil, nil); err != nil {
		t.Fatalf("mint with a nil stamp: %v", err)
	}
}

// THE SNAPSHOT ROUND-TRIPS. Everything AdoptUpgradeMR and MintAdoptedUpgradeTask
// read off a PRRef must survive a trip through the CRD and back, or a queued
// adoption mints from a lossy copy - and the LOSSY FIELD IS THE FORK GUARD.
func TestAdoptedUpgradeRefRoundTripsThePRRef(t *testing.T) {
	pr := scm.PRRef{
		Repo: "szymonrychu/charts", HeadRepo: "szymonrychu/charts", Number: 41,
		Title: "chore(deps): bump", Author: "tatara-bot", HeadSHA: "abc",
		HeadBranch: "renovate/cilium", Body: "notes", Labels: []string{"deps"},
	}
	got := PRRefFromAdopted(AdoptedUpgradeRefFromPR(pr))
	if got.Repo != pr.Repo || got.HeadRepo != pr.HeadRepo || got.Number != pr.Number ||
		got.Title != pr.Title || got.Author != pr.Author || got.HeadSHA != pr.HeadSHA ||
		got.HeadBranch != pr.HeadBranch || got.Body != pr.Body || len(got.Labels) != 1 {
		t.Fatalf("round trip lost data: %+v", got)
	}
}
