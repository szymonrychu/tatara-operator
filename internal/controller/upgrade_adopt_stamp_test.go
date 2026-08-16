package controller

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

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

// THE SNAPSHOT IS CLAMPED AT THE FUNNEL, and the funnel is the only clamp.
// AdoptedUpgradeRef.Body carries a MaxLength marker and nothing truncated it, so
// a grouped Renovate bump with per-dependency release notes made the QueuedEvent
// Create 422 - which the webhook answers with a 500 on every redelivery (GitLab
// counts consecutive failures toward auto-disabling the project hook) and which
// the sweep answers with enqueue_adopt_upgrade every pass forever.
//
// Asserted through AdoptedUpgradeRefFromPR because BOTH producers build their
// snapshot there and nowhere else; a per-producer test would prove nothing about
// the other one.
func TestAdoptedUpgradeRefFromPR_ClampsAnOversizedBody(t *testing.T) {
	cap := tatarav1alpha1.MergeRequestBodyMaxBytes
	pr := scm.PRRef{Number: 7, Body: strings.Repeat("x", cap+4096)}
	got := AdoptedUpgradeRefFromPR(pr)
	if len(got.Body) != cap {
		t.Fatalf("clamped body = %d bytes, want %d (the CRD marker's own value)", len(got.Body), cap)
	}
	// A body that already fits must come back BYTE-IDENTICAL: TruncateUTF8's own
	// contract, and callers compare stored values.
	fits := scm.PRRef{Number: 7, Body: "release notes"}
	if AdoptedUpgradeRefFromPR(fits).Body != "release notes" {
		t.Fatal("a body that already fits must not be rewritten")
	}
}

// THE CUT LANDS ON A RUNE BOUNDARY. A naive s[:max] on a multi-byte body
// produces invalid UTF-8, which the API server's JSON encoder rejects even once
// the LENGTH is legal - so an oversized CJK changelog would still 422 after a
// "fix" that only counted bytes.
func TestAdoptedUpgradeRefFromPR_ClampCutsOnARuneBoundary(t *testing.T) {
	cap := tatarav1alpha1.MergeRequestBodyMaxBytes
	// 3 bytes per rune divides 65536 with a remainder, so a naive cut splits one.
	body := strings.Repeat("世", cap)
	got := AdoptedUpgradeRefFromPR(scm.PRRef{Number: 7, Body: body}).Body
	if len(got) > cap {
		t.Fatalf("clamped body = %d bytes, want <= %d", len(got), cap)
	}
	if !utf8.ValidString(got) {
		t.Fatal("the clamp cut mid-rune: the API server rejects invalid UTF-8 regardless of length")
	}
}
