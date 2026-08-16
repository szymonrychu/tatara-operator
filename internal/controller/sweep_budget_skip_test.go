package controller

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// A CAP THAT IS DOING ITS JOB MUST NOT LOOK LIKE AN INCIDENT.
//
// operator_sweep_skipped_total{reason="mint_budget_bound"} used to be
// incremented once per DEFERRED item per pass, unconditionally - unlike
// capHit, which records the same fact once per pass. A project sitting at
// maxOpenTasks with a backlog therefore drove one series up by the size of the
// backlog on EVERY pass, which is the same alert-burying shape
// mint_budget_bound is EXCLUDED from TataraSweepSkipPersistent for today
// (upgrade_headroom_bound was the other exclusion until Task 8 retired the
// per-pass adoption headroom entirely): it fires permanently and hides
// reason=mr_claimed_by_other_task, the one condition the alert exists for.
//
// The LOG line stays per item - a counter cannot say which pull request went
// unanswered, which is the whole point of skipPR - and only the counter dedups.
func TestSweepBudgetBoundSkipIsCountedOncePerPass(t *testing.T) {
	proj := sweepProject("budget-dedup-proj")
	proj.Spec.MaxOpenTasks = 1
	proj.Spec.Scm.PRReactionScope = "all"
	repo := sweepRepo("budget-dedup-proj")
	c := newMirrorClient(t, proj, repo)

	prs := make([]scm.PRRef, 0, 4)
	for i := range 4 {
		prs = append(prs, scm.PRRef{
			Repo:       "szymonrychu/tatara-operator",
			HeadRepo:   "szymonrychu/tatara-operator",
			Number:     200 + i,
			Author:     "alice",
			Title:      "a human's pull request",
			HeadBranch: "feat/human",
			HeadSHA:    "sha",
		})
	}

	before := testutil.ToFloat64(
		obs.SweepSkippedTotal.WithLabelValues(proj.Name, SweepActivity, SweepSkipMintBudget))
	runSweep(t, c, proj, repo, &sweepReader{prs: prs})
	got := testutil.ToFloat64(
		obs.SweepSkippedTotal.WithLabelValues(proj.Name, SweepActivity, SweepSkipMintBudget)) - before

	if got == 0 {
		t.Fatal("the fixture never bound the creation budget; the test proves nothing")
	}
	if got != 1 {
		t.Fatalf("mint_budget_bound incremented %v times in ONE pass, want 1: a project at its "+
			"task cap with a backlog drives this series by the backlog size every pass, firing "+
			"TataraSweepSkipPersistent permanently and burying mr_claimed_by_other_task", got)
	}
}
