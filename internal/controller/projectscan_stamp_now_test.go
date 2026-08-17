package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// slowFakeReader wraps fakeReader with a ListOpenIssues that sleeps `delay`
// before returning - a controlled, deterministic stand-in for a sweep pass
// that takes real wall-clock time (SweepProject's forge reads), used to
// reproduce Finding 1 without racing real test-machine timing.
type slowFakeReader struct {
	fakeReader
	delay time.Duration
}

func (f *slowFakeReader) ListOpenIssues(ctx context.Context, owner, name string) ([]scm.IssueRef, error) {
	time.Sleep(f.delay)
	return f.fakeReader.ListOpenIssues(ctx, owner, name)
}

// TestRunScans_Finding1_NeverStampedRepoStillDueAfterSlowPass is the code
// review Finding 1 regression: stampScan and stampRepoScan used to read a
// FRESH wall clock (metav1.Now()) AFTER SweepProject returned, instead of the
// reconcile's evaluation `now`. A repo that already carries its own per-repo
// stamp is protected by repoIssueScanBase, but a repo that has NEVER been
// stamped falls back to the project-wide base - and if that base just jumped
// past the never-stamped repo's true fire (because the pass took real
// wall-clock time), the repo is deferred a full cron period even though it
// was never given a chance to be swept. Same starvation as defect 2, a
// different victim.
//
// Reproduced with a REAL pass (runScans) whose forge read is artificially
// slowed past the gap between two repos' fires, so the bug manifests
// deterministically rather than racing real test-machine timing:
//   - repoA's fire is searched to land at or just before "now" (bestWait <
//     1s), so it is due at the evaluation instant with minimal slack.
//   - repoB's fire is searched to land 2-5s after repoA's, so it is NOT due
//     yet at that same instant.
//   - the slow reader delays repoA's sweep by 7s (> the 2-5s gap), so any
//     stamp taken AFTER the sweep completes has stepped over repoB's fire.
//
// Must fail against the pre-fix stampScan/stampRepoScan (which read
// metav1.Now() internally): repoB gets deferred a full period. Passes once
// runScans threads its own evaluation `now` into both instead.
func TestRunScans_Finding1_NeverStampedRepoStillDueAfterSlowPass(t *testing.T) {
	ctx := context.Background()
	const schedule = "* * * * *" // 1-minute period: the finest granularity ParseStandard supports
	const period = time.Minute
	const gapMin = 2 * time.Second
	const gapMax = 5 * time.Second
	const passDelay = 7 * time.Second // > gapMax + the <1s search slack below

	cronSpec := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: schedule}}
	if _, err := cron.ParseStandard(schedule); err != nil {
		t.Fatal(err)
	}

	mkSecret(t, "finding1-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{}
	proj.Name = "finding1-never-stamped"
	proj.Namespace = testNS
	proj.Spec.ScmSecretRef = "finding1-scm"
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot", PriorityLabel: "tatara/priority", Cron: cronSpec}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}

	nowBeforeCall := time.Now()
	minuteMark := nowBeforeCall.Truncate(time.Minute) // an exact "* * * * *" boundary
	nowInMinute := nowBeforeCall.Sub(minuteMark)      // in [0, 60s)

	// Search a large candidate pool for the repo name whose scanOffset is
	// the CLOSEST to (and at or before) nowInMinute - its fire (minuteMark +
	// offset) then lands as close as possible to, and before, "now".
	const candidates = 5000
	bestName := ""
	var bestOff time.Duration
	bestWait := period
	for i := 0; i < candidates; i++ {
		name := fmt.Sprintf("finding1-repo-%d", i)
		off := scanOffset(proj.Name, name, "issueScan", period)
		if off > nowInMinute {
			continue // would fire in the future relative to now - not a candidate for "due now"
		}
		if wait := nowInMinute - off; wait < bestWait {
			bestWait, bestOff, bestName = wait, off, name
		}
	}
	if bestName == "" || bestWait > time.Second {
		t.Fatalf("could not find a repo offset within 1s of now (best wait=%v, name=%q); widen the candidate pool", bestWait, bestName)
	}

	// Second repo: offset in [bestOff+gapMin, bestOff+gapMax], so its fire is
	// gapMin-gapMax after repoA's - comfortably beyond bestWait, so the slow
	// pass (passDelay) reliably steps over it but the initial due-check does
	// not see it as due yet.
	hiName := ""
	var hiOff time.Duration
	for i := 0; i < candidates; i++ {
		name := fmt.Sprintf("finding1-hi-%d", i)
		off := scanOffset(proj.Name, name, "issueScan", period)
		if off < bestOff+gapMin || off > bestOff+gapMax {
			continue
		}
		hiName, hiOff = name, off
		break
	}
	if hiName == "" {
		t.Fatal("could not find a second repo offset in the target gap window; widen the candidate pool")
	}

	repoA := mkScanRepo(t, proj.Name, bestName, "https://github.com/o/"+bestName+".git")
	repoB := mkScanRepo(t, proj.Name, hiName, "https://github.com/o/"+hiName+".git")

	// Anchor the project-wide dueBase exactly at minuteMark (a real cron
	// boundary), matching the offsets computed above. Both repos start
	// unstamped, so both fall back to this base on the first evaluation.
	seedStamp := metav1.NewTime(minuteMark)
	proj.Status.LastIssueScan = &seedStamp
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("seed LastIssueScan: %v", err)
	}

	reader := &slowFakeReader{delay: passDelay}
	r := newScanReconciler(reader)

	if _, repos, _, _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans: %v", err)
	} else {
		var gotA, gotB *tatarav1alpha1.Repository
		for i := range repos {
			switch repos[i].Name {
			case repoA.Name:
				gotA = &repos[i]
			case repoB.Name:
				gotB = &repos[i]
			}
		}
		if gotA == nil || gotB == nil {
			t.Fatalf("runScans did not return both repos (gotA=%v gotB=%v)", gotA, gotB)
		}
		if gotB.Status.LastIssueScan != nil {
			t.Fatal("repoB got its own stamp even though it was never due - test fixture is broken")
		}
		// Sanity that repoA was actually swept: checked against etcd, not the
		// in-memory `repos` return - whether that in-memory copy also carries
		// the stamp is Finding 2's concern, not this test's.
		var etcdA tatarav1alpha1.Repository
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: repoA.Name}, &etcdA); err != nil {
			t.Fatalf("get repoA: %v", err)
		}
		if etcdA.Status.LastIssueScan == nil {
			t.Fatal("repoA (due at the evaluation now) was not stamped - test fixture is broken")
		}

		// repoB has no stamp of its own, so it falls back to the project-wide
		// base stampScan just wrote. If that write used a fresh wall-clock
		// read taken AFTER the slow sweep (pre-fix), it has stepped over
		// repoB's true fire and repoB is deferred a full period.
		due, _, ok := r.reposDueForScan(proj, "issueScan", []tatarav1alpha1.Repository{*gotB}, time.Now())
		if !ok {
			t.Fatal("reposDueForScan not ok")
		}
		if len(due) != 1 {
			t.Fatalf("repoB (fire = minuteMark+%v, gap %v after repoA) was legitimately due once its fire passed, "+
				"but a slow pass (delay=%v) deferred it a full period via the project-wide stamp",
				hiOff, hiOff-bestOff, passDelay)
		}
	}

	// Sanity: repoB was never given its own Repository stamp by this pass.
	var etcdB tatarav1alpha1.Repository
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: repoB.Name}, &etcdB); err != nil {
		t.Fatalf("get repoB: %v", err)
	}
	if etcdB.Status.LastIssueScan != nil {
		t.Fatal("repoB was stamped even though it was never in dueRepos")
	}
}
