package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ============================================================================
// B.5 owner-ref writes vs. cache staleness: issues #524, #530 and #545.
//
// All three are the same surface - releaseOwnership writing owner refs off a
// copy the controller-runtime cache handed it - so the fixtures are shared.
// ============================================================================

// raceIssue builds the artifact under release: an OPEN Issue controller-owned by
// `owner`, already carrying the terminal-comment marker so releaseTerminal's
// steps 1-2 do NOT write to it. That keeps the Issue's read/write sequence in
// these tests down to exactly what B.5 step 3 does, which is what they measure.
func raceIssue(repo string, number int, proj, owner string, extra ...metav1.OwnerReference) *tatarav1alpha1.Issue {
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name:            tatarav1alpha1.IssueName(repo, number),
			Namespace:       testNS,
			Annotations:     map[string]string{AnnTerminalCommented: owner},
			OwnerReferences: append([]metav1.OwnerReference{reapOwnerRef(owner, true)}, extra...),
		},
		Spec: tatarav1alpha1.IssueSpec{RepositoryRef: repo, Number: number, ProjectRef: proj},
	}
	iss.Status.State = "open"
	return iss
}

// raceWriter is a reapWriter whose forge calls all succeed: these tests are
// about the CR write, not the forge.
func raceWriter() *reapWriter {
	return &reapWriter{
		comment:  func(string, string) error { return nil },
		addLabel: func(string, string) error { return nil },
	}
}

// r0 is reapReconciler with the all-succeed forge writer these tests share.
func r0(c client.Client) *ProjectReconciler { return reapReconciler(c, raceWriter()) }

func gcBlocked() float64 {
	return testutil.ToFloat64(obs.GCBlockedTotal.WithLabelValues(obs.GCBlockedNoControllerOwner))
}

// TestReleaseOwnershipDecidesOnFreshStateNotTheCachedCopy is issue #524.
//
// releaseOwnership was the ONE owner-ref write in the tree that did not go
// through the fresh-Get + RetryOnConflict discipline ownership.go documents:
// ownedIssues/ownedMRs hand it a copy off the controller-runtime CACHE and it
// issued a bare Update on that copy. mr-tatara-operator-504 lost its controller
// owner that way on 2026-07-30.
//
// The fixture is that exact staleness: a live sibling became a plain owner of
// the artifact, and the reaper's cached read predates that write. On the STALE
// array there is no surviving heir, so B.5 drops the ref outright and orphans
// the artifact; on the CURRENT array there is one, so B.5 hands the controller
// flag over. Deciding off the cached copy is what turns a handover into an
// orphaning - and the bare Update carrying the stale resourceVersion 409s on
// top of it.
func TestReleaseOwnershipDecidesOnFreshStateNotTheCachedCopy(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("staleown")
	repo := reapRepo("staleown", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	dying := reapTask("staleown", "impl-task", "clarify",
		tatarav1alpha1.StateRejected, stage.ReasonDeclined, time.Now().Add(-time.Minute))
	dying.Status.IssueRefs = []string{tatarav1alpha1.IssueName(repo.Name, 9)}
	sib := reapTask("staleown", "sib-task", "clarify",
		tatarav1alpha1.StateUnderImplementation, "", time.Now())
	iss := raceIssue(repo.Name, 9, "staleown", "impl-task")

	// stale is served to the FIRST Get of the Issue and then cleared: a lagging
	// informer catches up, which is precisely what the retry loop's second Get
	// buys and what a bare Update cannot wait for.
	var (
		mu    sync.Mutex
		stale *tatarav1alpha1.Issue
	)
	c := newMirrorClientIntercepted(t, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey,
			obj client.Object, opts ...client.GetOption) error {
			mu.Lock()
			defer mu.Unlock()
			if got, ok := obj.(*tatarav1alpha1.Issue); ok && stale != nil && key.Name == stale.Name {
				stale.DeepCopyInto(got)
				stale = nil
				return nil
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	}, proj, repo, reapSecret(), dying, sib, iss)

	snapshot := mustGetIssue(t, c, iss.Name)

	// The concurrent write the reaper's cache has not observed yet.
	cur := mustGetIssue(t, c, iss.Name)
	require.True(t, own.AddPlainOwner(cur, sib))
	require.NoError(t, c.Update(ctx, cur))

	mu.Lock()
	stale = snapshot
	mu.Unlock()

	before := gcBlocked()
	require.NoError(t, r0(c).ReapTerminal(ctx, proj),
		"the release wrote off a stale cached copy and 409'd against the concurrent owner append")

	got := mustGetIssue(t, c, iss.Name)
	ctrl, ok := own.ControllerOwner(got)
	require.True(t, ok, "the artifact was left with ZERO controller owners (contract B.2 rule 5)")
	require.Equal(t, "sib-task", ctrl,
		"B.5 must hand the flag to the surviving owner it can only see on the CURRENT array")
	require.Len(t, got.OwnerReferences, 2,
		"the concurrent owner append was clobbered by a write computed from a stale array")
	require.Equal(t, before, gcBlocked(), "a completed handover is not a GC block")
}
