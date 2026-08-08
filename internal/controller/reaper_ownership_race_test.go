package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

func issueConflict(name string) error {
	return apierrors.NewConflict(
		schema.GroupResource{Group: tatarav1alpha1.GroupVersion.Group, Resource: "issues"},
		name, apierrors.NewBadRequest("the object has been modified"))
}

// TestReleaseOwnershipRetriesATransientConflictWithoutCountingIt is issue #530,
// first half: one 409 that the very next attempt resolves must neither fail the
// reap nor move operator_gc_blocked_total. Counting it pinned `Operator GC
// blocked` firing for ~1 h behind a false "accumulating in etcd" annotation,
// twice on 2026-08-05, for two conflicts that self-healed in under a second.
func TestReleaseOwnershipRetriesATransientConflictWithoutCountingIt(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("gcflap")
	repo := reapRepo("gcflap", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	dying := reapTask("gcflap", "rej-task", "clarify",
		tatarav1alpha1.StateRejected, stage.ReasonDeclined, time.Now().Add(-time.Minute))
	dying.Status.IssueRefs = []string{tatarav1alpha1.IssueName(repo.Name, 9)}
	iss := raceIssue(repo.Name, 9, "gcflap", "rej-task")

	var (
		mu      sync.Mutex
		updates int
	)
	c := newMirrorClientIntercepted(t, interceptor.Funcs{
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object,
			opts ...client.UpdateOption) error {
			if _, ok := obj.(*tatarav1alpha1.Issue); ok {
				mu.Lock()
				updates++
				first := updates == 1
				mu.Unlock()
				if first {
					return issueConflict(obj.GetName())
				}
			}
			return cl.Update(ctx, obj, opts...)
		},
	}, proj, repo, reapSecret(), dying, iss)

	before := gcBlocked()
	require.NoError(t, r0(c).ReapTerminal(ctx, proj),
		"a self-healing 409 on the owner-ref drop must be retried, not surfaced")
	require.Equal(t, before, gcBlocked(),
		"a 409 the retry resolved was counted as a permanent GC block")
	require.Empty(t, mustGetIssue(t, c, iss.Name).OwnerReferences,
		"the owner ref was never dropped: the release gave up on the first conflict")
}

// TestReleaseOwnershipDoesNotCountAnExhaustedConflictAsGCBlocked is issue #530,
// second half. Even when every retry conflicts, the release is not BLOCKED - the
// reconcile returns the error and controller-runtime requeues it, which is the
// resolution path the counter's alert claims does not exist. Only a failure the
// requeue will not resolve belongs in operator_gc_blocked_total.
func TestReleaseOwnershipDoesNotCountAnExhaustedConflictAsGCBlocked(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("gcexh")
	repo := reapRepo("gcexh", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	dying := reapTask("gcexh", "rej-task", "clarify",
		tatarav1alpha1.StateRejected, stage.ReasonDeclined, time.Now().Add(-time.Minute))
	dying.Status.IssueRefs = []string{tatarav1alpha1.IssueName(repo.Name, 9)}
	iss := raceIssue(repo.Name, 9, "gcexh", "rej-task")

	c := newMirrorClientIntercepted(t, interceptor.Funcs{
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object,
			opts ...client.UpdateOption) error {
			if _, ok := obj.(*tatarav1alpha1.Issue); ok {
				return issueConflict(obj.GetName())
			}
			return cl.Update(ctx, obj, opts...)
		},
	}, proj, repo, reapSecret(), dying, iss)

	before := gcBlocked()
	require.Error(t, r0(c).ReapTerminal(ctx, proj), "an unresolved conflict must still fail the reap")
	require.Equal(t, before, gcBlocked(),
		"a conflict the requeue will resolve was counted as a permanent GC block")
}

// TestReleaseOwnershipCountsAPermanentUpdateFailureAsGCBlocked is the other side
// of #530's boundary: the counter must keep firing for a failure a requeue does
// NOT fix. Narrowing "blocked" must not silence it.
func TestReleaseOwnershipCountsAPermanentUpdateFailureAsGCBlocked(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("gcperm")
	repo := reapRepo("gcperm", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	dying := reapTask("gcperm", "rej-task", "clarify",
		tatarav1alpha1.StateRejected, stage.ReasonDeclined, time.Now().Add(-time.Minute))
	dying.Status.IssueRefs = []string{tatarav1alpha1.IssueName(repo.Name, 9)}
	iss := raceIssue(repo.Name, 9, "gcperm", "rej-task")

	c := newMirrorClientIntercepted(t, interceptor.Funcs{
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object,
			opts ...client.UpdateOption) error {
			if _, ok := obj.(*tatarav1alpha1.Issue); ok {
				return apierrors.NewForbidden(
					schema.GroupResource{Group: tatarav1alpha1.GroupVersion.Group, Resource: "issues"},
					obj.GetName(), apierrors.NewBadRequest("denied by admission webhook"))
			}
			return cl.Update(ctx, obj, opts...)
		},
	}, proj, repo, reapSecret(), dying, iss)

	before := gcBlocked()
	require.Error(t, r0(c).ReapTerminal(ctx, proj))
	require.Greater(t, gcBlocked(), before,
		"a rejection no requeue can resolve IS a GC block and must still be counted")
}

// TestDeleteReapedTaskSkipsTheReleaseItAlreadyDid is issue #545.
//
// reapTerminal runs the release TWICE - releaseTerminal, then deleteReapedTask -
// and only the first was idempotent. ownedIssues keys off status.issueRefs, not
// off ownership, so the second pass re-derives the exact artifact set the first
// pass just released, ~22 ms later, off a cache that has usually but not always
// observed the first write. 4 of 25 releases over 48 h 409'd against the
// reaper's OWN commit that way.
//
// The fixture IS that stale view: the Task carries AnnTerminalReleased (the
// release provably completed) while the artifact copy the second pass would
// re-derive still shows it as controller owner. The second release must not
// happen at all - a retry loop only masks it, leaving the redundant re-Get and
// re-Update per reap.
func TestDeleteReapedTaskSkipsTheReleaseItAlreadyDid(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("dblrel")
	repo := reapRepo("dblrel", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	released := reapTask("dblrel", "rel-task", "clarify",
		tatarav1alpha1.StateRejected, stage.ReasonDeclined, time.Now().Add(-25*time.Hour))
	released.Annotations = map[string]string{AnnTerminalReleased: "true"}
	released.Status.IssueRefs = []string{tatarav1alpha1.IssueName(repo.Name, 9)}
	iss := raceIssue(repo.Name, 9, "dblrel", "rel-task")

	var (
		mu      sync.Mutex
		updates int
	)
	c := newMirrorClientIntercepted(t, interceptor.Funcs{
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object,
			opts ...client.UpdateOption) error {
			if _, ok := obj.(*tatarav1alpha1.Issue); ok {
				mu.Lock()
				updates++
				mu.Unlock()
			}
			return cl.Update(ctx, obj, opts...)
		},
	}, proj, repo, reapSecret(), released, iss)

	before := gcBlocked()
	require.NoError(t, r0(c).deleteReapedTask(ctx, proj, released, map[string]bool{"rel-task": true}))

	mu.Lock()
	got := updates
	mu.Unlock()
	require.Zero(t, got,
		"deleteReapedTask re-ran the release of an already-released Task; that redundant write is what 409s against the reaper's own commit")
	require.Equal(t, before, gcBlocked())

	_, alive := mustGetTask(t, c, "rel-task")
	require.False(t, alive, "the Task must still be collected")
}

// TestDeleteReapedTaskStillReleasesWhenNotYetReleased is the guard on #545's
// guard. resume.go's collection reaches deleteReapedTask WITHOUT a prior
// releaseTerminal, and so does reapDelivered and the backlog-sweep park branch;
// for those the release is the ONLY one there will ever be. Gating it on the
// annotation must not delete it.
func TestDeleteReapedTaskStillReleasesWhenNotYetReleased(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("firstrel")
	repo := reapRepo("firstrel", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	unreleased := reapTask("firstrel", "res-task", "clarify",
		tatarav1alpha1.StateDone, "", time.Now().Add(-25*time.Hour))
	unreleased.Status.IssueRefs = []string{tatarav1alpha1.IssueName(repo.Name, 9)}
	iss := raceIssue(repo.Name, 9, "firstrel", "res-task")

	c := newMirrorClientIntercepted(t, interceptor.Funcs{}, proj, repo, reapSecret(), unreleased, iss)

	require.NoError(t, r0(c).deleteReapedTask(ctx, proj, unreleased, map[string]bool{"res-task": true}))
	require.Empty(t, mustGetIssue(t, c, iss.Name).OwnerReferences,
		"a Task collected without a prior releaseTerminal must still have its artifacts released")
}
