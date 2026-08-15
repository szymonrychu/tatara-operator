package stage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// adoptedUpgradeTask is the shape internal/controller's MintAdoptedUpgradeTask
// creates: kind=upgrade, bound to a repo, minted ONTO a merge request that
// already existed.
func adoptedUpgradeTask() *v1alpha1.Task {
	t := taskOfKind(v1alpha1.StateNew, "upgrade")
	t.Spec.RepositoryRef = "charts"
	t.Spec.Source = &v1alpha1.TaskSource{IsPR: true, Number: 41}
	return t
}

// cronUpgradeTask is the cron mint: no repo, no source, InitialState
// under-implementation.
func cronUpgradeTask() *v1alpha1.Task {
	return taskOfKind(v1alpha1.StateNew, "upgrade")
}

// The one edge this change adds. An adopted upgrade Task enters at `new` and
// is triaged into the review lane, exactly as a kind=review Task is.
func TestLegalFor_AdoptedUpgradeMayEnterAwaitingReviewFromNew(t *testing.T) {
	require.True(t,
		stage.LegalFor(adoptedUpgradeTask(), nil, v1alpha1.StateNew, v1alpha1.StateAwaitingReview),
		"an adopted upgrade Task must be able to take new -> awaiting-review")
}

// GUARD 5 must still hold for everything else. A CRON-minted upgrade Task has
// no merge request to review; it mints straight into under-implementation and
// must never acquire a second, gate-free entry.
func TestLegalFor_Guard5StillRefusesEveryOtherKindFromNew(t *testing.T) {
	cases := []struct {
		name string
		task *v1alpha1.Task
	}{
		{"a cron-minted upgrade Task (no bound merge request)", cronUpgradeTask()},
		{"an implement Task", taskOfKind(v1alpha1.StateNew, "implement")},
		{"a takeover Task", taskOfKind(v1alpha1.StateNew, "takeover")},
		{"a brainstorm Task", taskOfKind(v1alpha1.StateNew, "brainstorm")},
		{"a documentation Task", taskOfKind(v1alpha1.StateNew, "documentation")},
		{"a nil Task", nil},
	}
	for _, tc := range cases {
		require.False(t,
			stage.LegalFor(tc.task, nil, v1alpha1.StateNew, v1alpha1.StateAwaitingReview),
			"%s: GUARD 5 must still refuse new -> awaiting-review", tc.name)
	}
	// An ADOPTED shape of a kind that is not upgrade is refused too: the guard
	// is a kind whitelist widened, not replaced by the discriminator.
	adoptedTakeover := taskOfKind(v1alpha1.StateNew, "takeover")
	adoptedTakeover.Spec.Source = &v1alpha1.TaskSource{IsPR: true, Number: 41}
	require.False(t,
		stage.LegalFor(adoptedTakeover, nil, v1alpha1.StateNew, v1alpha1.StateAwaitingReview),
		"a takeover Task bound to an MR must not acquire a second entry")
}

// The discriminator is spec.source, and it must be BOTH conditions. An upgrade
// Task carrying a source that is not a merge request, or a merge request with
// no number, is not adopted and must not get the review lane.
func TestAdoptedMR_RequiresAnActualMergeRequest(t *testing.T) {
	cases := []struct {
		name string
		src  *v1alpha1.TaskSource
		want bool
	}{
		{"a bound merge request", &v1alpha1.TaskSource{IsPR: true, Number: 41}, true},
		{"an issue, not a merge request", &v1alpha1.TaskSource{IsPR: false, Number: 41}, false},
		{"a merge request with no number", &v1alpha1.TaskSource{IsPR: true}, false},
		{"no source at all (the cron shape)", nil, false},
	}
	for _, tc := range cases {
		task := taskOfKind(v1alpha1.StateNew, "upgrade")
		task.Spec.Source = tc.src
		require.Equal(t, tc.want, stage.AdoptedMR(task), tc.name)
	}
	require.False(t, stage.AdoptedMR(nil), "AdoptedMR(nil) must be false")
}

// GUARD 1 is untouched and must stay untouched: it is what stops a kind=review
// Task merging a human's PR, and widening GUARD 5 must not have leaked into it.
func TestLegalFor_Guard1UnchangedByTheAdoptedEntry(t *testing.T) {
	rev := taskOfKind(v1alpha1.StateAwaitingReview, "review")
	rev.Spec.Source = &v1alpha1.TaskSource{IsPR: true, Number: 41}
	for _, to := range []string{v1alpha1.StateUnderImplementation, v1alpha1.StateMerged} {
		require.False(t, stage.LegalFor(rev, nil, v1alpha1.StateAwaitingReview, to),
			"GUARD 1 must still refuse a review Task into %s", to)
	}
}

// The two awaiting-review exits an adopted Task actually takes. Neither is a
// change - both are gated on spec.kind != review and upgrade satisfies both -
// so this test PASSES BEFORE the change and exists to keep it that way.
func TestLegalFor_AdoptedUpgradeTakesBothAwaitingReviewExits(t *testing.T) {
	mrs := []v1alpha1.MergeRequest{{}} // one owned MR, pendingReview nil: the gate is open
	task := adoptedUpgradeTask()
	task.Status.State = v1alpha1.StateAwaitingReview
	for _, to := range []string{v1alpha1.StateMerged, v1alpha1.StateUnderImplementation} {
		require.True(t, stage.LegalFor(task, mrs, v1alpha1.StateAwaitingReview, to),
			"an adopted upgrade Task must be able to take awaiting-review -> %s", to)
	}
}

// AdoptedMR IS A SUPERSET AND THAT IS THE TRAP. A takeover Task and a kind=review
// Task are BOTH minted onto an existing merge request and BOTH carry
// spec.source.isPR with a number, so AdoptedMR is true for them. Every
// behavioural difference of the adoption path must key on AdoptedUpgrade
// instead: keying on AdoptedMR strips merge-after-stand-down from every takeover
// and tells every review pod that approving merges the change.
func TestAdoptedUpgrade_IsNarrowerThanAdoptedMR(t *testing.T) {
	onAnMR := func(kind string) *v1alpha1.Task {
		return &v1alpha1.Task{Spec: v1alpha1.TaskSpec{
			Kind:   kind,
			Source: &v1alpha1.TaskSource{IsPR: true, Number: 41},
		}}
	}
	for _, kind := range []string{"takeover", "review", "implement"} {
		tk := onAnMR(kind)
		if !stage.AdoptedMR(tk) {
			t.Fatalf("kind %q on a merge request is not AdoptedMR; the test premise is wrong", kind)
		}
		if stage.AdoptedUpgrade(tk) {
			t.Errorf("AdoptedUpgrade is true for kind %q", kind)
		}
	}
	if !stage.AdoptedUpgrade(onAnMR(stage.AgentUpgrade)) {
		t.Error("AdoptedUpgrade is false for an adopted upgrade Task")
	}
	// A CRON-minted upgrade Task has no merge request and must stay out.
	cron := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: stage.AgentUpgrade}}
	if stage.AdoptedUpgrade(cron) {
		t.Error("AdoptedUpgrade is true for a cron-minted upgrade Task")
	}
	if stage.AdoptedUpgrade(nil) {
		t.Error("AdoptedUpgrade is true for nil")
	}
}
