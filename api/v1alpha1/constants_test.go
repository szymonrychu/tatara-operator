package v1alpha1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// THE CEILING INVARIANT, MACHINE-CHECKED. Until now this was prose in
// constants.go:31-37. At 5 versus 3 the live-pod ceiling could never bind:
// three chatty conversations saturate 100% of a project's agent concurrency
// before the live-pod cap does anything, starving every implement/review/merge
// Task indefinitely (2026-07-28 final review IMPORTANT 1). STRICTLY below, not
// at-or-below: equal caps leave zero slots for non-conversational work.
func TestMaxLivePodsIsStrictlyBelowMaxConcurrentAgents(t *testing.T) {
	require.Less(t, v1alpha1.DefaultMaxLivePods, v1alpha1.DefaultMaxConcurrentAgents,
		"DefaultMaxLivePods (%d) must be STRICTLY below DefaultMaxConcurrentAgents (%d)",
		v1alpha1.DefaultMaxLivePods, v1alpha1.DefaultMaxConcurrentAgents)
}

// The same invariant against a CONFIGURED Project, not just the defaults - a
// maintainer can raise maxLivePods per project and the ceiling must still bind.
//
// WITH ITS ONE EXCEPTION, ASSERTED HERE RATHER THAN GLOSSED OVER. At
// maxConcurrentAgents == 1 the clamp would yield a ceiling of 0, which
// deadlocks the project (no Task could ever go live), so MaxLivePods floors at
// 1 and the floor BEATS the clamp: ceiling == cap, not below it. Strict
// inequality is therefore the contract for agents >= 2 only.
func TestMaxLivePodsIsStrictlyBelowMaxConcurrentAgentsForAnyProject(t *testing.T) {
	for _, tc := range []struct{ live, agents, wantLive int }{
		{0, 0, v1alpha1.DefaultMaxLivePods}, // both unset: defaults
		{5, 3, 2},                           // over-configured: clamped below
		{2, 8, 2},                           // sane: unchanged
		{7, 8, 7},                           // sane at scale: unchanged
		{8, 8, 7},                           // equal: clamped to agents-1
		{1, 1, 1},                           // THE DOCUMENTED EXCEPTION: the floor beats the clamp
	} {
		p := &v1alpha1.Project{Spec: v1alpha1.ProjectSpec{
			MaxLivePods: tc.live, MaxConcurrentAgents: tc.agents}}
		got := v1alpha1.MaxLivePods(p)
		require.Equal(t, tc.wantLive, got)
		if agents := v1alpha1.MaxConcurrentAgents(p); agents == 1 {
			require.Equal(t, agents, got,
				"at maxConcurrentAgents == 1 the floor beats the clamp: the ceiling EQUALS the cap, "+
					"because a ceiling of 0 would deadlock the project")
		} else {
			require.Less(t, got, agents,
				"MaxLivePods must be strictly below MaxConcurrentAgents whenever the cap is >= 2")
		}
	}
}

// A PAUSED project (maxConcurrentAgents == 0) must never be readable as
// "3 agents" through this helper: the clamp helper defaults, the pause check
// does NOT. Nothing may use MaxConcurrentAgents(p) to detect a pause.
func TestMaxLivePodsNeverDropsBelowOne(t *testing.T) {
	p := &v1alpha1.Project{Spec: v1alpha1.ProjectSpec{MaxLivePods: 1, MaxConcurrentAgents: 1}}
	require.Equal(t, 1, v1alpha1.MaxLivePods(p),
		"a one-agent project still gets one live slot; the floor is 1, not 0")
}
