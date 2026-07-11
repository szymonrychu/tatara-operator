package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestStaleProposalWindow_DefaultOn is liveness-hardening finding #8: the stale-
// proposal reaper used to be OFF by default (StaleProposalDays==0 disabled it), so
// un-approved proposals awaited a human forever and accumulated unboundedly. The
// unset default must now enable the reaper with a generous-but-finite window;
// a negative value is the explicit opt-out.
func TestStaleProposalWindow_DefaultOn(t *testing.T) {
	mkProj := func(days int) *tatarav1alpha1.Project {
		return &tatarav1alpha1.Project{
			Spec: tatarav1alpha1.ProjectSpec{
				Scm: &tatarav1alpha1.ScmSpec{
					Cron: &tatarav1alpha1.ScmCron{
						Brainstorm: tatarav1alpha1.BrainstormActivity{StaleProposalDays: days},
					},
				},
			},
		}
	}

	// Unset (0) -> enabled with the generous default window.
	win, on := staleProposalWindow(mkProj(0))
	require.True(t, on, "unset StaleProposalDays must default the reaper ON")
	require.Equal(t, time.Duration(defaultStaleProposalDays)*24*time.Hour, win,
		"the default window must be defaultStaleProposalDays")
	require.Greater(t, defaultStaleProposalDays, 0, "the default must be non-zero (finite, generous)")

	// Positive -> that many days.
	win, on = staleProposalWindow(mkProj(14))
	require.True(t, on)
	require.Equal(t, 14*24*time.Hour, win)

	// Negative -> explicit opt-out (disabled).
	_, on = staleProposalWindow(mkProj(-1))
	require.False(t, on, "a negative StaleProposalDays must explicitly disable the reaper")
}
