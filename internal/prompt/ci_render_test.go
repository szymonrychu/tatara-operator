package prompt_test

// PR A item 4. The bundle has always fed ci="{{x .CI}}" straight from
// mr.status.ciStatus - and until PR A that field had one mint-time writer, so
// an MR the agent opened itself rendered ci="" for its entire life and every
// agent was shown a blank.
//
// With real data flowing, the render has one job left: say WHEN the status was
// observed. A green with no date is a green an agent cannot reason about, and
// "the mirror can be an hour stale" is the exact premise ci_gate.go was written
// around.

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/prompt"
)

func TestRender_CIStatusCarriesItsObservationDate(t *testing.T) {
	mr := canonicalMR(t)
	mr.Status.CIStatus = "red"
	observed := ts(t, "2026-08-10T09:15:00Z")
	mr.Status.CIUpdatedAt = &observed

	got, err := prompt.Render(prompt.Input{Task: canonicalTask(t), MergeRequests: []v1alpha1.MergeRequest{mr}, Assignment: "a"})
	require.NoError(t, err)
	require.Contains(t, got, `ci="red"`)
	require.Contains(t, got, `ci_updated_at="2026-08-10T09:15Z"`,
		"a CI status with no date cannot be told from a stale one")
}

// A mirror that has never seen a CI observation renders no date at all rather
// than a zero one. Absence is the honest signal; a fabricated timestamp would
// read as a fresh observation.
func TestRender_NeverObservedCI_RendersNoDate(t *testing.T) {
	mr := canonicalMR(t)
	mr.Status.CIStatus = ""
	mr.Status.CIUpdatedAt = nil

	got, err := prompt.Render(prompt.Input{Task: canonicalTask(t), MergeRequests: []v1alpha1.MergeRequest{mr}, Assignment: "a"})
	require.NoError(t, err)
	require.NotContains(t, got, "ci_updated_at=")
	require.Contains(t, got, `ci=""`)
}

// The attribute sits INSIDE the <merge_request> open tag, next to ci - not as a
// sibling element. The bundle's shape is load-bearing (E.2) and the agent
// prompts read these two as a pair.
func TestRender_CIDateIsAnAttributeOfTheSameElement(t *testing.T) {
	mr := canonicalMR(t)
	observed := ts(t, "2026-08-10T09:15:00Z")
	mr.Status.CIUpdatedAt = &observed

	got, err := prompt.Render(prompt.Input{Task: canonicalTask(t), MergeRequests: []v1alpha1.MergeRequest{mr}, Assignment: "a"})
	require.NoError(t, err)
	openTag := ""
	for _, line := range strings.Split(got, "\n") {
		if strings.Contains(line, "<merge_request ") {
			openTag = line
		}
	}
	require.NotEmpty(t, openTag, "no <merge_request> element rendered")
	require.Contains(t, openTag, `ci="green" ci_updated_at="2026-08-10T09:15Z"`)
}
