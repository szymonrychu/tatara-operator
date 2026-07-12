// Copyright 2026 tatara authors.

package controller

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMemoryDoc_NoStaleShippedClaimsAtHead is the F4 regression: the
// 2026-07-12 de-scope pass reverted Status.ImplementationLocked/
// ImplementationLockedAt, IssueOutcome.Locked, and the systemic-approval
// fan-out entirely, but the entries describing them as shipped stayed at the
// TOP of MEMORY.md, above the de-scope entry that reverted them. An agent
// reading MEMORY.md first (mandatory before non-trivial work in this repo -
// see CLAUDE.md) would conclude the fan-out is live. Per this repo's own
// MEMORY.md rule ("prune only when decision reversed"), the stale head
// entries must not precede the reversal.
func TestMemoryDoc_NoStaleShippedClaimsAtHead(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "MEMORY.md"))
	require.NoError(t, err)
	content := string(data)

	descopeIdx := strings.Index(content, "de-scope pass: ship a/b/c, DEFER d")
	require.Greater(t, descopeIdx, 0, "MEMORY.md must still record the de-scope pass that reverted the fan-out feature")

	head := content[:descopeIdx]
	for _, stale := range []string{
		"Request C/d approval fan-out RISK (accepted by user)",
		"Request C/d: added Status.ImplementationLocked (bool) + IssueOutcome.Locked (bool)",
	} {
		require.NotContains(t, head, stale,
			"a stale shipped-feature claim must not precede the de-scope entry that reverted it")
	}
}
