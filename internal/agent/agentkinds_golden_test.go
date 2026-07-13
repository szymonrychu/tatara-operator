package agent

import (
	"bufio"
	"os"
	"sort"
	"strings"
	"testing"
)

// TestKindProfilesMatchesAgentKindsGolden pins kindProfiles' key set to the
// SAME golden file tatara-cli pins its MCP profile table to
// (tatara-cli/internal/mcp/testdata/agent-kinds.txt, vendored here
// byte-identically).
//
// This is the guard whose ABSENCE caused a live P0 (contract L.5). The operator
// emitted TATARA_TOOL_PROFILE=clarify while tatara-cli had zero occurrences of
// the string "clarify", so resolveProfile fell through to the always-on set:
// every clarify pod in production listed 74 tools, could call 6 of them, and
// had NO terminal outcome tool - it could not report a result at all, on any
// path, ever.
//
// A golden that only ONE side checks is not a guard. tatara-cli asserts its
// half; this asserts ours. Both must fail if the seven kinds drift.
func TestKindProfilesMatchesAgentKindsGolden(t *testing.T) {
	want := readAgentKindsGolden(t)

	var got []string
	for k := range kindProfiles {
		got = append(got, k)
	}
	sort.Strings(got)

	if len(got) != len(want) {
		t.Fatalf("kindProfiles has %d agent kinds %v, golden has %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("agent kind %d: kindProfiles has %q, golden has %q", i, got[i], want[i])
		}
	}
}

// TestKindProfilesValuesAreTheKindItself guards the OTHER half of the contract:
// G.9 says TATARA_TOOL_PROFILE and TATARA_SKILL_PROFILE both carry the AGENT
// KIND. A mapping from an agent kind to some other profile string is exactly
// the indirection that let "clarify" -> "" go unnoticed.
func TestKindProfilesValuesAreTheKindItself(t *testing.T) {
	for _, k := range readAgentKindsGolden(t) {
		if got := kindProfiles[k]; got != k {
			t.Errorf("kindProfiles[%q] = %q, want %q (the profile IS the agent kind)", k, got, k)
		}
	}
}

// TestEveryAgentKindResolvesATerminalProfile asserts the property the P0 broke:
// every one of the seven agent kinds resolves to a NON-EMPTY profile. An empty
// profile is not a nil-safe default - it is a pod that cannot terminate.
func TestEveryAgentKindResolvesATerminalProfile(t *testing.T) {
	for _, k := range readAgentKindsGolden(t) {
		if profileForKind(k) == "" {
			t.Errorf("agent kind %q resolves to an EMPTY tool profile: the cli fails closed, "+
				"the pod gets no submit_outcome, and the Task can never terminate", k)
		}
	}
}

func readAgentKindsGolden(t *testing.T) []string {
	t.Helper()
	f, err := os.Open("testdata/agent-kinds.txt")
	if err != nil {
		t.Fatalf("open agent-kinds golden: %v", err)
	}
	defer func() { _ = f.Close() }()

	var kinds []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		kinds = append(kinds, line)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read agent-kinds golden: %v", err)
	}
	if len(kinds) != 7 {
		t.Fatalf("golden has %d agent kinds, want exactly 7 (contract G.9)", len(kinds))
	}
	sort.Strings(kinds)
	return kinds
}
