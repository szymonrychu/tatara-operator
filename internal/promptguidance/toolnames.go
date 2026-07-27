package promptguidance

import (
	"regexp"
	"sort"
	"strings"
)

// AgentVisibleTools is every MCP tool name an agent pod can actually call,
// across all seven TATARA_TOOL_PROFILE values. It is a HAND-MAINTAINED MIRROR.
//
// The source of truth is tatara-cli's registry: PlatformTools()
// (internal/mcp/tools.go, 7 tools), CodeTools() (tools_code.go, 4),
// MemoryTools() (tools_memory.go, 5), SCMTools() (tools_scm.go, 4) and
// OutcomeTool() (outcome.go, submit_outcome). The operator cannot import any of
// it - separate module, internal package - so this list is pinned by the
// goal-builder conformance tests instead of by hope, exactly the way the
// "PROPOSAL QUOTA" string and the <proposal_history> element are pinned. When
// tatara-cli adds, renames or reaps a tool, update this list and the count in
// TestAgentVisibleToolsIsSortedUniqueAndToolShaped.
//
// tatara-cli also publishes a machine-readable tool-manifest.json release asset
// (internal/mcp/manifest.go), which tatara-agent-skills lints against. The
// operator deliberately does NOT fetch it: `make test` runs offline, and a
// fetch that soft-warns on failure is a check that can silently stop checking.
//
// Two things are deliberately NOT modelled here. Profile gating: a goal naming
// a tool the reading pod's profile does not grant is a different, rarer bug,
// and catching it would mean mirroring profiles.go too. Enum values: names like
// "propose", "skip", "file_issue" and "memory_inconsistent" are ARGUMENT values
// of submit_outcome and report_internal_issue, not tools, and never belong here.
var AgentVisibleTools = []string{
	"code_context",
	"code_explain",
	"code_graph",
	"code_search",
	"issue_write",
	"memory_describe",
	"memory_edges",
	"memory_entity",
	"memory_query",
	"memory_write",
	"mr_takeover_request",
	"mr_write",
	"project_get",
	"repo_list",
	"report_internal_issue",
	"scm_read",
	"submit_outcome",
	"task_context",
	"task_get",
	"task_list",
	"task_note",
}

// backtickSpan captures the text inside each pair of backticks. Every goal
// builder marks the tools it names this way, so a backtick span is the only
// place a tool mention can appear.
var backtickSpan = regexp.MustCompile("`([^`]*)`")

// toolNameShape is the identifier shape every tatara MCP tool has: lowercase
// ASCII, at least one underscore, and no other punctuation at all. It is what
// separates a tool mention from ordinary backticked prose. `decks/`, `%ct`,
// `values.yaml`, `workspace/<owner>/<repo>`, `semver:<level>`, `decision=close`,
// `task/x`, `<proposal_history>`, `MEMORY_DEGRADED: ...` and
// `tatara-council-brainstorm` all fail it, so none of them can be reported as
// an unknown tool. That matters more than catching every last case: a checker
// with false positives grows an allow-list, and an allow-list is how the
// list_handoffs bug survived.
var toolNameShape = regexp.MustCompile(`^[a-z][a-z0-9]*(?:_[a-z0-9]+)+$`)

// UnknownToolNames returns every tool-shaped name the prose mentions in
// backticks that is not in AgentVisibleTools, sorted and deduplicated. It
// returns nil when the prose names only real tools.
//
// Call form is accepted: the argument list of `submit_outcome(action=skip,
// reason=...)` is stripped before the shape test, so that mention resolves to
// submit_outcome and the argument values inside are never tested as tools.
//
// It only sees BACKTICKED mentions. Prose that names a tool without backticks
// is invisible to it; that is a known and asserted limit, not an oversight.
func UnknownToolNames(prose string) []string {
	known := make(map[string]bool, len(AgentVisibleTools))
	for _, n := range AgentVisibleTools {
		known[n] = true
	}
	seen := map[string]bool{}
	var out []string
	for _, m := range backtickSpan.FindAllStringSubmatch(prose, -1) {
		name := m[1]
		if i := strings.IndexByte(name, '('); i >= 0 {
			name = name[:i]
		}
		name = strings.TrimSpace(name)
		if !toolNameShape.MatchString(name) || known[name] || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
