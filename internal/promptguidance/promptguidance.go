// Package promptguidance holds the turn-0 guidance literals shared across
// agent-goal packages (controller, incident, refine). It is a dependency-free
// leaf package (stdlib only) so each of those packages can import it directly
// instead of duplicating the literals to avoid an import cycle.
package promptguidance

// PlatformProblemGuidance is appended to every agent turn-0 directive. A
// platform/tooling failure (MCP error, missing access, a tatara tool failing)
// is self-reported via report_internal_issue, never filed as a tracker issue.
const PlatformProblemGuidance = "\n\n## Platform problems\n" +
	"If you are BLOCKED by a platform or tooling failure - an MCP server returning an error " +
	"(e.g. grafana 401/unreachable), missing access or credentials, a tatara tool failing, or a " +
	"required dependency you cannot reach - call `report_internal_issue` with the concrete details " +
	"(which tool, the exact error, what you were attempting). That self-report is the ONLY correct " +
	"channel for platform/tooling failures: it raises operator telemetry and an alert. Do NOT open, " +
	"propose, or comment on a tracker issue asking a human to fix the platform, and do NOT treat a " +
	"blocked tool as a reason to file your normal output - report it and stop."

// ToolingNoteGuidance is appended to proposer-agent prompts (brainstorm,
// healthCheck, refine, incident). It instructs the agent to fold any mise
// tooling it needed into the issue it files, so the implementer can add it to
// the repo's .mise.toml.
const ToolingNoteGuidance = "\n\n## Tooling you needed\n" +
	"If you used mise to install a CLI tool, runtime, or linter that was NOT already in the " +
	"target repo's .mise.toml to do this analysis, add a '## Tooling' section to the issue you " +
	"propose listing each tool (name@version + one-line why), so the implementation agent adds it " +
	"to the repo's .mise.toml. Do not file a separate issue for tooling; fold it into the issue " +
	"you are proposing."

// ToolingConsumeGuidance is appended to implementer-agent prompts. It
// instructs the agent to pick up any '## Tooling' section from the issue body
// and add each tool to the repo's .mise.toml as part of the implementation.
const ToolingConsumeGuidance = "\n\n## Tooling from the issue\n" +
	"If the issue body has a '## Tooling' section listing tools, add each to the appropriate " +
	"repo's root .mise.toml (pinned version) as part of your implementation, so future runs " +
	"have it preinstalled."
