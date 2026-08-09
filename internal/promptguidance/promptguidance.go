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

// MemoryDegradedGuidance is appended to a turn-0 directive ONLY when the
// project memory stack is not stably ready. Memory failure no longer holds a
// Task, so the agent runs with no recall and must be told that this is
// expected. Without it the last word the agent reads about a failing tool is
// PlatformProblemGuidance's "report it and stop", which is exactly the stall
// the platform must no longer take on a memory outage.
const MemoryDegradedGuidance = "\n\n## Memory recall is unavailable this turn\n" +
	"The recall subsystem is down for this project right now. Every memory and code-graph tool " +
	"will fail. This is EXPECTED and it is already alerted on, and it OVERRIDES the 'report it and " +
	"stop' rule above for those tools specifically: a failing memory tool is NOT a reason to stop.\n" +
	"  - Work from the repository itself: Serena/LSP, git history, and plain file reads give you " +
	"everything memory would have summarised, just more slowly.\n" +
	"  - Do NOT call `report_internal_issue` for this - the degraded condition is already tracked " +
	"and alerted on upstream, so a fresh report every turn would only manufacture duplicate " +
	"platform-problem alerts (tatara-operator#523). Do not retry the memory tools in a loop either.\n" +
	"  - Say in your outcome body (or in your decline reason) that memory recall was unavailable, " +
	"so a reviewer knows what you could not check.\n" +
	"  - COMPLETE the assignment with reduced recall. Declining for this reason alone is wrong."

// MemoryDisabledGuidance is appended to a turn-0 directive when the project has
// spec.memory.enabled=false.
//
// It exists because MemoryDegradedGuidance is WRONG for this case in one
// specific, expensive way: it instructs the agent to call report_internal_issue,
// which on a project that is CONFIGURED without memory manufactures one bogus
// platform-problem alert every single turn (tatara-operator#523). The
// operational advice is identical - work from the repository, do not stop, say
// so in your outcome - minus the incident report, because there is no incident.
const MemoryDisabledGuidance = "\n\n## This project has no memory or recall tooling\n" +
	"Memory and code-graph tooling is NOT AVAILABLE on this project. This is a deliberate " +
	"configuration, not a fault: nothing is broken, nothing is being fixed, and it will not come " +
	"back later in this turn. It OVERRIDES the 'report it and stop' rule above for those tools " +
	"specifically.\n" +
	"  - Do NOT call `report_internal_issue` about it, do not open or comment on a tracker issue " +
	"about it, and do not mention it as a platform problem. It is not one.\n" +
	"  - Work from the repository itself: Serena/LSP, git history, and plain file reads are the " +
	"whole toolset here, and they are sufficient.\n" +
	"  - Do not call the memory or code-graph tools at all; if you do, they will fail, and that " +
	"failure is still not a reason to stop.\n" +
	"  - COMPLETE the assignment. Declining because this project has no recall is wrong."

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

// ConcisenessGuidance is appended to every agent turn-0 directive,
// unconditionally, at internal/controller/assignment.go's single append site.
// It lives here and NOT duplicated into seven skills free to drift.
//
// Issue #506: tatara's own forge-visible writing (issue/MR bodies, discussion
// messages, comments) was too long for its actual audience.
const ConcisenessGuidance = "\n\n## Writing style for your own output\n" +
	"You are writing for a matter expert in this system: the person who built it. " +
	"Assume the architecture, the vocabulary and the failure modes are already known. " +
	"Say what changed and why it is not obvious; skip what a reader could derive. " +
	"For every issue comment, MR body, discussion message, and outcome body you write:\n" +
	"  - Bullets and tables over prose. No restated context the reader already has " +
	"(the diff, the thread, the goal).\n" +
	"  - Skip explaining what is obvious from the diff or from standard terms in " +
	"the domain. State conclusions and decisions, not the reasoning that got you there.\n" +
	"  - Less is more: shorter and precise beats thorough and long."

// ToolingConsumeGuidance is appended to implementer-agent prompts. It
// instructs the agent to pick up any '## Tooling' section from the issue body
// and add each tool to the repo's .mise.toml as part of the implementation.
const ToolingConsumeGuidance = "\n\n## Tooling from the issue\n" +
	"If the issue body has a '## Tooling' section listing tools, add each to the appropriate " +
	"repo's root .mise.toml (pinned version) as part of your implementation, so future runs " +
	"have it preinstalled."
