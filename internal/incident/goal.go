// Package incident builds the turn-0 goal for a Grafana-fired incident Task.
// It lives in its own package so both the webhook receiver and the controller
// can import it without a cycle.
package incident

import "strings"

// platformProblemGuidance is the same literal used in the controller package's
// turnloop.go. Duplicated here to avoid an import cycle (incident is a leaf
// package). Keep byte-identical; the test pins both via a shared substring.
const platformProblemGuidance = "\n\n## Platform problems\n" +
	"If you are BLOCKED by a platform or tooling failure - an MCP server returning an error " +
	"(e.g. grafana 401/unreachable), missing access or credentials, a tatara tool failing, or a " +
	"required dependency you cannot reach - call `report_internal_issue` with the concrete details " +
	"(which tool, the exact error, what you were attempting). That self-report is the ONLY correct " +
	"channel for platform/tooling failures: it raises operator telemetry and an alert. Do NOT open, " +
	"propose, or comment on a tracker issue asking a human to fix the platform, and do NOT treat a " +
	"blocked tool as a reason to file your normal output - report it and stop."

// GoalProject returns the turn-0 goal for a project-scoped incident Task fired
// by a Grafana alert. The agent investigates live (read-only) via the Grafana
// MCP server, then files exactly one evidence issue via propose_issue, choosing
// the repo the evidence implicates. alertCtx is a pre-rendered compact block of
// the alert (group key, status, labels, annotations, generator/external URLs).
func GoalProject(alertCtx string, slugs []string) string {
	repoList := strings.Join(slugs, ", ")
	return "A Grafana alert is FIRING for this project. Investigate it and hand a well-evidenced issue " +
		"to the team. Repositories in this project: " + repoList + ".\n\n" +
		"ALERT:\n" + alertCtx + "\n\n" +
		"Investigate LIVE using the `grafana` MCP server (read-only): query the relevant Prometheus/Loki " +
		"datasources, read the firing alert rule (follow its generatorURL), and inspect related dashboards. " +
		"Form a diagnosis backed by the queries you ran and their results.\n\n" +
		"Then call propose_issue(repo, body) EXACTLY ONCE. Choose the `repo` (from the list above) that the " +
		"evidence implicates. The body MUST contain: the alert summary, the queries/tools you ran and their " +
		"results, your diagnosis, and the Grafana links (generatorURL/externalURL). The issue lands with the " +
		"brainstorming label and the normal triage/brainstorm flow takes over.\n\n" +
		"If after investigation this is a confirmed false positive, finish with a one-line note and " +
		"do NOT open an issue.\n\n" +
		"This is a READ-ONLY investigation. Do NOT take any remediation, write, or corrective action on any " +
		"system. Your only output is the issue (or the false-positive note)." +
		platformProblemGuidance
}
