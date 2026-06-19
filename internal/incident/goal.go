// Package incident builds the turn-0 goal for a Grafana-fired incident Task.
// It lives in its own package so both the webhook receiver and the controller
// can import it without a cycle.
package incident

import "strings"

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
		"If the grafana MCP server is unreachable, still file the issue with the raw alert and note the MCP was " +
		"unavailable. If after investigation this is a confirmed false positive, finish with a one-line note and " +
		"do NOT open an issue.\n\n" +
		"This is a READ-ONLY investigation. Do NOT take any remediation, write, or corrective action on any " +
		"system. Your only output is the issue (or the false-positive note)."
}
