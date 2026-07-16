// Package incident builds the turn-0 goal for a Grafana-fired incident Task.
// It lives in its own package so both the webhook receiver and the controller
// can import it without a cycle.
package incident

import (
	"strings"

	"github.com/szymonrychu/tatara-operator/internal/promptguidance"
)

// platformProblemGuidance is the same literal used in the controller package's
// turnloop.go, both sourced from the dependency-free internal/promptguidance
// leaf package (incident is itself a leaf package, so it imports rather than
// duplicates). Keep byte-identical; the test pins both via a shared substring.
const platformProblemGuidance = promptguidance.PlatformProblemGuidance

// toolingNoteGuidance is the same literal used in the controller package's
// turnloop.go, sourced from internal/promptguidance. Keep byte-identical.
const toolingNoteGuidance = promptguidance.ToolingNoteGuidance

// GoalProject returns the turn-0 goal for a project-scoped incident Task fired
// by a Grafana alert. The agent investigates live (read-only) via the Grafana
// MCP server, surveys open trackers, then finishes with submit_outcome
// (file_issue, optionally issue.parent to link a related-but-distinct issue as
// a GitHub sub-issue under an open tracker, or false_positive). alertCtx is a
// pre-rendered compact block of the alert (group key, status, labels,
// annotations, generator/external URLs).
func GoalProject(alertCtx string, slugs []string) string {
	repoList := strings.Join(slugs, ", ")
	return "Invoke the `tatara-incident-sre` skill FIRST and follow its phases in order; consult " +
		"`tatara-incident-investigation` for evidence-gathering judgment.\n\n" +
		"A Grafana alert is FIRING for this project. Investigate it and hand a well-evidenced issue " +
		"to the team. Repositories in this project: " + repoList + ".\n\n" +
		"ALERT:\n" + alertCtx + "\n\n" +
		"Investigate LIVE using the `grafana` MCP server (read-only): query the relevant Prometheus/Loki " +
		"datasources, read the firing alert rule (follow its generatorURL), and inspect related dashboards. " +
		"Form a diagnosis backed by the queries you ran and their results.\n\n" +
		"Then SURVEY for existing trackers: (1) list open incident Tasks for this project with your " +
		"task_list tool, and (2) survey the project's EXISTING OPEN issues across the repos above with your " +
		"issue-listing tool. A DUPLICATE of the SAME alert rule never reaches you - it is suppressed at " +
		"admission - so your survey is only to find a RELATED open tracker.\n\n" +
		"Finish with `submit_outcome` exactly once:\n" +
		"- genuinely-new-but-RELATED to an open tracker you found: " +
		"`submit_outcome(action=file_issue, issue.repo, issue.title, issue.body, " +
		"issue.parent={repo, number})`. The operator links your new issue as a sub-issue under that " +
		"tracker and cross-references both. Choose the `repo` (from the list above) the evidence implicates.\n" +
		"- genuinely-new-and-UNRELATED: `submit_outcome(action=file_issue, ...)` with NO parent.\n" +
		"- confirmed FALSE POSITIVE: `submit_outcome(action=false_positive, reason=...)`, open no issue.\n\n" +
		"The issue body MUST contain: the alert summary, the queries/tools you ran and their results, your " +
		"diagnosis, and the Grafana links (generatorURL/externalURL). The issue lands and the normal " +
		"triage flow takes over.\n\n" +
		"This is a READ-ONLY investigation. Do NOT take any remediation, write, or corrective action on any " +
		"system. Your only output is the outcome above." +
		platformProblemGuidance + toolingNoteGuidance
}

// GoalTierRevert returns the turn-0 goal for an incident Task fired by a
// tier-quality alert (a model downgraded for `kind` has regressed). The agent
// proposes reverting that kind's model/effort tier in tatara-helmfile via a
// single, unmerged MR.
func GoalTierRevert(project, kind, model string) string {
	return "A quality-proxy alert is FIRING: kind \"" + kind + "\" on model \"" + model +
		"\" has regressed in project \"" + project + "\". Propose reverting this kind's tier: in " +
		"tatara-helmfile values/project-" + project + "/common.yaml, set agent.modelByKind[" + kind +
		"] back to claude-opus-4-8 and raise agent.effortByKind[" + kind + "] (to high). Open ONE MR " +
		"against tatara-helmfile with only that change and a short rationale citing the alert. Do NOT merge."
}
