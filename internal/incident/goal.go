// Package incident builds the turn-0 goal for a Grafana-fired incident Task.
// It lives in its own package so both the webhook receiver and the controller
// can import it without a cycle.
package incident

import (
	"strconv"
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
		"admission - so your survey is to find a tracker for the SAME incident (a different rule firing for " +
		"one shared root cause) or a merely RELATED open tracker.\n\n" +
		"Finish with `submit_outcome` exactly once:\n" +
		"- SAME incident as an open tracker you found (your evidence adds to the SAME root problem): " +
		"`submit_outcome(action=comment_issue, comment={repo, number, body})`, where body is your fresh " +
		"evidence (queries run, results, updated diagnosis, Grafana links). The operator appends it to that " +
		"tracker; NO new issue is filed. Use this instead of filing a near-duplicate.\n" +
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

// GoalEscalation returns the turn-0 goal for a RE-ADMITTED incident: an alert
// has persisted (refireCount recurrences) against an existing open tracker
// (trackerRef, owner/repo#N-shaped from the CR's repositoryRef+number) that the
// operator escalated for re-investigation. The agent must decide whether the
// root cause is still the same as that tracker or has CHANGED under one alert id
// (e.g. a password drift and a resource exhaustion hiding under one alert): same
// root cause -> comment_issue with fresh evidence; changed -> file_issue.
func GoalEscalation(alertCtx string, slugs []string, trackerRef string, refireCount int) string {
	repoList := strings.Join(slugs, ", ")
	return "Invoke the `tatara-incident-sre` skill FIRST and follow its phases in order; consult " +
		"`tatara-incident-investigation` for evidence-gathering judgment.\n\n" +
		"A Grafana alert has PERSISTED (" + strconv.Itoa(refireCount) + " recurrences) against the existing open " +
		"tracker " + trackerRef + ", which the operator has ESCALATED for re-investigation. Repositories in " +
		"this project: " + repoList + ".\n\n" +
		"ALERT:\n" + alertCtx + "\n\n" +
		"Investigate LIVE using the `grafana` MCP server (read-only): query the relevant Prometheus/Loki " +
		"datasources, read the firing alert rule (follow its generatorURL), and inspect related dashboards. " +
		"Then read tracker " + trackerRef + " and RE-EXAMINE whether its root cause is still the one firing, " +
		"or whether a DIFFERENT root cause is now firing under the same alert.\n\n" +
		"Finish with `submit_outcome` exactly once:\n" +
		"- SAME root cause as " + trackerRef + " (still unresolved): " +
		"`submit_outcome(action=comment_issue, comment={repo, number, body})` on " + trackerRef +
		", body = your fresh evidence and why it is still the same problem.\n" +
		"- DIFFERENT root cause under the same alert: `submit_outcome(action=file_issue, issue.repo, " +
		"issue.title, issue.body, issue.parent={repo, number})` for a NEW issue linked under " + trackerRef +
		" - do NOT comment a different problem onto the old tracker.\n" +
		"- confirmed FALSE POSITIVE: `submit_outcome(action=false_positive, reason=...)`.\n\n" +
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
