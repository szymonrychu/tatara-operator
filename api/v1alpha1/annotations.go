package v1alpha1

// ReingestRequestedAnnotation is the RFC3339 timestamp annotation the M2
// webhook sets to request an incremental re-ingest. The RepositoryReconciler
// reads this to decide whether to launch an ingest Job.
const ReingestRequestedAnnotation = "tatara.dev/reingest-requested"

// Turn-loop annotation keys, shared by the controller (agent-run state) and the
// webhook (reactivation must clear them so a fresh run starts clean).
const (
	AnnCurrentTurn   = "tatara.dev/current-turn"
	AnnTurnComplete  = "tatara.dev/turn-complete"
	AnnTurnStartedAt = "tatara.dev/turn-started-at"
	// AnnTurnLastActivity is the RFC3339 timestamp of the in-flight turn's most
	// recent agent activity (transcript stream event), read from the wrapper's
	// session/turn status and refreshed by the poll backstop. The turn-timeout
	// backstop anchors its deadline on max(turn-started-at, this) so an actively
	// streaming turn is not killed as if it were hung; it is absent until the
	// first backstop GetTurn, and consumers fall back to turn-started-at.
	AnnTurnLastActivity = "tatara.dev/turn-last-activity-at"
	AnnPodRecreations   = "tatara.dev/pod-recreations"
	// AnnReviewHeadBranch carries the PR/MR head (source) branch on a review Task
	// so its pod checks out the PR head read-only and can run/test it (issue #114
	// decision 4). The review agent never pushes (its TASK_BRANCH stays empty).
	AnnReviewHeadBranch = "tatara.dev/review-head-branch"
)

// AnnBrainstormSources is the annotation key carrying the comma-separated
// brainstorm source list stamped on brainstorm Tasks by projectscan and read by
// agent.BuildPod to gate the egress network label. Centralised here so the two
// sites cannot drift.
const AnnBrainstormSources = "tatara.dev/brainstorm-sources"

// Label keys shared between the sweep and the cron scans.
const (
	// LabelSourceKind is the activity kind ("mrScan", "issueScan", etc.).
	LabelSourceKind = "tatara.io/source-kind"
	// LabelActivity is the scan activity name.
	LabelActivity = "tatara.io/activity"
)

const (
	// AnnGrafanaAlert carries the rendered Grafana alert context on an incident Task.
	AnnGrafanaAlert = "tatara.dev/grafana-alert"
)

// Documentation-agent annotations: a documentation Task is repo-scoped to the
// DOCS repo (RepositoryRef), so the triggering component repo and its SHA
// range ride as annotations rather than Source, letting the skill shallow-
// clone the source repo and diff base..head.
const (
	AnnSourceRepo    = "tatara.dev/source-repo"
	AnnSourceBaseSHA = "tatara.dev/source-base-sha"
	AnnSourceHeadSHA = "tatara.dev/source-head-sha"
)
