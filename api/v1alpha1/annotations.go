package v1alpha1

// ReingestRequestedAnnotation is the RFC3339 timestamp annotation the M2
// webhook sets to request an incremental re-ingest. The RepositoryReconciler
// reads this to decide whether to launch an ingest Job.
const ReingestRequestedAnnotation = "tatara.dev/reingest-requested"

// Turn-loop annotation keys, shared by the controller (agent-run state) and the
// webhook (reactivation must clear them so a fresh run starts clean).
const (
	AnnCurrentTurn    = "tatara.dev/current-turn"
	AnnCurrentSubtask = "tatara.dev/current-subtask"
	AnnTurnComplete   = "tatara.dev/turn-complete"
	AnnTurnStartedAt  = "tatara.dev/turn-started-at"
	// AnnTurnLastActivity is the RFC3339 timestamp of the in-flight turn's most
	// recent agent activity (transcript stream event), read from the wrapper's
	// session/turn status and refreshed by the poll backstop. The turn-timeout
	// backstop anchors its deadline on max(turn-started-at, this) so an actively
	// streaming turn is not killed as if it were hung; it is absent until the
	// first backstop GetTurn, and consumers fall back to turn-started-at.
	AnnTurnLastActivity = "tatara.dev/turn-last-activity-at"
	AnnPodRecreations   = "tatara.dev/pod-recreations"
	// AnnPlanningSince is the RFC3339 timestamp stamped when a Task first enters
	// the Planning phase on a spawn. The spawn watchdog uses it to fail a Task
	// wedged in Planning that never acquires an in-flight turn (e.g. a duplicate
	// lifecycle Task whose pod name collided with the live one), which is
	// otherwise invisible to the turn-timeout backstop.
	AnnPlanningSince = "tatara.dev/planning-since"
	// AnnPendingHandoverResume, when "true", means the next Implement spawn should
	// resume from the compacted text Handover rather than a full conversation
	// replay (issue #114 decision 2: past HandoverThresholdPercent of the context
	// window, compact instead of full resume). Set by maybeMarkHandoverResume and
	// read by both implementPrompt (inject the handover block) and agent.BuildPod
	// (skip CONVERSATION_SESSION_ID so the two paths are mutually exclusive).
	AnnPendingHandoverResume = "tatara.dev/pending-handover-resume"
	// AnnParentConversationKey is stamped on a brainstorm proposal Task with the
	// S3 conversation key of the brainstorm that produced it (issue #114 decision
	// 3). AnnForkFromConversationKey is stamped on the resulting issueLifecycle
	// Task (correlated by repo+issue number) and injected into its first pod as
	// CONVERSATION_FORK_FROM_KEY so the wrapper forks (copies) the brainstorm
	// conversation onto the issue's own key.
	AnnParentConversationKey   = "tatara.dev/parent-conversation-key"
	AnnForkFromConversationKey = "tatara.dev/fork-from-conversation-key"
	// AnnReviewHeadBranch carries the PR/MR head (source) branch on a review Task
	// so its pod checks out the PR head read-only and can run/test it (issue #114
	// decision 4). The review agent never pushes (its TASK_BRANCH stays empty).
	AnnReviewHeadBranch = "tatara.dev/review-head-branch"
)

// LifecycleEntryAnnotation carries the entry DeployState for a newly
// created issueLifecycle Task. Set atomically at Task create time by the
// webhook binder and mrScan so the state is always present even if the
// first Status().Update is lost. Values: "Triage" (default), "Implement",
// "MRCI". reconcileLifecycle reads this on the first reconcile (when
// Status.DeployState == "").
const LifecycleEntryAnnotation = "tatara.dev/lifecycle-entry"

// AnnBrainstormSources is the annotation key carrying the comma-separated
// brainstorm source list stamped on brainstorm Tasks by projectscan and read by
// agent.BuildPod to gate the egress network label. Centralised here so the two
// sites cannot drift.
const AnnBrainstormSources = "tatara.dev/brainstorm-sources"

// Label keys shared between the webhook binder and cron mrScan/issueScan so
// their dedup keys are consistent.
const (
	// LabelSourceKind is the activity kind ("mrScan", "issueScan", etc.).
	LabelSourceKind = "tatara.io/source-kind"
	// LabelActivity is the scan activity name.
	LabelActivity = "tatara.io/activity"
	// LabelIsPR disambiguates a GitHub PR task from an issue task with the same
	// number. Values: "true" | "false". Set by the webhook binder on every
	// issueLifecycle Task; tasks without this label are treated as issues
	// (backward-compatible default). The label is NOT set on scan-created tasks
	// (they predate this label); its absence is interpreted as "false" (issue).
	LabelIsPR = "tatara.io/is-pr"
	// LabelAlertGroup is the per-alert-group dedup key on an incident Task.
	LabelAlertGroup = "tatara.dev/alert-group"
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
