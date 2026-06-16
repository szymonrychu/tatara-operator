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
	AnnPodRecreations = "tatara.dev/pod-recreations"
	// AnnPlanningSince is the RFC3339 timestamp stamped when a Task first enters
	// the Planning phase on a spawn. The spawn watchdog uses it to fail a Task
	// wedged in Planning that never acquires an in-flight turn (e.g. a duplicate
	// lifecycle Task whose pod name collided with the live one), which is
	// otherwise invisible to the turn-timeout backstop.
	AnnPlanningSince = "tatara.dev/planning-since"
)

// LifecycleEntryAnnotation carries the entry LifecycleState for a newly
// created issueLifecycle Task. Set atomically at Task create time by the
// webhook binder and mrScan so the state is always present even if the
// first Status().Update is lost. Values: "Triage" (default), "Implement",
// "MRCI". reconcileLifecycle reads this on the first reconcile (when
// Status.LifecycleState == "").
const LifecycleEntryAnnotation = "tatara.dev/lifecycle-entry"

// AnnBrainstormSources is the annotation key carrying the comma-separated
// brainstorm source list stamped on brainstorm Tasks by projectscan and read by
// agent.BuildPod to gate the egress network label. Centralised here so the two
// sites cannot drift.
const AnnBrainstormSources = "tatara.dev/brainstorm-sources"

// Label keys shared between the webhook binder and cron mrScan/issueScan so
// their dedup keys are consistent.
const (
	// LabelSourceRepo is the sanitized "owner.repo" slug that identifies which
	// repository a scan/webhook Task was created for.
	LabelSourceRepo = "tatara.io/source-repo"
	// LabelSourceNumber is the dedup key number (issue number when "Closes #N"
	// is present in a bot-PR body, else PR/issue number).
	LabelSourceNumber = "tatara.io/source-number"
	// LabelSourceKind is the activity kind ("mrScan", "issueScan", etc.).
	LabelSourceKind = "tatara.io/source-kind"
	// LabelHeadSHA is the PR head SHA, set on PR tasks for revision-level dedup.
	LabelHeadSHA = "tatara.io/head-sha"
	// LabelActivity is the scan activity name.
	LabelActivity = "tatara.io/activity"
	// LabelIsPR disambiguates a GitHub PR task from an issue task with the same
	// number. Values: "true" | "false". Set by the webhook binder on every
	// issueLifecycle Task; tasks without this label are treated as issues
	// (backward-compatible default). The label is NOT set on scan-created tasks
	// (they predate this label); its absence is interpreted as "false" (issue).
	LabelIsPR = "tatara.io/is-pr"
)
