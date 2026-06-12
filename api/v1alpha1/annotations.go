package v1alpha1

// ReingestRequestedAnnotation is the RFC3339 timestamp annotation the M2
// webhook sets to request an incremental re-ingest. The RepositoryReconciler
// reads this to decide whether to launch an ingest Job.
const ReingestRequestedAnnotation = "tatara.dev/reingest-requested"

// LifecycleEntryAnnotation carries the entry LifecycleState for a newly
// created issueLifecycle Task. Set atomically at Task create time by the
// webhook binder and mrScan so the state is always present even if the
// first Status().Update is lost. Values: "Triage" (default), "Implement",
// "MRCI". reconcileLifecycle reads this on the first reconcile (when
// Status.LifecycleState == "").
const LifecycleEntryAnnotation = "tatara.dev/lifecycle-entry"

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
)
