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
