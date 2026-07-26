package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// MirrorSyncTotal counts Issue/MergeRequest mirror syncs by outcome. The mirror
// IS the read path (contract C.1: scm_read(issues|comments|mr) is served from it
// and from nothing else), so a mirror that stops converging is an agent that
// silently reads a frozen forge. result is "ok" or "error".
var MirrorSyncTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_mirror_sync_total",
	Help: "Issue/MergeRequest mirror syncs, by kind and result (contract B.4).",
}, []string{"kind", "result"})

// MirrorCommentTruncatedTotal counts comment bodies cut at the 8192-byte ingest
// limit (contract A.1, fix E3). A rising rate means agents are reading partial
// threads; the bundle marks them truncated="true" so this is visible, not silent.
var MirrorCommentTruncatedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_mirror_comment_truncated_total",
	Help: "Comment bodies truncated at the 8192-byte ingest limit, by kind (contract A.1).",
}, []string{"kind"})

// MirrorWriteDroppedTotal counts best-effort mirror/webhook writes dropped on
// error at a specific call site. These are the objbudget.Fit* writes that a
// webhook or REST handler makes on the CONTINUE-ON-ERROR path: the caller
// still returns 200 to the forge on purpose (a webhook must never fail, and a
// REST accept must not unwind an already-committed stage transition), so the
// write loss was previously visible only as a log line - or, at two sites, not
// even that. The most common root cause is a tatara-memory outage: when the
// objbudget byte-budget guard cannot spill, every write through objbudget.Fit*
// fails by design (that blocking policy is deliberate, see MEMORY.md), so a
// memory outage shows up here as a burst across many (project, kind, site)
// series rather than as any single alert. site identifies the call site so a
// rising rate can be traced back to which drop (and which downstream
// consequence - a suppressed event, a stale cursor, a lost comment) it is.
var MirrorWriteDroppedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_mirror_write_dropped_total",
	Help: "Best-effort mirror/webhook writes dropped on error, by project, kind and call site.",
}, []string{"project", "kind", "site"})

func init() {
	ctrlmetrics.Registry.MustRegister(MirrorSyncTotal, MirrorCommentTruncatedTotal, MirrorWriteDroppedTotal)
}
