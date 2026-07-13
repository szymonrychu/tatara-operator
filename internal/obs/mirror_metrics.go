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

func init() {
	ctrlmetrics.Registry.MustRegister(MirrorSyncTotal, MirrorCommentTruncatedTotal)
}
