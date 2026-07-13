package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// scmRateLimitedTotal counts SCM requests the forge rejected with a rate-limit
// response (contract C.8). limit_type distinguishes GitHub's PRIMARY limit (429,
// or 403 with X-RateLimit-Remaining: 0) from its SECONDARY limit (403 with a
// "secondary rate limit" marker in the body and no X-RateLimit-Remaining header)
// - they have different budgets, different reset behaviour, and different fixes.
//
// It registers itself on the controller-runtime registry (the one cmd/manager
// serves /metrics from) rather than hanging off OperatorMetrics, because the
// internal/scm package emits it from deep inside the HTTP paths (doPagedGET,
// ghDo, glDo) where no *OperatorMetrics is threaded through - and threading one
// through every SCM call site to count a rejection would be a large change for
// no gain.
var scmRateLimitedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_scm_ratelimited_total",
	Help: "SCM requests rejected by the forge rate limiter, by provider, normalized API path, and limit type (primary|secondary).",
}, []string{"provider", "path", "limit_type"})

func init() {
	ctrlmetrics.Registry.MustRegister(scmRateLimitedTotal)
}

// SCMRateLimited increments operator_scm_ratelimited_total for one rate-limited
// SCM request. path must already be normalized to a bounded route template
// (internal/scm collapses ids, shas and owner/repo segments before calling).
func SCMRateLimited(provider, path, limitType string) {
	scmRateLimitedTotal.WithLabelValues(provider, path, limitType).Inc()
}

// SCMRateLimitedCounter returns the counter for (provider, path, limit_type) for
// test assertions.
func SCMRateLimitedCounter(provider, path, limitType string) prometheus.Counter {
	return scmRateLimitedTotal.WithLabelValues(provider, path, limitType)
}
