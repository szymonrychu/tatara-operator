// Copyright 2026 tatara authors.

// Package pushmetrics implements a lightweight push-receiver for short-lived
// pods (the agent wrappers) whose lifetime is too short to be reliably
// pull-scraped. Wrappers POST their /metrics text to the operator, which keys
// each run's series by run_id, ages them out with a TTL, and re-exposes the
// live set on the operator's own /metrics for normal Prometheus scraping.
//
// This is deliberately NOT the upstream Prometheus Pushgateway: pushed series
// here expire (TTL) so a hard-killed pod's series ages out instead of lingering
// forever, which is the silent-staleness footgun the gateway has.
package pushmetrics

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// Identity label names stamped onto every pushed series so concurrent and
// successive wrapper runs never clobber each other's metrics.
const (
	labelRunID = "run_id"
	labelPod   = "pod"
	labelJob   = "job"
)

// maxBodyBytes caps a single push body to keep a misbehaving client from
// exhausting operator memory.
const maxBodyBytes = 1 << 20 // 1 MiB

// DefaultAllowedPrefixes is the closed set of metric name prefixes accepted
// from pushed series when New is called with an empty prefix list. Any family
// whose name does not start with one of the receiver's allowed prefixes is
// dropped and counted under
// operator_push_series_dropped_total{reason="reserved_name"} so it cannot
// collide with operator-owned collectors on the shared registry. The deployed
// set is configurable (PUSH_METRICS_ALLOWED_PREFIXES) so new short-lived-pod
// producers can push their own families without a code change. This fallback is
// kept in sync with the chart default (pushMetricsAllowedPrefixes): it covers
// the real names the wrapper pods push (ccw_*, tatara_wrapper_*) and the repo
// ingester pushes (ingest_*, analyzer_*, semantic_*, scip_*, llm_*), per issue
// #129; wrapper_/agent_ remain reserved for future agent series.
var DefaultAllowedPrefixes = []string{
	"wrapper_", "agent_",
	"ccw_", "tatara_wrapper_",
	"ingest_", "analyzer_", "semantic_", "scip_", "llm_",
}

// run holds one wrapper run's last pushed snapshot plus the time of that push.
type run struct {
	lastPush time.Time
	families map[string]*dto.MetricFamily
}

// Receiver aggregates pushed wrapper metrics and re-exposes them as a
// prometheus.Collector. It is safe for concurrent use. Register it on the same
// registry that backs the operator's /metrics endpoint, and mount PushHandler
// on the internal (non-ingress) listener for wrappers to push to.
type Receiver struct {
	ttl             time.Duration
	allowedPrefixes []string
	now             func() time.Time

	mu         sync.Mutex
	runs       map[string]*run
	conflicted map[string]struct{} // "metricName:runID" pairs already counted

	receiveTotal       *prometheus.CounterVec
	evictedTotal       prometheus.Counter
	seriesDroppedTotal *prometheus.CounterVec
}

// New returns a Receiver that evicts a run's series ttl after its last push and
// accepts only series whose metric name starts with one of allowedPrefixes. An
// empty allowedPrefixes falls back to DefaultAllowedPrefixes.
func New(ttl time.Duration, allowedPrefixes []string) *Receiver {
	if len(allowedPrefixes) == 0 {
		allowedPrefixes = DefaultAllowedPrefixes
	}
	r := &Receiver{
		ttl:             ttl,
		allowedPrefixes: allowedPrefixes,
		now:             time.Now,
		runs:            map[string]*run{},
		conflicted:      map[string]struct{}{},
		receiveTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_push_receive_total",
			Help: "Total wrapper metric pushes received by result.",
		}, []string{"result"}),
		evictedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "operator_pushed_runs_evicted_total",
			Help: "Total wrapper runs whose pushed series were evicted by TTL.",
		}),
		seriesDroppedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_push_series_dropped_total",
			Help: "Pushed metric series silently dropped by the receiver, by reason.",
		}, []string{"reason"}),
	}
	// Pre-init label values so all result categories appear in Gather from
	// startup, even before the first push, delete, or rejection arrives.
	// "empty" covers pushes where every family was dropped for a reserved name.
	for _, result := range []string{"accepted", "rejected", "too_large", "deleted", "empty"} {
		r.receiveTotal.WithLabelValues(result)
	}
	for _, reason := range []string{"reserved_name", "type_conflict", "build_error"} {
		r.seriesDroppedTotal.WithLabelValues(reason)
	}
	return r
}

// Describe sends nothing, which registers the Receiver as an unchecked
// collector. That is required: it emits const metrics for arbitrary
// wrapper-chosen names that cannot be declared up front, and an unchecked
// collector is allowed to emit any descriptor at Collect time.
func (r *Receiver) Describe(chan<- *prometheus.Desc) {}

// Collect evicts expired runs, then emits every live run's series with a
// per-metric-name label set padded to the union of label names seen for that
// name, so the gathered output is dimensionally consistent even when different
// runs push the same metric with different labels.
//
// Type collisions (same name, different MetricType across runs) are resolved by
// keeping the first-seen type and counting dropped conflicting series under
// operator_push_series_dropped_total{reason="type_conflict"}.
func (r *Receiver) Collect(ch chan<- prometheus.Metric) {
	r.evictExpired()

	r.mu.Lock()
	ids := make([]string, 0, len(r.runs))
	for id := range r.runs {
		ids = append(ids, id)
	}
	active := len(r.runs)
	// Sort runs by run id so the first-seen type for a name (which wins on a
	// type conflict) and the per-scrape emission order are deterministic; map
	// iteration order would otherwise flicker the surviving series scrape-to-scrape.
	sort.Strings(ids)
	runs := make([]*run, 0, len(ids))
	for _, id := range ids {
		runs = append(runs, r.runs[id])
	}
	r.mu.Unlock()

	// Per metric name: stable help text, type, and the union of label names.
	// type_conflict tracks names where two runs disagree on MetricType.
	type schema struct {
		help         string
		typ          dto.MetricType
		labels       map[string]struct{}
		typeConflict bool
	}
	schemas := map[string]*schema{}
	for _, rn := range runs {
		for name, fam := range rn.families {
			s := schemas[name]
			if s == nil {
				s = &schema{help: fam.GetHelp(), typ: fam.GetType(), labels: map[string]struct{}{}}
				schemas[name] = s
			} else if s.typ != fam.GetType() {
				s.typeConflict = true
			}
			for _, m := range fam.GetMetric() {
				for _, lp := range m.GetLabel() {
					s.labels[lp.GetName()] = struct{}{}
				}
			}
		}
	}

	// newConflicted accumulates (name:runID) keys for type-conflicting series
	// detected this Collect pass; only newly-seen keys increment the drop counter.
	newConflicted := map[string]struct{}{}
	for i, id := range ids {
		rn := runs[i]
		for name, fam := range rn.families {
			s := schemas[name]
			// Skip entire family for this run when the type disagrees with the
			// first-seen schema. Increment the drop counter only the first time
			// this (name, runID) pair is flagged so repeated Collect calls do not
			// inflate the counter for persistent long-lived conflicts.
			if s.typeConflict && s.typ != fam.GetType() {
				key := name + ":" + id
				r.mu.Lock()
				_, alreadyCounted := r.conflicted[key]
				if !alreadyCounted {
					r.conflicted[key] = struct{}{}
				}
				r.mu.Unlock()
				if !alreadyCounted {
					r.seriesDroppedTotal.WithLabelValues("type_conflict").Add(float64(len(fam.GetMetric())))
				}
				newConflicted[key] = struct{}{}
				continue
			}
			labelNames := sortedKeys(s.labels)
			for _, m := range fam.GetMetric() {
				metric := constMetric(name, s.help, s.typ, labelNames, m)
				if metric == nil {
					// build_error: also count only once per (name, runID) to avoid
					// per-scrape inflation. Use a distinct key prefix.
					key := "build:" + name + ":" + id
					r.mu.Lock()
					_, alreadyCounted := r.conflicted[key]
					if !alreadyCounted {
						r.conflicted[key] = struct{}{}
					}
					r.mu.Unlock()
					if !alreadyCounted {
						r.seriesDroppedTotal.WithLabelValues("build_error").Inc()
					}
					newConflicted[key] = struct{}{}
					continue
				}
				ch <- metric
			}
		}
	}
	// Prune conflicted keys that are no longer in the active run set so the set
	// does not grow without bound when runs come and go.
	r.mu.Lock()
	for key := range r.conflicted {
		if _, live := newConflicted[key]; !live {
			delete(r.conflicted, key)
		}
	}
	r.mu.Unlock()

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("operator_pushed_runs", "Wrapper runs with live pushed series.", nil, nil),
		prometheus.GaugeValue, float64(active),
	)
	r.receiveTotal.Collect(ch)
	r.evictedTotal.Collect(ch)
	r.seriesDroppedTotal.Collect(ch)
}

// constMetric converts one parsed dto.Metric into a prometheus.Metric using the
// given union label-name ordering, padding absent labels with "". It returns
// nil (skipping the metric) if construction fails rather than poisoning the
// whole scrape.
func constMetric(name, help string, typ dto.MetricType, labelNames []string, m *dto.Metric) prometheus.Metric {
	values := make([]string, len(labelNames))
	present := map[string]string{}
	for _, lp := range m.GetLabel() {
		present[lp.GetName()] = lp.GetValue()
	}
	for i, n := range labelNames {
		values[i] = present[n]
	}
	desc := prometheus.NewDesc(name, help, labelNames, nil)

	var (
		metric prometheus.Metric
		err    error
	)
	switch typ {
	case dto.MetricType_COUNTER:
		metric, err = prometheus.NewConstMetric(desc, prometheus.CounterValue, m.GetCounter().GetValue(), values...)
	case dto.MetricType_GAUGE:
		metric, err = prometheus.NewConstMetric(desc, prometheus.GaugeValue, m.GetGauge().GetValue(), values...)
	case dto.MetricType_UNTYPED:
		metric, err = prometheus.NewConstMetric(desc, prometheus.UntypedValue, m.GetUntyped().GetValue(), values...)
	case dto.MetricType_HISTOGRAM:
		h := m.GetHistogram()
		buckets := map[float64]uint64{}
		for _, b := range h.GetBucket() {
			buckets[b.GetUpperBound()] = b.GetCumulativeCount()
		}
		metric, err = prometheus.NewConstHistogram(desc, h.GetSampleCount(), h.GetSampleSum(), buckets, values...)
	case dto.MetricType_SUMMARY:
		sm := m.GetSummary()
		quantiles := map[float64]float64{}
		for _, q := range sm.GetQuantile() {
			quantiles[q.GetQuantile()] = q.GetValue()
		}
		metric, err = prometheus.NewConstSummary(desc, sm.GetSampleCount(), sm.GetSampleSum(), quantiles, values...)
	default:
		return nil
	}
	if err != nil {
		return nil
	}
	return metric
}

// evictExpired drops runs whose last push is older than the TTL and counts them.
func (r *Receiver) evictExpired() {
	cutoff := r.now().Add(-r.ttl)
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, rn := range r.runs {
		if rn.lastPush.Before(cutoff) {
			delete(r.runs, id)
			r.evictedTotal.Inc()
		}
	}
}

// PushHandler returns the push endpoint as a plain http.HandlerFunc so callers
// can mount it directly on any mux without the extra inner-mux dispatch layer
// that Handler() added (KISS, finding 20). The outer mux owns the path routing.
func (r *Receiver) PushHandler() http.Handler {
	return http.HandlerFunc(r.handlePush)
}

// Handler returns an http.ServeMux with a single route /internal/metrics/push.
// Prefer PushHandler() when mounting on an existing mux.
//
// Deprecated: use PushHandler() and mount at the desired path on the outer mux.
func (r *Receiver) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/metrics/push", r.handlePush)
	return mux
}

// isMaxBytesError reports whether err signals that http.MaxBytesReader hit its
// limit. Uses errors.As against *http.MaxBytesError (Go 1.21+).
func isMaxBytesError(err error) bool {
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}

func (r *Receiver) handlePush(w http.ResponseWriter, req *http.Request) {
	runID := req.URL.Query().Get(labelRunID)
	if runID == "" {
		r.receiveTotal.WithLabelValues("rejected").Inc()
		http.Error(w, "run_id is required", http.StatusBadRequest)
		return
	}

	switch req.Method {
	case http.MethodDelete:
		r.mu.Lock()
		delete(r.runs, runID)
		r.mu.Unlock()
		r.receiveTotal.WithLabelValues("deleted").Inc()
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPost:
		identity := map[string]string{labelRunID: runID}
		if pod := req.URL.Query().Get(labelPod); pod != "" {
			identity[labelPod] = pod
		}
		if job := req.URL.Query().Get(labelJob); job != "" {
			identity[labelJob] = job
		}
		req.Body = http.MaxBytesReader(w, req.Body, maxBodyBytes)
		families, err := r.parseAndStamp(req.Body, identity)
		if err != nil {
			// http.MaxBytesReader sets the response status to 413 via
			// (*maxBytesReader).Read when the limit is exceeded; we mirror
			// that here for the metric label.
			result := "rejected"
			status := http.StatusBadRequest
			if isMaxBytesError(err) {
				result = "too_large"
				status = http.StatusRequestEntityTooLarge
			}
			r.receiveTotal.WithLabelValues(result).Inc()
			http.Error(w, fmt.Sprintf("parse metrics: %v", err), status)
			return
		}
		// Finding 27: if all families were dropped (e.g. every name used a
		// reserved prefix), do not store an empty run slot and count it as
		// "empty" so a misconfigured wrapper is visible instead of appearing
		// accepted with zero contributing series.
		if len(families) == 0 {
			r.receiveTotal.WithLabelValues("empty").Inc()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		r.mu.Lock()
		r.runs[runID] = &run{lastPush: r.now(), families: families}
		r.mu.Unlock()
		r.receiveTotal.WithLabelValues("accepted").Inc()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// parseAndStamp parses Prometheus text-format metrics, enforces the allowed
// name prefix set, and overwrites the identity labels on every series so a run
// can never spoof another's identity. Families whose names do not start with an
// allowed prefix are dropped and counted under
// operator_push_series_dropped_total{reason="reserved_name"}.
func (r *Receiver) parseAndStamp(body io.Reader, identity map[string]string) (map[string]*dto.MetricFamily, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(body)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*dto.MetricFamily, len(families))
	var dropped []string
	for name, fam := range families {
		if !r.hasAllowedPrefix(name) {
			r.seriesDroppedTotal.WithLabelValues("reserved_name").Add(float64(len(fam.GetMetric())))
			dropped = append(dropped, name)
			continue
		}
		for _, m := range fam.GetMetric() {
			m.Label = stampLabels(m.GetLabel(), identity)
		}
		out[name] = fam
	}
	// Issue #129: a silent drop here means a pushed metric family never reaches
	// Prometheus. The counter feeds TataraPushMetricsDropped; this WARN (once per
	// push) names the offending families so an operator can fix the allowlist
	// without reproducing the push.
	if len(dropped) > 0 {
		sort.Strings(dropped)
		slog.Warn("dropped pushed metric families: name prefix not in allowlist",
			slog.String("action", "push_series_dropped"),
			slog.String("reason", "reserved_name"),
			slog.String("run_id", identity[labelRunID]),
			slog.Int("dropped_families", len(dropped)),
			slog.String("families", strings.Join(dropped, ",")),
			slog.String("allowed_prefixes", strings.Join(r.allowedPrefixes, ",")),
		)
	}
	return out, nil
}

// hasAllowedPrefix reports whether name starts with one of the receiver's
// configured allowed prefixes.
func (r *Receiver) hasAllowedPrefix(name string) bool {
	for _, p := range r.allowedPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// stampLabels returns the label pairs with the identity labels forced to the
// given values (existing copies of those names are dropped first).
func stampLabels(existing []*dto.LabelPair, identity map[string]string) []*dto.LabelPair {
	out := make([]*dto.LabelPair, 0, len(existing)+len(identity))
	for _, lp := range existing {
		if _, isIdentity := identity[lp.GetName()]; isIdentity {
			continue
		}
		out = append(out, lp)
	}
	for name, value := range identity {
		n, v := name, value
		out = append(out, &dto.LabelPair{Name: &n, Value: &v})
	}
	return out
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
