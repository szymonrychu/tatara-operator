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
	"net/http"
	"sort"
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
	ttl time.Duration
	now func() time.Time

	mu   sync.Mutex
	runs map[string]*run

	receiveTotal       *prometheus.CounterVec
	evictedTotal       prometheus.Counter
	seriesDroppedTotal *prometheus.CounterVec
}

// New returns a Receiver that evicts a run's series ttl after its last push.
func New(ttl time.Duration) *Receiver {
	return &Receiver{
		ttl:  ttl,
		now:  time.Now,
		runs: map[string]*run{},
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
	runs := make([]*run, 0, len(r.runs))
	for _, rn := range r.runs {
		runs = append(runs, rn)
	}
	active := len(r.runs)
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

	for _, rn := range runs {
		for name, fam := range rn.families {
			s := schemas[name]
			// Skip entire family for this run when the type disagrees with the
			// first-seen schema, counting each dropped series.
			if s.typeConflict && s.typ != fam.GetType() {
				r.seriesDroppedTotal.WithLabelValues("type_conflict").Add(float64(len(fam.GetMetric())))
				continue
			}
			labelNames := sortedKeys(s.labels)
			for _, m := range fam.GetMetric() {
				metric := constMetric(name, s.help, s.typ, labelNames, m)
				if metric == nil {
					r.seriesDroppedTotal.WithLabelValues("build_error").Inc()
					continue
				}
				ch <- metric
			}
		}
	}

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
		families, err := parseAndStamp(req.Body, identity)
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
		r.mu.Lock()
		r.runs[runID] = &run{lastPush: r.now(), families: families}
		r.mu.Unlock()
		r.receiveTotal.WithLabelValues("accepted").Inc()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// parseAndStamp parses Prometheus text-format metrics and overwrites the
// identity labels on every series so a run can never spoof another's identity.
func parseAndStamp(body io.Reader, identity map[string]string) (map[string]*dto.MetricFamily, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(body)
	if err != nil {
		return nil, err
	}
	for _, fam := range families {
		for _, m := range fam.GetMetric() {
			m.Label = stampLabels(m.GetLabel(), identity)
		}
	}
	return families, nil
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
