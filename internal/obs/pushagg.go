package obs

import (
	"encoding/json"
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

// PushAggregator is the operator's push-receiver for short-lived pods (the
// agent wrappers). A wrapper pod can exit before Prometheus scrapes it, losing
// the run's metrics; instead each wrapper PUSHes its metrics here, keyed by a
// unique run_id, and the operator re-exposes the live (non-expired) series on
// its own /metrics for normal pull scraping.
//
// PushAggregator is both a prometheus.Collector (registered into the registry
// controller-runtime gathers, so pushed series appear on /metrics) and an
// http.Handler (the receiver: POST a run's metrics, DELETE to drop them).
//
// Staleness is operator-owned: a run's series are evicted ttl after its last
// push. A wrapper SHOULD best-effort DELETE its series on graceful exit; the
// TTL is the backstop so a hard-killed pod's series still age out.
type PushAggregator struct {
	ttl time.Duration
	// now is injectable for tests; defaults to time.Now.
	now func() time.Time

	mu   sync.Mutex
	runs map[string]*pushedRun
}

// pushedRun is one wrapper run's last-pushed snapshot. families is the parsed
// exposition keyed by metric family name. The run_id/pod/job identity labels
// are injected onto every emitted series so concurrent and successive runs
// never clobber each other.
type pushedRun struct {
	runID    string
	pod      string
	job      string
	families map[string]*dto.MetricFamily
	lastPush time.Time
}

// NewPushAggregator returns an aggregator that evicts a run's series ttl after
// its last push.
func NewPushAggregator(ttl time.Duration) *PushAggregator {
	return &PushAggregator{
		ttl:  ttl,
		now:  time.Now,
		runs: map[string]*pushedRun{},
	}
}

// pushEnvelope is the JSON body a wrapper POSTs: identity labels plus its
// metrics in Prometheus text exposition format.
type pushEnvelope struct {
	RunID   string `json:"runId"`
	Pod     string `json:"pod"`
	Job     string `json:"job"`
	Metrics string `json:"metrics"`
}

// ServeHTTP is the receiver. POST stores/updates a run's series from a
// pushEnvelope; DELETE (?runId=) drops a run's series for graceful cleanup.
func (a *PushAggregator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		a.servePush(w, r)
	case http.MethodDelete:
		runID := r.URL.Query().Get("runId")
		if runID == "" {
			http.Error(w, "runId is required", http.StatusBadRequest)
			return
		}
		a.delete(runID)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *PushAggregator) servePush(w http.ResponseWriter, r *http.Request) {
	var env pushEnvelope
	if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if env.RunID == "" {
		http.Error(w, "runId is required", http.StatusBadRequest)
		return
	}
	fams, err := parseExposition(env.Metrics)
	if err != nil {
		http.Error(w, "bad metrics: "+err.Error(), http.StatusBadRequest)
		return
	}
	a.push(env.RunID, env.Pod, env.Job, fams)
	w.WriteHeader(http.StatusNoContent)
}

func parseExposition(text string) (map[string]*dto.MetricFamily, error) {
	p := expfmt.NewTextParser(model.UTF8Validation)
	return p.TextToMetricFamilies(strings.NewReader(text))
}

// push records (or replaces) a run's snapshot and stamps its last-push time.
func (a *PushAggregator) push(runID, pod, job string, fams map[string]*dto.MetricFamily) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.runs[runID] = &pushedRun{
		runID:    runID,
		pod:      pod,
		job:      job,
		families: fams,
		lastPush: a.now(),
	}
}

// delete drops a run's series immediately (graceful-exit cleanup).
func (a *PushAggregator) delete(runID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.runs, runID)
}

// Describe leaves PushAggregator unchecked: the set of pushed series is
// dynamic, so no descriptors are declared up front.
func (a *PushAggregator) Describe(chan<- *prometheus.Desc) {}

// Collect emits every live run's series, injecting run_id/pod/job identity
// labels. Series whose run last pushed more than ttl ago are evicted here
// (lazy GC) so a hard-killed pod's series do not linger.
//
// To never break the whole /metrics scrape on a malformed or version-skewed
// pusher, Collect drops any metric whose label dimensions are inconsistent
// with the first metric seen for the same family name in this Collect pass
// (the registry rejects inconsistent dimensions for one fqName).
func (a *PushAggregator) Collect(ch chan<- prometheus.Metric) {
	a.mu.Lock()
	cutoff := a.now().Add(-a.ttl)
	for id, run := range a.runs {
		if run.lastPush.Before(cutoff) {
			delete(a.runs, id)
		}
	}
	// Snapshot live runs so metric construction happens outside the lock.
	live := make([]*pushedRun, 0, len(a.runs))
	for _, run := range a.runs {
		live = append(live, run)
	}
	a.mu.Unlock()

	descs := map[string]*prometheus.Desc{}
	dims := map[string][]string{}
	for _, run := range live {
		for name, fam := range run.families {
			for _, m := range fam.GetMetric() {
				emitMetric(ch, descs, dims, name, fam.GetHelp(), fam.GetType(), m, run)
			}
		}
	}
}

// emitMetric converts one dto.Metric into a prometheus.Metric with the run's
// identity labels injected, reusing a per-family Desc. It is a no-op when the
// metric's label dimensions disagree with the first metric seen for name.
func emitMetric(
	ch chan<- prometheus.Metric,
	descs map[string]*prometheus.Desc,
	dims map[string][]string,
	name, help string,
	typ dto.MetricType,
	m *dto.Metric,
	run *pushedRun,
) {
	names, values := labelPairs(m, run)
	if prev, ok := dims[name]; ok {
		if !sameStrings(prev, names) {
			return // inconsistent dimensions for this family; skip to protect the scrape
		}
	} else {
		dims[name] = names
		descs[name] = prometheus.NewDesc(name, help, names, nil)
	}
	desc := descs[name]

	switch typ {
	case dto.MetricType_COUNTER:
		ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, m.GetCounter().GetValue(), values...)
	case dto.MetricType_GAUGE:
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, m.GetGauge().GetValue(), values...)
	case dto.MetricType_UNTYPED:
		ch <- prometheus.MustNewConstMetric(desc, prometheus.UntypedValue, m.GetUntyped().GetValue(), values...)
	case dto.MetricType_HISTOGRAM:
		h := m.GetHistogram()
		buckets := map[float64]uint64{}
		for _, b := range h.GetBucket() {
			buckets[b.GetUpperBound()] = b.GetCumulativeCount()
		}
		ch <- prometheus.MustNewConstHistogram(desc, h.GetSampleCount(), h.GetSampleSum(), buckets, values...)
	case dto.MetricType_SUMMARY:
		s := m.GetSummary()
		quantiles := map[float64]float64{}
		for _, q := range s.GetQuantile() {
			quantiles[q.GetQuantile()] = q.GetValue()
		}
		ch <- prometheus.MustNewConstSummary(desc, s.GetSampleCount(), s.GetSampleSum(), quantiles, values...)
	}
}

// labelPairs builds sorted label name/value slices from the metric's own
// labels plus the run's run_id/pod/job identity. Identity labels overwrite any
// colliding pushed label so a metric never carries a duplicate name.
func labelPairs(m *dto.Metric, run *pushedRun) (names, values []string) {
	lbls := map[string]string{}
	for _, lp := range m.GetLabel() {
		lbls[lp.GetName()] = lp.GetValue()
	}
	lbls["run_id"] = run.runID
	lbls["pod"] = run.pod
	lbls["job"] = run.job

	names = make([]string, 0, len(lbls))
	for n := range lbls {
		names = append(names, n)
	}
	sort.Strings(names)
	values = make([]string, len(names))
	for i, n := range names {
		values[i] = lbls[n]
	}
	return names, values
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
