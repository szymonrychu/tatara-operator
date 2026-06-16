// Copyright 2026 tatara authors.

package pushmetrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

// fakeClock is a manually advanced clock for deterministic TTL tests.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestReceiver(clk *fakeClock, ttl time.Duration) *Receiver {
	r := New(ttl)
	r.now = clk.now
	return r
}

func push(t *testing.T, r *Receiver, query, body string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/internal/metrics/push?"+query, strings.NewReader(body))
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, req)
	return rec.Code
}

func TestPushRejectsMissingRunID(t *testing.T) {
	r := New(time.Minute)
	if code := push(t, r, "", "a_total 1\n"); code != http.StatusBadRequest {
		t.Fatalf("missing run_id: got %d, want 400", code)
	}
}

func TestPushRejectsBadBody(t *testing.T) {
	r := New(time.Minute)
	if code := push(t, r, "run_id=x", "this is not metrics text {{{"); code != http.StatusBadRequest {
		t.Fatalf("bad body: got %d, want 400", code)
	}
}

func TestPushStampsIdentityAndReExposes(t *testing.T) {
	r := New(time.Minute)
	body := "# TYPE wrapper_runs_total counter\nwrapper_runs_total{step=\"plan\"} 3\n"
	if code := push(t, r, "run_id=run-1&pod=pod-a&job=wrapper", body); code != http.StatusNoContent {
		t.Fatalf("push: got %d, want 204", code)
	}
	want := `
# HELP wrapper_runs_total
# TYPE wrapper_runs_total counter
wrapper_runs_total{job="wrapper",pod="pod-a",run_id="run-1",step="plan"} 3
`
	if err := testutil.CollectAndCompare(r, strings.NewReader(want), "wrapper_runs_total"); err != nil {
		t.Fatal(err)
	}
}

// A wrapper must not be able to spoof another run's identity: an inbound
// run_id label in the body is overwritten by the query parameter.
func TestPushOverwritesSpoofedIdentity(t *testing.T) {
	r := New(time.Minute)
	body := "# TYPE a_total counter\na_total{run_id=\"evil\"} 1\n"
	push(t, r, "run_id=real", body)
	want := `
# HELP a_total
# TYPE a_total counter
a_total{run_id="real"} 1
`
	if err := testutil.CollectAndCompare(r, strings.NewReader(want), "a_total"); err != nil {
		t.Fatal(err)
	}
}

// Two runs pushing the same metric name with different label sets must gather
// cleanly (union-padded), not error out the whole scrape.
func TestConcurrentRunsDifferentLabelsGatherCleanly(t *testing.T) {
	r := New(time.Minute)
	push(t, r, "run_id=run-1", "# TYPE q_total counter\nq_total{a=\"1\"} 1\n")
	push(t, r, "run_id=run-2", "# TYPE q_total counter\nq_total{b=\"2\"} 2\n")

	reg := prometheus.NewRegistry()
	reg.MustRegister(r)
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("gather with inconsistent label sets: %v", err)
	}
	if got := testutil.CollectAndCount(r, "q_total"); got != 2 {
		t.Fatalf("q_total series: got %d, want 2", got)
	}
}

func TestHistogramRoundTrips(t *testing.T) {
	r := New(time.Minute)
	body := `# TYPE lat histogram
lat_bucket{le="0.5"} 1
lat_bucket{le="1"} 2
lat_bucket{le="+Inf"} 2
lat_sum 1.3
lat_count 2
`
	if code := push(t, r, "run_id=h1", body); code != http.StatusNoContent {
		t.Fatalf("push histogram: got %d", code)
	}
	if got := testutil.CollectAndCount(r, "lat"); got != 1 {
		t.Fatalf("histogram series: got %d, want 1", got)
	}
}

func TestTTLEviction(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	r := newTestReceiver(clk, time.Minute)
	push(t, r, "run_id=run-1", "# TYPE a_total counter\na_total 1\n")

	if got := testutil.CollectAndCount(r, "a_total"); got != 1 {
		t.Fatalf("before TTL: got %d, want 1", got)
	}
	clk.advance(2 * time.Minute)
	if got := testutil.CollectAndCount(r, "a_total"); got != 0 {
		t.Fatalf("after TTL: got %d, want 0", got)
	}
	if got := testutil.ToFloat64(r.evictedTotal); got != 1 {
		t.Fatalf("evicted total: got %v, want 1", got)
	}
}

// A fresh push resets the TTL so an active run is never evicted.
func TestPushResetsTTL(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	r := newTestReceiver(clk, time.Minute)
	push(t, r, "run_id=run-1", "# TYPE a_total counter\na_total 1\n")
	clk.advance(40 * time.Second)
	push(t, r, "run_id=run-1", "# TYPE a_total counter\na_total 2\n")
	clk.advance(40 * time.Second)
	if got := testutil.CollectAndCount(r, "a_total"); got != 1 {
		t.Fatalf("active run wrongly evicted: got %d, want 1", got)
	}
}

func TestDeleteRemovesSeries(t *testing.T) {
	r := New(time.Minute)
	push(t, r, "run_id=run-1", "# TYPE a_total counter\na_total 1\n")

	req := httptest.NewRequest(http.MethodDelete, "/internal/metrics/push?run_id=run-1", nil)
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete: got %d, want 204", rec.Code)
	}
	if got := testutil.CollectAndCount(r, "a_total"); got != 0 {
		t.Fatalf("after delete: got %d, want 0", got)
	}
}

func TestActiveRunsGauge(t *testing.T) {
	r := New(time.Minute)
	push(t, r, "run_id=run-1", "# TYPE a_total counter\na_total 1\n")
	push(t, r, "run_id=run-2", "# TYPE a_total counter\na_total 1\n")
	want := "# HELP operator_pushed_runs Wrapper runs with live pushed series.\n# TYPE operator_pushed_runs gauge\noperator_pushed_runs 2\n"
	if err := testutil.CollectAndCompare(r, strings.NewReader(want), "operator_pushed_runs"); err != nil {
		t.Fatal(err)
	}
}

func TestRegistersOnSharedRegistryWithoutConflict(t *testing.T) {
	r := New(time.Minute)
	reg := prometheus.NewRegistry()
	if err := reg.Register(r); err != nil {
		t.Fatalf("register receiver: %v", err)
	}
	push(t, r, "run_id=run-1", "# TYPE wrapper_x_total counter\nwrapper_x_total 5\n")
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("gather: %v", err)
	}
}

// Finding 20: PushHandler returns an http.HandlerFunc, not a nested mux.
// Mounting it directly on the outer mux should serve the push endpoint.
func TestPushHandler_ServesDirectly(t *testing.T) {
	r := New(time.Minute)
	h := r.PushHandler()
	// h should handle a direct POST (no inner-mux path dispatch).
	req := httptest.NewRequest(http.MethodPost, "/internal/metrics/push?run_id=x",
		strings.NewReader("# TYPE a_total counter\na_total 1\n"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("PushHandler direct: got %d, want 204", rec.Code)
	}
}

// Finding 21: two runs pushing the same metric name with different types;
// the conflicting run's series must be dropped and counted.
func TestTypeConflict_DropsConflictingSeries(t *testing.T) {
	r := New(time.Minute)
	// run-1 publishes m as COUNTER
	push(t, r, "run_id=run-1", "# TYPE m counter\nm 1\n")
	// run-2 publishes m as GAUGE (type conflict)
	push(t, r, "run_id=run-2", "# TYPE m gauge\nm 2\n")

	reg := prometheus.NewRegistry()
	reg.MustRegister(r)
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("gather with type conflict: %v", err)
	}
	// Conflicting run-2 series should be dropped and counted.
	dropped := testutil.ToFloat64(r.seriesDroppedTotal.WithLabelValues("type_conflict"))
	if dropped < 1 {
		t.Fatalf("type_conflict dropped = %v, want >= 1", dropped)
	}
}

// TestTypeConflict_StableSurvivingSeries asserts that on a metric-name type
// conflict the SAME run's series survives across scrapes (residual #3). The
// first-seen type was previously decided by nondeterministic map iteration, so
// the surviving series flickered scrape-to-scrape. Collect must order runs
// deterministically by run key.
func TestTypeConflict_StableSurvivingSeries(t *testing.T) {
	r := New(time.Minute)
	// run-a publishes m as COUNTER value 1; run-z as GAUGE value 2. With a stable
	// (sorted) iteration order, run-a is first-seen, so its COUNTER value 1 wins
	// every scrape.
	push(t, r, "run_id=run-a", "# TYPE m counter\nm 1\n")
	push(t, r, "run_id=run-z", "# TYPE m gauge\nm 2\n")

	survivingM := func() float64 {
		ch := make(chan prometheus.Metric, 64)
		go func() { r.Collect(ch); close(ch) }()
		var vals []float64
		for metric := range ch {
			var pb dto.Metric
			if err := metric.Write(&pb); err != nil {
				continue
			}
			if !strings.HasPrefix(metric.Desc().String(), `Desc{fqName: "m"`) {
				continue
			}
			switch {
			case pb.Counter != nil:
				vals = append(vals, pb.Counter.GetValue())
			case pb.Gauge != nil:
				vals = append(vals, pb.Gauge.GetValue())
			}
		}
		if len(vals) != 1 {
			t.Fatalf("expected exactly 1 surviving 'm' series, got %d (%v)", len(vals), vals)
		}
		return vals[0]
	}

	first := survivingM()
	for i := 0; i < 20; i++ {
		if got := survivingM(); got != first {
			t.Fatalf("surviving 'm' series flickered: scrape %d = %v, first = %v", i, got, first)
		}
	}
}

// Finding 22: body larger than maxBodyBytes must be rejected with 413.
func TestOversizeBody_Rejected(t *testing.T) {
	r := New(time.Minute)
	// Build a body larger than maxBodyBytes (1 MiB).
	big := strings.Repeat("x", maxBodyBytes+1)
	req := httptest.NewRequest(http.MethodPost, "/internal/metrics/push?run_id=big",
		strings.NewReader(big))
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize body: got %d, want 413", rec.Code)
	}
	// must be counted as too_large not rejected
	if got := testutil.ToFloat64(r.receiveTotal.WithLabelValues("too_large")); got != 1 {
		t.Fatalf("receive_total{too_large} = %v, want 1", got)
	}
}
