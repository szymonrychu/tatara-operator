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
