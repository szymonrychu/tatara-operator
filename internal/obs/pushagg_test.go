package obs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeClock is a settable time source for TTL tests.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func newTestAgg(ttl time.Duration, clk *fakeClock) *PushAggregator {
	a := NewPushAggregator(ttl)
	a.now = clk.now
	return a
}

const sampleExposition = `# HELP ccw_hook_received_total Stop-hook callbacks received.
# TYPE ccw_hook_received_total counter
ccw_hook_received_total 3
`

func postPush(t *testing.T, a *PushAggregator, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/internal/metrics/push", strings.NewReader(body))
	rec := httptest.NewRecorder()
	a.ServeHTTP(rec, req)
	return rec
}

func TestPushThenCollectExposesSeries(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	a := newTestAgg(5*time.Minute, clk)

	rec := postPush(t, a, `{"runId":"r1","pod":"p1","job":"wrapper","metrics":`+jsonString(sampleExposition)+`}`)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("push status = %d, want 204 (body=%q)", rec.Code, rec.Body.String())
	}

	want := `
# HELP ccw_hook_received_total Stop-hook callbacks received.
# TYPE ccw_hook_received_total counter
ccw_hook_received_total{job="wrapper",pod="p1",run_id="r1"} 3
`
	if err := testutil.CollectAndCompare(a, strings.NewReader(want), "ccw_hook_received_total"); err != nil {
		t.Fatalf("collect mismatch: %v", err)
	}
}

func TestConcurrentRunsKeyedByRunID(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	a := newTestAgg(5*time.Minute, clk)

	postPush(t, a, `{"runId":"r1","pod":"p1","job":"wrapper","metrics":`+jsonString(sampleExposition)+`}`)
	postPush(t, a, `{"runId":"r2","pod":"p2","job":"wrapper","metrics":`+jsonString(strings.Replace(sampleExposition, " 3", " 7", 1))+`}`)

	if n := testutil.CollectAndCount(a, "ccw_hook_received_total"); n != 2 {
		t.Fatalf("series count = %d, want 2 (one per run_id)", n)
	}
	want := `
# HELP ccw_hook_received_total Stop-hook callbacks received.
# TYPE ccw_hook_received_total counter
ccw_hook_received_total{job="wrapper",pod="p1",run_id="r1"} 3
ccw_hook_received_total{job="wrapper",pod="p2",run_id="r2"} 7
`
	if err := testutil.CollectAndCompare(a, strings.NewReader(want), "ccw_hook_received_total"); err != nil {
		t.Fatalf("collect mismatch: %v", err)
	}
}

func TestRepushReplacesSeries(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	a := newTestAgg(5*time.Minute, clk)

	postPush(t, a, `{"runId":"r1","pod":"p1","job":"wrapper","metrics":`+jsonString(sampleExposition)+`}`)
	postPush(t, a, `{"runId":"r1","pod":"p1","job":"wrapper","metrics":`+jsonString(strings.Replace(sampleExposition, " 3", " 9", 1))+`}`)

	if n := testutil.CollectAndCount(a, "ccw_hook_received_total"); n != 1 {
		t.Fatalf("series count = %d, want 1 (re-push replaces)", n)
	}
	want := `
# HELP ccw_hook_received_total Stop-hook callbacks received.
# TYPE ccw_hook_received_total counter
ccw_hook_received_total{job="wrapper",pod="p1",run_id="r1"} 9
`
	if err := testutil.CollectAndCompare(a, strings.NewReader(want), "ccw_hook_received_total"); err != nil {
		t.Fatalf("collect mismatch: %v", err)
	}
}

func TestTTLEvictsStaleSeriesAtCollect(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	a := newTestAgg(5*time.Minute, clk)

	postPush(t, a, `{"runId":"r1","pod":"p1","job":"wrapper","metrics":`+jsonString(sampleExposition)+`}`)
	if n := testutil.CollectAndCount(a, "ccw_hook_received_total"); n != 1 {
		t.Fatalf("before TTL: series count = %d, want 1", n)
	}

	// Advance past the TTL; the stale run must be evicted lazily at Collect.
	clk.t = clk.t.Add(6 * time.Minute)
	if n := testutil.CollectAndCount(a, "ccw_hook_received_total"); n != 0 {
		t.Fatalf("after TTL: series count = %d, want 0", n)
	}
}

func TestDeleteDropsSeriesImmediately(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	a := newTestAgg(5*time.Minute, clk)

	postPush(t, a, `{"runId":"r1","pod":"p1","job":"wrapper","metrics":`+jsonString(sampleExposition)+`}`)

	req := httptest.NewRequest(http.MethodDelete, "/internal/metrics/push?runId=r1", nil)
	rec := httptest.NewRecorder()
	a.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204", rec.Code)
	}
	if n := testutil.CollectAndCount(a, "ccw_hook_received_total"); n != 0 {
		t.Fatalf("after delete: series count = %d, want 0", n)
	}
}

func TestHistogramRoundTrips(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	a := newTestAgg(5*time.Minute, clk)

	hist := `# HELP ccw_turn_seconds Turn duration.
# TYPE ccw_turn_seconds histogram
ccw_turn_seconds_bucket{le="1"} 1
ccw_turn_seconds_bucket{le="5"} 2
ccw_turn_seconds_bucket{le="+Inf"} 2
ccw_turn_seconds_sum 4.5
ccw_turn_seconds_count 2
`
	postPush(t, a, `{"runId":"r1","pod":"p1","job":"wrapper","metrics":`+jsonString(hist)+`}`)

	want := `
# HELP ccw_turn_seconds Turn duration.
# TYPE ccw_turn_seconds histogram
ccw_turn_seconds_bucket{job="wrapper",pod="p1",run_id="r1",le="1"} 1
ccw_turn_seconds_bucket{job="wrapper",pod="p1",run_id="r1",le="5"} 2
ccw_turn_seconds_bucket{job="wrapper",pod="p1",run_id="r1",le="+Inf"} 2
ccw_turn_seconds_sum{job="wrapper",pod="p1",run_id="r1"} 4.5
ccw_turn_seconds_count{job="wrapper",pod="p1",run_id="r1"} 2
`
	if err := testutil.CollectAndCompare(a, strings.NewReader(want), "ccw_turn_seconds"); err != nil {
		t.Fatalf("histogram collect mismatch: %v", err)
	}
}

func TestRegistersAndScrapesViaRegistry(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	a := newTestAgg(5*time.Minute, clk)
	reg := prometheus.NewRegistry()
	reg.MustRegister(a)

	postPush(t, a, `{"runId":"r1","pod":"p1","job":"wrapper","metrics":`+jsonString(sampleExposition)+`}`)

	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var found bool
	for _, f := range fams {
		if f.GetName() == "ccw_hook_received_total" {
			found = true
		}
	}
	if !found {
		t.Fatalf("ccw_hook_received_total not present in registry gather")
	}
}

func TestPushBadRequests(t *testing.T) {
	a := NewPushAggregator(5 * time.Minute)

	// Missing runId.
	rec := postPush(t, a, `{"pod":"p1","metrics":`+jsonString(sampleExposition)+`}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing runId status = %d, want 400", rec.Code)
	}

	// Malformed JSON.
	rec = postPush(t, a, `{not json`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json status = %d, want 400", rec.Code)
	}

	// Wrong method.
	req := httptest.NewRequest(http.MethodGet, "/internal/metrics/push", nil)
	rec = httptest.NewRecorder()
	a.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405", rec.Code)
	}
}

// jsonString quotes s as a JSON string literal for embedding in test bodies.
func jsonString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
