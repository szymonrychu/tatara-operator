package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// TestProbeToolSurfaceRoute_Classification verifies the tool-surface classifier:
// a 2xx is "ok", a 401/403/other-4xx is "present" (route + process served, no
// token to drive a 2xx), a 404 is "absent" (route gone), and a 5xx is "error".
func TestProbeToolSurfaceRoute_Classification(t *testing.T) {
	cases := []struct {
		name   string
		status int
		want   string
	}{
		{"ok-200", http.StatusOK, "ok"},
		{"created-201", http.StatusCreated, "ok"},
		{"unauthorized-401", http.StatusUnauthorized, "present"},
		{"forbidden-403", http.StatusForbidden, "present"},
		{"bad-request-400", http.StatusBadRequest, "present"},
		{"method-not-allowed-405", http.StatusMethodNotAllowed, "present"},
		{"not-found-404", http.StatusNotFound, "absent"},
		{"server-error-500", http.StatusInternalServerError, "error"},
		{"unavailable-503", http.StatusServiceUnavailable, "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()
			got, err := probeToolSurfaceRoute(context.Background(), srv.Client(), http.MethodGet, srv.URL+"/projects")
			if got != tc.want {
				t.Fatalf("status %d classified %q, want %q", tc.status, got, tc.want)
			}
			if err != nil {
				t.Fatalf("status %d returned non-nil error %v (HTTP responses classify without error)", tc.status, err)
			}
		})
	}
}

func TestProbeToolSurfaceRoute_TransportFailureIsUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	client := srv.Client()
	srv.Close()
	got, err := probeToolSurfaceRoute(context.Background(), client, http.MethodGet, url+"/readyz")
	if got != "unreachable" {
		t.Fatalf("transport failure classified %q, want unreachable", got)
	}
	if err == nil {
		t.Fatal("transport failure returned nil error, want the dial error for the log line")
	}
}

// gatherToolSurfaceCounter reads
// operator_tool_surface_probe_total{backend,vantage="in-cluster",result}.
func gatherToolSurfaceCounter(t *testing.T, reg *prometheus.Registry, backend, result string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_tool_surface_probe_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var b, res string
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "backend":
					b = lp.GetValue()
				case "result":
					res = lp.GetValue()
				}
			}
			if b == backend && res == result {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// TestUpdateToolSurfaceProbe_DebouncesTransientUnreachable verifies a failing
// backend is not metered until it has failed toolSurfaceUnhealthyThreshold cycles
// in a row: a transient transport failure (the operator's own rollout churn
// dialing a still-serving chat, issue #253) must not trip the `> 0` alert, while a
// sustained outage meters from the threshold cycle on.
func TestUpdateToolSurfaceProbe_DebouncesTransientUnreachable(t *testing.T) {
	ctx := logfIntoTestCtx()
	r, reg := newMemoryReconcilerWithReg()

	// A closed server yields a transport failure -> "unreachable", the incident's
	// class.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	r.OperatorURL = srv.URL
	r.ToolSurfaceHTTP = srv.Client()
	srv.Close()

	// Below the threshold: transient unreachable is logged but not metered.
	for i := 1; i < toolSurfaceUnhealthyThreshold; i++ {
		r.updateToolSurfaceProbe(ctx)
		if got := gatherToolSurfaceCounter(t, reg, "operator", "unreachable"); got != 0 {
			t.Fatalf("after %d transient cycle(s) probe{operator,unreachable} = %v, want 0 (debounced)", i, got)
		}
	}
	// The threshold cycle meters it: a sustained outage is real signal.
	r.updateToolSurfaceProbe(ctx)
	if got := gatherToolSurfaceCounter(t, reg, "operator", "unreachable"); got != 1 {
		t.Fatalf("at threshold probe{operator,unreachable} = %v, want 1 (sustained outage metered)", got)
	}
	// Every subsequent sustained-unhealthy cycle keeps metering.
	r.updateToolSurfaceProbe(ctx)
	if got := gatherToolSurfaceCounter(t, reg, "operator", "unreachable"); got != 2 {
		t.Fatalf("past threshold probe{operator,unreachable} = %v, want 2", got)
	}
}

// TestUpdateToolSurfaceProbe_HealthyCycleResetsDebounce verifies a healthy cycle
// clears the consecutive-failure run, so failures on either side of a recovery do
// not accumulate across it.
func TestUpdateToolSurfaceProbe_HealthyCycleResetsDebounce(t *testing.T) {
	ctx := logfIntoTestCtx()
	r, reg := newMemoryReconcilerWithReg()

	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer healthy.Close()
	down := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	downURL := down.URL
	down.Close()

	r.ToolSurfaceHTTP = healthy.Client() // plain-HTTP client reaches both httptest servers
	r.OperatorURL = downURL

	// Two transient-unhealthy cycles (below threshold), still suppressed.
	r.updateToolSurfaceProbe(ctx)
	r.updateToolSurfaceProbe(ctx)
	if got := gatherToolSurfaceCounter(t, reg, "operator", "unreachable"); got != 0 {
		t.Fatalf("probe{operator,unreachable} = %v after 2 transient cycles, want 0", got)
	}
	// A healthy cycle clears the run.
	r.OperatorURL = healthy.URL
	r.updateToolSurfaceProbe(ctx)
	// Two more transient cycles must again be suppressed (the run restarted at 0).
	r.OperatorURL = downURL
	r.updateToolSurfaceProbe(ctx)
	r.updateToolSurfaceProbe(ctx)
	if got := gatherToolSurfaceCounter(t, reg, "operator", "unreachable"); got != 0 {
		t.Fatalf("probe{operator,unreachable} = %v; a healthy cycle must reset the debounce", got)
	}
}

// TestUpdateToolSurfaceProbe_SkipsOperatorWhenURLUnset verifies the operator
// backend is not probed when OperatorURL is empty (unwired), so an unset URL
// does not emit a permanent "unreachable" false alarm.
func TestUpdateToolSurfaceProbe_SkipsOperatorWhenURLUnset(t *testing.T) {
	ctx := logfIntoTestCtx()
	r, reg := newMemoryReconcilerWithReg()

	r.updateToolSurfaceProbe(ctx)

	for _, res := range []string{"ok", "present", "absent", "error", "unreachable"} {
		if got := gatherToolSurfaceCounter(t, reg, "operator", res); got != 0 {
			t.Fatalf("probe{operator,%s} = %v, want 0 (OperatorURL unset)", res, got)
		}
	}
}
