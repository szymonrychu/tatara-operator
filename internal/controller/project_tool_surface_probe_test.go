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

// gatherToolSurfaceDurationCount reads the sample count of
// operator_tool_surface_probe_duration_seconds{backend}.
func gatherToolSurfaceDurationCount(t *testing.T, reg *prometheus.Registry, backend string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_tool_surface_probe_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "backend" && lp.GetValue() == backend {
					return m.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return 0
}

// TestUpdateToolSurfaceProbe_OperatorAndChat drives the probe against fake
// operator (401 -> present, the healthy OIDC-gated state) and chat (200 -> ok)
// backends and asserts both are metered with a recorded latency.
func TestUpdateToolSurfaceProbe_OperatorAndChat(t *testing.T) {
	ctx := logfIntoTestCtx()
	r, reg := newMemoryReconcilerWithReg()
	r.MemoryConfig.ChatPathPrefix = "/api/v1/chat"

	mkMemoryProject(t, "ts-ready")
	setMemoryPhaseReady(t, "ts-ready")

	opSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized) // operator-write route present, auth gate working
	}))
	defer opSrv.Close()
	chatSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK) // chat /readyz healthy
	}))
	defer chatSrv.Close()

	r.ToolSurfaceHTTP = opSrv.Client()
	r.OperatorURL = opSrv.URL
	chatProbes := 0
	r.ChatBaseURL = func() string { chatProbes++; return chatSrv.URL }

	r.updateToolSurfaceProbe(ctx)

	if got := gatherToolSurfaceCounter(t, reg, "operator", "present"); got < 1 {
		t.Fatalf("probe{operator,present} = %v, want >= 1", got)
	}
	if got := gatherToolSurfaceCounter(t, reg, "chat", "ok"); got < 1 {
		t.Fatalf("probe{chat,ok} = %v, want >= 1", got)
	}
	if got := gatherToolSurfaceDurationCount(t, reg, "operator"); got < 1 {
		t.Fatalf("operator latency samples = %d, want >= 1", got)
	}
	// chat is a single shared service: probed exactly once, not once per Project.
	if chatProbes != 1 {
		t.Fatalf("chat probed %d times, want exactly 1 (shared service, no per-project fan-out)", chatProbes)
	}
}

// TestUpdateToolSurfaceProbe_SkipsChatWhenDisabled verifies chat is never probed
// when ChatPathPrefix is empty, while the operator backend still is.
func TestUpdateToolSurfaceProbe_SkipsChatWhenDisabled(t *testing.T) {
	ctx := logfIntoTestCtx()
	r, reg := newMemoryReconcilerWithReg()
	r.MemoryConfig.ChatPathPrefix = "" // chat disabled platform-wide

	opSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer opSrv.Close()
	r.ToolSurfaceHTTP = opSrv.Client()
	r.OperatorURL = opSrv.URL
	probedChat := false
	r.ChatBaseURL = func() string { probedChat = true; return "http://unused" }

	r.updateToolSurfaceProbe(ctx)

	if probedChat {
		t.Fatal("chat was probed despite ChatPathPrefix being empty")
	}
	if got := gatherToolSurfaceCounter(t, reg, "operator", "present"); got < 1 {
		t.Fatalf("operator probe should still run: probe{operator,present} = %v", got)
	}
	for _, res := range []string{"ok", "present", "absent", "error", "unreachable"} {
		if got := gatherToolSurfaceCounter(t, reg, "chat", res); got != 0 {
			t.Fatalf("probe{chat,%s} = %v, want 0 (chat disabled)", res, got)
		}
	}
}

// TestUpdateToolSurfaceProbe_ChatProbedOnceRegardlessOfProjects verifies chat is
// probed exactly once per cycle when enabled: it is a single shared service, so
// its probing is decoupled from Project count and readiness (no per-project
// fan-out, which was the bug that reported the shared chat service unreachable).
func TestUpdateToolSurfaceProbe_ChatProbedOnceRegardlessOfProjects(t *testing.T) {
	ctx := logfIntoTestCtx()
	r, reg := newMemoryReconcilerWithReg()
	r.MemoryConfig.ChatPathPrefix = "/api/v1/chat"

	// A mix of non-Ready and Ready projects must not change chat probing.
	mkMemoryProject(t, "ts-once-prov") // leave status.memory unset -> not Ready
	mkMemoryProject(t, "ts-once-ready")
	setMemoryPhaseReady(t, "ts-once-ready")

	chatSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer chatSrv.Close()
	r.ToolSurfaceHTTP = chatSrv.Client()
	chatProbes := 0
	r.ChatBaseURL = func() string { chatProbes++; return chatSrv.URL }

	r.updateToolSurfaceProbe(ctx)

	if chatProbes != 1 {
		t.Fatalf("chat probed %d times, want exactly 1 regardless of Project count/readiness", chatProbes)
	}
	if got := gatherToolSurfaceCounter(t, reg, "chat", "ok"); got != 1 {
		t.Fatalf("probe{chat,ok} = %v, want 1", got)
	}
}

// TestUpdateToolSurfaceProbe_DebouncesTransientUnreachable verifies a failing
// backend is not metered until it has failed toolSurfaceUnhealthyThreshold cycles
// in a row: a transient transport failure (the operator's own rollout churn
// dialing a still-serving chat, issue #253) must not trip the `> 0` alert, while a
// sustained outage meters from the threshold cycle on.
func TestUpdateToolSurfaceProbe_DebouncesTransientUnreachable(t *testing.T) {
	ctx := logfIntoTestCtx()
	r, reg := newMemoryReconcilerWithReg()
	r.MemoryConfig.ChatPathPrefix = "/api/v1/chat"
	r.OperatorURL = "" // isolate the chat backend

	// A closed server yields a transport failure -> "unreachable", the incident's
	// class.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closedURL := srv.URL
	r.ToolSurfaceHTTP = srv.Client()
	srv.Close()
	r.ChatBaseURL = func() string { return closedURL }

	// Below the threshold: transient unreachable is logged but not metered.
	for i := 1; i < toolSurfaceUnhealthyThreshold; i++ {
		r.updateToolSurfaceProbe(ctx)
		if got := gatherToolSurfaceCounter(t, reg, "chat", "unreachable"); got != 0 {
			t.Fatalf("after %d transient cycle(s) probe{chat,unreachable} = %v, want 0 (debounced)", i, got)
		}
	}
	// The threshold cycle meters it: a sustained outage is real signal.
	r.updateToolSurfaceProbe(ctx)
	if got := gatherToolSurfaceCounter(t, reg, "chat", "unreachable"); got != 1 {
		t.Fatalf("at threshold probe{chat,unreachable} = %v, want 1 (sustained outage metered)", got)
	}
	// Every subsequent sustained-unhealthy cycle keeps metering.
	r.updateToolSurfaceProbe(ctx)
	if got := gatherToolSurfaceCounter(t, reg, "chat", "unreachable"); got != 2 {
		t.Fatalf("past threshold probe{chat,unreachable} = %v, want 2", got)
	}
}

// TestUpdateToolSurfaceProbe_HealthyCycleResetsDebounce verifies a healthy cycle
// clears the consecutive-failure run, so failures on either side of a recovery do
// not accumulate across it.
func TestUpdateToolSurfaceProbe_HealthyCycleResetsDebounce(t *testing.T) {
	ctx := logfIntoTestCtx()
	r, reg := newMemoryReconcilerWithReg()
	r.MemoryConfig.ChatPathPrefix = "/api/v1/chat"
	r.OperatorURL = "" // isolate the chat backend

	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer healthy.Close()
	down := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	downURL := down.URL
	down.Close()

	r.ToolSurfaceHTTP = healthy.Client() // plain-HTTP client reaches both httptest servers
	target := downURL
	r.ChatBaseURL = func() string { return target }

	// Two transient-unhealthy cycles (below threshold), still suppressed.
	r.updateToolSurfaceProbe(ctx)
	r.updateToolSurfaceProbe(ctx)
	if got := gatherToolSurfaceCounter(t, reg, "chat", "unreachable"); got != 0 {
		t.Fatalf("probe{chat,unreachable} = %v after 2 transient cycles, want 0", got)
	}
	// A healthy cycle clears the run.
	target = healthy.URL
	r.updateToolSurfaceProbe(ctx)
	// Two more transient cycles must again be suppressed (the run restarted at 0).
	target = downURL
	r.updateToolSurfaceProbe(ctx)
	r.updateToolSurfaceProbe(ctx)
	if got := gatherToolSurfaceCounter(t, reg, "chat", "unreachable"); got != 0 {
		t.Fatalf("probe{chat,unreachable} = %v; a healthy cycle must reset the debounce", got)
	}
}

// TestUpdateToolSurfaceProbe_SkipsOperatorWhenURLUnset verifies the operator
// backend is not probed when OperatorURL is empty (unwired), so an unset URL
// does not emit a permanent "unreachable" false alarm.
func TestUpdateToolSurfaceProbe_SkipsOperatorWhenURLUnset(t *testing.T) {
	ctx := logfIntoTestCtx()
	r, reg := newMemoryReconcilerWithReg()
	r.MemoryConfig.ChatPathPrefix = "" // isolate the operator branch

	r.updateToolSurfaceProbe(ctx)

	for _, res := range []string{"ok", "present", "absent", "error", "unreachable"} {
		if got := gatherToolSurfaceCounter(t, reg, "operator", res); got != 0 {
			t.Fatalf("probe{operator,%s} = %v, want 0 (OperatorURL unset)", res, got)
		}
	}
}
