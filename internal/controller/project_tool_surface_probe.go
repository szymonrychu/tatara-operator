package controller

import (
	"context"
	"io"
	"net/http"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// toolSurfaceProbeTimeout bounds a single tool-surface probe so a wedged backend
// cannot stall the serialised reconcile path. Matches memoryProbeTimeout.
const toolSurfaceProbeTimeout = 3 * time.Second

// toolSurfaceVantage labels every series updateToolSurfaceProbe emits. Only the
// in-cluster agent vantage (the TATARA_*_URL agent pods receive) is probed today;
// the label is reserved so an external agent-facing ingress vantage can be added
// later without resetting the series.
const toolSurfaceVantage = "in-cluster"

// toolSurfaceUnhealthyThreshold is how many consecutive unhealthy probe cycles
// (one per ~60s gauge recompute) a backend must fail before probeToolSurface
// meters the failing result. Below it the failure is logged but not metered, so
// the operator's OWN rollout churn - transient transport failures dialing a
// demonstrably-serving backend from a pod that is itself starting up or shutting
// down - does not trip the `> 0 for 15m` tool-surface alert against a healthy
// backend (issue #253). Three cycles (~3 min) rides out a single operator
// rollout wave while a genuine sustained outage still meters from the third cycle
// on. Mirrors memoryRetrievalUnhealthyThreshold.
const toolSurfaceUnhealthyThreshold = 3

// updateToolSurfaceProbe probes the agent-facing tool backends the autonomous
// loop acts through - the operator-write REST surface - from the same in-cluster URLs agent pods
// receive, and meters the served contract per backend. It is the
// operator-write sibling of updateMemoryRetrievalProbe (which already covers
// tatara-memory): it runs on the 60s gauge cadence, is best-effort (a probe never
// fails the reconcile), and is purely observational - it folds into no Project
// condition and gates no agent dispatch. A failing result is debounced per
// backend (toolSurfaceUnhealthyThreshold consecutive cycles) before it is metered,
// so the operator's own rollout churn does not report a still-serving backend as
// failing.
//
// The operator holds no OIDC token carrying these backends' audiences, so the
// representative read is unauthenticated: a 401/403 ("present") proves the route
// and auth gate are served without asserting handler health, while a 2xx ("ok"),
// 404 ("absent" -> route drift / stale binary), 5xx ("error" -> handler broken),
// or transport failure ("unreachable") each pinpoint a distinct break. This is
// the /readyz-style fallback the issue's SP3 decision allows; an authenticated
// 2xx read can be added per backend later if the operator gains a token for that
// audience, behind the same metric.
func (r *ProjectReconciler) updateToolSurfaceProbe(ctx context.Context) {
	if r.Metrics == nil {
		return
	}
	httpc := r.ToolSurfaceHTTP
	if httpc == nil {
		httpc = &http.Client{Timeout: toolSurfaceProbeTimeout}
	}
	if r.toolSurfaceUnhealthyCycles == nil {
		r.toolSurfaceUnhealthyCycles = map[string]int{}
	}

	// operator-write surface: a single shared instance, so probe one
	// representative READ on the REST base URL agents reach via
	// TATARA_OPERATOR_URL. GET /projects sits behind the OIDC gate, so a healthy
	// operator answers 401 ("present"); a 404 means the route drifted away, a 5xx
	// means the handler chain broke, and a transport failure means the REST
	// listener is down - every operator-write tool shares this listener and gate.
	if r.OperatorURL != "" {
		r.probeToolSurface(ctx, httpc, "operator", http.MethodGet, r.OperatorURL+"/projects")
	}

}

// probeToolSurface sends one request to a tool-backend route, classifies the
// served contract, and meters the result and latency under (backend, vantage).
// A healthy result (ok/present) clears the backend's unhealthy run and is metered
// immediately. An unhealthy result (anything else) is always logged but only
// metered once it has recurred toolSurfaceUnhealthyThreshold cycles in a row, so a
// transient transport failure the operator emits against a still-serving backend
// while its own pod is starting up or shutting down (issue #253) is not counted
// against the `> 0 for 15m` tool-surface alert; a sustained outage still meters
// from the threshold cycle on.
func (r *ProjectReconciler) probeToolSurface(ctx context.Context, httpc *http.Client, backend, method, url string) {
	start := time.Now()
	result, err := probeToolSurfaceRoute(ctx, httpc, method, url)
	elapsed := time.Since(start).Seconds()

	if result == "ok" || result == "present" {
		delete(r.toolSurfaceUnhealthyCycles, backend)
		r.Metrics.ToolSurfaceProbe(backend, toolSurfaceVantage, result, elapsed)
		return
	}

	r.toolSurfaceUnhealthyCycles[backend]++
	run := r.toolSurfaceUnhealthyCycles[backend]
	kv := []any{
		"action", "tool_surface_probe",
		"backend", backend,
		"vantage", toolSurfaceVantage,
		"result", result,
		"url", url,
		"consecutive", run,
	}
	// Include the transport error on "unreachable" so a dial failure is a
	// one-look diagnosis (no-such-host DNS vs connection-refused vs timeout)
	// instead of requiring source cross-referencing.
	if err != nil {
		kv = append(kv, "error", err.Error())
	}
	log.FromContext(ctx).Info("tool surface probe unhealthy", kv...)

	// Debounce: suppress the metric until the failure is sustained, so operator
	// self-deploy rollout churn (a burst of transient failures per short-lived
	// pod) does not trip the alert on a backend that is demonstrably serving.
	if run < toolSurfaceUnhealthyThreshold {
		return
	}
	r.Metrics.ToolSurfaceProbe(backend, toolSurfaceVantage, result, elapsed)
}

// probeToolSurfaceRoute sends one unauthenticated request and classifies the
// served status: "ok" (2xx), "present" (401/403 or any other 4xx: the route and
// process are served but we hold no token to drive a 2xx), "absent" (404 -> route
// missing / stale binary), "error" (5xx -> handler broken), or "unreachable"
// (transport failure -> process down). The classification is the whole signal;
// the returned error is non-nil only on an "unreachable" transport failure and is
// surfaced in the log line to make a dial failure a one-look diagnosis.
func probeToolSurfaceRoute(ctx context.Context, httpc *http.Client, method, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return "unreachable", err
	}
	resp, err := httpc.Do(req)
	if err != nil {
		return "unreachable", err
	}
	defer func() { _ = resp.Body.Close() }()
	// Drain a little so the keep-alive connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<10))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return "ok", nil
	case resp.StatusCode == http.StatusNotFound:
		return "absent", nil
	case resp.StatusCode >= 500:
		return "error", nil
	default:
		// Any other 4xx (401/403/400/405/...) proves the route and process are
		// served; only a 404 means the route itself is gone.
		return "present", nil
	}
}
