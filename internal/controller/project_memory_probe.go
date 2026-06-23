package controller

import (
	"context"
	"io"
	"net/http"
	"time"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// memoryProbeTimeout bounds a single retrieval-surface probe so a wedged memory
// pod cannot stall the serialised reconcile path.
const memoryProbeTimeout = 3 * time.Second

// memoryRetrievalUnhealthyThreshold is how many consecutive unhealthy probe
// cycles (one per ~60s gauge recompute) reconcileMemory requires before folding
// the failure into MemoryReady. Three cycles (~3 min) rides out a single
// transient blip or a rolling memory-pod restart without flapping the platform's
// primary memory health signal.
const memoryRetrievalUnhealthyThreshold = 3

// memoryProbeRoutes are the representative agent-facing routes probed
// unauthenticated to detect retrieval-contract drift. Each sits inside
// tatara-memory's cfg.Verify group, so a route-present binary answers an auth
// status (401) while a drifted/stale binary 404s; the probe therefore needs no
// memory-audience token. POST /queries (the query surface) and GET
// /code-graph/stats (the code-graph surface) together cover both retrieval
// families agents consume. The path doubles as the metric route label, so it
// must stay in sync with the pre-seed in obs.NewOperatorMetrics.
var memoryProbeRoutes = []struct {
	method string
	path   string
}{
	{http.MethodPost, "/queries"},
	{http.MethodGet, "/code-graph/stats"},
}

// updateMemoryRetrievalProbe probes the tatara-memory retrieval surface of every
// Ready project and folds the result into per-project consecutive-unhealthy-cycle
// state for reconcileMemory to read. It mirrors updateLightragDocCounts: it runs
// on the 60s gauge cadence, is best-effort (a probe never fails the reconcile),
// and only touches Ready stacks so a still-provisioning memory pod is not probed.
// Each route's result is metered; each unhealthy route is logged. A cycle is
// healthy only when every probed route is present; one absent/error route makes
// the whole cycle unhealthy and increments the project's run, while a fully
// healthy cycle clears it.
func (r *ProjectReconciler) updateMemoryRetrievalProbe(ctx context.Context) {
	if r.Metrics == nil {
		return
	}
	var list tataradevv1alpha1.ProjectList
	if err := r.List(ctx, &list); err != nil {
		return
	}
	httpc := r.MemoryHTTP
	if httpc == nil {
		httpc = &http.Client{Timeout: memoryProbeTimeout}
	}
	if r.memoryUnhealthyCycles == nil {
		r.memoryUnhealthyCycles = map[string]int{}
	}
	ready := make(map[string]bool, len(list.Items))
	for i := range list.Items {
		p := &list.Items[i]
		if p.Status.Memory == nil || p.Status.Memory.Phase != "Ready" {
			continue
		}
		ready[p.Name] = true
		base := r.memoryBaseURL(p.Name)
		healthy := true
		for _, route := range memoryProbeRoutes {
			result := probeMemoryRoute(ctx, httpc, route.method, base+route.path)
			r.Metrics.MemoryRetrievalProbe(route.path, result)
			if result != "present" {
				healthy = false
				log.FromContext(ctx).Info("memory retrieval probe unhealthy",
					"action", "memory_retrieval_probe",
					"resource_id", p.Name,
					"route", route.path,
					"result", result)
			}
		}
		if healthy {
			delete(r.memoryUnhealthyCycles, p.Name)
		} else {
			r.memoryUnhealthyCycles[p.Name]++
		}
	}
	// Drop entries for projects no longer Ready (retired or reverted to
	// Provisioning) so the map does not leak and a stale unhealthy run cannot
	// carry back into a future Ready window.
	for name := range r.memoryUnhealthyCycles {
		if !ready[name] {
			delete(r.memoryUnhealthyCycles, name)
		}
	}
}

// memoryBaseURL returns the in-cluster base URL of a project's tatara-memory
// Service, or the test override when set.
func (r *ProjectReconciler) memoryBaseURL(project string) string {
	if r.MemoryBaseURL != nil {
		return r.MemoryBaseURL(project)
	}
	return memory.Endpoint(project, r.MemoryConfig.Namespace)
}

// probeMemoryRoute sends one unauthenticated request to a tatara-memory route and
// classifies the served contract: "absent" on a 404 (route missing -> drifted or
// stale binary), "error" on a transport failure (process down / unreachable), and
// "present" on any other served status (the route exists; the cfg.Verify auth
// middleware rejecting an unauthenticated request with 401 still proves
// presence). It never returns an error; the classification is the whole signal.
func probeMemoryRoute(ctx context.Context, httpc *http.Client, method, url string) string {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return "error"
	}
	resp, err := httpc.Do(req)
	if err != nil {
		return "error"
	}
	defer func() { _ = resp.Body.Close() }()
	// Drain a little so the keep-alive connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<10))
	if resp.StatusCode == http.StatusNotFound {
		return "absent"
	}
	return "present"
}
