package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
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

// maxMemoryProbeBody caps how much of a probe response body is read for the
// well-formed-JSON assertion. A top_k=1 retrieval or a zero-count code-graph
// stats response is tiny; the 1 MiB cap (matching tatara-memory's own small-body
// limit) bounds memory use while never truncating a healthy body.
const maxMemoryProbeBody = 1 << 20

// memoryProbeQueryBody is the fixed canary retrieval sent to POST /queries.
// "naive" mode is deliberate: it exercises the exact minimal contract every
// agent retrieval depends on (auth -> handler -> query embedding -> vector store
// -> JSON body) without the keyword-extraction LLM call the graph modes
// (hybrid/local/global) make, so MemoryReady is not coupled to response-LLM
// provider uptime or a per-probe LLM cost. top_k=1 keeps the retrieval minimal.
// Neo4j graph-traversal liveness is intentionally not asserted here (it needs a
// graph mode); see MEMORY.md.
const memoryProbeQueryBody = `{"mode":"naive","text":"tatara-operator synthetic memory probe","top_k":1}`

// memoryProbeStatsRepo is a fixed sentinel repo for the GET /code-graph/stats
// probe. tatara-memory answers an unknown repo with a zero-count 200 (a plain
// SELECT count(*) WHERE repo=$1), so the probe exercises the code-graph read
// path (auth -> handler -> Postgres) and asserts a well-formed body without
// depending on any project's actually-ingested repos. A Postgres-down stats call
// instead maps to 5xx -> degraded.
const memoryProbeStatsRepo = "__tatara_operator_probe__"

// memoryProbeRoutes are the representative agent-facing retrieval routes probed
// with an authenticated request to assert the contract agents actually use. Both
// sit inside tatara-memory's auth group, so an authenticated caller drives the
// real handler (and a valid-token rejection is the auth-contract drift the old
// presence-only probe could not see). POST /queries (the query surface) and GET
// /code-graph/stats (the code-graph surface) together cover both retrieval
// families agents consume. path doubles as the metric route label, so it must
// stay in sync with the pre-seed in obs.NewOperatorMetrics.
var memoryProbeRoutes = []struct {
	method string
	path   string
	query  string
	body   string
}{
	{http.MethodPost, "/queries", "", memoryProbeQueryBody},
	{http.MethodGet, "/code-graph/stats", "repo=" + memoryProbeStatsRepo, ""},
}

// updateMemoryRetrievalProbe sends one authenticated functional probe per
// retrieval route of every Ready project and folds the result into per-project
// consecutive-unhealthy-cycle state for reconcileMemory to read. It mirrors
// updateLightragDocCounts: it runs on the 60s gauge cadence, is best-effort (a
// probe never fails the reconcile), and only touches Ready stacks so a
// still-provisioning memory pod is not probed. The memory-audience token is
// minted once per cycle (one audience for all projects; the source caches). Each
// route's result is metered; each unhealthy route is logged. A cycle is healthy
// only when every probed route returns "present" (HTTP 2xx + well-formed JSON
// body); any absent/error/unauthorized/degraded route makes the whole cycle
// unhealthy and increments the project's run, while a fully healthy cycle clears
// it.
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

	// Mint the memory-audience token once per cycle. A mint failure means the
	// probe cannot authenticate, so every route is metered "error" and counts
	// unhealthy this cycle: from here a token path the operator cannot use is
	// indistinguishable from memory being unreachable, and the 3-cycle MemoryReady
	// fold rides out a transient mint blip.
	var token string
	var tokenErr error
	if r.MemoryToken != nil {
		if token, tokenErr = r.MemoryToken(ctx); tokenErr != nil {
			log.FromContext(ctx).Error(tokenErr, "memory retrieval probe: token mint failed",
				"action", "memory_retrieval_probe")
		}
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
			result := "error"
			if tokenErr == nil {
				result = probeMemoryRoute(ctx, httpc, route.method, memoryProbeURL(base, route.path, route.query), route.body, token)
			}
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

// memoryProbeURL joins a base, path, and optional query string into a probe URL.
func memoryProbeURL(base, path, query string) string {
	if query == "" {
		return base + path
	}
	return base + path + "?" + query
}

// probeMemoryRoute sends one authenticated request to a tatara-memory retrieval
// route and classifies the served contract:
//   - "absent": 404 (route missing -> drifted or stale binary).
//   - "unauthorized": 401/403 (the operator's valid memory-audience token was
//     rejected -> auth/contract drift; this is the 401-for-everyone class the old
//     presence-only probe read as healthy).
//   - "degraded": auth passed but the contract is broken - any non-2xx other than
//     404/401/403 (a 5xx handler/backend error, or a drifted 4xx), or a 2xx whose
//     body is empty/malformed/non-JSON (a 200-with-broken-body).
//   - "error": transport failure (process down / unreachable) or the request
//     could not be built.
//   - "present": HTTP 2xx with a well-formed, non-null JSON body - the healthy
//     contract agents actually consume.
//
// It never returns an error; the classification is the whole signal.
func probeMemoryRoute(ctx context.Context, httpc *http.Client, method, url, body, token string) string {
	var reqBody io.Reader
	if body != "" {
		reqBody = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return "error"
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := httpc.Do(req)
	if err != nil {
		return "error"
	}
	defer func() { _ = resp.Body.Close() }()
	// Read the whole (capped) body so the JSON assertion sees it and the
	// keep-alive connection can be reused.
	data, _ := io.ReadAll(io.LimitReader(resp.Body, maxMemoryProbeBody))
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return "absent"
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return "unauthorized"
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return "degraded"
	case !wellFormedJSON(data):
		return "degraded"
	default:
		return "present"
	}
}

// wellFormedJSON reports whether b is a non-empty, syntactically valid JSON value
// that is not literal null - the minimal "the handler served a real body"
// assertion. An empty body (a decode error) or a bare null both read as broken.
func wellFormedJSON(b []byte) bool {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return false
	}
	return v != nil
}
