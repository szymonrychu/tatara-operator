package controller

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// stubMemoryToken returns a MemoryToken func that always yields tok.
func stubMemoryToken(tok string) func(context.Context) (string, error) {
	return func(context.Context) (string, error) { return tok, nil }
}

// TestProbeMemoryRoute_Classification verifies the authenticated functional
// classifier: a 2xx with a well-formed JSON body is "present" (healthy), a 2xx
// with an empty/garbage body is "degraded", 401/403 is "unauthorized" (a valid
// token was rejected), 404 is "absent" (route gone), and any other non-2xx
// (5xx/4xx contract drift) is "degraded".
func TestProbeMemoryRoute_Classification(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"ok-json-object", http.StatusOK, `{"matches":[]}`, "present"},
		{"ok-json-null-matches", http.StatusOK, `{"matches":null}`, "present"},
		{"ok-empty-body", http.StatusOK, "", "degraded"},
		{"ok-garbage-body", http.StatusOK, "not json", "degraded"},
		{"ok-bare-null", http.StatusOK, "null", "degraded"},
		{"unauthenticated-401", http.StatusUnauthorized, "missing bearer token", "unauthorized"},
		{"forbidden-403", http.StatusForbidden, "forbidden", "unauthorized"},
		{"bad-request-400", http.StatusBadRequest, `{"error":"bad"}`, "degraded"},
		{"server-error-500", http.StatusInternalServerError, "internal error", "degraded"},
		{"bad-gateway-502", http.StatusBadGateway, "upstream error", "degraded"},
		{"service-unavailable-503", http.StatusServiceUnavailable, "not ready", "degraded"},
		{"not-found-404", http.StatusNotFound, "404 page not found", "absent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				if tc.body != "" {
					_, _ = w.Write([]byte(tc.body))
				}
			}))
			defer srv.Close()
			got := probeMemoryRoute(context.Background(), srv.Client(), http.MethodPost, srv.URL+"/queries", memoryProbeQueryBody, "tok")
			if got != tc.want {
				t.Fatalf("status %d body %q classified %q, want %q", tc.status, tc.body, got, tc.want)
			}
		})
	}
}

// TestProbeMemoryRoute_AttachesTokenAndBody verifies the probe sends the bearer
// token and request body, and that a missing token reads as "unauthorized".
func TestProbeMemoryRoute_AttachesTokenAndBody(t *testing.T) {
	var gotAuth, gotBody, gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		if gotAuth == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"matches":[]}`))
	}))
	defer srv.Close()

	if got := probeMemoryRoute(context.Background(), srv.Client(), http.MethodPost, srv.URL+"/queries", memoryProbeQueryBody, "tok"); got != "present" {
		t.Fatalf("authenticated probe = %q, want present", got)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("Authorization = %q, want %q", gotAuth, "Bearer tok")
	}
	if gotBody != memoryProbeQueryBody {
		t.Fatalf("request body = %q, want %q", gotBody, memoryProbeQueryBody)
	}
	if gotCT != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", gotCT)
	}

	// No token -> server answers 401 -> classified unauthorized.
	if got := probeMemoryRoute(context.Background(), srv.Client(), http.MethodPost, srv.URL+"/queries", memoryProbeQueryBody, ""); got != "unauthorized" {
		t.Fatalf("tokenless probe = %q, want unauthorized", got)
	}
}

func TestProbeMemoryRoute_TransportFailureIsError(t *testing.T) {
	// A closed server yields a connection-refused transport error.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	client := srv.Client()
	srv.Close()
	if got := probeMemoryRoute(context.Background(), client, http.MethodGet, url+"/code-graph/stats", "", "tok"); got != "error" {
		t.Fatalf("transport failure classified %q, want error", got)
	}
}

// setMemoryPhaseReady forces a Project's status.memory.phase to Ready so
// updateMemoryRetrievalProbe will probe it, without standing up a full stack.
func setMemoryPhaseReady(t *testing.T, name string) {
	t.Helper()
	p := getProject(t, name)
	p.Status.Memory = &tataradevv1alpha1.MemoryStatus{
		Phase:    "Ready",
		Endpoint: memory.Endpoint(name, testNS),
	}
	mustStatusUpdate(t, context.Background(), p)
}

// gatherProbeCounter reads operator_memory_retrieval_probe_total{route,result}.
func gatherProbeCounter(t *testing.T, reg *prometheus.Registry, route, result string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_memory_retrieval_probe_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var r, res string
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "route" {
					r = lp.GetValue()
				}
				if lp.GetName() == "result" {
					res = lp.GetValue()
				}
			}
			if r == route && res == result {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// TestUpdateMemoryRetrievalProbe_UnhealthyIncrementsHealthyClears drives the poll
// loop against a toggleable server: consecutive unhealthy (404) cycles increment
// the per-project run, a healthy (2xx + JSON) cycle clears it, the result is
// metered, and a no-longer-Ready project is pruned from the map.
func TestUpdateMemoryRetrievalProbe_UnhealthyIncrementsHealthyClears(t *testing.T) {
	ctx := logfIntoTestCtx()
	r, reg := newMemoryReconcilerWithReg()
	r.MemoryToken = stubMemoryToken("tok")

	mkMemoryProject(t, "probe-orch")
	setMemoryPhaseReady(t, "probe-orch")

	var unhealthy int32 = 1
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.LoadInt32(&unhealthy) == 1 {
			w.WriteHeader(http.StatusNotFound) // route absent -> drifted binary
			return
		}
		_, _ = w.Write([]byte(`{"matches":[]}`)) // 2xx + well-formed body -> present
	}))
	defer srv.Close()
	r.MemoryHTTP = srv.Client()
	r.MemoryBaseURL = func(string) string { return srv.URL }

	// A stale entry for a project that is not Ready must be pruned each pass.
	r.memoryUnhealthyCycles = map[string]int{"ghost-not-ready": 5}

	r.updateMemoryRetrievalProbe(ctx)
	if got := r.memoryUnhealthyCycles["probe-orch"]; got != 1 {
		t.Fatalf("after pass 1 (unhealthy), cycles = %d, want 1", got)
	}
	if _, ok := r.memoryUnhealthyCycles["ghost-not-ready"]; ok {
		t.Fatal("stale non-Ready project was not pruned from the cycle map")
	}

	r.updateMemoryRetrievalProbe(ctx)
	if got := r.memoryUnhealthyCycles["probe-orch"]; got != 2 {
		t.Fatalf("after pass 2 (unhealthy), cycles = %d, want 2", got)
	}
	// Each unhealthy pass meters both routes as absent; two passes -> >= 2.
	if got := gatherProbeCounter(t, reg, "/queries", "absent"); got < 2 {
		t.Fatalf("probe{/queries,absent} = %v, want >= 2", got)
	}

	// Surface recovers: a healthy cycle clears the run.
	atomic.StoreInt32(&unhealthy, 0)
	r.updateMemoryRetrievalProbe(ctx)
	if _, ok := r.memoryUnhealthyCycles["probe-orch"]; ok {
		t.Fatalf("healthy cycle did not clear the run: cycles = %d", r.memoryUnhealthyCycles["probe-orch"])
	}
	if got := gatherProbeCounter(t, reg, "/queries", "present"); got < 1 {
		t.Fatalf("probe{/queries,present} = %v, want >= 1 after healthy pass", got)
	}
}

// TestUpdateMemoryRetrievalProbe_UnauthorizedWhenTokenRejected covers the
// auth-contract drift class: an authenticated request whose token memory rejects
// with 401 is metered "unauthorized" and counts unhealthy (the old presence-only
// probe read this same 401 as healthy).
func TestUpdateMemoryRetrievalProbe_UnauthorizedWhenTokenRejected(t *testing.T) {
	ctx := logfIntoTestCtx()
	r, reg := newMemoryReconcilerWithReg()
	r.MemoryToken = stubMemoryToken("tok")

	mkMemoryProject(t, "probe-401")
	setMemoryPhaseReady(t, "probe-401")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized) // route + auth present, token rejected
	}))
	defer srv.Close()
	r.MemoryHTTP = srv.Client()
	r.MemoryBaseURL = func(string) string { return srv.URL }

	r.updateMemoryRetrievalProbe(ctx)
	if got := r.memoryUnhealthyCycles["probe-401"]; got != 1 {
		t.Fatalf("401 cycle: cycles = %d, want 1 (unhealthy)", got)
	}
	if got := gatherProbeCounter(t, reg, "/queries", "unauthorized"); got < 1 {
		t.Fatalf("probe{/queries,unauthorized} = %v, want >= 1", got)
	}
}

// TestUpdateMemoryRetrievalProbe_TokenMintFailureIsUnhealthy verifies a token
// mint failure meters every route "error", counts the cycle unhealthy, and does
// not even reach the memory server.
func TestUpdateMemoryRetrievalProbe_TokenMintFailureIsUnhealthy(t *testing.T) {
	ctx := logfIntoTestCtx()
	r, reg := newMemoryReconcilerWithReg()
	r.MemoryToken = func(context.Context) (string, error) {
		return "", context.DeadlineExceeded
	}

	mkMemoryProject(t, "probe-mint-fail")
	setMemoryPhaseReady(t, "probe-mint-fail")

	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		_, _ = w.Write([]byte(`{"matches":[]}`))
	}))
	defer srv.Close()
	r.MemoryHTTP = srv.Client()
	r.MemoryBaseURL = func(string) string { return srv.URL }

	r.updateMemoryRetrievalProbe(ctx)
	if got := r.memoryUnhealthyCycles["probe-mint-fail"]; got != 1 {
		t.Fatalf("mint failure: cycles = %d, want 1 (unhealthy)", got)
	}
	if got := atomic.LoadInt32(&hits); got != 0 {
		t.Fatalf("memory server hit %d times on mint failure, want 0", got)
	}
	if got := gatherProbeCounter(t, reg, "/queries", "error"); got < 1 {
		t.Fatalf("probe{/queries,error} = %v, want >= 1", got)
	}
}

// TestUpdateMemoryRetrievalProbe_SkipsNonReady verifies a Provisioning stack is
// never probed (the replica gate is the precondition).
func TestUpdateMemoryRetrievalProbe_SkipsNonReady(t *testing.T) {
	ctx := logfIntoTestCtx()
	r := newMemoryReconciler()
	r.MemoryToken = stubMemoryToken("tok")

	mkMemoryProject(t, "probe-provisioning")
	// Leave status.memory unset (no phase) -> not Ready.

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"matches":[]}`))
	}))
	defer srv.Close()
	r.MemoryHTTP = srv.Client()
	r.MemoryBaseURL = func(string) string { return srv.URL }

	r.updateMemoryRetrievalProbe(ctx)
	if _, ok := r.memoryUnhealthyCycles["probe-provisioning"]; ok {
		t.Fatal("a non-Ready project was probed and tracked")
	}
}

// TestReconcileMemory_FoldsSustainedRetrievalFailure verifies the condition
// fold-in: a replica-Ready stack whose retrieval surface has been unhealthy for
// memoryRetrievalUnhealthyThreshold consecutive cycles reads
// MemoryReady=False/RetrievalUnreachable while phase stays Ready; below the
// threshold it stays Ready/True.
func TestReconcileMemory_FoldsSustainedRetrievalFailure(t *testing.T) {
	ctx := logfIntoTestCtx()
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "fold-retrieval")

	// Apply the stack, then make every replica healthy so phase computes Ready.
	if _, err := r.reconcileMemory(ctx, p); err != nil {
		t.Fatalf("apply reconcile: %v", err)
	}
	fakeStackHealthy(t, p.Name)

	// Healthy retrieval surface (no tracked cycles): MemoryReady True/Ready.
	if _, err := r.reconcileMemory(ctx, p); err != nil {
		t.Fatalf("ready reconcile: %v", err)
	}
	if p.Status.Memory.Phase != "Ready" {
		t.Fatalf("phase = %q, want Ready", p.Status.Memory.Phase)
	}
	if c := apimeta.FindStatusCondition(p.Status.Conditions, "MemoryReady"); c == nil ||
		c.Status != metav1.ConditionTrue || c.Reason != "Ready" {
		t.Fatalf("MemoryReady = %+v, want True/Ready", c)
	}

	// One short of the threshold: still green.
	r.memoryUnhealthyCycles = map[string]int{p.Name: memoryRetrievalUnhealthyThreshold - 1}
	if _, err := r.reconcileMemory(ctx, p); err != nil {
		t.Fatalf("sub-threshold reconcile: %v", err)
	}
	if c := apimeta.FindStatusCondition(p.Status.Conditions, "MemoryReady"); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("MemoryReady at %d cycles = %+v, want still True", memoryRetrievalUnhealthyThreshold-1, c)
	}

	// At the threshold: fold the failure into the condition, phase stays Ready.
	r.memoryUnhealthyCycles[p.Name] = memoryRetrievalUnhealthyThreshold
	if _, err := r.reconcileMemory(ctx, p); err != nil {
		t.Fatalf("threshold reconcile: %v", err)
	}
	if p.Status.Memory.Phase != "Ready" {
		t.Fatalf("phase = %q, want Ready (probe must keep running)", p.Status.Memory.Phase)
	}
	c := apimeta.FindStatusCondition(p.Status.Conditions, "MemoryReady")
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "RetrievalUnreachable" {
		t.Fatalf("MemoryReady at threshold = %+v, want False/RetrievalUnreachable", c)
	}

	// Recovery: run cleared -> condition returns to True/Ready.
	delete(r.memoryUnhealthyCycles, p.Name)
	if _, err := r.reconcileMemory(ctx, p); err != nil {
		t.Fatalf("recovery reconcile: %v", err)
	}
	if c := apimeta.FindStatusCondition(p.Status.Conditions, "MemoryReady"); c == nil ||
		c.Status != metav1.ConditionTrue || c.Reason != "Ready" {
		t.Fatalf("MemoryReady after recovery = %+v, want True/Ready", c)
	}
}
