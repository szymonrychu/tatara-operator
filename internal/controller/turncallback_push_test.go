package controller

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestCallbackServer_MountsPushRoute verifies the wrapper push-receiver is
// reachable on the internal listener when PushMetrics is set, and that the
// route is absent (404) when it is not.
func TestCallbackServer_MountsPushRoute(t *testing.T) {
	var hit bool
	cb := &CallbackServer{
		Client:    k8sClient,
		Metrics:   newCallbackServer().Metrics,
		Namespace: testNS,
		PushMetrics: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hit = true
			w.WriteHeader(http.StatusNoContent)
		}),
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/metrics/push", nil)
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if !hit {
		t.Fatal("push handler was not invoked for /internal/metrics/push")
	}
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
}

func TestCallbackServer_NoPushRouteWhenUnset(t *testing.T) {
	cb := newCallbackServer() // PushMetrics nil
	req := httptest.NewRequest(http.MethodPost, "/internal/metrics/push", nil)
	w := httptest.NewRecorder()
	cb.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (route should be absent)", w.Code)
	}
}
