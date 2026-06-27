package webhook_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/webhook"
)

func TestRunnableStartStop(t *testing.T) {
	c := seedClient(t)
	srv := webhook.NewServer(webhook.Config{Client: c, Namespace: ns, Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())})
	r := webhook.NewRunnable(srv, "127.0.0.1:0")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	// serveHTTP bounds graceful shutdown by its own 10s shutdownCtx, so the stop
	// must be allowed at least that long plus scheduling headroom; a tight 2s
	// deadline flaked on loaded -race CI runners ("runnable did not stop on
	// context cancel"). A genuine never-stops regression still fails here, well
	// under the package test timeout.
	case <-time.After(15 * time.Second):
		t.Fatal("runnable did not stop on context cancel")
	}

	// sanity: handler still serves
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/operator/webhooks/x", nil))
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
}
