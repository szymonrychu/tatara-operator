package restapi_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
)

// TestSharedMux_RESTAndWebhookCoexist asserts REST paths and a webhook-prefixed
// path can live on the same mux without collision.
func TestSharedMux_RESTAndWebhookCoexist(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	fc := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := chi.NewRouter()
	// stand-in for the M2 webhook mount
	r.Post("/operator/webhooks/{project}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	restapi.NewServer(restapi.Config{Client: fc, Namespace: "tatara"}).Mount(r, nil)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects", nil))
	require.Equal(t, http.StatusOK, w.Code)

	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/operator/webhooks/demo", nil))
	require.Equal(t, http.StatusAccepted, w.Code)
}
