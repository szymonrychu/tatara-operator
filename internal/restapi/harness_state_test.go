package restapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHarnessState_GetMissingReturnsEmpty(t *testing.T) {
	r := buildRouter(t, project("alpha"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/alpha/harness-state/LENS_CYCLE", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var e map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &e))
	require.Equal(t, "LENS_CYCLE", e["key"])
	require.Equal(t, "", e["value"])
	require.Equal(t, "", e["version"])
}

func TestHarnessState_UnknownProject404(t *testing.T) {
	r := buildRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/ghost/harness-state/LENS_CYCLE", nil))
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHarnessState_CASCreateThenGet(t *testing.T) {
	r := buildRouter(t, project("alpha"))
	// Create with empty version.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/projects/alpha/harness-state/LENS_CYCLE",
		strings.NewReader(`{"value":"failure-modes","version":""}`)))
	require.Equal(t, http.StatusOK, w.Code)
	var created map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))
	require.Equal(t, "failure-modes", created["value"])
	require.NotEmpty(t, created["version"])
	// Read it back.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/projects/alpha/harness-state/LENS_CYCLE", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var got map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Equal(t, "failure-modes", got["value"])
	require.Equal(t, created["version"], got["version"])
}

func TestHarnessState_CASStaleVersion409(t *testing.T) {
	r := buildRouter(t, project("alpha"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/projects/alpha/harness-state/LENS_CYCLE",
		strings.NewReader(`{"value":"failure-modes","version":""}`)))
	require.Equal(t, http.StatusOK, w.Code)
	// Second create-with-empty-version now that it exists must 409.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/projects/alpha/harness-state/LENS_CYCLE",
		strings.NewReader(`{"value":"coupling","version":""}`)))
	require.Equal(t, http.StatusConflict, w.Code)
}
