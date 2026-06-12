// Copyright 2026 tatara authors.

package restapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/internal/restapi"
)

func TestHandover_StoresHandover(t *testing.T) {
	r := buildRouter(t, task("t-ho1", "alpha"))
	body := strings.NewReader(`{"handover":"# Summary\nDid X, Y. Branch: feat/x"}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t-ho1/handover", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, "# Summary\nDid X, Y. Branch: feat/x", out.Status.Handover)
}

func TestHandover_TaskNotFound(t *testing.T) {
	r := buildRouter(t)
	req := httptest.NewRequest(http.MethodPost, "/tasks/nope/handover", strings.NewReader(`{"handover":"x"}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandover_EmptyHandoverAccepted(t *testing.T) {
	r := buildRouter(t, task("t-ho2", "alpha"))
	body := strings.NewReader(`{"handover":""}`)
	req := httptest.NewRequest(http.MethodPost, "/tasks/t-ho2/handover", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandover_Capped_At16KB(t *testing.T) {
	// 20KB handover doc - must be capped to 16KB on store.
	bigDoc := strings.Repeat("x", 20*1024)
	bodyStr, _ := json.Marshal(map[string]string{"handover": bigDoc})
	r := buildRouter(t, task("t-ho3", "alpha"))
	req := httptest.NewRequest(http.MethodPost, "/tasks/t-ho3/handover", strings.NewReader(string(bodyStr)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var out restapi.TaskDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.LessOrEqual(t, len(out.Status.Handover), 16*1024)
}
