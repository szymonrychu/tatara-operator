package restapi_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/internal/auth"
)

// TestAuthorizeForTask_ValidSubjectAnyIdentityPasses guards the regression where
// per-task authz compared Claims.Subject/PreferredUsername against the agent Pod
// name. Every agent mints its token via a single shared client-credentials OIDC
// client, so its sub is the service-account UUID (never the Pod name): the
// pod-name comparison would 403 every legitimate agent write. A valid (non-empty
// Subject) caller must be allowed regardless of how its identity relates to the
// task's Pod name.
func TestAuthorizeForTask_ValidSubjectAnyIdentityPasses(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"))
	req := httptest.NewRequest(http.MethodPatch, "/tasks/t1",
		strings.NewReader(`{"resultSummary":"progress"}`))
	req.Header.Set("Content-Type", "application/json")
	// Inject a verified identity whose Subject is the shared service account -
	// deliberately NOT the agent Pod name for task t1.
	ctx := auth.ContextWithClaims(req.Context(), &auth.Claims{
		Subject:           "service-account-tatara-cli",
		PreferredUsername: "service-account-tatara-cli",
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req.WithContext(ctx))
	require.Equal(t, http.StatusOK, w.Code,
		"a valid-token caller must not be 403'd just because its sub is not the Pod name")
}

// TestAuthorizeForTask_EmptySubjectRejected confirms a Claims with no verified
// Subject is rejected with 403. (In production the verifier never yields an empty
// Subject - it errors first - so this is defence-in-depth.)
func TestAuthorizeForTask_EmptySubjectRejected(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"))
	req := httptest.NewRequest(http.MethodPatch, "/tasks/t1",
		strings.NewReader(`{"resultSummary":"progress"}`))
	req.Header.Set("Content-Type", "application/json")
	ctx := auth.ContextWithClaims(req.Context(), &auth.Claims{Subject: ""})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req.WithContext(ctx))
	require.Equal(t, http.StatusForbidden, w.Code)
}

// TestAuthorizeForTask_NoClaimsSkipsEnforcement confirms the check is skipped
// when no middleware injected claims (the test/unauthenticated path).
func TestAuthorizeForTask_NoClaimsSkipsEnforcement(t *testing.T) {
	r := buildRouter(t, task("t1", "alpha"))
	req := httptest.NewRequest(http.MethodPatch, "/tasks/t1",
		strings.NewReader(`{"resultSummary":"progress"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}
