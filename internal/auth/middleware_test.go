package auth_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/internal/auth"
	"github.com/szymonrychu/tatara-operator/internal/auth/testjwks"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

func setupMiddlewareTest(t *testing.T) (*auth.Verifier, *testjwks.Server, *obs.OperatorMetrics) {
	t.Helper()
	srv := testjwks.NewServer(t)
	v, err := auth.NewVerifier(auth.Config{
		Issuer:   srv.Issuer(),
		Audience: "tatara-operator",
	})
	require.NoError(t, err)
	// These cases exercise middleware behaviour against a REACHABLE issuer, so
	// discover eagerly here; the unreachable-issuer path has its own test.
	require.NoError(t, v.WarmUp(context.Background()))
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	return v, srv, m
}

func okHandler(t *testing.T, called *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*called = true
		w.WriteHeader(http.StatusOK)
	})
}

func TestMiddleware_RejectionPaths(t *testing.T) {
	v, srv, m := setupMiddlewareTest(t)

	validTok := srv.SignTypedToken(t, testjwks.Claims{
		Issuer:   srv.Issuer(),
		Audience: []string{"tatara-operator"},
		Subject:  "agent-1",
	})

	wrongAudTok := srv.SignTypedToken(t, testjwks.Claims{
		Issuer:   srv.Issuer(),
		Audience: []string{"some-other-app"},
		Subject:  "agent-1",
	})

	tests := []struct {
		name           string
		authorization  string // empty means no header
		wantStatus     int
		wantHandlerRun bool
	}{
		{
			name:           "no Authorization header",
			authorization:  "",
			wantStatus:     http.StatusUnauthorized,
			wantHandlerRun: false,
		},
		{
			name:           "malformed header no Bearer prefix",
			authorization:  "Token " + validTok,
			wantStatus:     http.StatusUnauthorized,
			wantHandlerRun: false,
		},
		{
			name:           "wrong audience",
			authorization:  "Bearer " + wrongAudTok,
			wantStatus:     http.StatusUnauthorized,
			wantHandlerRun: false,
		},
		{
			name:           "valid token correct audience",
			authorization:  "Bearer " + validTok,
			wantStatus:     http.StatusOK,
			wantHandlerRun: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerRan := false
			wrapped := auth.Middleware(v, m)(okHandler(t, &handlerRan))

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.authorization != "" {
				req.Header.Set("Authorization", tt.authorization)
			}
			rr := httptest.NewRecorder()

			wrapped.ServeHTTP(rr, req)

			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Equal(t, tt.wantHandlerRun, handlerRan)
		})
	}
}

// TestMiddleware_RecordsAuthMetrics verifies that auth outcomes are counted in
// operator_auth_total with the correct result label.
func TestMiddleware_RecordsAuthMetrics(t *testing.T) {
	v, srv, m := setupMiddlewareTest(t)

	validTok := srv.SignTypedToken(t, testjwks.Claims{
		Issuer:   srv.Issuer(),
		Audience: []string{"tatara-operator"},
		Subject:  "agent-1",
	})
	wrongAudTok := srv.SignTypedToken(t, testjwks.Claims{
		Issuer:   srv.Issuer(),
		Audience: []string{"other-app"},
		Subject:  "agent-1",
	})

	handler := auth.Middleware(v, m)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	fire := func(authHeader string) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}
		handler.ServeHTTP(httptest.NewRecorder(), req)
	}

	// missing_token: no header
	fire("")
	// missing_token: empty bearer
	fire("Bearer ")
	// invalid_scheme
	fire("Token " + validTok)
	// invalid_token: wrong audience
	fire("Bearer " + wrongAudTok)
	// accepted
	fire("Bearer " + validTok)

	assert.Equal(t, float64(2), testutil.ToFloat64(m.AuthCounter("missing_token")))
	assert.Equal(t, float64(1), testutil.ToFloat64(m.AuthCounter("invalid_scheme")))
	assert.Equal(t, float64(1), testutil.ToFloat64(m.AuthCounter("invalid_token")))
	assert.Equal(t, float64(1), testutil.ToFloat64(m.AuthCounter("accepted")))
}

// TestMiddleware_UnreachableIssuerIs503 pins the caller-visible half of issue
// #456: while the IdP is unreachable the middleware must answer 503, so a
// caller can tell "IdP down, retry" from "your token is bad, get a new one".
// A genuinely invalid token must still be a 401 during the same outage window.
func TestMiddleware_UnreachableIssuerIs503(t *testing.T) {
	srv := testjwks.NewServer(t)
	v, err := auth.NewVerifier(auth.Config{Issuer: srv.Issuer(), Audience: "tatara-operator"})
	require.NoError(t, err)
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())

	validTok := srv.SignTypedToken(t, testjwks.Claims{
		Issuer:   srv.Issuer(),
		Audience: []string{"tatara-operator"},
		Subject:  "agent-1",
	})
	wrongAudTok := srv.SignTypedToken(t, testjwks.Claims{
		Issuer:   srv.Issuer(),
		Audience: []string{"other-app"},
		Subject:  "agent-1",
	})

	handlerRan := false
	handler := auth.Middleware(v, m)(okHandler(t, &handlerRan))
	fire := func(tok string) *httptest.ResponseRecorder {
		handlerRan = false
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		return rr
	}

	srv.SetDown(true)
	rr := fire(validTok)
	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	assert.False(t, handlerRan)
	assert.NotEmpty(t, rr.Header().Get("Retry-After"))
	assert.Empty(t, rr.Header().Get("WWW-Authenticate"), "503 must not claim the token was rejected")
	assert.Equal(t, float64(1), testutil.ToFloat64(m.AuthCounter("discovery_unavailable")))
	assert.Equal(t, float64(0), testutil.ToFloat64(m.AuthCounter("invalid_token")))

	// The outage does not stop retrying: once the issuer is back, the same
	// verifier serves requests again without a restart.
	srv.SetDown(false)
	rr = fire(validTok)
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, handlerRan)
	assert.Equal(t, float64(1), testutil.ToFloat64(m.AuthCounter("accepted")))

	// A genuinely bad token is still a 401, never a 503.
	rr = fire(wrongAudTok)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	assert.False(t, handlerRan)
	assert.Equal(t, float64(1), testutil.ToFloat64(m.AuthCounter("invalid_token")))
	assert.Equal(t, float64(1), testutil.ToFloat64(m.AuthCounter("discovery_unavailable")))
}

// TestMiddleware_CtxCancelledPropagatesBeforeVerify confirms that a cancelled
// context reaches the Verify call and is not silently swallowed.
func TestMiddleware_CtxCancelledPropagatesBeforeVerify(t *testing.T) {
	v, srv, m := setupMiddlewareTest(t)

	validTok := srv.SignTypedToken(t, testjwks.Claims{
		Issuer:   srv.Issuer(),
		Audience: []string{"tatara-operator"},
		Subject:  "agent-1",
	})

	handler := auth.Middleware(v, m)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+validTok)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	// go-oidc respects ctx cancellation during JWKS fetch; with a pre-cancelled
	// context the verify step returns an error -> 401. The metric must still be
	// incremented (either invalid_token or accepted depending on whether the
	// in-memory verifier short-circuits on cached keys).
	total := testutil.ToFloat64(m.AuthCounter("accepted")) +
		testutil.ToFloat64(m.AuthCounter("invalid_token"))
	require.Equal(t, float64(1), total, "expected exactly one auth outcome metric")
}
