package auth_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/internal/auth"
	"github.com/szymonrychu/tatara-operator/internal/auth/testjwks"
)

func setupMiddlewareTest(t *testing.T) (*auth.Verifier, *testjwks.Server) {
	t.Helper()
	srv := testjwks.NewServer(t)
	v, err := auth.NewVerifier(context.Background(), auth.Config{
		Issuer:   srv.Issuer(),
		Audience: "tatara-operator",
	})
	require.NoError(t, err)
	return v, srv
}

func okHandler(t *testing.T, called *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*called = true
		w.WriteHeader(http.StatusOK)
	})
}

func TestMiddleware_RejectionPaths(t *testing.T) {
	v, srv := setupMiddlewareTest(t)

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
			wrapped := auth.Middleware(v)(okHandler(t, &handlerRan))

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
