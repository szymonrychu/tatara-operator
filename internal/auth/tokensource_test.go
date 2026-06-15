package auth_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/internal/auth"
)

func TestTokenSource_MintsAndSendsAudience(t *testing.T) {
	var gotForm url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		gotForm = r.Form
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "minted-token",
			"token_type":   "Bearer",
			"expires_in":   300,
		})
	}))
	defer srv.Close()

	ts := auth.NewTokenSource(auth.TokenSourceConfig{
		TokenURL:     srv.URL,
		ClientID:     "tatara-operator",
		ClientSecret: "shh",
		Audience:     "tatara-memory",
	})

	tok, err := ts.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, "minted-token", tok)
	require.Equal(t, "client_credentials", gotForm.Get("grant_type"))
	require.Equal(t, "tatara-memory", gotForm.Get("audience"))
}

func TestTokenSource_PropagatesError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusUnauthorized)
	}))
	defer srv.Close()

	ts := auth.NewTokenSource(auth.TokenSourceConfig{
		TokenURL:     srv.URL,
		ClientID:     "tatara-operator",
		ClientSecret: "wrong",
		Audience:     "tatara-memory",
	})

	_, err := ts.Token(context.Background())
	require.Error(t, err)
}

// TestTokenSource_RespectsHTTPTimeout verifies that a hung token endpoint does
// not block the caller indefinitely. The HTTP client baked into the source must
// carry a finite timeout so a slow/wedged Keycloak cannot hold a reconcile
// worker forever.
func TestTokenSource_RespectsHTTPTimeout(t *testing.T) {
	// Server hangs until the test ends; the TokenSource timeout must fire first.
	unblock := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-unblock // block forever
	}))
	defer func() {
		close(unblock)
		srv.Close()
	}()

	ts := auth.NewTokenSource(auth.TokenSourceConfig{
		TokenURL:     srv.URL,
		ClientID:     "tatara-operator",
		ClientSecret: "shh",
		Audience:     "tatara-memory",
	})

	start := time.Now()
	// Give generous headroom; the baked timeout must be << this.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := ts.Token(ctx)
	elapsed := time.Since(start)

	require.Error(t, err, "expected error from hung token endpoint")
	// oauth2 may probe auth-style twice on first call, so allow 2*tokenMintTimeout + buffer.
	// Without any timeout the call would block until the test deadline (60 s).
	require.Less(t, elapsed, 25*time.Second,
		"Token() hung for %s - HTTP timeout not applied", elapsed)
}
