package auth_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

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
