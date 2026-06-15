package auth

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// tokenMintTimeout caps the HTTP round-trip to the OIDC token endpoint so a
// slow or wedged Keycloak cannot hold a controller-runtime reconcile worker
// indefinitely.
const tokenMintTimeout = 10 * time.Second

// TokenSourceConfig configures a client-credentials TokenSource.
type TokenSourceConfig struct {
	TokenURL     string
	ClientID     string
	ClientSecret string
	Audience     string
}

// TokenSource mints bearer tokens via the OIDC client-credentials grant,
// passing the target audience as a Keycloak "audience" form value. Tokens are
// cached internally by the underlying oauth2 source until near expiry.
type TokenSource struct {
	src oauth2.TokenSource
}

// NewTokenSource returns a caching client-credentials TokenSource.
func NewTokenSource(cfg TokenSourceConfig) *TokenSource {
	c := clientcredentials.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		TokenURL:     cfg.TokenURL,
		EndpointParams: map[string][]string{
			"audience": {cfg.Audience},
		},
	}
	// Bake a finite-timeout HTTP client into the base context so a hung
	// Keycloak cannot block the calling goroutine indefinitely. The
	// ReuseTokenSource cache is preserved by the oauth2 library.
	httpClient := &http.Client{Timeout: tokenMintTimeout}
	baseCtx := context.WithValue(context.Background(), oauth2.HTTPClient, httpClient)
	return &TokenSource{src: c.TokenSource(baseCtx)}
}

// Token returns a valid bearer access token, refreshing if the cached one is
// near expiry.
func (t *TokenSource) Token(ctx context.Context) (string, error) {
	tok, err := t.src.Token()
	if err != nil {
		return "", fmt.Errorf("auth: mint client-credentials token: %w", err)
	}
	return tok.AccessToken, nil
}
