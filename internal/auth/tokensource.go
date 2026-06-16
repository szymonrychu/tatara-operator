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
// passing the target audience as a Keycloak "audience" form value. Each Token
// call derives a fresh per-call source bound to the caller's context so
// cancellations and deadlines are honoured. Caching is per-call (no cross-call
// token reuse), but the baked tokenMintTimeout caps the worst-case latency.
type TokenSource struct {
	cfg        clientcredentials.Config
	httpClient *http.Client
}

// NewTokenSource returns a client-credentials TokenSource.
func NewTokenSource(cfg TokenSourceConfig) *TokenSource {
	c := clientcredentials.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		TokenURL:     cfg.TokenURL,
		EndpointParams: map[string][]string{
			"audience": {cfg.Audience},
		},
	}
	httpClient := &http.Client{Timeout: tokenMintTimeout}
	return &TokenSource{cfg: c, httpClient: httpClient}
}

// Token returns a valid bearer access token. The caller's ctx is honoured:
// cancellation or deadline expiry aborts the mint immediately.
func (t *TokenSource) Token(ctx context.Context) (string, error) {
	ctxWithClient := context.WithValue(ctx, oauth2.HTTPClient, t.httpClient)
	tok, err := t.cfg.TokenSource(ctxWithClient).Token()
	if err != nil {
		return "", fmt.Errorf("auth: mint client-credentials token: %w", err)
	}
	return tok.AccessToken, nil
}
