package auth

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// tokenMintTimeout caps the HTTP round-trip to the OIDC token endpoint so a
// slow or wedged Keycloak cannot hold a controller-runtime reconcile worker
// indefinitely.
const tokenMintTimeout = 10 * time.Second

// expirySlack is how far before the token's stated expiry we treat it as stale
// and mint a fresh one. Matches the oauth2 library's own default.
const expirySlack = 10 * time.Second

// TokenSourceConfig configures a client-credentials TokenSource.
type TokenSourceConfig struct {
	TokenURL     string
	ClientID     string
	ClientSecret string
	Audience     string
}

// TokenSource mints bearer tokens via the OIDC client-credentials grant,
// passing the target audience as a Keycloak "audience" form value. Tokens are
// cached across calls until near-expiry, so repeated calls within a token
// lifetime make no HTTP round-trip. The caller's ctx is honoured for every
// real mint; the baked tokenMintTimeout provides an absolute cap.
type TokenSource struct {
	cfg        clientcredentials.Config
	httpClient *http.Client

	mu     sync.Mutex
	cached *oauth2.Token
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

// Token returns a valid bearer access token. The token is cached until
// expirySlack before its stated expiry; only a cache miss mints a new token
// via the Keycloak token endpoint. The caller's ctx governs the HTTP
// round-trip, so cancellation and deadlines are honoured on every mint.
func (t *TokenSource) Token(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.cached != nil && t.cached.Expiry.After(time.Now().Add(expirySlack)) {
		return t.cached.AccessToken, nil
	}

	ctxWithClient := context.WithValue(ctx, oauth2.HTTPClient, t.httpClient)
	tok, err := t.cfg.TokenSource(ctxWithClient).Token()
	if err != nil {
		return "", fmt.Errorf("auth: mint client-credentials token: %w", err)
	}
	t.cached = tok
	return tok.AccessToken, nil
}
