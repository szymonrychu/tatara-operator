package auth

import (
	"context"
	"fmt"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

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
	return &TokenSource{src: c.TokenSource(context.Background())}
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
