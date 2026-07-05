package accountusage

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// SecretBundle is the persisted OAuth credential the refreshing token source
// reads and writes. Access tokens are short-lived; the refresh token rotates.
type SecretBundle struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    time.Time
}

// RefreshingTokenSourceConfig wires a self-refreshing OAuth token source. Load
// reads the current bundle (e.g. from a k8s Secret) and Save persists a rotated
// one, so the refresh-token chain survives restarts and leader re-election.
type RefreshingTokenSourceConfig struct {
	Load    func(context.Context) (SecretBundle, error)
	Save    func(context.Context, SecretBundle) error
	Refresh OAuthRefreshConfig
	HTTP    *http.Client
	Margin  time.Duration // refresh proactively once now >= ExpiresAt-Margin
	Now     func() time.Time
}

// NewRefreshingTokenSource returns a func() (string, error) suitable for
// accountusage.ClientConfig.TokenSource. On each call it loads the bundle,
// returns the current access token while it is comfortably valid, and otherwise
// exchanges the refresh token for a fresh access token (persisting the rotated
// bundle). If the refresh fails but the current access token has not yet hard-
// expired, it falls back to the current token so a transient refresh outage does
// not immediately blind the poller.
func NewRefreshingTokenSource(cfg RefreshingTokenSourceConfig) func() (string, error) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Margin <= 0 {
		cfg.Margin = 5 * time.Minute
	}
	var mu sync.Mutex
	return func() (string, error) {
		mu.Lock()
		defer mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout+5*time.Second)
		defer cancel()

		b, err := cfg.Load(ctx)
		if err != nil {
			return "", fmt.Errorf("accountusage: load token bundle: %w", err)
		}
		now := cfg.Now()
		if b.AccessToken != "" && now.Before(b.ExpiresAt.Add(-cfg.Margin)) {
			return b.AccessToken, nil // comfortably valid
		}
		if b.RefreshToken == "" {
			return "", fmt.Errorf("accountusage: token bundle has no refresh token (seed the secret from an interactive `claude login`)")
		}
		tok, err := RefreshOAuth(ctx, cfg.HTTP, cfg.Refresh, b.RefreshToken, cfg.Now)
		if err != nil {
			// Refresh failed: if the current access token is still (barely) valid, use
			// it rather than blinding the poller on a transient token-endpoint outage.
			if b.AccessToken != "" && now.Before(b.ExpiresAt) {
				slog.Warn("accountusage token refresh failed, using still-valid access token", "action", "usage_token_refresh_fail_fallback", "error", err)
				return b.AccessToken, nil
			}
			return "", fmt.Errorf("accountusage: token refresh: %w", err)
		}
		nb := SecretBundle(tok)
		if err := cfg.Save(ctx, nb); err != nil {
			// The refresh succeeded but persistence failed. The rotated refresh token
			// is now the only valid one and we could not store it; returning the new
			// access token still lets THIS poll succeed, but warn loudly (rule 12) since
			// a restart before the next successful Save would lose the chain.
			slog.Warn("accountusage refreshed token but failed to persist rotated bundle", "action", "usage_token_persist_fail", "error", err)
		}
		slog.Info("accountusage OAuth access token refreshed", "action", "usage_token_refreshed", "expires_at", nb.ExpiresAt.Format(time.RFC3339))
		return nb.AccessToken, nil
	}
}
