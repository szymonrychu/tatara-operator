package accountusage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// OAuthToken is a Claude Code OAuth credential bundle. Access tokens are
// short-lived and refreshed via the refresh token; the refresh token itself
// rotates on each refresh (single-use), so callers MUST persist the returned
// RefreshToken.
type OAuthToken struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    time.Time
}

// OAuthRefreshConfig configures the refresh call. TokenURL and ClientID are the
// Claude Code OAuth token endpoint and public client id; both are injected
// (never hardcoded in logic) so they stay configurable and testable.
type OAuthRefreshConfig struct {
	TokenURL  string
	ClientID  string
	UserAgent string
}

type refreshResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
}

// RefreshOAuth exchanges a refresh token for a fresh access token via the OAuth
// refresh_token grant. If the response omits a rotated refresh_token, the caller's
// existing one is preserved. now supplies the clock for ExpiresAt (testable).
func RefreshOAuth(ctx context.Context, httpc *http.Client, cfg OAuthRefreshConfig, refreshToken string, now func() time.Time) (OAuthToken, error) {
	if now == nil {
		now = time.Now
	}
	if httpc == nil {
		httpc = &http.Client{Timeout: requestTimeout}
	}
	body, err := json.Marshal(map[string]string{
		"grant_type":    "refresh_token",
		"refresh_token": refreshToken,
		"client_id":     cfg.ClientID,
	})
	if err != nil {
		return OAuthToken{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.TokenURL, bytes.NewReader(body))
	if err != nil {
		return OAuthToken{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.UserAgent != "" {
		req.Header.Set("User-Agent", cfg.UserAgent)
	}
	resp, err := httpc.Do(req)
	if err != nil {
		return OAuthToken{}, fmt.Errorf("accountusage: oauth refresh: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode != http.StatusOK {
		return OAuthToken{}, fmt.Errorf("accountusage: oauth refresh status %d: %s", resp.StatusCode, string(raw))
	}
	var rr refreshResponse
	if err := json.Unmarshal(raw, &rr); err != nil {
		return OAuthToken{}, fmt.Errorf("accountusage: oauth refresh decode: %w", err)
	}
	if rr.AccessToken == "" {
		return OAuthToken{}, fmt.Errorf("accountusage: oauth refresh returned no access_token")
	}
	out := OAuthToken{
		AccessToken:  rr.AccessToken,
		RefreshToken: rr.RefreshToken,
		ExpiresAt:    now().Add(time.Duration(rr.ExpiresIn) * time.Second),
	}
	if out.RefreshToken == "" {
		out.RefreshToken = refreshToken // no rotation this time; keep the current one
	}
	return out, nil
}
