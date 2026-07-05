package accountusage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	defaultBaseURL = "https://api.anthropic.com"
	usagePath      = "/api/oauth/usage"
	anthropicBeta  = "oauth-2025-04-20"
	requestTimeout = 15 * time.Second
)

type ClientConfig struct {
	BaseURL     string
	TokenSource func() (string, error)
	UserAgent   string
	AuthMode    string // "bearer" (default) or "x-api-key"
	HTTP        *http.Client
}

type Client struct {
	base  string
	token func() (string, error)
	ua    string
	auth  string
	http  *http.Client
}

func NewClient(cfg ClientConfig) *Client {
	base := cfg.BaseURL
	if base == "" {
		base = defaultBaseURL
	}
	h := cfg.HTTP
	if h == nil {
		h = &http.Client{Timeout: requestTimeout}
	}
	auth := cfg.AuthMode
	if auth == "" {
		auth = "bearer"
	}
	return &Client{base: base, token: cfg.TokenSource, ua: cfg.UserAgent, auth: auth, http: h}
}

type usageWindow struct {
	Utilization float64 `json:"utilization"`
	ResetsAt    string  `json:"resets_at"`
}

type usageResponse struct {
	FiveHour *usageWindow `json:"five_hour"`
	SevenDay *usageWindow `json:"seven_day"`
	Opus     *usageWindow `json:"seven_day_opus"`
	Sonnet   *usageWindow `json:"seven_day_sonnet"`
	Extra    *struct {
		Enabled      bool    `json:"is_enabled"`
		MonthlyLimit float64 `json:"monthly_limit"`
		UsedCredits  float64 `json:"used_credits"`
		Utilization  float64 `json:"utilization"`
	} `json:"extra_usage"`
}

func (c *Client) Fetch(ctx context.Context) (Snapshot, error) {
	tok, err := c.token()
	if err != nil {
		return Snapshot{}, fmt.Errorf("accountusage: token: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+usagePath, nil)
	if err != nil {
		return Snapshot{}, err
	}
	req.Header.Set("User-Agent", c.ua)
	req.Header.Set("Content-Type", "application/json")
	if c.auth == "x-api-key" {
		req.Header.Set("x-api-key", tok)
		req.Header.Set("anthropic-beta", "claude-code-20250219,"+anthropicBeta)
	} else {
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("anthropic-beta", anthropicBeta)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Snapshot{}, fmt.Errorf("accountusage: request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode != http.StatusOK {
		return Snapshot{}, fmt.Errorf("accountusage: status %d: %s", resp.StatusCode, string(body))
	}
	var ur usageResponse
	if err := json.Unmarshal(body, &ur); err != nil {
		return Snapshot{}, fmt.Errorf("accountusage: decode: %w", err)
	}
	if ur.FiveHour == nil && ur.SevenDay == nil {
		return Snapshot{}, fmt.Errorf("accountusage: schema drift: no five_hour/seven_day fields")
	}
	snap := Snapshot{Healthy: true, UpdatedAt: time.Now()}
	snap.FiveHour = toWindow(ur.FiveHour)
	snap.Weekly = toWindow(ur.SevenDay)
	snap.Opus = toWindow(ur.Opus)
	snap.Sonnet = toWindow(ur.Sonnet)
	if ur.Extra != nil {
		snap.Overage = Overage{Enabled: ur.Extra.Enabled, Percent: normalizeUtil(ur.Extra.Utilization), Used: ur.Extra.UsedCredits, Limit: ur.Extra.MonthlyLimit}
	}
	return snap, nil
}

func toWindow(w *usageWindow) Window {
	if w == nil {
		return Window{}
	}
	out := Window{Percent: normalizeUtil(w.Utilization)}
	if w.ResetsAt != "" {
		if t, err := time.Parse(time.RFC3339, w.ResetsAt); err == nil {
			out.Reset = t
		}
	}
	return out
}

// normalizeUtil returns the utilization as a 0..100 percent. The 2026-07-05
// in-cluster spike confirmed /api/oauth/usage already reports whole-number
// percentages (e.g. five_hour=2.0, seven_day=36.0), NOT a 0..1 fraction, so no
// scaling is applied; values are clamped to [0,100] as a defensive bound.
func normalizeUtil(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}
