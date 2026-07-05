package accountusage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

const sampleUsage = `{
  "five_hour": {"utilization": 42.5, "resets_at": "2999-01-01T00:00:00Z"},
  "seven_day": {"utilization": 71.0, "resets_at": "2999-01-02T00:00:00Z"},
  "seven_day_opus": {"utilization": 80.0, "resets_at": "2999-01-02T00:00:00Z"},
  "seven_day_sonnet": null,
  "extra_usage": {"is_enabled": true, "monthly_limit": 100, "used_credits": 25, "utilization": 25.0}
}`

func TestFetchParsesWindowsAndHeaders(t *testing.T) {
	var gotUA, gotAuth, gotBeta string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		gotAuth = r.Header.Get("Authorization")
		gotBeta = r.Header.Get("anthropic-beta")
		_, _ = w.Write([]byte(sampleUsage))
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{
		BaseURL:     srv.URL,
		TokenSource: func() (string, error) { return "tok123", nil },
		UserAgent:   "claude-code/9.9.9",
		AuthMode:    "bearer",
	})
	snap, err := c.Fetch(context.Background())
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if snap.FiveHour.Percent != 42.5 || snap.Weekly.Percent != 71.0 || snap.Opus.Percent != 80.0 {
		t.Fatalf("windows: %+v", snap)
	}
	if snap.Sonnet.Percent != 0 { // null -> zero window
		t.Fatalf("null sonnet must be zero, got %+v", snap.Sonnet)
	}
	if !snap.Overage.Enabled || snap.Overage.Percent != 25.0 {
		t.Fatalf("overage: %+v", snap.Overage)
	}
	if !snap.Healthy {
		t.Fatal("successful fetch must be Healthy")
	}
	if gotUA != "claude-code/9.9.9" || gotAuth != "Bearer tok123" || gotBeta != "oauth-2025-04-20" {
		t.Fatalf("headers UA=%q auth=%q beta=%q", gotUA, gotAuth, gotBeta)
	}
}

func TestFetchXAPIKeyMode(t *testing.T) {
	var gotKey, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("x-api-key")
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(sampleUsage))
	}))
	defer srv.Close()
	c := NewClient(ClientConfig{BaseURL: srv.URL, TokenSource: func() (string, error) { return "k", nil }, UserAgent: "claude-cli/1", AuthMode: "x-api-key"})
	if _, err := c.Fetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gotKey != "k" || gotAuth != "" {
		t.Fatalf("x-api-key mode: key=%q auth=%q", gotKey, gotAuth)
	}
}

func TestFetchNon200IsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(429) }))
	defer srv.Close()
	c := NewClient(ClientConfig{BaseURL: srv.URL, TokenSource: func() (string, error) { return "k", nil }, UserAgent: "x", AuthMode: "bearer"})
	if _, err := c.Fetch(context.Background()); err == nil {
		t.Fatal("429 must error")
	}
}

func TestNormalizeUtilizationIsPercentPassthroughClamped(t *testing.T) {
	// The 2026-07-05 spike confirmed /api/oauth/usage reports whole-number
	// percentages (five_hour=2.0, seven_day=36.0), NOT 0..1 fractions, so a small
	// value like 0.42 means 0.42%, not 42%. Passthrough, clamped to [0,100].
	if got := normalizeUtil(0.42); got != 0.42 {
		t.Fatalf("percent passthrough: %v", got)
	}
	if got := normalizeUtil(36.0); got != 36.0 {
		t.Fatalf("percent passthrough: %v", got)
	}
	if got := normalizeUtil(-5); got != 0 {
		t.Fatalf("clamp low: %v", got)
	}
	if got := normalizeUtil(150); got != 100 {
		t.Fatalf("clamp high: %v", got)
	}
}
