package accountusage

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type memBundle struct {
	mu     sync.Mutex
	b      SecretBundle
	saves  int
	loadEr error
	saveEr error
}

func (m *memBundle) load(context.Context) (SecretBundle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.loadEr != nil {
		return SecretBundle{}, m.loadEr
	}
	return m.b, nil
}
func (m *memBundle) save(_ context.Context, b SecretBundle) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveEr != nil {
		return m.saveEr
	}
	m.b = b
	m.saves++
	return nil
}

func refreshSrv(t *testing.T, access, refresh string, expiresIn int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": access, "refresh_token": refresh, "expires_in": expiresIn})
	}))
}

func TestRefreshingSource_ReturnsCurrentWhenFresh(t *testing.T) {
	now := fixedNow
	m := &memBundle{b: SecretBundle{AccessToken: "cur", RefreshToken: "r", ExpiresAt: now().Add(time.Hour)}}
	srv := refreshSrv(t, "SHOULD-NOT-BE-USED", "x", 3600)
	defer srv.Close()
	src := NewRefreshingTokenSource(RefreshingTokenSourceConfig{
		Load: m.load, Save: m.save, HTTP: srv.Client(), Margin: 5 * time.Minute, Now: now,
		Refresh: OAuthRefreshConfig{TokenURL: srv.URL, ClientID: "c"},
	})
	tok, err := src()
	if err != nil || tok != "cur" {
		t.Fatalf("tok=%q err=%v", tok, err)
	}
	if m.saves != 0 {
		t.Fatalf("should not have refreshed/saved, saves=%d", m.saves)
	}
}

func TestRefreshingSource_RefreshesWhenNearExpiryAndPersists(t *testing.T) {
	now := fixedNow
	m := &memBundle{b: SecretBundle{AccessToken: "old", RefreshToken: "r-old", ExpiresAt: now().Add(2 * time.Minute)}} // within 5m margin
	srv := refreshSrv(t, "new-access", "r-new", 3600)
	defer srv.Close()
	src := NewRefreshingTokenSource(RefreshingTokenSourceConfig{
		Load: m.load, Save: m.save, HTTP: srv.Client(), Margin: 5 * time.Minute, Now: now,
		Refresh: OAuthRefreshConfig{TokenURL: srv.URL, ClientID: "c"},
	})
	tok, err := src()
	if err != nil || tok != "new-access" {
		t.Fatalf("tok=%q err=%v", tok, err)
	}
	if m.b.AccessToken != "new-access" || m.b.RefreshToken != "r-new" || m.saves != 1 {
		t.Fatalf("bundle not persisted: %+v saves=%d", m.b, m.saves)
	}
	if want := now().Add(3600 * time.Second); !m.b.ExpiresAt.Equal(want) {
		t.Fatalf("expiresAt=%v want %v", m.b.ExpiresAt, want)
	}
}

func TestRefreshingSource_RefreshFailWithValidAccessReturnsOld(t *testing.T) {
	now := fixedNow
	// access still valid (past margin but not expired): now+2m, margin 1m -> within margin so it tries refresh; refresh fails; access not yet hard-expired -> return old
	m := &memBundle{b: SecretBundle{AccessToken: "still-ok", RefreshToken: "r", ExpiresAt: now().Add(2 * time.Minute)}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer srv.Close()
	src := NewRefreshingTokenSource(RefreshingTokenSourceConfig{
		Load: m.load, Save: m.save, HTTP: srv.Client(), Margin: 5 * time.Minute, Now: now,
		Refresh: OAuthRefreshConfig{TokenURL: srv.URL, ClientID: "c"},
	})
	tok, err := src()
	if err != nil || tok != "still-ok" {
		t.Fatalf("expected stale access fallback, tok=%q err=%v", tok, err)
	}
}

func TestRefreshingSource_RefreshFailWithExpiredAccessErrors(t *testing.T) {
	now := fixedNow
	m := &memBundle{b: SecretBundle{AccessToken: "dead", RefreshToken: "r", ExpiresAt: now().Add(-time.Minute)}} // already expired
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer srv.Close()
	src := NewRefreshingTokenSource(RefreshingTokenSourceConfig{
		Load: m.load, Save: m.save, HTTP: srv.Client(), Margin: 5 * time.Minute, Now: now,
		Refresh: OAuthRefreshConfig{TokenURL: srv.URL, ClientID: "c"},
	})
	if _, err := src(); err == nil {
		t.Fatal("expected error when access expired and refresh fails")
	}
}

func TestRefreshingSource_LoadErrorPropagates(t *testing.T) {
	m := &memBundle{loadEr: errors.New("boom")}
	src := NewRefreshingTokenSource(RefreshingTokenSourceConfig{Load: m.load, Save: m.save, Now: fixedNow, Margin: time.Minute})
	if _, err := src(); err == nil {
		t.Fatal("expected load error")
	}
}
