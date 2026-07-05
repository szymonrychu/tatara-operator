package accountusage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func fixedNow() time.Time { return time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC) }

func TestRefreshOAuth_Success_RotatesAndSetsExpiry(t *testing.T) {
	var gotBody map[string]any
	var gotCT, gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		gotUA = r.Header.Get("User-Agent")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "sk-ant-oat-new",
			"refresh_token": "sk-ant-ort-new",
			"expires_in":    3600,
		})
	}))
	defer srv.Close()

	tok, err := RefreshOAuth(context.Background(), srv.Client(), OAuthRefreshConfig{
		TokenURL:  srv.URL,
		ClientID:  "client-xyz",
		UserAgent: "claude-code/1.0.0",
	}, "sk-ant-ort-old", fixedNow)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if tok.AccessToken != "sk-ant-oat-new" || tok.RefreshToken != "sk-ant-ort-new" {
		t.Fatalf("tokens not parsed: %+v", tok)
	}
	if want := fixedNow().Add(3600 * time.Second); !tok.ExpiresAt.Equal(want) {
		t.Fatalf("expiresAt = %v, want %v", tok.ExpiresAt, want)
	}
	if gotBody["grant_type"] != "refresh_token" || gotBody["refresh_token"] != "sk-ant-ort-old" || gotBody["client_id"] != "client-xyz" {
		t.Fatalf("bad refresh body: %+v", gotBody)
	}
	if gotCT != "application/json" || gotUA != "claude-code/1.0.0" {
		t.Fatalf("bad headers: ct=%q ua=%q", gotCT, gotUA)
	}
}

func TestRefreshOAuth_NoRotation_KeepsOldRefresh(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// server omits refresh_token (no rotation)
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "a2", "expires_in": 60})
	}))
	defer srv.Close()
	tok, err := RefreshOAuth(context.Background(), srv.Client(), OAuthRefreshConfig{TokenURL: srv.URL, ClientID: "c"}, "keep-me", fixedNow)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if tok.RefreshToken != "keep-me" {
		t.Fatalf("expected old refresh kept, got %q", tok.RefreshToken)
	}
}

func TestRefreshOAuth_NonOK_Errors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
	}))
	defer srv.Close()
	_, err := RefreshOAuth(context.Background(), srv.Client(), OAuthRefreshConfig{TokenURL: srv.URL, ClientID: "c"}, "r", fixedNow)
	if err == nil {
		t.Fatal("expected error on 401")
	}
}
