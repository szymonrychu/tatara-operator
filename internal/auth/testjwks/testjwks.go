package testjwks

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

// Server is an in-process OIDC-compatible test server backed by an RSA key pair.
type Server struct {
	t      *testing.T
	srv    *httptest.Server
	key    *rsa.PrivateKey
	kid    string
	issuer string

	closeOnce sync.Once
}

// NewServer creates a new test JWKS server and registers cleanup on t.
func NewServer(t *testing.T) *Server {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	s := &Server{t: t, key: key, kid: "test-kid-1"}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":   s.issuer,
			"jwks_uri": s.issuer + "/jwks.json",
		})
	})
	mux.HandleFunc("/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]any{{
				"kty": "RSA",
				"use": "sig",
				"alg": "RS256",
				"kid": s.kid,
				"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
				"e":   base64.RawURLEncoding.EncodeToString([]byte{1, 0, 1}),
			}},
		})
	})

	s.srv = httptest.NewServer(mux)
	s.issuer = s.srv.URL
	t.Cleanup(s.Close)
	return s
}

// Close shuts down the test server. Idempotent.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		s.srv.Close()
	})
}

// Issuer returns the base URL of the test server (acts as OIDC issuer).
func (s *Server) Issuer() string { return s.issuer }

// Claims holds parameters for signing a test token.
type Claims struct {
	Issuer    string
	Audience  []string
	Subject   string
	NotBefore time.Time
	IssuedAt  time.Time
	ExpiresAt time.Time
	Extra     map[string]any
}

// SignTypedToken signs a JWT from a strongly-typed Claims struct.
func (s *Server) SignTypedToken(t *testing.T, c Claims) string {
	t.Helper()
	now := time.Now()
	if c.IssuedAt.IsZero() {
		c.IssuedAt = now
	}
	if c.NotBefore.IsZero() {
		c.NotBefore = now
	}
	if c.ExpiresAt.IsZero() {
		c.ExpiresAt = now.Add(time.Hour)
	}

	mc := jwt.MapClaims{
		"iss": c.Issuer,
		"aud": c.Audience,
		"sub": c.Subject,
		"iat": c.IssuedAt.Unix(),
		"nbf": c.NotBefore.Unix(),
		"exp": c.ExpiresAt.Unix(),
	}
	for k, v := range c.Extra {
		mc[k] = v
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, mc)
	tok.Header["kid"] = s.kid
	signed, err := tok.SignedString(s.key)
	require.NoError(t, err)
	return signed
}

// SignTokenWithKey signs a JWT with a foreign RSA key (for bad-signature tests).
func (s *Server) SignTokenWithKey(t *testing.T, key *rsa.PrivateKey, c Claims) string {
	t.Helper()
	now := time.Now()
	if c.IssuedAt.IsZero() {
		c.IssuedAt = now
	}
	if c.NotBefore.IsZero() {
		c.NotBefore = now
	}
	if c.ExpiresAt.IsZero() {
		c.ExpiresAt = now.Add(time.Hour)
	}

	mc := jwt.MapClaims{
		"iss": c.Issuer,
		"aud": c.Audience,
		"sub": c.Subject,
		"iat": c.IssuedAt.Unix(),
		"nbf": c.NotBefore.Unix(),
		"exp": c.ExpiresAt.Unix(),
	}
	for k, v := range c.Extra {
		mc[k] = v
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, mc)
	tok.Header["kid"] = s.kid
	signed, err := tok.SignedString(key)
	require.NoError(t, err)
	return signed
}
