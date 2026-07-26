package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/internal/auth"
	"github.com/szymonrychu/tatara-operator/internal/auth/testjwks"
)

func TestVerifier_ValidToken(t *testing.T) {
	srv := testjwks.NewServer(t)
	ctx := context.Background()

	v, err := auth.NewVerifier(auth.Config{Issuer: srv.Issuer(), Audience: "tatara-operator"})
	require.NoError(t, err)

	tok := srv.SignTypedToken(t, testjwks.Claims{
		Issuer:   srv.Issuer(),
		Audience: []string{"tatara-operator"},
		Subject:  "agent-1",
		Extra:    map[string]any{"preferred_username": "agent"},
	})

	claims, err := v.Verify(ctx, tok)
	require.NoError(t, err)
	require.Equal(t, "agent-1", claims.Subject)
	require.Equal(t, "agent", claims.PreferredUsername)
}

func TestVerifier_Rejections(t *testing.T) {
	srv := testjwks.NewServer(t)
	ctx := context.Background()
	v, err := auth.NewVerifier(auth.Config{Issuer: srv.Issuer(), Audience: "tatara-operator"})
	require.NoError(t, err)

	foreign, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tests := []struct {
		name string
		sign func() string
	}{
		{
			name: "expired",
			sign: func() string {
				return srv.SignTypedToken(t, testjwks.Claims{
					Issuer:    srv.Issuer(),
					Audience:  []string{"tatara-operator"},
					Subject:   "agent-1",
					IssuedAt:  time.Now().Add(-2 * time.Hour),
					NotBefore: time.Now().Add(-2 * time.Hour),
					ExpiresAt: time.Now().Add(-time.Hour),
				})
			},
		},
		{
			name: "wrong-issuer",
			sign: func() string {
				return srv.SignTypedToken(t, testjwks.Claims{
					Issuer:   "https://evil.example/realms/master",
					Audience: []string{"tatara-operator"},
					Subject:  "agent-1",
				})
			},
		},
		{
			name: "wrong-audience",
			sign: func() string {
				return srv.SignTypedToken(t, testjwks.Claims{
					Issuer:   srv.Issuer(),
					Audience: []string{"some-other-app"},
					Subject:  "agent-1",
				})
			},
		},
		{
			name: "bad-signature",
			sign: func() string {
				return srv.SignTokenWithKey(t, foreign, testjwks.Claims{
					Issuer:   srv.Issuer(),
					Audience: []string{"tatara-operator"},
					Subject:  "agent-1",
				})
			},
		},
		{
			name: "missing-sub",
			sign: func() string {
				return srv.SignTypedToken(t, testjwks.Claims{
					Issuer:   srv.Issuer(),
					Audience: []string{"tatara-operator"},
				})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := v.Verify(ctx, tt.sign())
			require.Error(t, err)
		})
	}
}

// TestVerifier_ConstructionDoesNotDialIssuer is the core of issue #456: building
// the verifier must not touch the network, so an issuer that is down at startup
// cannot take the manager (reconcilers, HMAC webhooks) with it.
func TestVerifier_ConstructionDoesNotDialIssuer(t *testing.T) {
	// 127.0.0.1:1 is a reserved port nothing listens on: connection refused.
	v, err := auth.NewVerifier(auth.Config{Issuer: "http://127.0.0.1:1/realms/master", Audience: "tatara-operator"})
	require.NoError(t, err)
	require.NotNil(t, v)

	// Static misconfiguration is still caught, and still without dialing.
	_, err = auth.NewVerifier(auth.Config{Audience: "tatara-operator"})
	require.Error(t, err)
}

// TestVerifier_RetriesDiscoveryAfterIssuerRecovers proves the failed discovery
// is NOT memoized: the first Verify fails while the issuer is down and a later
// Verify succeeds once it is back, with no restart and no new Verifier.
func TestVerifier_RetriesDiscoveryAfterIssuerRecovers(t *testing.T) {
	srv := testjwks.NewServer(t)
	ctx := context.Background()
	srv.SetDown(true)

	v, err := auth.NewVerifier(auth.Config{Issuer: srv.Issuer(), Audience: "tatara-operator"})
	require.NoError(t, err)
	require.ErrorIs(t, v.WarmUp(ctx), auth.ErrDiscovery, "warm-up must report a reachability failure, not succeed")

	tok := srv.SignTypedToken(t, testjwks.Claims{
		Issuer:   srv.Issuer(),
		Audience: []string{"tatara-operator"},
		Subject:  "agent-1",
		Extra:    map[string]any{"preferred_username": "agent"},
	})

	_, err = v.Verify(ctx, tok)
	require.ErrorIs(t, err, auth.ErrDiscovery, "issuer down must surface as a discovery error, not a bad token")

	srv.SetDown(false)

	claims, err := v.Verify(ctx, tok)
	require.NoError(t, err, "discovery must be retried after the earlier failure")
	require.Equal(t, "agent-1", claims.Subject)

	// And once discovered it stays memoized: the issuer going away again does
	// not break verification of an already-fetched key set.
	claims, err = v.Verify(ctx, tok)
	require.NoError(t, err)
	require.Equal(t, "agent-1", claims.Subject)
}

// TestVerifier_BadTokenIsNotADiscoveryError guards the distinction the
// middleware relies on to choose 503 over 401.
func TestVerifier_BadTokenIsNotADiscoveryError(t *testing.T) {
	srv := testjwks.NewServer(t)
	ctx := context.Background()
	v, err := auth.NewVerifier(auth.Config{Issuer: srv.Issuer(), Audience: "tatara-operator"})
	require.NoError(t, err)

	_, err = v.Verify(ctx, srv.SignTypedToken(t, testjwks.Claims{
		Issuer:   srv.Issuer(),
		Audience: []string{"some-other-app"},
		Subject:  "agent-1",
	}))
	require.Error(t, err)
	require.NotErrorIs(t, err, auth.ErrDiscovery)
}

func TestConfig_Validate(t *testing.T) {
	require.Error(t, auth.Config{Audience: "x"}.Validate())
	require.Error(t, auth.Config{Issuer: "x"}.Validate())
	require.NoError(t, auth.Config{Issuer: "x", Audience: "y"}.Validate())
}
