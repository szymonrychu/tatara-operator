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

	v, err := auth.NewVerifier(ctx, auth.Config{Issuer: srv.Issuer(), Audience: "tatara-operator"})
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
	v, err := auth.NewVerifier(ctx, auth.Config{Issuer: srv.Issuer(), Audience: "tatara-operator"})
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

func TestConfig_Validate(t *testing.T) {
	require.Error(t, auth.Config{Audience: "x"}.Validate())
	require.Error(t, auth.Config{Issuer: "x"}.Validate())
	require.NoError(t, auth.Config{Issuer: "x", Audience: "y"}.Validate())
}
