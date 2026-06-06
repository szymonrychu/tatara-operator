package auth

import (
	"context"
	"fmt"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Verifier validates JWT bearer tokens against an OIDC provider.
type Verifier struct {
	cfg      Config
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
}

// Claims holds the parsed and validated token claims.
type Claims struct {
	Subject           string `json:"sub"`
	PreferredUsername string `json:"preferred_username"`
	Issuer            string `json:"iss"`
}

// NewVerifier discovers the OIDC provider at cfg.Issuer and returns a ready Verifier.
func NewVerifier(ctx context.Context, cfg Config) (*Verifier, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("auth: discover issuer: %w", err)
	}
	v := provider.Verifier(&oidc.Config{ClientID: cfg.Audience})
	return &Verifier{cfg: cfg, provider: provider, verifier: v}, nil
}

// Verify validates raw and returns parsed claims on success.
func (v *Verifier) Verify(ctx context.Context, raw string) (*Claims, error) {
	tok, err := v.verifier.Verify(ctx, raw)
	if err != nil {
		return nil, fmt.Errorf("auth: verify token: %w", err)
	}
	var c Claims
	if err := tok.Claims(&c); err != nil {
		return nil, fmt.Errorf("auth: decode claims: %w", err)
	}
	c.Issuer = tok.Issuer
	c.Subject = tok.Subject
	if c.Subject == "" {
		return nil, fmt.Errorf("auth: missing sub claim")
	}
	return &c, nil
}
