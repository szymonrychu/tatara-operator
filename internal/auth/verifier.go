package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// discoveryTimeout caps the HTTP round-trip to the issuer's discovery document
// and, through the same client, to its JWKS endpoint, so a wedged Keycloak
// cannot hold an HTTP handler indefinitely. Mirrors tokenMintTimeout on the
// token-mint side.
const discoveryTimeout = 10 * time.Second

// ErrDiscovery marks a failure to REACH the OIDC issuer, as opposed to a
// genuinely bad token. Callers match it with errors.Is to answer 503 rather
// than 401: "the identity provider is unreachable" is not "your token is
// invalid", and a caller must be able to tell the two apart.
var ErrDiscovery = errors.New("auth: oidc discovery unavailable")

// Verifier validates JWT bearer tokens against an OIDC provider.
//
// Discovery is lazy. NewVerifier does no network I/O; the provider is
// discovered on the first Verify that needs it and memoized under mu. A FAILED
// discovery is deliberately not memoized, so the next call retries. This is the
// same shape TokenSource uses for the client-credentials mint, and it is why
// mint failures were always survivable while the verifier's were not: an issuer
// blip must degrade the OIDC-gated REST surface for its duration, not crash-loop
// the whole manager and take the reconcilers and HMAC webhook paths with it
// (issue #456).
type Verifier struct {
	cfg Config

	mu       sync.Mutex
	verifier *oidc.IDTokenVerifier
}

// Claims holds the parsed and validated token claims.
// Subject and Issuer are set from the verifier-validated token fields
// (tok.Subject / tok.Issuer), not from the raw JSON payload, so they carry
// no json tags. Only PreferredUsername is decoded via Claims() from the payload.
type Claims struct {
	Subject           string
	PreferredUsername string `json:"preferred_username"`
	Issuer            string
}

// NewVerifier returns a Verifier for cfg without dialing the issuer. Only
// static misconfiguration (missing issuer or audience) is an error here.
func NewVerifier(cfg Config) (*Verifier, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Verifier{cfg: cfg}, nil
}

// WarmUp performs one discovery attempt, so a permanently wrong issuer is loud
// at startup instead of only on the first request. It is advisory: the caller
// logs the failure and carries on, because Verify retries discovery anyway.
func (v *Verifier) WarmUp(ctx context.Context) error {
	_, err := v.idTokenVerifier(ctx)
	return err
}

// idTokenVerifier returns the memoized token verifier, discovering the issuer
// on first use. On failure nothing is cached, so the next caller retries.
func (v *Verifier) idTokenVerifier(ctx context.Context) (*oidc.IDTokenVerifier, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.verifier != nil {
		return v.verifier, nil
	}
	ctx = oidc.ClientContext(ctx, &http.Client{Timeout: discoveryTimeout})
	provider, err := oidc.NewProvider(ctx, v.cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("%w: issuer %s: %w", ErrDiscovery, v.cfg.Issuer, err)
	}
	v.verifier = provider.Verifier(&oidc.Config{ClientID: v.cfg.Audience})
	return v.verifier, nil
}

// Verify validates raw and returns parsed claims on success. A returned error
// wrapping ErrDiscovery means the issuer could not be reached, not that the
// token is bad.
func (v *Verifier) Verify(ctx context.Context, raw string) (*Claims, error) {
	idv, err := v.idTokenVerifier(ctx)
	if err != nil {
		return nil, err
	}
	tok, err := idv.Verify(ctx, raw)
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
