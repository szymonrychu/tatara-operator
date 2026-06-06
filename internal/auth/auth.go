package auth

import "errors"

// Config holds OIDC verifier settings.
type Config struct {
	Issuer   string
	Audience string
}

// Validate returns an error if required fields are missing.
func (c Config) Validate() error {
	if c.Issuer == "" {
		return errors.New("auth: issuer is required")
	}
	if c.Audience == "" {
		return errors.New("auth: audience is required")
	}
	return nil
}
