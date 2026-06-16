package auth

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/szymonrychu/tatara-operator/internal/obs"
)

type ctxKey struct{}

// ClaimsFromContext retrieves validated claims from the request context.
func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	c, ok := ctx.Value(ctxKey{}).(*Claims)
	return c, ok
}

const wwwAuthenticate = `Bearer realm="tatara-operator"`

// Middleware returns a chi-compatible middleware that verifies the Bearer token,
// injects parsed Claims into the request context, and records auth outcomes via m.
func Middleware(v *Verifier, m *obs.OperatorMetrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, reason := bearerToken(r)
			if raw == "" {
				slog.WarnContext(r.Context(), "auth: rejected", "reason", reason)
				m.RecordAuth(reason)
				w.Header().Set("WWW-Authenticate", wwwAuthenticate)
				http.Error(w, "missing bearer token", http.StatusUnauthorized)
				return
			}
			claims, err := v.Verify(r.Context(), raw)
			if err != nil {
				slog.WarnContext(r.Context(), "auth: rejected", "reason", "invalid_token")
				m.RecordAuth("invalid_token")
				w.Header().Set("WWW-Authenticate", wwwAuthenticate)
				http.Error(w, "invalid token", http.StatusUnauthorized)
				return
			}
			m.RecordAuth("accepted")
			ctx := context.WithValue(r.Context(), ctxKey{}, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// bearerToken extracts the token from the Authorization header.
// Returns the token (empty on failure) and a rejection reason string.
func bearerToken(r *http.Request) (string, string) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return "", "missing_token"
	}
	const prefix = "Bearer "
	if len(h) < len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", "invalid_scheme"
	}
	tok := strings.TrimSpace(h[len(prefix):])
	if tok == "" {
		return "", "missing_token"
	}
	return tok, ""
}
