package auth

import (
	"context"
	"errors"
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

// ContextWithClaims returns ctx with claims injected under the same key the
// middleware uses, so handlers and downstream authz checks see them. Exported
// for tests and any caller that needs to simulate an authenticated request.
func ContextWithClaims(ctx context.Context, claims *Claims) context.Context {
	return context.WithValue(ctx, ctxKey{}, claims)
}

const wwwAuthenticate = `Bearer realm="tatara-operator"`

// retryAfterSeconds is the Retry-After hint sent with a 503 while the issuer is
// unreachable. Short, because discovery is retried on the very next request.
const retryAfterSeconds = "5"

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
				// An unreachable issuer is OUR problem, not the caller's: answer
				// 503 so the caller can retry, instead of a 401 that says "your
				// token is bad" and makes it throw a valid token away (#456).
				if errors.Is(err, ErrDiscovery) {
					slog.ErrorContext(r.Context(), "auth: identity provider unreachable",
						"reason", "discovery_unavailable", "error", err.Error())
					m.RecordAuth("discovery_unavailable")
					w.Header().Set("Retry-After", retryAfterSeconds)
					http.Error(w, "identity provider unavailable", http.StatusServiceUnavailable)
					return
				}
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
