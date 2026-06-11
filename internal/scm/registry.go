package scm

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// Select returns the Client for the provider indicated by request headers.
func Select(h http.Header) (Client, error) {
	switch {
	case h.Get("X-GitHub-Event") != "":
		return &GitHub{}, nil
	case h.Get("X-Gitlab-Event") != "":
		return &GitLab{}, nil
	default:
		return nil, errors.New("scm: unrecognized provider headers")
	}
}

// SameRemote reports whether two git remote URLs refer to the same repository,
// ignoring a trailing .git or /, and lowercasing the host.
func SameRemote(a, b string) bool {
	na, ok1 := normalizeRemote(a)
	nb, ok2 := normalizeRemote(b)
	if !ok1 || !ok2 {
		return false
	}
	return na == nb
}

// ByProvider returns the real SCMWriter for a provider name ("github"|"gitlab").
func ByProvider(name string) (SCMWriter, error) {
	switch name {
	case "github":
		return &GitHub{}, nil
	case "gitlab":
		return &GitLab{}, nil
	default:
		return nil, fmt.Errorf("scm: unknown provider %q", name)
	}
}

// ReaderByProvider returns a token-bound SCMReader for a provider name.
func ReaderByProvider(name, token string) (SCMReader, error) {
	switch name {
	case "github":
		return &GitHub{token: token}, nil
	case "gitlab":
		return &GitLab{token: token}, nil
	default:
		return nil, fmt.Errorf("scm: unknown provider %q", name)
	}
}

func normalizeRemote(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	path := strings.TrimSuffix(u.Path, "/")
	path = strings.TrimSuffix(path, ".git")
	return strings.ToLower(u.Host) + path, true
}
