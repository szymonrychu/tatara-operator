package ingest

import (
	"strings"
	"testing"
)

func TestNamespacePath(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{"https github with .git", "https://github.com/szymonrychu/tatara-cli.git", "szymonrychu/tatara-cli"},
		{"https github no .git", "https://github.com/szymonrychu/tatara-cli", "szymonrychu/tatara-cli"},
		{"https gitlab subgroups", "https://gitlab.com/szymonrychu/infra/helmfile", "szymonrychu/infra/helmfile"},
		{"scp-like git@", "git@github.com:szymonrychu/tatara-cli.git", "szymonrychu/tatara-cli"},
		{"ssh url with port", "ssh://git@host:22/group/sub/repo.git", "group/sub/repo"},
		{"trailing slash", "https://github.com/acme/widgets/", "acme/widgets"},
		{"https with userinfo", "https://x-access-token@github.com/acme/widgets.git", "acme/widgets"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := namespacePath(tt.url); got != tt.want {
				t.Errorf("namespacePath(%q) = %q, want %q", tt.url, got, tt.want)
			}
		})
	}
}

// TestNamespacePath_DegenerateURL verifies the known degenerate cases: URLs
// with no path component return an empty string or a host-only string (no
// slash). BuildJob must detect these and use a per-repo fallback to prevent
// clones colliding at /workspace or /workspace/<host>.
func TestNamespacePath_DegenerateURL(t *testing.T) {
	degenerate := []struct {
		name string
		url  string
	}{
		{"bare host", "https://github.com"},
		{"bare host with trailing slash", "https://github.com/"},
		{"empty string", ""},
	}
	for _, tt := range degenerate {
		t.Run(tt.name, func(t *testing.T) {
			got := namespacePath(tt.url)
			// The result must be empty or contain no slash (host-only),
			// confirming that BuildJob's guard is needed.
			if got != "" && strings.Contains(got, "/") {
				t.Errorf("namespacePath(%q) = %q: expected empty or no-slash result for degenerate URL", tt.url, got)
			}
		})
	}
}
