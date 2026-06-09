package ingest

import "testing"

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
