package scm

import "testing"

func TestRepoSlugFromURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		want    string
		wantErr bool
	}{
		{name: "github https with .git", url: "https://github.com/acme/repo1.git", want: "acme/repo1"},
		{name: "github https no .git", url: "https://github.com/acme/repo1", want: "acme/repo1"},
		{name: "github trailing slash", url: "https://github.com/acme/repo1/", want: "acme/repo1"},
		{name: "gitlab subgroup", url: "https://gitlab.com/group/subgroup/repo.git", want: "group/subgroup/repo"},
		{name: "github deep path rejected", url: "https://github.com/acme/team/repo.git", wantErr: true},
		{name: "github ssh form rejected", url: "git@github.com:acme/repo1.git", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RepoSlugFromURL(tt.url)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("RepoSlugFromURL(%q) = %q, want error", tt.url, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("RepoSlugFromURL(%q) unexpected error: %v", tt.url, err)
			}
			if got != tt.want {
				t.Errorf("RepoSlugFromURL(%q) = %q, want %q", tt.url, got, tt.want)
			}
		})
	}
}
