package scm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestGitHubGetDefaultBranchHeadSHA(t *testing.T) {
	tests := []struct {
		name       string
		defBranch  string
		commitSHA  string
		wantSHA    string
		repoStatus int
		wantErr    bool
	}{
		{name: "resolves main head", defBranch: "main", commitSHA: "abc123", wantSHA: "abc123"},
		{name: "non-default branch name", defBranch: "trunk", commitSHA: "def456", wantSHA: "def456"},
		{name: "repo 404 errors", defBranch: "main", repoStatus: http.StatusNotFound, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/repos/o/r":
					if tc.repoStatus != 0 {
						w.WriteHeader(tc.repoStatus)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"default_branch": tc.defBranch})
				case "/repos/o/r/commits/" + tc.defBranch:
					_ = json.NewEncoder(w).Encode(map[string]any{"sha": tc.commitSHA})
				default:
					t.Errorf("unexpected path %q", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer srv.Close()
			c := &GitHub{apiBase: srv.URL}
			got, err := c.GetDefaultBranchHeadSHA(context.Background(), "o", "r")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got sha %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetDefaultBranchHeadSHA: %v", err)
			}
			if got != tc.wantSHA {
				t.Fatalf("sha = %q, want %q", got, tc.wantSHA)
			}
		})
	}
}

func TestGitLabGetDefaultBranchHeadSHA(t *testing.T) {
	const proj = "grp/proj"
	tests := []struct {
		name      string
		defBranch string
		commitID  string
		wantSHA   string
	}{
		{name: "resolves default branch", defBranch: "main", commitID: "sha-main", wantSHA: "sha-main"},
		{name: "custom default branch", defBranch: "develop", commitID: "sha-dev", wantSHA: "sha-dev"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			esc := url.PathEscape(proj)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				rawPath := r.URL.RawPath
				if rawPath == "" {
					rawPath = r.URL.Path
				}
				if rawPath == "/projects/"+esc {
					_ = json.NewEncoder(w).Encode(map[string]any{"default_branch": tc.defBranch})
				} else if strings.HasPrefix(rawPath, "/projects/"+esc+"/repository/branches/") {
					_ = json.NewEncoder(w).Encode(map[string]any{"commit": map[string]any{"id": tc.commitID}})
				} else {
					t.Errorf("unexpected path %q (rawPath=%q)", r.URL.Path, rawPath)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer srv.Close()
			c := &GitLab{apiBase: srv.URL}
			got, err := c.GetDefaultBranchHeadSHA(context.Background(), proj, "")
			if err != nil {
				t.Fatalf("GetDefaultBranchHeadSHA: %v", err)
			}
			if got != tc.wantSHA {
				t.Fatalf("sha = %q, want %q", got, tc.wantSHA)
			}
		})
	}
}
