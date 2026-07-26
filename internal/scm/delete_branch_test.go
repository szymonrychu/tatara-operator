package scm

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// TestGitHubDeleteBranch pins the ref-delete call shape and the two ways GitHub
// says "that branch is already gone" (issue #443). The reaper's
// isPermanentTargetGone predicate keys on 404/410, so a permanently-gone branch
// MUST surface as one of those - anything else requeues the reap forever.
func TestGitHubDeleteBranch(t *testing.T) {
	t.Run("deletes the ref", func(t *testing.T) {
		var gotMethod, gotPath string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotMethod, gotPath = r.Method, r.URL.Path
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()
		c := &GitHub{apiBase: srv.URL}
		if err := c.DeleteBranch(context.Background(), "https://github.com/o/r", "tok", "tatara/task-x"); err != nil {
			t.Fatalf("DeleteBranch: %v", err)
		}
		if gotMethod != http.MethodDelete || gotPath != "/repos/o/r/git/refs/heads/tatara/task-x" {
			t.Fatalf("method=%q path=%q", gotMethod, gotPath)
		}
	})

	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"404 is gone", http.StatusNotFound, `{"message":"Not Found"}`},
		{"422 reference does not exist is gone", http.StatusUnprocessableEntity, `{"message":"Reference does not exist"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			c := &GitHub{apiBase: srv.URL}
			err := c.DeleteBranch(context.Background(), "https://github.com/o/r", "tok", "tatara/task-x")
			var he *HTTPError
			if !errors.As(err, &he) {
				t.Fatalf("err = %v, want an *HTTPError", err)
			}
			if he.Status != http.StatusNotFound && he.Status != http.StatusGone {
				t.Fatalf("status = %d, want 404/410 so the reaper reads it as permanently gone", he.Status)
			}
		})
	}

	t.Run("403 stays an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"Resource not accessible"}`))
		}))
		defer srv.Close()
		c := &GitHub{apiBase: srv.URL}
		err := c.DeleteBranch(context.Background(), "https://github.com/o/r", "tok", "tatara/task-x")
		var he *HTTPError
		if !errors.As(err, &he) || he.Status != http.StatusForbidden {
			t.Fatalf("err = %v, want a 403 *HTTPError (a permission problem is NOT a gone branch)", err)
		}
	})
}

// TestGitLabDeleteBranch pins the GitLab equivalent: the branch name is a PATH
// SEGMENT there, so it must be escaped or "tatara/task-x" splits into two.
func TestGitLabDeleteBranch(t *testing.T) {
	t.Run("deletes the branch", func(t *testing.T) {
		var gotMethod, gotPath string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotMethod, gotPath = r.Method, r.URL.EscapedPath()
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()
		c := &GitLab{apiBase: srv.URL}
		if err := c.DeleteBranch(context.Background(), "https://gitlab.com/g/p", "tok", "tatara/task-x"); err != nil {
			t.Fatalf("DeleteBranch: %v", err)
		}
		want := "/projects/" + url.PathEscape("g/p") + "/repository/branches/" + url.PathEscape("tatara/task-x")
		if gotMethod != http.MethodDelete || gotPath != want {
			t.Fatalf("method=%q path=%q, want DELETE %s", gotMethod, gotPath, want)
		}
	})

	t.Run("404 is gone", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"404 Branch Not Found"}`))
		}))
		defer srv.Close()
		c := &GitLab{apiBase: srv.URL}
		err := c.DeleteBranch(context.Background(), "https://gitlab.com/g/p", "tok", "tatara/task-x")
		var he *HTTPError
		if !errors.As(err, &he) || he.Status != http.StatusNotFound {
			t.Fatalf("err = %v, want a 404 *HTTPError", err)
		}
	})
}
