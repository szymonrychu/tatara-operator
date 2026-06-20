package scm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestGitLabRequestChangesTolerates404Unapprove verifies that RequestChanges
// does not abort when the unapprove call 404s. GitLab returns 404 from
// /merge_requests/:iid/unapprove when the caller never approved the MR, which is
// the common case for the review bot; the request-changes verb (thumbsdown +
// note) must still land.
func TestGitLabRequestChangesTolerates404Unapprove(t *testing.T) {
	var awardPosted, notePosted bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/unapprove"):
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"404 Not Found"}`))
		case strings.HasSuffix(r.URL.Path, "/award_emoji"):
			awardPosted = true
			w.WriteHeader(http.StatusCreated)
		case strings.HasSuffix(r.URL.Path, "/notes"):
			notePosted = true
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	if err := c.RequestChanges(context.Background(), "https://gitlab.com/g/p", "tok", 7, "fix it"); err != nil {
		t.Fatalf("RequestChanges should tolerate a 404 from unapprove: %v", err)
	}
	if !awardPosted || !notePosted {
		t.Fatalf("after tolerated 404 unapprove: award=%v note=%v (both must be true)", awardPosted, notePosted)
	}
}

// TestGitLabRequestChangesPropagatesNon404Unapprove verifies a non-404 unapprove
// error still aborts (not all failures are benign).
func TestGitLabRequestChangesPropagatesNon404Unapprove(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/unapprove") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	if err := c.RequestChanges(context.Background(), "https://gitlab.com/g/p", "tok", 7, "fix it"); err == nil {
		t.Fatal("RequestChanges must propagate a non-404 unapprove error")
	}
}
