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

// TestGitLabApproveTolerates401AlreadyApproved verifies Approve does not abort
// when the approve call 401s. GitLab returns 401 from /merge_requests/:iid/approve
// when the caller has already approved the MR (idempotency-via-error); the
// approval already stands, so the verb is benign and the optional note must land.
func TestGitLabApproveTolerates401AlreadyApproved(t *testing.T) {
	var notePosted bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/approve"):
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"401 Unauthorized"}`))
		case strings.HasSuffix(r.URL.Path, "/notes"):
			notePosted = true
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	if err := c.Approve(context.Background(), "https://gitlab.com/g/p", "tok", 7, "lgtm"); err != nil {
		t.Fatalf("Approve should tolerate a 401 already-approved from /approve: %v", err)
	}
	if !notePosted {
		t.Fatal("after a tolerated 401 approve, the note must still post")
	}
}

// TestGitLabApprovePropagatesNon401 verifies a non-401 approve error still aborts.
func TestGitLabApprovePropagatesNon401(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/approve") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	if err := c.Approve(context.Background(), "https://gitlab.com/g/p", "tok", 7, "lgtm"); err == nil {
		t.Fatal("Approve must propagate a non-401 error from /approve")
	}
}

// TestGitLabRequestChangesTolerates404AwardEmoji verifies RequestChanges does not
// abort when the award_emoji call 404s. GitLab returns 404 ("Award Emoji Name has
// already been taken") when the thumbsdown was already awarded on a prior pass;
// it is benign and the note must still land.
func TestGitLabRequestChangesTolerates404AwardEmoji(t *testing.T) {
	var notePosted bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/award_emoji"):
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"404 Award Emoji Name has already been taken Not Found"}`))
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
		t.Fatalf("RequestChanges should tolerate a 404 already-awarded from award_emoji: %v", err)
	}
	if !notePosted {
		t.Fatal("after a tolerated 404 award_emoji, the note must still post")
	}
}

// TestGitLabRequestChangesPropagatesNon404AwardEmoji verifies a non-404 award_emoji
// error still aborts.
func TestGitLabRequestChangesPropagatesNon404AwardEmoji(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/award_emoji") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	if err := c.RequestChanges(context.Background(), "https://gitlab.com/g/p", "tok", 7, "fix it"); err == nil {
		t.Fatal("RequestChanges must propagate a non-404 award_emoji error")
	}
}
