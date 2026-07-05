package scm

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestDoPagedGET_GitHubHeaders exercises doPagedGET with GitHub's header set
// (Authorization bearer token + Accept vnd.github+json) and Link peek header,
// pinning it in isolation ahead of ghDoWithHeaders wiring into it (S25).
func TestDoPagedGET_GitHubHeaders(t *testing.T) {
	var gotAuth, gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotAccept = r.Header.Get("Accept")
		w.Header().Set("Link", `<https://example.com/next>; rel="next"`)
		_, _ = w.Write([]byte(`{"a":1}`))
	}))
	defer srv.Close()

	var out struct {
		A int `json:"a"`
	}
	peeked, err := doPagedGET(context.Background(), srv.URL+"/x", "github", srv.URL+"/x", map[string]string{
		"Authorization": "Bearer tok",
		"Accept":        "application/vnd.github+json",
	}, "Link", &out)
	if err != nil {
		t.Fatalf("doPagedGET: %v", err)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("Authorization header = %q, want %q", gotAuth, "Bearer tok")
	}
	if gotAccept != "application/vnd.github+json" {
		t.Fatalf("Accept header = %q, want %q", gotAccept, "application/vnd.github+json")
	}
	if peeked != `<https://example.com/next>; rel="next"` {
		t.Fatalf("peeked = %q", peeked)
	}
	if out.A != 1 {
		t.Fatalf("decoded out.A = %d, want 1", out.A)
	}
}

// TestDoPagedGET_GitLabHeaders exercises doPagedGET with GitLab's header set
// (PRIVATE-TOKEN + Accept application/json) and X-Next-Page peek header.
func TestDoPagedGET_GitLabHeaders(t *testing.T) {
	var gotToken, gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("PRIVATE-TOKEN")
		gotAccept = r.Header.Get("Accept")
		w.Header().Set("X-Next-Page", "2")
		_, _ = w.Write([]byte(`[{"a":1}]`))
	}))
	defer srv.Close()

	var out []struct {
		A int `json:"a"`
	}
	peeked, err := doPagedGET(context.Background(), srv.URL+"/projects/1/issues", "gitlab", "/projects/1/issues", map[string]string{
		"PRIVATE-TOKEN": "tok",
		"Accept":        "application/json",
	}, "X-Next-Page", &out)
	if err != nil {
		t.Fatalf("doPagedGET: %v", err)
	}
	if gotToken != "tok" {
		t.Fatalf("PRIVATE-TOKEN header = %q, want %q", gotToken, "tok")
	}
	if gotAccept != "application/json" {
		t.Fatalf("Accept header = %q, want %q", gotAccept, "application/json")
	}
	if peeked != "2" {
		t.Fatalf("peeked = %q, want %q", peeked, "2")
	}
	if len(out) != 1 || out[0].A != 1 {
		t.Fatalf("decoded out = %+v", out)
	}
}

// TestDoPagedGET_ErrorPath pins HTTPError.Path to the caller-supplied errPath
// (not necessarily the request URL), and errPrefix to the wrapped error text,
// on a 4xx response.
func TestDoPagedGET_ErrorPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("nope"))
	}))
	defer srv.Close()

	_, err := doPagedGET(context.Background(), srv.URL+"/x", "gitlab", "/x", nil, "X-Next-Page", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *HTTPError, got %T: %v", err, err)
	}
	if he.Status != http.StatusNotFound {
		t.Fatalf("Status = %d, want 404", he.Status)
	}
	if he.Path != "/x" {
		t.Fatalf("Path = %q, want %q", he.Path, "/x")
	}
}
