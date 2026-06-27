package scm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGitHubEnsureLabel_CreateThenPatchOnConflict(t *testing.T) {
	var patched bool
	conflict := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/labels"):
			if conflict {
				w.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = w.Write([]byte(`{"message":"Validation Failed"}`))
				return
			}
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPatch:
			patched = true
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL, token: "t"}
	if err := c.EnsureLabel(context.Background(), "https://github.com/o/r", "t", "tatara-incident", "d73a4a"); err != nil {
		t.Fatalf("fresh create: %v", err)
	}
	conflict = true
	if err := c.EnsureLabel(context.Background(), "https://github.com/o/r", "t", "tatara-incident", "d73a4a"); err != nil {
		t.Fatalf("existing -> patch: %v", err)
	}
	if !patched {
		t.Fatal("expected PATCH to update color on 422 conflict")
	}
}

func TestGitLabEnsureLabel_CreateThenPutOnConflict(t *testing.T) {
	var put bool
	conflict := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/labels"):
			if conflict {
				w.WriteHeader(http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPut:
			put = true
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "t"}
	if err := c.EnsureLabel(context.Background(), "https://gitlab.com/g/p", "t", "tatara-incident", "d73a4a"); err != nil {
		t.Fatalf("fresh create: %v", err)
	}
	conflict = true
	if err := c.EnsureLabel(context.Background(), "https://gitlab.com/g/p", "t", "tatara-incident", "d73a4a"); err != nil {
		t.Fatalf("existing -> put: %v", err)
	}
	if !put {
		t.Fatal("expected PUT new_color on 409 conflict")
	}
}
