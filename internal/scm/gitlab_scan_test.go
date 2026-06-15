package scm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGitLabListOpenPRs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if glTestPath(r) != "/projects/g%2Fp/merge_requests" || r.URL.Query().Get("state") != "opened" {
			t.Fatalf("path=%q query=%q", glTestPath(r), r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"iid": 5, "sha": "abc", "author": map[string]any{"username": "alice"},
				"labels": []string{"tatara/priority"}, "updated_at": "2026-06-10T12:00:00Z"},
		})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	prs, err := c.ListOpenPRs(context.Background(), "g", "p")
	if err != nil {
		t.Fatalf("ListOpenPRs: %v", err)
	}
	if len(prs) != 1 || prs[0].Repo != "g/p" || prs[0].Number != 5 || prs[0].Author != "alice" || prs[0].HeadSHA != "abc" || prs[0].Labels[0] != "tatara/priority" {
		t.Fatalf("prs = %+v", prs)
	}
}

func TestGitLabListOpenIssues(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if glTestPath(r) != "/projects/g%2Fp/issues" || r.URL.Query().Get("state") != "opened" {
			t.Fatalf("path=%q query=%q", glTestPath(r), r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"iid": 7, "labels": []string{"bug"}, "author": map[string]any{"username": "bob"}, "updated_at": "2026-06-10T12:00:00Z"},
		})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	iss, err := c.ListOpenIssues(context.Background(), "g", "p")
	if err != nil {
		t.Fatalf("ListOpenIssues: %v", err)
	}
	if len(iss) != 1 || iss[0].Repo != "g/p" || iss[0].Number != 7 || iss[0].IsPR || iss[0].Labels[0] != "bug" || iss[0].Author != "bob" {
		t.Fatalf("iss = %+v", iss)
	}
}

func TestGitLabGetIssue(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if glTestPath(r) != "/projects/g%2Fp/issues/21" {
			t.Fatalf("unexpected path: %q", glTestPath(r))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"iid":         21,
			"title":       "gl title",
			"description": "gl body",
		})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	content, err := c.GetIssue(context.Background(), "g/p", "", 21)
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if content.Title != "gl title" {
		t.Errorf("Title = %q, want %q", content.Title, "gl title")
	}
	if content.Body != "gl body" {
		t.Errorf("Body = %q, want %q", content.Body, "gl body")
	}
}

func TestGitLabCloseIssue(t *testing.T) {
	paths := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths[r.Method+" "+glTestPath(r)] = true
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	if err := c.CloseIssue(context.Background(), "tok", "g/p", 7, "closing"); err != nil {
		t.Fatalf("CloseIssue: %v", err)
	}
	noteKey := "POST " + "/projects/" + "g%2Fp" + "/issues/7/notes"
	closeKey := "PUT " + "/projects/" + "g%2Fp" + "/issues/7"
	if !paths[noteKey] {
		t.Fatalf("missing note POST; got %+v", paths)
	}
	if !paths[closeKey] {
		t.Fatalf("missing PUT close; got %+v", paths)
	}
}
