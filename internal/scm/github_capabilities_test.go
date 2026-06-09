package scm

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGitHubIssueAuthor(t *testing.T) {
	t.Run("CreateIssue", func(t *testing.T) {
		var gotPath, gotAuth string
		var gotBody map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")
			b, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(b, &gotBody)
			_ = json.NewEncoder(w).Encode(map[string]any{"number": 42, "html_url": "https://gh/o/r/issues/42"})
		}))
		defer srv.Close()
		c := &GitHub{apiBase: srv.URL}
		ref, err := c.CreateIssue(context.Background(), "https://github.com/o/r", "tok", IssueReq{Title: "T", Body: "B", Labels: []string{"l1"}})
		if err != nil {
			t.Fatalf("CreateIssue: %v", err)
		}
		if gotPath != "/repos/o/r/issues" {
			t.Fatalf("path = %q", gotPath)
		}
		if gotAuth != "Bearer tok" {
			t.Fatalf("auth = %q", gotAuth)
		}
		if gotBody["title"] != "T" || gotBody["body"] != "B" {
			t.Fatalf("body = %+v", gotBody)
		}
		labels, _ := gotBody["labels"].([]any)
		if len(labels) != 1 || labels[0] != "l1" {
			t.Fatalf("labels = %+v", gotBody["labels"])
		}
		if ref.Ref != "o/r#42" || ref.URL != "https://gh/o/r/issues/42" {
			t.Fatalf("ref = %+v", ref)
		}
	})
	t.Run("AddLabel", func(t *testing.T) {
		var gotPath string
		var gotBody map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			b, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(b, &gotBody)
		}))
		defer srv.Close()
		c := &GitHub{apiBase: srv.URL}
		if err := c.AddLabel(context.Background(), "tok", "o/r#7", "tatara/awaiting-approval"); err != nil {
			t.Fatalf("AddLabel: %v", err)
		}
		if gotPath != "/repos/o/r/issues/7/labels" {
			t.Fatalf("path = %q", gotPath)
		}
		labels, _ := gotBody["labels"].([]any)
		if len(labels) != 1 || labels[0] != "tatara/awaiting-approval" {
			t.Fatalf("labels = %+v", gotBody["labels"])
		}
	})
	t.Run("RemoveLabel", func(t *testing.T) {
		var gotPath, gotMethod string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath, gotMethod = r.URL.Path, r.Method
		}))
		defer srv.Close()
		c := &GitHub{apiBase: srv.URL}
		if err := c.RemoveLabel(context.Background(), "tok", "o/r#7", "tatara/awaiting-approval"); err != nil {
			t.Fatalf("RemoveLabel: %v", err)
		}
		if gotMethod != http.MethodDelete {
			t.Fatalf("method = %q", gotMethod)
		}
		if gotPath != "/repos/o/r/issues/7/labels/tatara/awaiting-approval" {
			t.Fatalf("path = %q", gotPath)
		}
	})
}
