package scm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// glTestPath returns the raw request path, preserving %-encoding (RawPath when set).
func glTestPath(r *http.Request) string {
	if r.URL.RawPath != "" {
		return r.URL.RawPath
	}
	return r.URL.Path
}

func TestGitLabCapabilities(t *testing.T) {
	t.Run("CreateIssue", func(t *testing.T) {
		var gotPath string
		var body map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = glTestPath(r)
			b, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(b, &body)
			_ = json.NewEncoder(w).Encode(map[string]any{"iid": 4, "web_url": "https://gl/g/p/-/issues/4"})
		}))
		defer srv.Close()
		c := &GitLab{apiBase: srv.URL}
		ref, err := c.CreateIssue(context.Background(), "https://gitlab.com/g/p", "tok", IssueReq{Title: "T", Body: "B", Labels: []string{"l1", "l2"}})
		if err != nil {
			t.Fatalf("CreateIssue: %v", err)
		}
		if gotPath != "/projects/"+url.PathEscape("g/p")+"/issues" {
			t.Fatalf("path = %q", gotPath)
		}
		if body["title"] != "T" || body["labels"] != "l1,l2" {
			t.Fatalf("body = %+v", body)
		}
		if ref.Ref != "g/p#4" || ref.URL != "https://gl/g/p/-/issues/4" {
			t.Fatalf("ref = %+v", ref)
		}
	})
	t.Run("Approve", func(t *testing.T) {
		var gotPath string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { gotPath = glTestPath(r) }))
		defer srv.Close()
		c := &GitLab{apiBase: srv.URL}
		if err := c.Approve(context.Background(), "https://gitlab.com/g/p", "tok", 5, ""); err != nil {
			t.Fatalf("Approve: %v", err)
		}
		if gotPath != "/projects/"+url.PathEscape("g/p")+"/merge_requests/5/approve" {
			t.Fatalf("path = %q", gotPath)
		}
	})
	t.Run("RequestChanges", func(t *testing.T) {
		paths := map[string]bool{}
		awards := map[string]any{}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			paths[glTestPath(r)] = true
			if strings.HasSuffix(r.URL.Path, "/award_emoji") {
				b, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(b, &awards)
			}
		}))
		defer srv.Close()
		c := &GitLab{apiBase: srv.URL}
		if err := c.RequestChanges(context.Background(), "https://gitlab.com/g/p", "tok", 5, "nope"); err != nil {
			t.Fatalf("RequestChanges: %v", err)
		}
		base := "/projects/" + url.PathEscape("g/p") + "/merge_requests/5"
		if !paths[base+"/unapprove"] || !paths[base+"/award_emoji"] || !paths[base+"/notes"] {
			t.Fatalf("missing call; paths=%+v", paths)
		}
		if awards["name"] != "thumbsdown" {
			t.Fatalf("award = %+v", awards)
		}
	})
	t.Run("Merge", func(t *testing.T) {
		var gotPath, gotMethod string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath, gotMethod = glTestPath(r), r.Method
		}))
		defer srv.Close()
		c := &GitLab{apiBase: srv.URL}
		if _, err := c.Merge(context.Background(), "https://gitlab.com/g/p", "tok", 5, "squash", ""); err != nil {
			t.Fatalf("Merge: %v", err)
		}
		if gotPath != "/projects/"+url.PathEscape("g/p")+"/merge_requests/5/merge" || gotMethod != http.MethodPut {
			t.Fatalf("path=%q method=%q", gotPath, gotMethod)
		}
	})
	t.Run("ClosePR", func(t *testing.T) {
		var body map[string]any
		var gotMethod string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut {
				gotMethod = r.Method
				b, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(b, &body)
			}
		}))
		defer srv.Close()
		c := &GitLab{apiBase: srv.URL}
		if err := c.ClosePR(context.Background(), "https://gitlab.com/g/p", "tok", 5, "rejecting"); err != nil {
			t.Fatalf("ClosePR: %v", err)
		}
		if gotMethod != http.MethodPut || body["state_event"] != "close" {
			t.Fatalf("method=%q body=%+v", gotMethod, body)
		}
	})
	t.Run("GetPRState", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"author":        map[string]any{"username": "bob"},
				"sha":           "sha1",
				"source_branch": "feat",
				"merge_status":  "can_be_merged",
				"head_pipeline": map[string]any{"status": "success"},
			})
		}))
		defer srv.Close()
		c := &GitLab{apiBase: srv.URL}
		st, err := c.GetPRState(context.Background(), "https://gitlab.com/g/p", "tok", 5)
		if err != nil {
			t.Fatalf("GetPRState: %v", err)
		}
		if st.Author != "bob" || st.HeadSHA != "sha1" || st.HeadBranch != "feat" || st.CIStatus != "success" {
			t.Fatalf("state = %+v", st)
		}
	})
	t.Run("SetBoardColumn", func(t *testing.T) {
		var body map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut {
				b, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(b, &body)
			}
		}))
		defer srv.Close()
		c := &GitLab{apiBase: srv.URL}
		board := BoardRef{Provider: "gitlab", GitLabBoardID: 7}
		if err := c.SetBoardColumn(context.Background(), "tok", board, "https://gitlab.com/g/p/-/issues/4", "Proposed"); err != nil {
			t.Fatalf("SetBoardColumn: %v", err)
		}
		if body["add_labels"] != "board::Proposed" {
			t.Fatalf("add_labels = %+v", body["add_labels"])
		}
	})
	t.Run("AddBoardItem", func(t *testing.T) {
		var body map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut {
				b, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(b, &body)
			}
		}))
		defer srv.Close()
		c := &GitLab{apiBase: srv.URL}
		board := BoardRef{Provider: "gitlab", GitLabBoardID: 7}
		if err := c.AddBoardItem(context.Background(), "tok", board, "https://gitlab.com/g/p/-/issues/4"); err != nil {
			t.Fatalf("AddBoardItem: %v", err)
		}
		if body["add_labels"] != "board::Open" {
			t.Fatalf("add_labels = %+v", body["add_labels"])
		}
	})
}

// TestGitLabMergeConflict verifies that Merge returns ErrMergeConflict on 405/406/409.
func TestGitLabMergeConflict(t *testing.T) {
	for _, status := range []int{405, 406, 409} {
		status := status
		t.Run(fmt.Sprintf("http%d", status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()
			c := &GitLab{apiBase: srv.URL}
			_, err := c.Merge(context.Background(), "https://gitlab.com/g/p", "tok", 5, "squash", "")
			if !errors.Is(err, ErrMergeConflict) {
				t.Fatalf("expected ErrMergeConflict for HTTP %d, got %v", status, err)
			}
		})
	}
}

func TestGitLabDetectAndVerifyFields(t *testing.T) {
	const secret = "tok"
	t.Run("issue unlabeled", func(t *testing.T) {
		payload := []byte(`{"object_kind":"issue","user":{"username":"alice"},"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},"object_attributes":{"iid":7,"title":"T","description":"D","url":"https://gl/g/p/-/issues/7","action":"update"},"changes":{"labels":{"previous":[{"title":"tatara/awaiting-approval"}],"current":[]}},"labels":[]}`)
		h := http.Header{}
		h.Set("X-Gitlab-Event", "Issue Hook")
		h.Set("X-Gitlab-Token", secret)
		ev, err := (&GitLab{}).DetectAndVerify(h, payload, secret)
		if err != nil {
			t.Fatalf("DetectAndVerify: %v", err)
		}
		if ev.Kind != "issue" || ev.AuthorLogin != "alice" || ev.ActorLogin != "alice" || ev.Number != 7 || ev.Action != "unlabeled" || ev.ChangedLabel != "tatara/awaiting-approval" {
			t.Fatalf("event = %+v", ev)
		}
	})
	t.Run("merge request opened", func(t *testing.T) {
		payload := []byte(`{"object_kind":"merge_request","user":{"username":"bob"},"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},"object_attributes":{"iid":9,"title":"MR","description":"D","url":"https://gl/g/p/-/merge_requests/9","action":"open","source_branch":"feat","last_commit":{"id":"sha9"}},"labels":[]}`)
		h := http.Header{}
		h.Set("X-Gitlab-Event", "Merge Request Hook")
		h.Set("X-Gitlab-Token", secret)
		ev, err := (&GitLab{}).DetectAndVerify(h, payload, secret)
		if err != nil {
			t.Fatalf("DetectAndVerify: %v", err)
		}
		if ev.Kind != "mr" || !ev.IsPR || ev.AuthorLogin != "bob" || ev.ActorLogin != "bob" || ev.Number != 9 || ev.Action != "opened" || ev.HeadSHA != "sha9" || ev.HeadBranch != "feat" {
			t.Fatalf("event = %+v", ev)
		}
	})
}
