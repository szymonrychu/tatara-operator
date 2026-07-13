package scm

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
			// Use RawPath when set so URL-encoded segments are not decoded.
			raw := r.URL.RawPath
			if raw == "" {
				raw = r.URL.Path
			}
			gotPath, gotMethod = raw, r.Method
		}))
		defer srv.Close()
		c := &GitHub{apiBase: srv.URL}
		if err := c.RemoveLabel(context.Background(), "tok", "o/r#7", "tatara/awaiting-approval"); err != nil {
			t.Fatalf("RemoveLabel: %v", err)
		}
		if gotMethod != http.MethodDelete {
			t.Fatalf("method = %q", gotMethod)
		}
		// The slash in "tatara/awaiting-approval" must be URL-encoded to %2F
		// so the DELETE hits the correct GitHub API route.
		const want = "/repos/o/r/issues/7/labels/tatara%2Fawaiting-approval"
		if gotPath != want {
			t.Fatalf("path = %q, want %q", gotPath, want)
		}
	})
	t.Run("RemoveLabel404Benign", func(t *testing.T) {
		// GitHub answers DELETE .../labels/{name} with 404 "Label does not
		// exist" when the label is not currently applied. That is a benign
		// no-op and must not surface as a write error (it inflated the SCM
		// write-failure ratio alert before the swallow, see issue #159).
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"Label does not exist"}`))
		}))
		defer srv.Close()
		c := &GitHub{apiBase: srv.URL}
		if err := c.RemoveLabel(context.Background(), "tok", "o/r#7", "tatara-brainstorming"); err != nil {
			t.Fatalf("RemoveLabel must swallow a 404: %v", err)
		}
	})
	t.Run("RemoveLabelPropagatesNon404", func(t *testing.T) {
		// A real failure (here 500) is not the benign already-absent case and
		// must propagate so it is counted and surfaced.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()
		c := &GitHub{apiBase: srv.URL}
		err := c.RemoveLabel(context.Background(), "tok", "o/r#7", "tatara-brainstorming")
		if err == nil {
			t.Fatal("RemoveLabel must propagate a non-404 error")
		}
		if got := ErrorStatus(err); got != "500" {
			t.Fatalf("ErrorStatus = %q, want %q", got, "500")
		}
	})
}

func TestGitHubGetPRState(t *testing.T) {
	cases := []struct {
		name   string
		runs   []map[string]string // status,conclusion
		wantCI string
	}{
		{"no runs", nil, ""},
		{"in progress", []map[string]string{{"status": "in_progress", "conclusion": ""}}, "pending"},
		{"all success", []map[string]string{{"status": "completed", "conclusion": "success"}}, "success"},
		{"one failure", []map[string]string{{"status": "completed", "conclusion": "success"}, {"status": "completed", "conclusion": "failure"}}, "failure"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/repos/o/r/pulls/5":
					_ = json.NewEncoder(w).Encode(map[string]any{
						"user": map[string]any{"login": "alice"},
						"head": map[string]any{"sha": "abc", "ref": "feature"},
					})
				case "/repos/o/r/commits/abc/check-runs":
					runs := make([]map[string]any, 0, len(tc.runs))
					for _, run := range tc.runs {
						runs = append(runs, map[string]any{"status": run["status"], "conclusion": run["conclusion"]})
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"check_runs": runs})
				case "/repos/o/r/commits/abc/status":
					// No legacy commit statuses; check-runs dominate.
					_ = json.NewEncoder(w).Encode(map[string]any{"state": "pending", "total_count": 0})
				default:
					t.Fatalf("unexpected path %q", r.URL.Path)
				}
			}))
			defer srv.Close()
			c := &GitHub{apiBase: srv.URL}
			st, err := c.GetPRState(context.Background(), "https://github.com/o/r", "tok", 5)
			if err != nil {
				t.Fatalf("GetPRState: %v", err)
			}
			if st.Author != "alice" || st.HeadSHA != "abc" || st.HeadBranch != "feature" {
				t.Fatalf("state = %+v", st)
			}
			if st.CIStatus != tc.wantCI {
				t.Fatalf("CIStatus = %q, want %q", st.CIStatus, tc.wantCI)
			}
		})
	}
}

// TestGitHubGetPRState_CombinedStatusFolded verifies that GetPRState folds
// the legacy combined-status API (via GetCommitCIStatus), so CI systems that
// report via Commit Statuses are visible to the merge gate.
func TestGitHubGetPRState_CombinedStatusFolded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/o/r/pulls/5":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"user": map[string]any{"login": "bot"},
				"head": map[string]any{"sha": "abc", "ref": "feat"},
			})
		case "/repos/o/r/commits/abc/check-runs":
			// No check-runs; CI reports via commit statuses only.
			_ = json.NewEncoder(w).Encode(map[string]any{"check_runs": []any{}})
		case "/repos/o/r/commits/abc/status":
			// One legacy commit status reporting success.
			_ = json.NewEncoder(w).Encode(map[string]any{"state": "success", "total_count": 1})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	st, err := c.GetPRState(context.Background(), "https://github.com/o/r", "tok", 5)
	if err != nil {
		t.Fatalf("GetPRState: %v", err)
	}
	if st.CIStatus != "success" {
		t.Fatalf("CIStatus = %q, want %q (combined-status not folded)", st.CIStatus, "success")
	}
}

// TestGitHubGetPRState_UsesPerCallToken is a regression test: GetPRState must
// derive CI status using the per-call token, not the (empty) client token. The
// writer client from ByProvider has an empty c.token and supplies the token per
// call; delegating CI derivation to a c.token-based path issued an
// unauthenticated request that failed on private repos. The server here rejects
// any commit request that does not carry the per-call bearer token.
func TestGitHubGetPRState_UsesPerCallToken(t *testing.T) {
	const wantToken = "per-call-tok"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/commits/") {
			if got := r.Header.Get("Authorization"); got != "Bearer "+wantToken {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		switch r.URL.Path {
		case "/repos/o/r/pulls/5":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"user": map[string]any{"login": "bot"},
				"head": map[string]any{"sha": "abc", "ref": "feat"},
			})
		case "/repos/o/r/commits/abc/check-runs":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"check_runs": []map[string]any{{"status": "completed", "conclusion": "success"}},
			})
		case "/repos/o/r/commits/abc/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"state": "pending", "total_count": 0})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer srv.Close()
	// Empty client token, exactly the ByProvider writer-client configuration.
	c := &GitHub{apiBase: srv.URL}
	st, err := c.GetPRState(context.Background(), "https://github.com/o/r", wantToken, 5)
	if err != nil {
		t.Fatalf("GetPRState: %v (CI request likely unauthenticated)", err)
	}
	if st.CIStatus != "success" {
		t.Fatalf("CIStatus = %q, want success", st.CIStatus)
	}
}

// TestGitHubMergeConflict verifies that Merge returns ErrMergeConflict on 405/409.
func TestGitHubMergeConflict(t *testing.T) {
	for _, status := range []int{405, 409} {
		status := status
		t.Run(fmt.Sprintf("http%d", status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()
			c := &GitHub{apiBase: srv.URL}
			_, err := c.Merge(context.Background(), "https://github.com/o/r", "tok", 5, "squash", "")
			if !errors.Is(err, ErrMergeConflict) {
				t.Fatalf("expected ErrMergeConflict for HTTP %d, got %v", status, err)
			}
		})
	}
}

func TestGitHubReviewVerbs(t *testing.T) {
	t.Run("Approve", func(t *testing.T) {
		var gotPath string
		var body map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			b, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(b, &body)
		}))
		defer srv.Close()
		c := &GitHub{apiBase: srv.URL}
		if err := c.Approve(context.Background(), "https://github.com/o/r", "tok", 5, "lgtm"); err != nil {
			t.Fatalf("Approve: %v", err)
		}
		if gotPath != "/repos/o/r/pulls/5/reviews" || body["event"] != "APPROVE" || body["body"] != "lgtm" {
			t.Fatalf("path=%q body=%+v", gotPath, body)
		}
	})
	t.Run("RequestChanges", func(t *testing.T) {
		var body map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(b, &body)
		}))
		defer srv.Close()
		c := &GitHub{apiBase: srv.URL}
		if err := c.RequestChanges(context.Background(), "https://github.com/o/r", "tok", 5, "nope"); err != nil {
			t.Fatalf("RequestChanges: %v", err)
		}
		if body["event"] != "REQUEST_CHANGES" {
			t.Fatalf("event = %v", body["event"])
		}
	})
	t.Run("Suggest", func(t *testing.T) {
		var body map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(b, &body)
		}))
		defer srv.Close()
		c := &GitHub{apiBase: srv.URL}
		err := c.Suggest(context.Background(), "https://github.com/o/r", "tok", 5, []Suggestion{{Path: "a.go", Line: 12, Body: "x := 1"}})
		if err != nil {
			t.Fatalf("Suggest: %v", err)
		}
		if body["event"] != "COMMENT" {
			t.Fatalf("event = %v", body["event"])
		}
		comments, _ := body["comments"].([]any)
		if len(comments) != 1 {
			t.Fatalf("comments = %+v", body["comments"])
		}
		first, _ := comments[0].(map[string]any)
		if first["path"] != "a.go" || first["line"].(float64) != 12 {
			t.Fatalf("comment = %+v", first)
		}
		if cbody, _ := first["body"].(string); cbody != "```suggestion\nx := 1\n```" {
			t.Fatalf("comment body = %q", cbody)
		}
	})
	t.Run("Merge", func(t *testing.T) {
		var gotPath, gotMethod string
		var body map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath, gotMethod = r.URL.Path, r.Method
			b, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(b, &body)
		}))
		defer srv.Close()
		c := &GitHub{apiBase: srv.URL}
		if _, err := c.Merge(context.Background(), "https://github.com/o/r", "tok", 5, "squash", ""); err != nil {
			t.Fatalf("Merge: %v", err)
		}
		if gotPath != "/repos/o/r/pulls/5/merge" || gotMethod != http.MethodPut || body["merge_method"] != "squash" {
			t.Fatalf("path=%q method=%q body=%+v", gotPath, gotMethod, body)
		}
	})
	t.Run("ClosePR", func(t *testing.T) {
		paths := map[string]bool{}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			paths[r.Method+" "+r.URL.Path] = true
		}))
		defer srv.Close()
		c := &GitHub{apiBase: srv.URL}
		if err := c.ClosePR(context.Background(), "https://github.com/o/r", "tok", 5, "rejecting"); err != nil {
			t.Fatalf("ClosePR: %v", err)
		}
		if !paths["PATCH /repos/o/r/pulls/5"] {
			t.Fatalf("missing PATCH; got %+v", paths)
		}
		if !paths["POST /repos/o/r/issues/5/comments"] {
			t.Fatalf("missing comment; got %+v", paths)
		}
	})
}

func ghSign(payload []byte, secret string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(payload)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}

func TestGitHubDetectAndVerifyFields(t *testing.T) {
	const secret = "s3cr3t"
	t.Run("issue labeled", func(t *testing.T) {
		payload := []byte(`{"action":"labeled","sender":{"login":"alice"},"label":{"name":"tatara/awaiting-approval"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},"issue":{"number":7,"title":"T","body":"B","user":{"login":"tatara-bot"},"html_url":"https://gh/o/r/issues/7","labels":[{"name":"tatara"}]}}`)
		h := http.Header{}
		h.Set("X-GitHub-Event", "issues")
		h.Set("X-Hub-Signature-256", ghSign(payload, secret))
		ev, err := (&GitHub{}).DetectAndVerify(h, payload, secret)
		if err != nil {
			t.Fatalf("DetectAndVerify: %v", err)
		}
		if ev.Kind != "issue" || ev.Action != "labeled" || ev.AuthorLogin != "tatara-bot" || ev.ActorLogin != "alice" || ev.Number != 7 || ev.IsPR || ev.ChangedLabel != "tatara/awaiting-approval" {
			t.Fatalf("event = %+v", ev)
		}
	})
	t.Run("pull_request opened", func(t *testing.T) {
		payload := []byte(`{"action":"opened","sender":{"login":"bob"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},"pull_request":{"number":9,"title":"PR","body":"body","user":{"login":"tatara-bot"},"html_url":"https://gh/o/r/pull/9","head":{"sha":"deadbeef","ref":"feature"}}}`)
		h := http.Header{}
		h.Set("X-GitHub-Event", "pull_request")
		h.Set("X-Hub-Signature-256", ghSign(payload, secret))
		ev, err := (&GitHub{}).DetectAndVerify(h, payload, secret)
		if err != nil {
			t.Fatalf("DetectAndVerify: %v", err)
		}
		if ev.Kind != "mr" || !ev.IsPR || ev.AuthorLogin != "tatara-bot" || ev.ActorLogin != "bob" || ev.Number != 9 || ev.HeadSHA != "deadbeef" || ev.HeadBranch != "feature" || ev.Action != "opened" {
			t.Fatalf("event = %+v", ev)
		}
	})
}
