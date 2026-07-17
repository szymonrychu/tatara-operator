package scm

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func ghSig(secret string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(body)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}

func ghHeader(event, secret string, body []byte) http.Header {
	h := http.Header{}
	h.Set("X-GitHub-Event", event)
	h.Set("X-Hub-Signature-256", ghSig(secret, body))
	return h
}

func TestGitHubDetectAndVerify(t *testing.T) {
	const secret = "s3cr3t"
	pushBody := []byte(`{"ref":"refs/heads/main","before":"def456","after":"abc123","repository":{"clone_url":"https://github.com/o/r.git"}}`)
	issueBody := []byte(`{"action":"opened","sender":{"login":"alice"},"issue":{"number":7,"title":"Fix bug","body":"do it","user":{"login":"author1"},"labels":[{"name":"tatara"},{"name":"bug"}],"html_url":"https://github.com/o/r/issues/7"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	prBody := []byte(`{"action":"opened","sender":{"login":"alice"},"pull_request":{"number":9,"title":"PR title","body":"pr body","user":{"login":"author2"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/9"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)

	tests := []struct {
		name  string
		event string
		body  []byte
		want  WebhookEvent
	}{
		{"push", "push", pushBody, WebhookEvent{Kind: "push", Repo: "https://github.com/o/r.git", Branch: "main", HeadSHA: "abc123", BaseSHA: "def456"}},
		{"issue", "issues", issueBody, WebhookEvent{Kind: "issue", Repo: "https://github.com/o/r.git", Labels: []string{"tatara", "bug"}, Title: "Fix bug", Body: "do it", IssueRef: "o/r#7", URL: "https://github.com/o/r/issues/7", AuthorLogin: "author1", ActorLogin: "alice", Action: "opened", Number: 7}},
		{"pr", "pull_request", prBody, WebhookEvent{Kind: "mr", Repo: "https://github.com/o/r.git", Labels: []string{"tatara"}, Title: "PR title", Body: "pr body", IssueRef: "o/r#9", URL: "https://github.com/o/r/pull/9", AuthorLogin: "author2", ActorLogin: "alice", Action: "opened", Number: 9, IsPR: true}},
		{"other", "ping", []byte(`{}`), WebhookEvent{Kind: "other"}},
	}
	c := &GitHub{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.DetectAndVerify(ghHeader(tt.event, secret, tt.body), tt.body, secret)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGitHubIssueCommentPRDetection(t *testing.T) {
	const secret = "s3cr3t"
	c := &GitHub{}
	t.Run("comment on a PR routes to mr with IsPR true", func(t *testing.T) {
		body := []byte(`{"action":"created","sender":{"login":"alice"},"issue":{"number":9,"title":"PR title","body":"x","user":{"login":"tatara-bot"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/9","pull_request":{"url":"https://api.github.com/repos/o/r/pulls/9"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
		ev, err := c.DetectAndVerify(ghHeader("issue_comment", secret, body), body, secret)
		require.NoError(t, err)
		require.Equal(t, "mr", ev.Kind)
		require.True(t, ev.IsPR)
		require.Equal(t, "tatara-bot", ev.AuthorLogin)
		require.Equal(t, "alice", ev.ActorLogin)
	})
	t.Run("comment on a plain issue stays issue with IsPR false", func(t *testing.T) {
		body := []byte(`{"action":"created","sender":{"login":"alice"},"issue":{"number":7,"title":"An issue","body":"x","user":{"login":"author1"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/7"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
		ev, err := c.DetectAndVerify(ghHeader("issue_comment", secret, body), body, secret)
		require.NoError(t, err)
		require.Equal(t, "issue", ev.Kind)
		require.False(t, ev.IsPR)
		require.Equal(t, "author1", ev.AuthorLogin)
		require.Equal(t, "alice", ev.ActorLogin)
	})
}

func TestGitHubBadSignature(t *testing.T) {
	const secret = "s3cr3t"
	body := []byte(`{"ref":"refs/heads/main","after":"x","repository":{"clone_url":"https://github.com/o/r.git"}}`)
	h := http.Header{}
	h.Set("X-GitHub-Event", "push")
	h.Set("X-Hub-Signature-256", ghSig("wrong", body))
	_, err := (&GitHub{}).DetectAndVerify(h, body, secret)
	require.Error(t, err)
}

func TestGitHubMissingSignature(t *testing.T) {
	body := []byte(`{}`)
	h := http.Header{}
	h.Set("X-GitHub-Event", "push")
	_, err := (&GitHub{}).DetectAndVerify(h, body, "s")
	require.Error(t, err)
}

func TestGitHub_PullRequestReview_ParsesStateAndID(t *testing.T) {
	const secret = "s"
	body := []byte(`{"action":"submitted",
		"review":{"id":900,"state":"changes_requested","commit_id":"deadbeef","user":{"login":"maint"}},
		"pull_request":{"number":42,"user":{"login":"alice"},"head":{"sha":"deadbeef","ref":"fix"},"html_url":"u"},
		"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},
		"sender":{"login":"maint"}}`)
	ev, err := (&GitHub{}).DetectAndVerify(ghHeader("pull_request_review", secret, body), body, secret)
	require.NoError(t, err)
	require.True(t, ev.IsReview)
	require.Equal(t, "changes_requested", ev.ReviewState)
	require.Equal(t, "900", ev.ReviewID)
	require.Equal(t, "deadbeef", ev.ReviewCommitSHA)
	require.Equal(t, "maint", ev.ActorLogin)
	require.Equal(t, 42, ev.Number)
	require.Equal(t, "mr", ev.Kind)
}

func TestGitHub_PullRequestReview_CommentedCarriesBody(t *testing.T) {
	const secret = "s"
	body := []byte(`{"action":"submitted",
		"review":{"id":901,"state":"commented","commit_id":"deadbeef","body":"please rename this var","user":{"login":"maint"}},
		"pull_request":{"number":42,"user":{"login":"alice"},"head":{"sha":"deadbeef","ref":"fix"},"html_url":"u"},
		"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},
		"sender":{"login":"maint"}}`)
	ev, err := (&GitHub{}).DetectAndVerify(ghHeader("pull_request_review", secret, body), body, secret)
	require.NoError(t, err)
	require.Equal(t, "commented", ev.ReviewState)
	require.Equal(t, "please rename this var", ev.CommentBody)
}
