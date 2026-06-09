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
	pushBody := []byte(`{"ref":"refs/heads/main","after":"abc123","repository":{"clone_url":"https://github.com/o/r.git"}}`)
	issueBody := []byte(`{"action":"opened","issue":{"number":7,"title":"Fix bug","body":"do it","labels":[{"name":"tatara"},{"name":"bug"}],"html_url":"https://github.com/o/r/issues/7"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	prBody := []byte(`{"action":"opened","pull_request":{"number":9,"title":"PR title","body":"pr body","labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/9"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)

	tests := []struct {
		name  string
		event string
		body  []byte
		want  WebhookEvent
	}{
		{"push", "push", pushBody, WebhookEvent{Kind: "push", Repo: "https://github.com/o/r.git", Branch: "main"}},
		{"issue", "issues", issueBody, WebhookEvent{Kind: "issue", Repo: "https://github.com/o/r.git", Labels: []string{"tatara", "bug"}, Title: "Fix bug", Body: "do it", IssueRef: "o/r#7", URL: "https://github.com/o/r/issues/7", Action: "opened", Number: 7}},
		{"pr", "pull_request", prBody, WebhookEvent{Kind: "mr", Repo: "https://github.com/o/r.git", Labels: []string{"tatara"}, Title: "PR title", Body: "pr body", IssueRef: "o/r#9", URL: "https://github.com/o/r/pull/9", Action: "opened", Number: 9, IsPR: true}},
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
