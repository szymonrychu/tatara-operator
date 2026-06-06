package scm

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func glHeader(event, token string) http.Header {
	h := http.Header{}
	h.Set("X-Gitlab-Event", event)
	h.Set("X-Gitlab-Token", token)
	return h
}

func TestGitLabDetectAndVerify(t *testing.T) {
	const secret = "glt0ken"
	pushBody := []byte(`{"ref":"refs/heads/main","project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"}}`)
	issueBody := []byte(`{"object_kind":"issue","project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},"object_attributes":{"iid":12,"title":"An issue","description":"desc","url":"https://gitlab.com/g/p/-/issues/12"},"labels":[{"title":"tatara"},{"title":"ops"}]}`)
	mrBody := []byte(`{"object_kind":"merge_request","project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},"object_attributes":{"iid":34,"title":"An MR","description":"mr desc","url":"https://gitlab.com/g/p/-/merge_requests/34"},"labels":[{"title":"tatara"}]}`)

	tests := []struct {
		name  string
		event string
		body  []byte
		want  WebhookEvent
	}{
		{"push", "Push Hook", pushBody, WebhookEvent{Kind: "push", Repo: "https://gitlab.com/g/p.git", Branch: "main"}},
		{"issue", "Issue Hook", issueBody, WebhookEvent{Kind: "issue", Repo: "https://gitlab.com/g/p.git", Labels: []string{"tatara", "ops"}, Title: "An issue", Body: "desc", IssueRef: "g/p!12", URL: "https://gitlab.com/g/p/-/issues/12"}},
		{"mr", "Merge Request Hook", mrBody, WebhookEvent{Kind: "mr", Repo: "https://gitlab.com/g/p.git", Labels: []string{"tatara"}, Title: "An MR", Body: "mr desc", IssueRef: "g/p!34", URL: "https://gitlab.com/g/p/-/merge_requests/34"}},
		{"other", "Pipeline Hook", []byte(`{}`), WebhookEvent{Kind: "other"}},
	}
	c := &GitLab{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.DetectAndVerify(glHeader(tt.event, secret), tt.body, secret)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGitLabBadToken(t *testing.T) {
	body := []byte(`{"ref":"refs/heads/main","project":{"git_http_url":"https://gitlab.com/g/p.git"}}`)
	_, err := (&GitLab{}).DetectAndVerify(glHeader("Push Hook", "wrong"), body, "glt0ken")
	require.Error(t, err)
}

func TestGitLabMissingToken(t *testing.T) {
	h := http.Header{}
	h.Set("X-Gitlab-Event", "Push Hook")
	_, err := (&GitLab{}).DetectAndVerify(h, []byte(`{}`), "glt0ken")
	require.Error(t, err)
}
