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
	pushBody := []byte(`{"ref":"refs/heads/main","before":"def456","after":"abc123","project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"}}`)
	issueBody := []byte(`{"object_kind":"issue","user":{"username":"alice"},"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},"object_attributes":{"iid":12,"title":"An issue","description":"desc","url":"https://gitlab.com/g/p/-/issues/12"},"labels":[{"title":"tatara"},{"title":"ops"}]}`)
	mrBody := []byte(`{"object_kind":"merge_request","user":{"username":"bob"},"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},"object_attributes":{"iid":34,"title":"An MR","description":"mr desc","url":"https://gitlab.com/g/p/-/merge_requests/34"},"labels":[{"title":"tatara"}]}`)

	tests := []struct {
		name  string
		event string
		body  []byte
		want  WebhookEvent
	}{
		{"push", "Push Hook", pushBody, WebhookEvent{Kind: "push", Repo: "https://gitlab.com/g/p.git", Branch: "main", HeadSHA: "abc123", BaseSHA: "def456"}},
		{"issue", "Issue Hook", issueBody, WebhookEvent{Kind: "issue", Repo: "https://gitlab.com/g/p.git", Labels: []string{"tatara", "ops"}, Title: "An issue", Body: "desc", IssueRef: "g/p#12", URL: "https://gitlab.com/g/p/-/issues/12", AuthorLogin: "alice", ActorLogin: "alice", Action: "other", Number: 12}},
		{"mr", "Merge Request Hook", mrBody, WebhookEvent{Kind: "mr", Repo: "https://gitlab.com/g/p.git", Labels: []string{"tatara"}, Title: "An MR", Body: "mr desc", IssueRef: "g/p!34", URL: "https://gitlab.com/g/p/-/merge_requests/34", AuthorLogin: "bob", ActorLogin: "bob", IsPR: true, Action: "other", Number: 34}},
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

func TestGitLabNoteHook(t *testing.T) {
	const secret = "glt0ken"
	c := &GitLab{}
	t.Run("issue comment derives iid from nested issue", func(t *testing.T) {
		// object_attributes.iid here is the note id (99), NOT the issue iid (12).
		body := []byte(`{"object_kind":"note","user":{"username":"alice"},"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},"object_attributes":{"iid":99,"description":"a comment","url":"https://gitlab.com/g/p/-/issues/12#note_99"},"issue":{"iid":12},"labels":[{"title":"tatara"}]}`)
		ev, err := c.DetectAndVerify(glHeader("Note Hook", secret), body, secret)
		require.NoError(t, err)
		require.Equal(t, "issue", ev.Kind)
		require.False(t, ev.IsPR)
		require.Equal(t, 12, ev.Number)
		require.Equal(t, "g/p#12", ev.IssueRef)
		require.Equal(t, "created", ev.Action)
		require.Equal(t, "alice", ev.ActorLogin)
	})
	t.Run("MR comment derives iid from nested merge_request", func(t *testing.T) {
		body := []byte(`{"object_kind":"note","user":{"username":"bob"},"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},"object_attributes":{"iid":77,"description":"mr comment","url":"https://gitlab.com/g/p/-/merge_requests/34#note_77"},"merge_request":{"iid":34,"source_branch":"feat"},"labels":[{"title":"tatara"}]}`)
		ev, err := c.DetectAndVerify(glHeader("Note Hook", secret), body, secret)
		require.NoError(t, err)
		require.Equal(t, "mr", ev.Kind)
		require.True(t, ev.IsPR)
		require.Equal(t, 34, ev.Number)
		require.Equal(t, "g/p!34", ev.IssueRef)
		require.Equal(t, "feat", ev.HeadBranch)
		require.Equal(t, "bob", ev.ActorLogin)
	})
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

func TestGitLab_MRApproval_MapsToReviewApproved(t *testing.T) {
	const secret = "glt0ken"
	body := []byte(`{"object_kind":"merge_request",
		"user":{"username":"maint"},
		"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},
		"object_attributes":{"iid":42,"action":"approved","last_commit":{"id":"deadbeef"},"source_branch":"fix"}}`)
	ev, err := (&GitLab{}).DetectAndVerify(glHeader("Merge Request Hook", secret), body, secret)
	require.NoError(t, err)
	require.True(t, ev.IsReview)
	require.Equal(t, "approved", ev.ReviewState)
	require.Equal(t, "deadbeef", ev.ReviewCommitSHA)
}

// TestGitLab_MRMerge_MapsToMerged is the PR-merged-out-of-band signal on GitLab:
// action=merge surfaces action "merged" + ev.Merged.
func TestGitLab_MRMerge_MapsToMerged(t *testing.T) {
	const token = "t"
	body := []byte(`{"object_kind":"merge_request","user":{"username":"bob"},
		"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},
		"object_attributes":{"iid":34,"action":"merge","source_branch":"task/x","last_commit":{"id":"abc"},"url":"u"}}`)
	ev, err := (&GitLab{}).DetectAndVerify(glHeader("Merge Request Hook", token), body, token)
	require.NoError(t, err)
	require.Equal(t, "merged", ev.Action)
	require.True(t, ev.Merged)
	require.True(t, ev.IsPR)
}

// TestGitLab_IssueUpdate_MapsToSynchronize is WS3-I2 on GitLab: an issue body
// edit arrives as action=update -> "synchronize" (the I2 handler diffs the body).
func TestGitLab_IssueUpdate_MapsToSynchronize(t *testing.T) {
	const token = "t"
	body := []byte(`{"object_kind":"issue","user":{"username":"alice"},
		"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},
		"object_attributes":{"iid":12,"title":"t","description":"edited body","action":"update","url":"u"}}`)
	ev, err := (&GitLab{}).DetectAndVerify(glHeader("Issue Hook", token), body, token)
	require.NoError(t, err)
	require.Equal(t, "synchronize", ev.Action)
	require.False(t, ev.IsPR)
	require.Equal(t, "edited body", ev.Body)
}
