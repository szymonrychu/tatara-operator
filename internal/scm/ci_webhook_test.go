package scm

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// PR A item 1. GitHub check_suite / check_run / status and GitLab's Pipeline
// Hook used to fall through DetectAndVerify's default arm and land on
// webhook/server.go's "ignored" branch, which is why MergeRequest.status.ciStatus
// had exactly one writer and every agent was shown ci="".
//
// THE AGGREGATION RULE IS THE POINT OF THESE TESTS. A single check_run is one
// check out of many: it proves RED on its own (one failing required check makes
// the PR unmergeable immediately) and it can NEVER prove GREEN. Only an
// aggregate - a check_suite conclusion, a GitLab pipeline status, or the
// reconciler's live GetCommitCIStatus backstop - writes green.

func TestGitHubDetectAndVerify_CIEvents(t *testing.T) {
	const secret = "s3cr3t"
	c := &GitHub{}

	tests := []struct {
		name  string
		event string
		body  string
		want  WebhookEvent
	}{
		{
			name:  "check_suite completed success is the aggregate green",
			event: "check_suite",
			body:  `{"action":"completed","check_suite":{"head_sha":"abc123","status":"completed","conclusion":"success"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`,
			want:  WebhookEvent{Kind: "ci", Repo: "https://github.com/o/r.git", HeadSHA: "abc123", CIStatus: CIMirrorGreen},
		},
		{
			name:  "check_suite completed failure is red",
			event: "check_suite",
			body:  `{"action":"completed","check_suite":{"head_sha":"abc123","status":"completed","conclusion":"failure"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`,
			want:  WebhookEvent{Kind: "ci", Repo: "https://github.com/o/r.git", HeadSHA: "abc123", CIStatus: CIMirrorRed},
		},
		{
			name:  "check_suite queued is pending",
			event: "check_suite",
			body:  `{"action":"requested","check_suite":{"head_sha":"abc123","status":"queued","conclusion":null},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`,
			want:  WebhookEvent{Kind: "ci", Repo: "https://github.com/o/r.git", HeadSHA: "abc123", CIStatus: CIMirrorPending},
		},
		{
			name:  "check_suite in_progress is running",
			event: "check_suite",
			body:  `{"action":"requested","check_suite":{"head_sha":"abc123","status":"in_progress","conclusion":null},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`,
			want:  WebhookEvent{Kind: "ci", Repo: "https://github.com/o/r.git", HeadSHA: "abc123", CIStatus: CIMirrorRunning},
		},
		{
			name:  "a FAILING check_run is red on its own",
			event: "check_run",
			body:  `{"action":"completed","check_run":{"head_sha":"abc123","status":"completed","conclusion":"failure"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`,
			want:  WebhookEvent{Kind: "ci", Repo: "https://github.com/o/r.git", HeadSHA: "abc123", CIStatus: CIMirrorRed},
		},
		{
			name:  "a PASSING check_run is NOT green - one check is not the pull request",
			event: "check_run",
			body:  `{"action":"completed","check_run":{"head_sha":"abc123","status":"completed","conclusion":"success"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`,
			want:  WebhookEvent{Kind: "ci", Repo: "https://github.com/o/r.git", HeadSHA: "abc123", CIStatus: CIMirrorRunning},
		},
		{
			name:  "an in-flight check_run is running",
			event: "check_run",
			body:  `{"action":"created","check_run":{"head_sha":"abc123","status":"in_progress","conclusion":null},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`,
			want:  WebhookEvent{Kind: "ci", Repo: "https://github.com/o/r.git", HeadSHA: "abc123", CIStatus: CIMirrorRunning},
		},
		{
			name:  "a check_run cancelled by a rerun is red",
			event: "check_run",
			body:  `{"action":"completed","check_run":{"head_sha":"abc123","status":"completed","conclusion":"cancelled"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`,
			want:  WebhookEvent{Kind: "ci", Repo: "https://github.com/o/r.git", HeadSHA: "abc123", CIStatus: CIMirrorRed},
		},
		{
			name:  "a failing commit status is red",
			event: "status",
			body:  `{"sha":"abc123","state":"failure","context":"ci/build","repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`,
			want:  WebhookEvent{Kind: "ci", Repo: "https://github.com/o/r.git", HeadSHA: "abc123", CIStatus: CIMirrorRed},
		},
		{
			name:  "an errored commit status is red",
			event: "status",
			body:  `{"sha":"abc123","state":"error","context":"ci/build","repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`,
			want:  WebhookEvent{Kind: "ci", Repo: "https://github.com/o/r.git", HeadSHA: "abc123", CIStatus: CIMirrorRed},
		},
		{
			name:  "a SUCCESSFUL commit status is NOT green - it is one context of many",
			event: "status",
			body:  `{"sha":"abc123","state":"success","context":"ci/build","repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`,
			want:  WebhookEvent{Kind: "ci", Repo: "https://github.com/o/r.git", HeadSHA: "abc123", CIStatus: CIMirrorRunning},
		},
		{
			name:  "a CI delivery with no head sha is nothing we can match",
			event: "check_suite",
			body:  `{"action":"completed","check_suite":{"head_sha":"","status":"completed","conclusion":"success"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`,
			want:  WebhookEvent{Kind: "other"},
		},
		{
			name:  "a check_run delivery with no check_run object is not a CI event",
			event: "check_run",
			body:  `{"action":"completed","repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`,
			want:  WebhookEvent{Kind: "other"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(tt.body)
			got, err := c.DetectAndVerify(ghHeader(tt.event, secret, body), body, secret)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGitLabDetectAndVerify_PipelineHook(t *testing.T) {
	const secret = "gl-token"
	c := &GitLab{}

	tests := []struct {
		name string
		body string
		want WebhookEvent
	}{
		{
			name: "a successful pipeline IS the aggregate, so it may report green",
			body: `{"object_kind":"pipeline","object_attributes":{"id":31,"sha":"abc123","status":"success"},"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"}}`,
			want: WebhookEvent{Kind: "ci", Repo: "https://gitlab.com/g/p.git", HeadSHA: "abc123", CIStatus: CIMirrorGreen},
		},
		{
			name: "a failed pipeline is red",
			body: `{"object_kind":"pipeline","object_attributes":{"id":31,"sha":"abc123","status":"failed"},"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"}}`,
			want: WebhookEvent{Kind: "ci", Repo: "https://gitlab.com/g/p.git", HeadSHA: "abc123", CIStatus: CIMirrorRed},
		},
		{
			name: "a canceled pipeline is red, matching glCIStatus",
			body: `{"object_kind":"pipeline","object_attributes":{"id":31,"sha":"abc123","status":"canceled"},"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"}}`,
			want: WebhookEvent{Kind: "ci", Repo: "https://gitlab.com/g/p.git", HeadSHA: "abc123", CIStatus: CIMirrorRed},
		},
		{
			name: "a skipped pipeline is neutral, so green, matching glCIStatus",
			body: `{"object_kind":"pipeline","object_attributes":{"id":31,"sha":"abc123","status":"skipped"},"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"}}`,
			want: WebhookEvent{Kind: "ci", Repo: "https://gitlab.com/g/p.git", HeadSHA: "abc123", CIStatus: CIMirrorGreen},
		},
		{
			name: "a running pipeline is running",
			body: `{"object_kind":"pipeline","object_attributes":{"id":31,"sha":"abc123","status":"running"},"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"}}`,
			want: WebhookEvent{Kind: "ci", Repo: "https://gitlab.com/g/p.git", HeadSHA: "abc123", CIStatus: CIMirrorRunning},
		},
		{
			name: "a manual pipeline has not started, so pending",
			body: `{"object_kind":"pipeline","object_attributes":{"id":31,"sha":"abc123","status":"manual"},"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"}}`,
			want: WebhookEvent{Kind: "ci", Repo: "https://gitlab.com/g/p.git", HeadSHA: "abc123", CIStatus: CIMirrorPending},
		},
		{
			name: "a pipeline delivery with no sha is nothing we can match",
			body: `{"object_kind":"pipeline","object_attributes":{"id":31,"status":"success"},"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"}}`,
			want: WebhookEvent{Kind: "other"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			h.Set("X-Gitlab-Event", "Pipeline Hook")
			h.Set("X-Gitlab-Token", secret)
			got, err := c.DetectAndVerify(h, []byte(tt.body), secret)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// MirrorCIStatus is the bridge between the two CI vocabularies this repo
// carries: the PRState / GetCommitCIStatus one ("" | pending | success |
// failure) that the merge gate reads, and the MergeRequest.status.ciStatus
// mirror one (none | pending | running | green | red) that the CRD pins an enum
// on. Writing "success" onto the CR is rejected by the API server.
func TestMirrorCIStatus(t *testing.T) {
	for in, want := range map[string]string{
		"success": CIMirrorGreen,
		"failure": CIMirrorRed,
		"pending": CIMirrorPending,
		"":        CIMirrorNone,
		"garbage": CIMirrorNone,
	} {
		require.Equal(t, want, MirrorCIStatus(in), "MirrorCIStatus(%q)", in)
	}
}
