package scm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// glCIFake serves the four routes both CI derivations consult for one MR:
// the MR itself, its pipeline's jobs, its pipeline's bridges, and the head
// commit's statuses. Every route is optional; an unset one is not registered
// so an unexpected consult fails the test loudly.
type glCIFake struct {
	mr       map[string]any
	jobs     []map[string]any
	bridges  []map[string]any
	statuses []map[string]any

	bridgesHits int
}

func (f *glCIFake) server(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.EscapedPath() {
		case "/projects/g%2Fp/merge_requests/9":
			_ = json.NewEncoder(w).Encode(f.mr)
		case "/projects/g%2Fp/pipelines/555/jobs":
			_ = json.NewEncoder(w).Encode(f.jobs)
		case "/projects/g%2Fp/pipelines/555/bridges":
			f.bridgesHits++
			_ = json.NewEncoder(w).Encode(f.bridges)
		case "/projects/g%2Fp/repository/commits/deadbeef/statuses":
			_ = json.NewEncoder(w).Encode(f.statuses)
		default:
			t.Errorf("unexpected path %q", r.URL.EscapedPath())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

// TestGitLabPRChecksFailedChildPipeline is the live repro from #609:
// containers!1280 @ 049288e9 - parent pipeline failed, all four of its own
// jobs succeeded, and the real gating lives in a child pipeline reached
// through a trigger bridge. /jobs alone cannot see it.
func TestGitLabPRChecksFailedChildPipeline(t *testing.T) {
	f := &glCIFake{
		mr: map[string]any{
			"sha":          "deadbeef",
			"merge_status": "can_be_merged",
			"head_pipeline": map[string]any{
				"id":     int64(555),
				"sha":    "deadbeef",
				"status": "failed",
			},
		},
		jobs: []map[string]any{
			{"id": int64(101), "name": "lint:yaml", "status": "success", "web_url": "https://gitlab.example/j/101"},
			{"id": int64(102), "name": "lint:python", "status": "success", "web_url": "https://gitlab.example/j/102"},
			{"id": int64(103), "name": "parse", "status": "success", "web_url": "https://gitlab.example/j/103"},
			{"id": int64(104), "name": "check", "status": "success", "web_url": "https://gitlab.example/j/104"},
		},
		bridges: []map[string]any{
			{
				"id":      int64(201),
				"name":    "trigger:template",
				"status":  "failed",
				"web_url": "https://gitlab.example/b/201",
				"downstream_pipeline": map[string]any{
					"id":      int64(2763688809),
					"status":  "failed",
					"web_url": "https://gitlab.example/p/2763688809",
				},
			},
		},
	}
	srv := f.server(t)
	defer srv.Close()

	c := &GitLab{apiBase: srv.URL}
	got, err := c.PRChecks(context.Background(), "https://gitlab.example/g/p.git", "tok", 9)
	require.NoError(t, err)

	require.Equal(t, CIMirrorRed, got.Status, "the pipeline's own status is failed")
	require.False(t, got.Mergeable, "can_be_merged answers conflicts, not CI")

	require.Len(t, got.Checks, 5, "four jobs plus the bridge")
	bridge := got.Checks[4]
	require.Equal(t, "trigger:template", bridge.Name)
	require.Equal(t, "completed", bridge.Status)
	require.Equal(t, "failure", bridge.Conclusion)
	require.Equal(t, "https://gitlab.example/p/2763688809", bridge.URL, "the downstream pipeline is what an agent needs to open")
	require.Empty(t, bridge.JobID, "a bridge id is not a job id: /jobs/{id}/trace would 404")
	require.Equal(t, 1, f.bridgesHits)
}

// TestGitLabPRChecksBridgeWithoutDownstream covers a bridge whose downstream
// pipeline is absent (not yet created, or trigger without strategy:depend):
// the row still appears, pointing at the bridge's own web_url.
func TestGitLabPRChecksBridgeWithoutDownstream(t *testing.T) {
	f := &glCIFake{
		mr: map[string]any{
			"sha":           "deadbeef",
			"merge_status":  "can_be_merged",
			"head_pipeline": map[string]any{"id": int64(555), "sha": "deadbeef", "status": "running"},
		},
		jobs: []map[string]any{},
		bridges: []map[string]any{
			{"id": int64(201), "name": "trigger:template", "status": "created", "web_url": "https://gitlab.example/b/201"},
		},
	}
	srv := f.server(t)
	defer srv.Close()

	c := &GitLab{apiBase: srv.URL}
	got, err := c.PRChecks(context.Background(), "https://gitlab.example/g/p.git", "tok", 9)
	require.NoError(t, err)
	require.Equal(t, CIMirrorRunning, got.Status)
	require.Len(t, got.Checks, 1)
	require.Equal(t, "https://gitlab.example/b/201", got.Checks[0].URL)
	require.Equal(t, "queued", got.Checks[0].Status)
	require.Empty(t, got.Checks[0].JobID)
}

// TestGitLabCIStatusAgreement is the regression guard for the bug class in
// #609: two derivations of one fact that drifted apart. It drives BOTH
// PRChecks and GetPRState against the same fake and asserts they agree once
// the mirror vocabulary is narrowed to the gate one.
func TestGitLabCIStatusAgreement(t *testing.T) {
	tests := []struct {
		name string
		// head is the head_pipeline object, or nil for the no-pipeline case.
		head     map[string]any
		jobs     []map[string]any
		bridges  []map[string]any
		statuses []map[string]any
		wantGate string
	}{
		{
			name:     "pipeline success",
			head:     map[string]any{"id": int64(555), "sha": "deadbeef", "status": "success"},
			jobs:     []map[string]any{{"id": int64(101), "name": "build", "status": "success"}},
			wantGate: "success",
		},
		{
			name:     "pipeline failed behind green jobs",
			head:     map[string]any{"id": int64(555), "sha": "deadbeef", "status": "failed"},
			jobs:     []map[string]any{{"id": int64(101), "name": "build", "status": "success"}},
			bridges:  []map[string]any{{"id": int64(201), "name": "trigger:template", "status": "failed"}},
			wantGate: "failure",
		},
		{
			name:     "pipeline running behind green jobs",
			head:     map[string]any{"id": int64(555), "sha": "deadbeef", "status": "running"},
			jobs:     []map[string]any{{"id": int64(101), "name": "build", "status": "success"}},
			wantGate: "pending",
		},
		{
			name:     "pipeline canceled",
			head:     map[string]any{"id": int64(555), "sha": "deadbeef", "status": "canceled"},
			jobs:     []map[string]any{},
			wantGate: "failure",
		},
		{
			name:     "pipeline skipped is neutral",
			head:     map[string]any{"id": int64(555), "sha": "deadbeef", "status": "skipped"},
			jobs:     []map[string]any{},
			wantGate: "success",
		},
		{
			name:     "pipeline status absent",
			head:     map[string]any{"id": int64(555), "sha": "deadbeef"},
			jobs:     []map[string]any{},
			wantGate: "",
		},
		{
			name:     "pipeline pending",
			head:     map[string]any{"id": int64(555), "sha": "deadbeef", "status": "created"},
			jobs:     []map[string]any{},
			wantGate: "pending",
		},
		{
			name:     "stale pipeline sha",
			head:     map[string]any{"id": int64(555), "sha": "0ldc0mm1t", "status": "success"},
			jobs:     []map[string]any{{"id": int64(101), "name": "build", "status": "success"}},
			wantGate: "pending",
		},
		{
			name:     "no head pipeline, no commit statuses",
			head:     nil,
			statuses: []map[string]any{},
			wantGate: "",
		},
		{
			name:     "no head pipeline, failing commit status",
			head:     nil,
			statuses: []map[string]any{{"status": "failed"}},
			wantGate: "failure",
		},
		{
			name:     "no head pipeline, passing commit status",
			head:     nil,
			statuses: []map[string]any{{"status": "success"}},
			wantGate: "success",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mr := map[string]any{
				"sha":           "deadbeef",
				"merge_status":  "can_be_merged",
				"state":         "opened",
				"source_branch": "feat/x",
				"head_pipeline": nil,
			}
			if tc.head != nil {
				mr["head_pipeline"] = tc.head
			}
			f := &glCIFake{mr: mr, jobs: tc.jobs, bridges: tc.bridges, statuses: tc.statuses}
			srv := f.server(t)
			defer srv.Close()

			c := &GitLab{apiBase: srv.URL}
			checks, err := c.PRChecks(context.Background(), "https://gitlab.example/g/p.git", "tok", 9)
			require.NoError(t, err)
			state, err := c.GetPRState(context.Background(), "https://gitlab.example/g/p.git", "tok", 9)
			require.NoError(t, err)

			require.Equal(t, tc.wantGate, state.CIStatus, "GetPRState is the merge gate's input and must not move")
			require.Equal(t, tc.wantGate, gateCIStatus(checks.Status),
				"PRChecks (%q) and GetPRState (%q) derive the same fact", checks.Status, state.CIStatus)
		})
	}
}

// TestGateCIStatus pins the documented inverse of MirrorCIStatus.
func TestGateCIStatus(t *testing.T) {
	tests := []struct{ in, want string }{
		{CIMirrorGreen, "success"},
		{CIMirrorRed, "failure"},
		{CIMirrorPending, "pending"},
		{CIMirrorRunning, "pending"},
		{CIMirrorNone, ""},
		{"nonsense", ""},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			require.Equal(t, tc.want, gateCIStatus(tc.in))
		})
	}
}

// TestGitLabPRChecksBridgesError asserts a /bridges failure propagates rather
// than silently degrading to the job-only view the bug was made of.
func TestGitLabPRChecksBridgesError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.EscapedPath() {
		case "/projects/g%2Fp/merge_requests/9":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"sha":           "deadbeef",
				"merge_status":  "can_be_merged",
				"head_pipeline": map[string]any{"id": int64(555), "sha": "deadbeef", "status": "failed"},
			})
		case "/projects/g%2Fp/pipelines/555/jobs":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/projects/g%2Fp/pipelines/555/bridges":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"message":"boom"}`))
		default:
			t.Errorf("unexpected path %q", r.URL.EscapedPath())
		}
	}))
	defer srv.Close()

	c := &GitLab{apiBase: srv.URL}
	_, err := c.PRChecks(context.Background(), "https://gitlab.example/g/p.git", "tok", 9)
	require.Error(t, err)
	var he *HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, http.StatusInternalServerError, he.Status)
}
