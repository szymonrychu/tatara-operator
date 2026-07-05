package scm

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSemverTag(t *testing.T) {
	cases := []struct {
		in    string
		ok    bool
		major int
		minor int
		patch int
	}{
		{"v1.4.0", true, 1, 4, 0},
		{"1.4.0", true, 1, 4, 0},
		{"v2.0.1-rc1", true, 2, 0, 1},
		{"v0.0.0-gabc123", true, 0, 0, 0},
		{"main", false, 0, 0, 0},
		{"v1.4", false, 0, 0, 0},
		{"vx.y.z", false, 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			tr, ok := parseSemverTag(tc.in)
			require.Equal(t, tc.ok, ok)
			if ok {
				require.Equal(t, tc.major, tr.major)
				require.Equal(t, tc.minor, tr.minor)
				require.Equal(t, tc.patch, tr.patch)
			}
		})
	}
}

func TestSemverTripleGreater(t *testing.T) {
	require.True(t, semverTriple{2, 0, 0}.greater(semverTriple{1, 9, 9}))
	require.True(t, semverTriple{1, 5, 0}.greater(semverTriple{1, 4, 9}))
	require.True(t, semverTriple{1, 4, 2}.greater(semverTriple{1, 4, 1}))
	require.False(t, semverTriple{1, 4, 1}.greater(semverTriple{1, 4, 1}))
}

func TestGitHub_LatestSemverTag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.True(t, strings.Contains(r.URL.Path, "/repos/o/repo/tags"))
		_, _ = w.Write([]byte(`[{"name":"v1.2.0"},{"name":"v1.10.0"},{"name":"latest"},{"name":"v1.9.9"}]`))
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	tag, ok, err := c.LatestSemverTag(context.Background(), "o", "repo")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "v1.10.0", tag, "10 > 9 numerically, not lexically")
}

func TestGitHub_LatestSemverTag_None(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"name":"main"},{"name":"latest"}]`))
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	_, ok, err := c.LatestSemverTag(context.Background(), "o", "repo")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestGitHub_LatestWorkflowRun(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Contains(t, r.URL.Path, "/repos/o/hf/actions/workflows/apply.yaml/runs")
		require.Equal(t, "main", r.URL.Query().Get("branch"))
		_, _ = w.Write([]byte(`{"workflow_runs":[{"head_sha":"deadbeef","status":"completed","conclusion":"success","html_url":"https://run/9","created_at":"2026-06-28T10:00:00Z"}]}`))
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	run, ok, err := c.LatestWorkflowRun(context.Background(), "o", "hf", "apply.yaml", "main")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "deadbeef", run.HeadSHA)
	require.Equal(t, "completed", run.Status)
	require.Equal(t, "success", run.Conclusion)
}

func TestGitHub_LatestWorkflowRun_None(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"workflow_runs":[]}`))
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	_, ok, err := c.LatestWorkflowRun(context.Background(), "o", "hf", "apply.yaml", "main")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestGitHub_GetFileContent(t *testing.T) {
	want := "  version: 1.4.0\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Contains(t, r.URL.Path, "/repos/o/hf/contents/helmfile.yaml.gotmpl")
		require.Equal(t, "abc123", r.URL.Query().Get("ref"))
		enc := base64.StdEncoding.EncodeToString([]byte(want))
		// GitHub wraps base64 at 60 cols, escaping the wraps as \n in the JSON
		// string. Emulate one to exercise the newline strip in GetFileContent.
		enc = enc[:4] + `\n` + enc[4:]
		_, _ = w.Write([]byte(`{"content":"` + enc + `","encoding":"base64"}`))
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	got, err := c.GetFileContent(context.Background(), "o", "hf", "helmfile.yaml.gotmpl", "abc123")
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestGitHub_GetFileContent_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Not Found"}`))
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	got, err := c.GetFileContent(context.Background(), "o", "hf", "missing.yaml", "main")
	require.NoError(t, err, "404 is benign for pin-file probing")
	require.Equal(t, "", got)
}
