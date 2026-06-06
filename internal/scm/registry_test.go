package scm

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectByHeader(t *testing.T) {
	gh := http.Header{}
	gh.Set("X-GitHub-Event", "push")
	c, err := Select(gh)
	require.NoError(t, err)
	require.Equal(t, "github", c.Provider())

	gl := http.Header{}
	gl.Set("X-Gitlab-Event", "Push Hook")
	c, err = Select(gl)
	require.NoError(t, err)
	require.Equal(t, "gitlab", c.Provider())

	_, err = Select(http.Header{})
	require.Error(t, err)
}

func TestSameRemote(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"https://github.com/o/r.git", "https://github.com/o/r", true},
		{"https://github.com/o/r/", "https://github.com/o/r.git", true},
		{"https://GitHub.com/o/r.git", "https://github.com/o/r", true},
		{"https://github.com/o/r.git", "https://github.com/o/other.git", false},
		{"https://gitlab.com/g/p.git", "https://gitlab.com/g/p", true},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, SameRemote(tt.a, tt.b), "%s vs %s", tt.a, tt.b)
	}
}
