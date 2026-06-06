package scm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestByProvider(t *testing.T) {
	gh, err := ByProvider("github")
	require.NoError(t, err)
	require.Equal(t, "github", gh.Provider())

	gl, err := ByProvider("gitlab")
	require.NoError(t, err)
	require.Equal(t, "gitlab", gl.Provider())

	_, err = ByProvider("bitbucket")
	require.Error(t, err)
}
