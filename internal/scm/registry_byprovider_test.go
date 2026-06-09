package scm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestByProvider(t *testing.T) {
	gh, err := ByProvider("github")
	require.NoError(t, err)
	ghClient, ok := gh.(Client)
	require.True(t, ok, "github SCMWriter must also implement Client")
	require.Equal(t, "github", ghClient.Provider())

	gl, err := ByProvider("gitlab")
	require.NoError(t, err)
	glClient, ok := gl.(Client)
	require.True(t, ok, "gitlab SCMWriter must also implement Client")
	require.Equal(t, "gitlab", glClient.Provider())

	_, err = ByProvider("bitbucket")
	require.Error(t, err)
}
