package scm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReaderByProvider(t *testing.T) {
	cases := []struct {
		name     string
		provider string
		wantErr  bool
	}{
		{"github", "github", false},
		{"gitlab", "gitlab", false},
		{"unknown", "bitbucket", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rd, err := ReaderByProvider(tc.provider, "tok")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error for %q", tc.provider)
				}
				return
			}
			if err != nil || rd == nil {
				t.Fatalf("ReaderByProvider(%q): %v", tc.provider, err)
			}
		})
	}
}

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
