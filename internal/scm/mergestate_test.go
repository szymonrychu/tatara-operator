package scm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeStateConstants(t *testing.T) {
	cases := []struct {
		got  MergeState
		want string
	}{
		{MergeStateUnknown, "unknown"},
		{MergeStateClean, "clean"},
		{MergeStateDirty, "dirty"},
		{MergeStateBlocked, "blocked"},
		{MergeStateBehind, "behind"},
	}
	for _, c := range cases {
		t.Run(c.want, func(t *testing.T) {
			assert.Equal(t, c.want, string(c.got))
		})
	}
}
