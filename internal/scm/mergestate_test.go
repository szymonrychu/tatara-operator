package scm

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		{MergeStateUnstable, "unstable"},
	}
	for _, c := range cases {
		t.Run(c.want, func(t *testing.T) {
			assert.Equal(t, c.want, string(c.got))
		})
	}
}

// requiredChecksStub answers ONLY the required-context question. SCMWriter is
// embedded as a nil interface, so any other call panics: this proves
// CIRedSuppressed reads nothing else.
type requiredChecksStub struct {
	SCMWriter
	required bool
	err      error
	calls    int
}

func (s *requiredChecksStub) HasRequiredChecks(context.Context, string, string, int) (bool, error) {
	s.calls++
	return s.required, s.err
}

// noRequiredChecksCapability is a writer that cannot answer the question at all
// - the GitLab shape, and the shape of any adapter added later.
type noRequiredChecksCapability struct{ SCMWriter }

// THE SUPPRESSION IS TWO CONDITIONS, NOT ONE, AND BOTH FAIL CLOSED.
//
// GitHub reports mergeable_state=unstable for a red NON-REQUIRED check - the
// tatara-helmfile#424 shape this whole carve-out exists for. It reports exactly
// the same thing for a repo with NO required contexts at all: no branch
// protection, or protection with zero required status checks. On such a repo
// EVERY red PR is unstable, so a suppression keyed on the merge state alone
// deletes the entire ci-red axis there. The second condition - the PR actually
// carrying a required context - is what tells the two apart.
func TestCIRedSuppressed(t *testing.T) {
	errForge := errors.New("403 from the forge")
	t.Run("unstable with a required context is the #424 shape and suppresses", func(t *testing.T) {
		w := &requiredChecksStub{required: true}
		got, err := CIRedSuppressed(context.Background(), w, "https://github.com/o/r", "tok", 424, MergeStateUnstable)
		require.NoError(t, err)
		assert.True(t, got)
		assert.Equal(t, 1, w.calls)
	})
	t.Run("unstable with NO required context is an unprotected repo and must NOT suppress", func(t *testing.T) {
		w := &requiredChecksStub{required: false}
		got, err := CIRedSuppressed(context.Background(), w, "https://github.com/o/r", "tok", 7, MergeStateUnstable)
		require.NoError(t, err)
		assert.False(t, got, "on a repo with no required contexts, unstable is what EVERY red PR reports")
	})
	t.Run("an unreadable required-context set fails CLOSED", func(t *testing.T) {
		w := &requiredChecksStub{required: true, err: errForge}
		got, err := CIRedSuppressed(context.Background(), w, "https://github.com/o/r", "tok", 7, MergeStateUnstable)
		require.ErrorIs(t, err, errForge, "the caller must be able to log WHY it refused to suppress")
		assert.False(t, got)
	})
	t.Run("a writer that cannot answer the question never suppresses", func(t *testing.T) {
		got, err := CIRedSuppressed(context.Background(), &noRequiredChecksCapability{},
			"https://gitlab.com/o/r", "tok", 7, MergeStateUnstable)
		require.NoError(t, err)
		assert.False(t, got)
	})
	// EVERY OTHER MERGE STATE KEEPS TODAY'S FAIL-CLOSED BEHAVIOUR, and clean is
	// the one that matters: GitLab answers can_be_merged with a RED pipeline, so
	// suppressing on clean would stop honouring red CI on every GitLab MR. The
	// required-context read must not even be issued for these - it is a forge
	// call, and a call made on a path that can never suppress is pure cost.
	for _, ms := range []MergeState{MergeStateClean, MergeStateDirty, MergeStateBlocked, MergeStateBehind, MergeStateUnknown} {
		t.Run("state "+string(ms)+" never suppresses", func(t *testing.T) {
			w := &requiredChecksStub{required: true}
			got, err := CIRedSuppressed(context.Background(), w, "https://github.com/o/r", "tok", 7, ms)
			require.NoError(t, err)
			assert.False(t, got)
			assert.Zero(t, w.calls, "the required-context read must not be issued when the state can never suppress")
		})
	}
}
