package v1alpha1_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestIntakeTaskName_DeterministicAndBounded(t *testing.T) {
	a := v1alpha1.IntakeTaskName("tatara", "clarify", "tatara-operator", 353)
	b := v1alpha1.IntakeTaskName("tatara", "clarify", "tatara-operator", 353)
	require.Equal(t, a, b, "same natural key must yield the same name")
	require.LessOrEqual(t, len(a), v1alpha1.MaxTaskNameLength)
	require.False(t, v1alpha1.TaskNameTooLong(a))
	require.False(t, strings.HasPrefix(a, "-"))
	require.False(t, strings.HasSuffix(a, "-"))
}

func TestIntakeTaskName_DistinctByKeyPart(t *testing.T) {
	base := v1alpha1.IntakeTaskName("tatara", "clarify", "tatara-operator", 353)
	require.NotEqual(t, base, v1alpha1.IntakeTaskName("tatara", "review", "tatara-operator", 353))
	require.NotEqual(t, base, v1alpha1.IntakeTaskName("tatara", "clarify", "tatara-operator", 354))
	require.NotEqual(t, base, v1alpha1.IntakeTaskName("tatara", "clarify", "tatara-cli", 353))
	require.NotEqual(t, base, v1alpha1.IntakeTaskName("other", "clarify", "tatara-operator", 353))
}

// A very long repoRef must still produce a valid, bounded name.
func TestIntakeTaskName_LongRepoRefStaysBounded(t *testing.T) {
	long := strings.Repeat("x", 200)
	n := v1alpha1.IntakeTaskName("tatara", "clarify", long, 1)
	require.LessOrEqual(t, len(n), v1alpha1.MaxTaskNameLength)
	require.False(t, strings.HasSuffix(n, "-"))
}

func TestIntakeTaskName_UppercaseKindStaysDNSSafe(t *testing.T) {
	n := v1alpha1.IntakeTaskName("tatara", "REVIEW", "tatara-operator", 42)
	require.LessOrEqual(t, len(n), v1alpha1.MaxTaskNameLength)
	require.False(t, strings.HasPrefix(n, "-"))
	require.False(t, strings.HasSuffix(n, "-"))
	for _, r := range n {
		require.True(t, (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-',
			"name must be DNS-1123-label-safe, got %q", n)
	}
}
