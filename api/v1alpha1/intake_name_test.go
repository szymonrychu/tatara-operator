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

// F1-1: the literal issue/PR number is IN the visible name, and even with the
// repoRef truncated to nothing the name stays unique per (repo, number) and
// DNS-safe. The number is unique per repo, so two natural keys can never share a
// name regardless of how their repoRefs sanitize.
func TestIntakeTaskName_NumberIsLiteralAndBounded(t *testing.T) {
	long := strings.Repeat("z", 200)
	// A repoRef long enough to consume the whole repo budget still leaves the
	// number literally in the name.
	a := v1alpha1.IntakeTaskName("tatara", "clarify", long, 353)
	b := v1alpha1.IntakeTaskName("tatara", "clarify", long, 354)
	require.NotEqual(t, a, b, "distinct numbers must yield distinct names even when repoRef is fully truncated")
	require.Contains(t, a, "-353-", "the literal number must appear in the visible name")
	require.Contains(t, b, "-354-")
	require.LessOrEqual(t, len(a), v1alpha1.MaxTaskNameLength)
	require.LessOrEqual(t, len(b), v1alpha1.MaxTaskNameLength)
}

// F1-1: a wide (64-bit) suffix makes a hash collision across distinct keys
// astronomically unlikely - the old 32-bit suffix had a ~1% birthday risk at
// 10k names. Sample distinct numbers on one repo: all names distinct.
func TestIntakeTaskName_NoCollisionAcrossNumbers(t *testing.T) {
	seen := map[string]int{}
	for i := 0; i < 5000; i++ {
		n := v1alpha1.IntakeTaskName("tatara", "clarify", "tatara-operator", i)
		if prev, ok := seen[n]; ok {
			t.Fatalf("collision: number %d and %d both produced %q", prev, i, n)
		}
		seen[n] = i
	}
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
