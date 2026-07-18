package webhook

// F8-1 / F6: the human-review (review.id, state) dedup is ONE bounded annotation
// (annReviewed), a drop-oldest FIFO of hashed entries. White-box tests for the
// dedup helpers because they are unexported.

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// The dedup annotation NAME segment must always fit the k8s 63-char limit, for
// ANY forge review id - including GitLab's synthesized
// "gl-approve-<IID>-<40hexsha>" which overflowed the old per-review key scheme.
func TestReviewDedup_AnnotationNameBounded(t *testing.T) {
	nameSeg := annReviewed
	if i := strings.IndexByte(annReviewed, '/'); i >= 0 {
		nameSeg = annReviewed[i+1:]
	}
	require.LessOrEqual(t, len(nameSeg), 63, "annotation name segment must fit the k8s limit")
}

// A GitLab-style long review id round-trips through stamp -> reprocess-check: the
// entry persists (the old scheme silently failed the Update for IID>=100, so
// GitLab dedup never persisted at all).
func TestReviewDedup_GitLabLongIDPersistsAndDedups(t *testing.T) {
	glID := "gl-approve-100-" + strings.Repeat("a", 40) // 55 chars; old key blew 63
	task := &tatarav1.Task{ObjectMeta: metav1.ObjectMeta{Name: "t"}}

	require.False(t, reviewAlreadyProcessed(task, glID, "approved"))
	// Simulate the stamp's append onto the annotation.
	if task.Annotations == nil {
		task.Annotations = map[string]string{}
	}
	task.Annotations[annReviewed] = appendReviewedEntry("", reviewEntry(glID, "approved"), maxReviewedEntries)
	require.True(t, reviewAlreadyProcessed(task, glID, "approved"), "the GitLab review must now dedup")
	require.False(t, reviewAlreadyProcessed(task, glID, "changes_requested"),
		"dedup is per (id, STATE): a different state is not deduped")
}

// The FIFO caps at maxReviewedEntries, drop-oldest, and a re-stamp of the same
// entry is idempotent (no growth, no duplicate).
func TestReviewDedup_FIFOBoundedAndIdempotent(t *testing.T) {
	cur := ""
	for i := 0; i < maxReviewedEntries+10; i++ {
		cur = appendReviewedEntry(cur, reviewEntry(string(rune('a'+i%26))+intToStr(i), "approved"), maxReviewedEntries)
	}
	require.LessOrEqual(t, len(strings.Split(cur, ",")), maxReviewedEntries, "FIFO must be bounded")

	// Idempotent re-stamp: appending an entry already present does not grow it.
	e := reviewEntry("gh-777", "approved")
	one := appendReviewedEntry("", e, maxReviewedEntries)
	two := appendReviewedEntry(one, e, maxReviewedEntries)
	require.Equal(t, one, two, "re-stamping the same entry is a no-op")

	// Drop-oldest: the newest entry survives; the oldest falls off past the cap.
	oldest := reviewEntry("gh-oldest", "approved")
	filled := appendReviewedEntry("", oldest, maxReviewedEntries)
	for i := 0; i < maxReviewedEntries; i++ {
		filled = appendReviewedEntry(filled, reviewEntry("gh-"+intToStr(i), "approved"), maxReviewedEntries)
	}
	require.NotContains(t, filled, strings.Split(oldest, ":")[0], "the oldest entry is evicted past the cap")
}

func intToStr(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
