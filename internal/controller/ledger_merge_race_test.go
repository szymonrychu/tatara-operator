package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestUpsertWorkItem_MergeRefreshPreservesConcurrentAppendAndCarriesMemberFields is
// the finding-1 regression: the umbrella/backstop refresh persist must merge each
// refreshed member into the freshly-read Task via UpsertWorkItem, NOT assign the
// snapshot slice wholesale. A blind `fresh.Status.WorkItems = <snapshot>` under
// RetryOnConflict never conflicts (full overwrite) so it drops a member appended
// concurrently by another writer (e.g. joinStreamReview adding a stream PR to the
// review span between the read and the persist), which would make a later review
// approve/merge a stream MISSING a PR.
//
// It also locks that UpsertWorkItem now carries the umbrella member-state fields
// (Labels/Body/CIStatus/Mergeable) so the merge-refresh is lossless.
func TestUpsertWorkItem_MergeRefreshPreservesConcurrentAppendAndCarriesMemberFields(t *testing.T) {
	// fresh simulates the freshly-read Task inside the retry closure: it holds the
	// original member A plus a PR B appended concurrently by another writer.
	fresh := &tatarav1alpha1.Task{}
	fresh.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/a", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/b", Number: 9, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadBranch: "feat/x"}, // concurrent append
	}

	// refreshed is the poll result for A only (B was not in the snapshot the poller saw).
	refreshed := []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/a", Number: 7, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIClosed,
			Labels: []string{"tatara-implementation"}, Body: "updated body", CIStatus: "success", Mergeable: "clean"},
	}
	for i := range refreshed {
		UpsertWorkItem(fresh, refreshed[i])
	}

	// The concurrently-appended PR B must survive the refresh persist.
	require.Len(t, fresh.Status.WorkItems, 2, "the refresh must not drop the concurrently-appended PR")
	var b *tatarav1alpha1.WorkItemRef
	for i := range fresh.Status.WorkItems {
		if fresh.Status.WorkItems[i].Repo == "o/b" && fresh.Status.WorkItems[i].Number == 9 {
			b = &fresh.Status.WorkItems[i]
		}
	}
	require.NotNil(t, b, "concurrently-appended stream PR must not be dropped by the refresh persist")
	require.Equal(t, tatarav1alpha1.RoleOpenedPR, b.Role)

	// Member A refreshed in place, including the umbrella member-state fields.
	a := fresh.Status.WorkItems[0]
	require.Equal(t, tatarav1alpha1.WIClosed, a.State)
	require.Equal(t, []string{"tatara-implementation"}, a.Labels, "Labels must be carried by the merge")
	require.Equal(t, "updated body", a.Body, "Body must be carried by the merge")
	require.Equal(t, "success", a.CIStatus, "CIStatus must be carried by the merge")
	require.Equal(t, "clean", a.Mergeable, "Mergeable must be carried by the merge")
}
