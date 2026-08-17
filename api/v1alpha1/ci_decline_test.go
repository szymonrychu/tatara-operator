package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func evMR(name, state, ownership, ci, head string) MergeRequest {
	mr := MergeRequest{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"}}
	mr.Status.State = state
	mr.Status.Ownership = ownership
	mr.Status.CIStatus = ci
	mr.Status.HeadSHA = head
	return mr
}

// CIDeclineEvidence is called on BOTH sides of the CI-recovery bound - the
// decline stamps what it returns, the driver re-derives it and compares - so
// every filtering rule it carries has to be asserted once, here, rather than
// twice in two packages that could drift.
func TestCIDeclineEvidence(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mrs       []MergeRequest
		wantCI    string
		wantHeads string
	}{
		{
			name:      "one red tatara-owned open MR is the measured shape",
			mrs:       []MergeRequest{evMR("mr-containers-1281", "open", OwnershipTatara, "red", "4c11cad2")},
			wantCI:    CIEvidenceRed,
			wantHeads: "mr-containers-1281@4c11cad2",
		},
		{
			name:      "all green",
			mrs:       []MergeRequest{evMR("mr-a-1", "open", OwnershipTatara, "green", "aaa")},
			wantCI:    CIEvidenceGreen,
			wantHeads: "mr-a-1@aaa",
		},
		{
			name:      "no CI verdict at all is unknown, never green",
			mrs:       []MergeRequest{evMR("mr-a-1", "open", OwnershipTatara, "", "aaa")},
			wantCI:    CIEvidenceUnknown,
			wantHeads: "mr-a-1@aaa",
		},
		{
			name:      "pending is unknown, not green and not red",
			mrs:       []MergeRequest{evMR("mr-a-1", "open", OwnershipTatara, "pending", "aaa")},
			wantCI:    CIEvidenceUnknown,
			wantHeads: "mr-a-1@aaa",
		},
		{
			name: "red beats unknown whatever the order",
			mrs: []MergeRequest{
				evMR("mr-b-2", "open", OwnershipTatara, "pending", "bbb"),
				evMR("mr-a-1", "open", OwnershipTatara, "red", "aaa"),
			},
			wantCI:    CIEvidenceRed,
			wantHeads: "mr-a-1@aaa,mr-b-2@bbb",
		},
		{
			name: "green only when EVERY one of them is green",
			mrs: []MergeRequest{
				evMR("mr-a-1", "open", OwnershipTatara, "green", "aaa"),
				evMR("mr-b-2", "open", OwnershipTatara, "running", "bbb"),
			},
			wantCI:    CIEvidenceUnknown,
			wantHeads: "mr-a-1@aaa,mr-b-2@bbb",
		},
		{
			name:      "an EXTERNAL merge request is a human's and is never in the set",
			mrs:       []MergeRequest{evMR("mr-a-1", "open", OwnershipExternal, "green", "aaa")},
			wantCI:    "",
			wantHeads: "",
		},
		{
			name:      "an unclassified merge request is not tatara's yet",
			mrs:       []MergeRequest{evMR("mr-a-1", "open", "", "green", "aaa")},
			wantCI:    "",
			wantHeads: "",
		},
		{
			name:      "a merged merge request leaves the set",
			mrs:       []MergeRequest{evMR("mr-a-1", "merged", OwnershipTatara, "green", "aaa")},
			wantCI:    "",
			wantHeads: "",
		},
		{
			name:      "a closed merge request leaves the set",
			mrs:       []MergeRequest{evMR("mr-a-1", "closed", OwnershipTatara, "green", "aaa")},
			wantCI:    "",
			wantHeads: "",
		},
		{
			name:      "an unsynced state still reads as open",
			mrs:       []MergeRequest{evMR("mr-a-1", "", OwnershipTatara, "green", "aaa")},
			wantCI:    CIEvidenceGreen,
			wantHeads: "mr-a-1@aaa",
		},
		{
			name: "an unknown head makes the whole fingerprint meaningless",
			mrs: []MergeRequest{
				evMR("mr-a-1", "open", OwnershipTatara, "red", "aaa"),
				evMR("mr-b-2", "open", OwnershipTatara, "red", ""),
			},
			wantCI:    "",
			wantHeads: "",
		},
		{
			name:      "no owned merge request at all",
			mrs:       nil,
			wantCI:    "",
			wantHeads: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ci, heads := CIDeclineEvidence(tc.mrs)
			require.Equal(t, tc.wantCI, ci)
			require.Equal(t, tc.wantHeads, heads)
		})
	}
}

// The fingerprint is ORDER-INDEPENDENT. The two sides load the owned merge
// requests through different helpers (restapi by controller-ownership, the
// driver off status.mrRefs), and a fingerprint that depended on slice order
// would compare unequal for the same world and silently disable the recovery.
func TestCIDeclineEvidence_FingerprintIsOrderIndependent(t *testing.T) {
	a := evMR("mr-a-1", "open", OwnershipTatara, "green", "aaa")
	b := evMR("mr-b-2", "open", OwnershipTatara, "green", "bbb")

	_, one := CIDeclineEvidence([]MergeRequest{a, b})
	_, two := CIDeclineEvidence([]MergeRequest{b, a})
	require.Equal(t, one, two)
}
