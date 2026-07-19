package v1alpha1

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMergeRequest_JSONRoundTrip(t *testing.T) {
	now := metav1.NewTime(time.Now().Truncate(time.Second))
	mr := &MergeRequest{
		Spec: MergeRequestSpec{
			RepositoryRef: "tatara-cli",
			Number:        80,
			URL:           "https://github.com/szymonrychu/tatara-cli/pull/80",
			ProjectRef:    "tatara",
		},
		Status: MergeRequestStatus{
			Title:      "title",
			Author:     "someone",
			Body:       "body text",
			CreatedAt:  &now,
			UpdatedAt:  &now,
			State:      "open",
			Status:     "new",
			HeadBranch: "feat/x",
			HeadSHA:    "deadbeef",
			CIStatus:   "green",
			Mergeable:  true,
			Comments: []Comment{
				{ExternalID: "1", Author: "reviewer", Body: "lgtm", CreatedAt: now},
			},
			CommentCount:         1,
			SpilledComments:      1,
			SpilledCommentsRefs:  []string{"track-1"},
			CommentsRetainedFrom: &now,
			MergedAt:             &now,
			DeployedAt:           &now,
			DeployedVersion:      "v1.2.3",
			Significance:         "minor",
			ReviewedSHA:          "cafebabe",
			ReviewRounds:         2,
			PendingReview: &PendingReview{
				Body: "review body",
				Findings: []ReviewFinding{
					{Path: "main.go", Line: intPtr(1), Body: "issue", Severity: "medium"},
				},
				SHA:   "deadbeef",
				Round: 1,
			},
			PendingComments: []PendingComment{
				{RequestID: "r1", Action: "comment", Body: "hi"},
			},
			LastSyncedAt: &now,
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Synced", LastTransitionTime: now},
			},
		},
	}

	data, err := json.Marshal(mr)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	for _, tag := range []string{
		`"repositoryRef"`, `"number"`, `"url"`, `"projectRef"`,
		`"headSHA"`, `"ciStatus"`, `"mergeable"`, `"significance"`,
		`"reviewedSHA"`, `"reviewRounds"`, `"deployedAt"`, `"deployedVersion"`,
		`"mergedAt"`, `"pendingReview"`, `"pendingComments"`,
	} {
		if !strings.Contains(string(data), tag) {
			t.Errorf("marshaled MergeRequest missing tag %s\ngot: %s", tag, data)
		}
	}

	var round MergeRequest
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if round.Spec.RepositoryRef != mr.Spec.RepositoryRef || round.Spec.Number != mr.Spec.Number {
		t.Errorf("Spec did not round-trip: got %+v", round.Spec)
	}
	if round.Status.HeadSHA != "deadbeef" {
		t.Errorf("HeadSHA did not round-trip")
	}
	if round.Status.Significance != "minor" {
		t.Errorf("Significance did not round-trip")
	}
	if round.Status.PendingReview == nil || round.Status.PendingReview.Round != 1 {
		t.Fatalf("PendingReview did not round-trip")
	}
	if len(round.Status.PendingReview.Findings) != 1 {
		t.Fatalf("PendingReview.Findings did not round-trip")
	}
	if len(round.Status.PendingComments) != 1 {
		t.Fatalf("PendingComments did not round-trip")
	}
	if round.Status.ReviewRounds != 2 {
		t.Errorf("ReviewRounds did not round-trip")
	}
}

func TestMergeRequestStatus_PendingReviewNilMeansNoReviewOwed(t *testing.T) {
	st := MergeRequestStatus{}
	data, err := json.Marshal(st)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(data), `"pendingReview"`) {
		t.Errorf("nil PendingReview must be omitted from the wire (omitempty): %s", data)
	}
	var round MergeRequestStatus
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if round.PendingReview != nil {
		t.Errorf("PendingReview should stay nil")
	}
}
