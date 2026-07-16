package v1alpha1

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var rfc1123LabelRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func TestIssueName(t *testing.T) {
	got := IssueName("tatara-operator", 291)
	want := "iss-tatara-operator-291"
	if got != want {
		t.Fatalf("IssueName() = %q, want %q", got, want)
	}
	if len(got) > 63 {
		t.Fatalf("IssueName() = %q, exceeds 63 chars", got)
	}
	if !rfc1123LabelRE.MatchString(got) {
		t.Fatalf("IssueName() = %q, not a valid RFC-1123 label", got)
	}
}

func TestMergeRequestName(t *testing.T) {
	got := MergeRequestName("tatara-cli", 80)
	want := "mr-tatara-cli-80"
	if got != want {
		t.Fatalf("MergeRequestName() = %q, want %q", got, want)
	}
	if !rfc1123LabelRE.MatchString(got) {
		t.Fatalf("MergeRequestName() = %q, not a valid RFC-1123 label", got)
	}
}

func TestIssue_JSONRoundTrip(t *testing.T) {
	now := metav1.NewTime(time.Now().Truncate(time.Second))
	iss := &Issue{
		Spec: IssueSpec{
			RepositoryRef: "tatara-operator",
			Number:        291,
			URL:           "https://github.com/szymonrychu/tatara-operator/issues/291",
			ProjectRef:    "tatara",
		},
		Status: IssueStatus{
			Title:     "title",
			Author:    "someone",
			Body:      "body text",
			CreatedAt: &now,
			UpdatedAt: &now,
			State:     "open",
			Status:    "new",
			Labels:    []string{"bug"},
			Comments: []Comment{
				{
					ExternalID:  "123",
					Author:      "reviewer",
					Body:        "lgtm",
					CreatedAt:   now,
					IsBot:       false,
					Truncated:   false,
					Path:        "main.go",
					Line:        42,
					InReplyTo:   "122",
					ReviewRound: 1,
				},
			},
			CommentCount:        1,
			SpilledComments:     2,
			SpilledCommentsRefs: []string{"track-1", "track-2"},
			Approval: &ApprovalEvidence{
				Login:     "maintainer",
				CommentID: "999",
				CreatedAt: now,
				Phrase:    "LGTM approve",
				Auto:      false,
			},
			CommentsRetainedFrom: &now,
			PendingComments: []PendingComment{
				{
					RequestID: "req-1",
					Action:    "comment",
					Body:      "hi",
					InReplyTo: "1",
				},
			},
			LastSyncedAt: &now,
			Conditions: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					Reason:             "Synced",
					LastTransitionTime: now,
				},
			},
		},
	}

	data, err := json.Marshal(iss)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	for _, tag := range []string{
		`"repositoryRef"`, `"number"`, `"url"`, `"projectRef"`,
		`"title"`, `"author"`, `"body"`, `"createdAt"`, `"updatedAt"`,
		`"state"`, `"status"`, `"labels"`, `"comments"`, `"commentCount"`,
		`"spilledComments"`, `"spilledCommentsRefs"`, `"approval"`,
		`"commentsRetainedFrom"`, `"lastSyncedAt"`, `"conditions"`,
	} {
		if !strings.Contains(string(data), tag) {
			t.Errorf("marshaled Issue missing tag %s\ngot: %s", tag, data)
		}
	}

	var round Issue
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if round.Spec.RepositoryRef != iss.Spec.RepositoryRef {
		t.Errorf("RepositoryRef = %q, want %q", round.Spec.RepositoryRef, iss.Spec.RepositoryRef)
	}
	if round.Spec.Number != iss.Spec.Number {
		t.Errorf("Number = %d, want %d", round.Spec.Number, iss.Spec.Number)
	}
	if round.Spec.URL != iss.Spec.URL {
		t.Errorf("URL = %q, want %q", round.Spec.URL, iss.Spec.URL)
	}
	if round.Spec.ProjectRef != iss.Spec.ProjectRef {
		t.Errorf("ProjectRef = %q, want %q", round.Spec.ProjectRef, iss.Spec.ProjectRef)
	}
	if round.Status.Status != "new" {
		t.Errorf("Status.Status = %q, want new", round.Status.Status)
	}
	if len(round.Status.PendingComments) != 1 {
		t.Fatalf("PendingComments did not round-trip")
	}
	if round.Status.Comments[0].ReviewRound != 1 {
		t.Errorf("Comment.ReviewRound did not round-trip")
	}
	if round.Status.Approval == nil || round.Status.Approval.Phrase != "LGTM approve" {
		t.Errorf("Approval did not round-trip")
	}
	if round.Status.CommentsRetainedFrom == nil {
		t.Errorf("CommentsRetainedFrom did not round-trip")
	}
	if len(round.Status.SpilledCommentsRefs) != 2 {
		t.Errorf("SpilledCommentsRefs did not round-trip (accumulation)")
	}
}

func TestComment_JSONRoundTrip(t *testing.T) {
	now := metav1.NewTime(time.Now().Truncate(time.Second))
	c := Comment{
		ExternalID:  "42",
		Author:      "octocat",
		Body:        "inline finding",
		CreatedAt:   now,
		IsBot:       true,
		Truncated:   true,
		Path:        "internal/foo.go",
		Line:        7,
		InReplyTo:   "41",
		ReviewRound: 3,
	}
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, tag := range []string{
		`"externalId"`, `"author"`, `"body"`, `"createdAt"`, `"isBot"`,
		`"truncated"`, `"path"`, `"line"`, `"inReplyTo"`, `"reviewRound"`,
	} {
		if !strings.Contains(string(data), tag) {
			t.Errorf("marshaled Comment missing tag %s\ngot: %s", tag, data)
		}
	}
	var round Comment
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if round != c {
		t.Errorf("round-trip mismatch: got %+v, want %+v", round, c)
	}
}

func TestApprovalEvidence_JSONRoundTrip(t *testing.T) {
	now := metav1.NewTime(time.Now().Truncate(time.Second))
	a := ApprovalEvidence{
		Login:     "maintainer",
		CommentID: "1001",
		CreatedAt: now,
		Phrase:    "approved",
		Auto:      true,
	}
	data, err := json.Marshal(a)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, tag := range []string{`"login"`, `"commentId"`, `"createdAt"`, `"phrase"`, `"auto"`} {
		if !strings.Contains(string(data), tag) {
			t.Errorf("marshaled ApprovalEvidence missing tag %s\ngot: %s", tag, data)
		}
	}
	var round ApprovalEvidence
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if round != a {
		t.Errorf("round-trip mismatch: got %+v, want %+v", round, a)
	}
}

func TestPendingReview_JSONRoundTrip(t *testing.T) {
	pr := PendingReview{
		Body: "review body",
		Findings: []ReviewFinding{
			{Path: "main.go", Line: 10, Body: "fix this", Severity: "high"},
		},
		SHA:   "deadbeef",
		Round: 2,
	}
	data, err := json.Marshal(pr)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, tag := range []string{`"body"`, `"findings"`, `"sha"`, `"round"`} {
		if !strings.Contains(string(data), tag) {
			t.Errorf("marshaled PendingReview missing tag %s\ngot: %s", tag, data)
		}
	}
	if strings.Contains(string(data), `"event"`) {
		t.Errorf("PendingReview must NOT have an event field on the wire: %s", data)
	}
	var round PendingReview
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if round.SHA != pr.SHA || round.Round != pr.Round || round.Body != pr.Body {
		t.Errorf("round-trip mismatch: got %+v, want %+v", round, pr)
	}
	if len(round.Findings) != 1 {
		t.Fatalf("Findings did not round-trip")
	}
}

func TestReviewFinding_JSONRoundTrip(t *testing.T) {
	rf := ReviewFinding{
		Path:     "internal/foo.go",
		Line:     5,
		Body:     "unhandled error",
		Severity: "critical",
	}
	data, err := json.Marshal(rf)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, tag := range []string{`"path"`, `"line"`, `"body"`, `"severity"`} {
		if !strings.Contains(string(data), tag) {
			t.Errorf("marshaled ReviewFinding missing tag %s\ngot: %s", tag, data)
		}
	}
	var round ReviewFinding
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if round != rf {
		t.Errorf("round-trip mismatch: got %+v, want %+v", round, rf)
	}
}

func TestPendingComment_JSONRoundTrip(t *testing.T) {
	pc := PendingComment{
		RequestID: "req-42",
		Action:    "reply",
		Body:      "thanks, fixed",
		InReplyTo: "41",
	}
	data, err := json.Marshal(pc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, tag := range []string{`"requestId"`, `"action"`, `"body"`, `"inReplyTo"`} {
		if !strings.Contains(string(data), tag) {
			t.Errorf("marshaled PendingComment missing tag %s\ngot: %s", tag, data)
		}
	}
	var round PendingComment
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if round != pc {
		t.Errorf("round-trip mismatch: got %+v, want %+v", round, pc)
	}
}

func TestIssueStatus_PendingCommentsField(t *testing.T) {
	st := IssueStatus{
		PendingComments: []PendingComment{
			{RequestID: "r1", Action: "comment", Body: "hi"},
		},
	}
	data, err := json.Marshal(st)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), `"pendingComments"`) {
		t.Errorf("marshaled IssueStatus missing pendingComments tag: %s", data)
	}
	var round IssueStatus
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(round.PendingComments) != 1 {
		t.Fatalf("PendingComments did not round-trip")
	}
}

// TestIssueStatus_RefireFields asserts the A4 coalesced-refire-comment fields
// (RefireCount, LastRefireCommentAt) both DeepCopy and JSON round-trip.
func TestIssueStatus_RefireFields(t *testing.T) {
	now := metav1.Now()
	in := IssueStatus{LastRefireCommentAt: &now, RefireCount: 3}
	out := in.DeepCopy()
	if out.RefireCount != 3 || out.LastRefireCommentAt == nil {
		t.Fatalf("refire fields did not round-trip: %+v", out)
	}

	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), `"refireCount"`) || !strings.Contains(string(data), `"lastRefireCommentAt"`) {
		t.Errorf("marshaled IssueStatus missing refire fields: %s", data)
	}
	var round IssueStatus
	if err := json.Unmarshal(data, &round); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if round.RefireCount != 3 || round.LastRefireCommentAt == nil {
		t.Fatalf("refire fields did not JSON round-trip: %+v", round)
	}
}
