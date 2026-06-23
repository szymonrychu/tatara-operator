package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// siblingFakeSCM implements both scm.SCMReader and scm.SCMWriter for
// commentSiblingMarker tests. Only ListIssueComments and Comment are used.
type siblingFakeSCM struct {
	scm.SCMReader
	scm.SCMWriter
	comments     map[string][]scm.IssueComment
	commentCalls int
}

func (f *siblingFakeSCM) ListIssueComments(_ context.Context, owner, repo string, number int) ([]scm.IssueComment, error) {
	key := fmt.Sprintf("%s/%s#%d", owner, repo, number)
	return f.comments[key], nil
}

func (f *siblingFakeSCM) Comment(_ context.Context, _, _, _ string) error {
	f.commentCalls++
	return nil
}

func TestCommentSiblingMarker_Idempotent(t *testing.T) {
	marker := systemicMarker(12)
	// reader with a comment containing the marker -> writer must NOT be called.
	reader := &siblingFakeSCM{comments: map[string][]scm.IssueComment{
		"o/r1#15": {{Author: "bot", Body: "earlier " + marker + " trailing"}},
	}}
	writer := &siblingFakeSCM{}
	if err := commentSiblingMarker(context.Background(), reader, writer, "tok", "o/r1", 15, 12); err != nil {
		t.Fatal(err)
	}
	if writer.commentCalls != 0 {
		t.Fatalf("marker already present, must not re-post; got %d calls", writer.commentCalls)
	}
	// fresh issue (no existing comments) -> must post once.
	reader2 := &siblingFakeSCM{comments: map[string][]scm.IssueComment{}}
	writer2 := &siblingFakeSCM{}
	if err := commentSiblingMarker(context.Background(), reader2, writer2, "tok", "o/r1", 16, 12); err != nil {
		t.Fatal(err)
	}
	if writer2.commentCalls != 1 {
		t.Fatalf("fresh issue must post once; got %d", writer2.commentCalls)
	}
}

func TestElectSystemicLeads(t *testing.T) {
	cands := []candidate{
		{repo: "o/r1", number: 15, labels: []string{"tatara/systemic-abc"}, title: "C"},
		{repo: "o/r1", number: 12, labels: []string{"tatara/systemic-abc"}, title: "A"},
		{repo: "o/r2", number: 9, labels: []string{"tatara/systemic-abc"}, title: "B"},
		{repo: "o/r1", number: 7, labels: []string{"bug"}, title: "standalone"},
	}
	got := electSystemicLeads(cands)
	if _, ok := got["o/r1#7"]; ok {
		t.Fatal("standalone (no systemic label) must not be in the map")
	}
	lead := got["o/r1#12"]
	if !lead.isLead || lead.leadNumber != 12 {
		t.Fatalf("o/r1#12 should be lead: %+v", lead)
	}
	if len(lead.sameRepoSiblings) != 1 || lead.sameRepoSiblings[0] != 15 {
		t.Fatalf("lead sameRepoSiblings want [15]: %+v", lead.sameRepoSiblings)
	}
	if len(lead.crossRepo) != 1 || lead.crossRepo[0] != "o/r2#9 - B" {
		t.Fatalf("lead crossRepo want [o/r2#9 - B]: %+v", lead.crossRepo)
	}
	sib := got["o/r1#15"]
	if sib.isLead || sib.leadNumber != 12 {
		t.Fatalf("o/r1#15 should be non-lead pointing at 12: %+v", sib)
	}
	r2lead := got["o/r2#9"]
	if !r2lead.isLead || r2lead.leadNumber != 9 {
		t.Fatalf("o/r2#9 should be its repo's lead: %+v", r2lead)
	}
}
