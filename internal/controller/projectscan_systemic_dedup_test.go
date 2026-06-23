package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
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

// TestIssueScan_SystemicDedup_CollapsesSiblings verifies that when two issues in
// the same repo share a tatara/systemic-<id> label, only the lead (lowest number)
// gets a QE and the sibling is skipped with a metric increment.
func TestIssueScan_SystemicDedup_CollapsesSiblings(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *"}}
	proj, _ := seedScanProject(t, "systemic-dedup-same", cron)
	repos := []tatarav1alpha1.Repository{
		mkScanRepo(t, "systemic-dedup-same", "systemic-dedup-same-r", "https://github.com/o/r.git"),
	}
	// Two issues with the same systemic label in the same repo; #12 is lead (lowest).
	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 15, Labels: []string{"tatara/systemic-abc"}, UpdatedAt: time.Unix(200, 0), Title: "C"},
		{Repo: "o/r", Number: 12, Labels: []string{"tatara/systemic-abc"}, UpdatedAt: time.Unix(100, 0), Title: "A"},
	}}
	reg := prometheus.NewRegistry()
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(reg)

	r.issueScan(context.Background(), proj, reader, repos, nil, cron.IssueScan)

	qes := listScanQEs(t, "systemic-dedup-same")
	if len(qes) != 1 {
		t.Fatalf("want 1 QE (lead only), got %d", len(qes))
	}
	if qes[0].Spec.Payload.Source == nil || qes[0].Spec.Payload.Source.Number != 12 {
		t.Fatalf("want QE for lead #12, got source=%+v", qes[0].Spec.Payload.Source)
	}
	// Lead QE must carry SystemicGroup.
	sg := qes[0].Spec.Payload.SystemicGroup
	if sg == nil || sg.SystemicID != "abc" {
		t.Fatalf("lead QE must carry SystemicGroup with SystemicID=abc, got %+v", sg)
	}
	if len(sg.SameRepoSiblings) != 1 || sg.SameRepoSiblings[0] != 15 {
		t.Fatalf("lead QE SameRepoSiblings want [15], got %v", sg.SameRepoSiblings)
	}
	// Sibling #15 must be counted as skipped_systemic_sibling.
	sibCount := counterValue(t, reg, "tatara_scan_items_total", map[string]string{"activity": "issueScan", "outcome": "skipped_systemic_sibling"})
	if sibCount < 1 {
		t.Fatalf("skipped_systemic_sibling counter = %v, want >= 1", sibCount)
	}
	// SystemicSiblingCollapsed counter.
	collapsedCount := counterValue(t, reg, "tatara_systemic_siblings_collapsed_total", map[string]string{"project": "systemic-dedup-same"})
	if collapsedCount < 1 {
		t.Fatalf("tatara_systemic_siblings_collapsed_total = %v, want >= 1", collapsedCount)
	}
}
