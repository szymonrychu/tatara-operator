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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// siblingFakeSCM implements both scm.SCMReader and scm.SCMWriter for
// commentSiblingMarker tests. Only ListIssueComments and Comment are used.
type siblingFakeSCM struct {
	scm.SCMReader
	scm.SCMWriter
	comments     map[string][]scm.IssueComment
	commentCalls int
}

func (f *siblingFakeSCM) GetIssueState(_ context.Context, _, _ string, _ int) (scm.IssueState, error) {
	return scm.IssueState{}, nil
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
	pr := &ProjectReconciler{Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	proj := &tatarav1alpha1.Project{}
	// reader with a comment containing the marker -> writer must NOT be called.
	reader := &siblingFakeSCM{comments: map[string][]scm.IssueComment{
		"o/r1#15": {{Author: "bot", Body: "earlier " + marker + " trailing"}},
	}}
	writer := &siblingFakeSCM{}
	if err := commentSiblingMarker(context.Background(), pr, reader, writer, proj, nil, "tok", "github", "o/r1", 15, 12); err != nil {
		t.Fatal(err)
	}
	if writer.commentCalls != 0 {
		t.Fatalf("marker already present, must not re-post; got %d calls", writer.commentCalls)
	}
	// fresh issue (no existing comments) -> must post once.
	reader2 := &siblingFakeSCM{comments: map[string][]scm.IssueComment{}}
	writer2 := &siblingFakeSCM{}
	if err := commentSiblingMarker(context.Background(), pr, reader2, writer2, proj, nil, "tok", "github", "o/r1", 16, 12); err != nil {
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
	got := electSystemicLeads(cands, "tatara-declined")
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

// TestIssueScan_SystemicDedup_LeadInFlightStillCollapsesSibling guards the seam
// where the elected lead already has a live (non-terminal) Task and is therefore
// deduped out of the eligible set. Election must still run over the full
// candidate set so the surviving sibling is recognised as a non-lead and
// collapsed, instead of being promoted to lead and spawning a second agent.
func TestIssueScan_SystemicDedup_LeadInFlightStillCollapsesSibling(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *"}}
	proj, _ := seedScanProject(t, "systemic-dedup-inflight", cron)
	repo := mkScanRepo(t, "systemic-dedup-inflight", "systemic-dedup-inflight-r", "https://github.com/o/r.git")

	// Lead #12 already has a live Task (in-flight) -> deduped out of eligible.
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "scan-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/r", number: 12}, "issueScan", "issueLifecycle")
	pre.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "systemic-dedup-inflight", RepositoryRef: repo.Name, Goal: "g", Kind: "issueLifecycle",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#12", Number: 12}}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.Status.Phase = "Running"
	_ = k8sClient.Status().Update(context.Background(), pre)

	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 12, Labels: []string{"tatara/systemic-abc"}, UpdatedAt: time.Unix(100, 0), Title: "A"},
		{Repo: "o/r", Number: 15, Labels: []string{"tatara/systemic-abc"}, UpdatedAt: time.Unix(200, 0), Title: "C"},
	}}
	reg := prometheus.NewRegistry()
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(reg)

	r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, []tatarav1alpha1.Task{*pre}, cron.IssueScan)

	// No new QE: lead is in-flight (deduped), sibling is collapsed (no agent).
	if qes := listScanQEs(t, "systemic-dedup-inflight"); len(qes) != 0 {
		t.Fatalf("want 0 new QEs (lead in-flight, sibling collapsed), got %d: %+v", len(qes), qes)
	}
	// Sibling #15 must be collapsed, not promoted to lead.
	collapsedCount := counterValue(t, reg, "tatara_systemic_siblings_collapsed_total", map[string]string{"project": "systemic-dedup-inflight"})
	if collapsedCount < 1 {
		t.Fatalf("sibling must be collapsed even with lead in-flight; collapsed counter = %v", collapsedCount)
	}
}

// TestIssueScan_SystemicDedup_StaleLockClearedOnHumanActivity is the M4
// regression: a collapsed systemic sibling never gets a superseding Task
// (issueScanPickOne skips task creation for it entirely - "no separate
// agent"), so an ImplementationLocked sibling whose lock predates a NEW human
// comment must have its lock cleared right here, at the one place a collapsed
// sibling is still visited every scan - otherwise the lock stands forever and
// the approved lead's PR would force-close a sibling with live open
// questions.
func TestIssueScan_SystemicDedup_StaleLockClearedOnHumanActivity(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *"}}
	proj, _ := seedScanProject(t, "systemic-stale-lock", cron)
	repo := mkScanRepo(t, "systemic-stale-lock", "systemic-stale-lock-r", "https://github.com/o/r.git")

	// Real-clock-relative timestamps (issue dedup ALSO gates on human activity
	// since the Task's real CreationTimestamp, so Unix-epoch stub times would
	// make the candidate look stale and never reach issueScanPickOne at all).
	t0 := time.Now()
	sib := &tatarav1alpha1.Task{}
	sib.GenerateName = "clarify-"
	sib.Namespace = testNS
	sib.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "systemic-stale-lock", RepositoryRef: repo.Name, Goal: "g", Kind: "clarify",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#15", Number: 15}}
	if err := k8sClient.Create(context.Background(), sib); err != nil {
		t.Fatalf("pre-create sibling: %v", err)
	}
	lockedAt := metav1.NewTime(t0.Add(1 * time.Minute))
	sib.Status.Phase = "Succeeded"
	sib.Status.ImplementationLocked = true
	sib.Status.ImplementationLockedAt = &lockedAt
	if err := k8sClient.Status().Update(context.Background(), sib); err != nil {
		t.Fatalf("seed lock: %v", err)
	}

	// Human comment on #15 AFTER the lock timestamp: the conversation reopened.
	reader := &fakeReader{
		issues: []scm.IssueRef{
			{Repo: "o/r", Number: 12, Labels: []string{"tatara/systemic-abc"}, UpdatedAt: time.Unix(100, 0), Title: "A"},
			{Repo: "o/r", Number: 15, Labels: []string{"tatara/systemic-abc"}, UpdatedAt: time.Unix(200, 0), Title: "C"},
		},
		comments: []scm.IssueComment{
			{Author: "a-human", Body: "wait, I have another question", CreatedAt: t0.Add(2 * time.Minute)},
		},
	}
	reg := prometheus.NewRegistry()
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(reg)

	r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, []tatarav1alpha1.Task{*sib}, cron.IssueScan)

	var got tatarav1alpha1.Task
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: sib.Name}, &got); err != nil {
		t.Fatalf("re-get sibling: %v", err)
	}
	if got.Status.ImplementationLocked {
		t.Fatalf("stale lock must be cleared after a human comment postdating ImplementationLockedAt")
	}
	if got.Status.ImplementationLockedAt != nil {
		t.Fatalf("ImplementationLockedAt must be cleared alongside the lock")
	}
}

// TestIssueScan_SystemicDedup_FreshLockNotClearedWithoutNewActivity is the
// negative case: a human comment exists (so the candidate still clears the
// issueScan dedup gate and reaches the collapsed-sibling branch), but it
// PREdates the lock, so the lock must survive untouched (the fan-out path
// still needs it).
func TestIssueScan_SystemicDedup_FreshLockNotClearedWithoutNewActivity(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *"}}
	proj, _ := seedScanProject(t, "systemic-fresh-lock", cron)
	repo := mkScanRepo(t, "systemic-fresh-lock", "systemic-fresh-lock-r", "https://github.com/o/r.git")

	t0 := time.Now()
	sib := &tatarav1alpha1.Task{}
	sib.GenerateName = "clarify-"
	sib.Namespace = testNS
	sib.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "systemic-fresh-lock", RepositoryRef: repo.Name, Goal: "g", Kind: "clarify",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#15", Number: 15}}
	if err := k8sClient.Create(context.Background(), sib); err != nil {
		t.Fatalf("pre-create sibling: %v", err)
	}
	lockedAt := metav1.NewTime(t0.Add(1 * time.Minute))
	sib.Status.Phase = "Succeeded"
	sib.Status.ImplementationLocked = true
	sib.Status.ImplementationLockedAt = &lockedAt
	if err := k8sClient.Status().Update(context.Background(), sib); err != nil {
		t.Fatalf("seed lock: %v", err)
	}

	// Comment exists (clears the dedup gate) but is BEFORE the lock timestamp:
	// no new activity since the lock was set.
	reader := &fakeReader{
		issues: []scm.IssueRef{
			{Repo: "o/r", Number: 12, Labels: []string{"tatara/systemic-abc"}, UpdatedAt: time.Unix(100, 0), Title: "A"},
			{Repo: "o/r", Number: 15, Labels: []string{"tatara/systemic-abc"}, UpdatedAt: time.Unix(200, 0), Title: "C"},
		},
		comments: []scm.IssueComment{
			{Author: "a-human", Body: "original question", CreatedAt: t0.Add(30 * time.Second)},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, []tatarav1alpha1.Task{*sib}, cron.IssueScan)

	var got tatarav1alpha1.Task
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: sib.Name}, &got); err != nil {
		t.Fatalf("re-get sibling: %v", err)
	}
	if !got.Status.ImplementationLocked {
		t.Fatalf("lock must survive when no new human activity postdates it")
	}
}
