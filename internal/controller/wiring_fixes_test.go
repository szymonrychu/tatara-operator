package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

func wfMetrics() *obs.OperatorMetrics { return obs.NewOperatorMetrics(prometheus.NewRegistry()) }

// ---------------------------------------------------------------------------
// C5: the create-edge honors Spec.InitialStage, so a backlog-swept issue lands
// parked(backlog-sweep) even when the TaskReconciler runs the create-edge FIRST
// (before the sweep's status stamp). Before the fix the reconciler stamped
// triaging and the sweep's stale non-retrying Status().Update 409'd, leaving the
// cold-backlog issue actively triaged - the 150-issue storm B.4 exists to stop.
// ---------------------------------------------------------------------------

func TestCreateEdge_HonorsInitialStage(t *testing.T) {
	cases := []struct {
		name                  string
		initStage, initReason string
		wantStage, wantReason string
	}{
		{"backlog-sweep", tatarav1alpha1.StageParked, stage.ReasonBacklogSweep, tatarav1alpha1.StageParked, stage.ReasonBacklogSweep},
		{"active-sweep-triaging", tatarav1alpha1.StageTriaging, "", tatarav1alpha1.StageTriaging, ""},
		{"empty-defaults-triaging", "", "", tatarav1alpha1.StageTriaging, ""},
		{"doc-batch-documenting", tatarav1alpha1.StageDocumenting, "", tatarav1alpha1.StageDocumenting, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "t-" + tc.name, Namespace: mdNS, UID: types.UID("uid-" + tc.name)},
				Spec: tatarav1alpha1.TaskSpec{
					Kind: "clarify", ProjectRef: "proj", Goal: "g",
					InitialStage: tc.initStage, InitialStageReason: tc.initReason,
				},
			}
			c := newMirrorClient(t, task)
			r := tsReconciler(c)
			got := tsReconcile(t, r, tsProject(3), task, time.Now())
			if got.Status.Stage != tc.wantStage || got.Status.StageReason != tc.wantReason {
				t.Fatalf("create-edge stamped %s(%s), want %s(%s)",
					got.Status.Stage, got.Status.StageReason, tc.wantStage, tc.wantReason)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// W3: driveUnparks is the F.6 re-entry DRIVER. Before it, stage.Unpark had full
// re-entry bodies for six reasons but only identity-unverified had a production
// caller (the webhook), so a parked(merge-timeout) delivery was stranded forever.
// One case per reason that the park actually re-enters.
// ---------------------------------------------------------------------------

func wfProject() *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "proj", Namespace: mdNS},
		Spec: tatarav1alpha1.ProjectSpec{
			MaxOpenTasks: 6,
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: "tatara-bot"},
		},
	}
}

func wfParkedTask(name, kind, reason string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mdNS, UID: types.UID("uid-" + name)},
		Spec:       tatarav1alpha1.TaskSpec{Kind: kind, ProjectRef: "proj", Goal: "g"},
		Status: tatarav1alpha1.TaskStatus{
			Stage:          tatarav1alpha1.StageParked,
			StageReason:    reason,
			StageEnteredAt: &metav1.Time{Time: time.Now().Add(-time.Hour)},
		},
	}
}

func wfMR(name, state string, owner *tatarav1alpha1.Task) *tatarav1alpha1.MergeRequest {
	return &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: mdNS, UID: types.UID("uid-" + name),
			OwnerReferences: mdCtrlOwnerRefs(owner),
		},
		Status: tatarav1alpha1.MergeRequestStatus{State: state},
	}
}

func TestDriveUnparks_TimeBasedReasonsReEnter(t *testing.T) {
	cases := []struct {
		name, reason, mrState, wantStage string
		withMR                           bool
	}{
		{name: "merge-timeout", reason: stage.ReasonMergeTimeout, withMR: true, mrState: "open", wantStage: tatarav1alpha1.StageMerging},
		{name: "deploy-timeout", reason: stage.ReasonDeployTimeout, withMR: true, mrState: "merged", wantStage: tatarav1alpha1.StageDeploying},
		{name: "no-outcome", reason: stage.ReasonNoOutcome, withMR: false, wantStage: tatarav1alpha1.StageImplementing},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := wfParkedTask("t-"+tc.name, "implement", tc.reason)
			if tc.reason == stage.ReasonNoOutcome {
				// #406: no-outcome only re-drives when parked FROM implementing
				// or reviewing (a real pod ran a turn). This is exactly that case.
				task.Status.ParkedFromStage = tatarav1alpha1.StageImplementing
			}
			objs := []client.Object{task}
			if tc.withMR {
				mr := wfMR("mr-"+tc.name, tc.mrState, task)
				task.Status.MRRefs = []string{mr.Name}
				objs = append(objs, mr)
			}
			c := newMirrorClient(t, objs...)
			r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: wfMetrics()}
			if err := r.driveUnparks(context.Background(), wfProject(), time.Now()); err != nil {
				t.Fatalf("driveUnparks: %v", err)
			}
			got := mdGetTask(t, c, task.Name)
			if got.Status.Stage != tc.wantStage {
				t.Fatalf("park(%s) re-entered %s, want %s", tc.reason, got.Status.Stage, tc.wantStage)
			}
		})
	}
}

func TestDriveUnparks_BacklogSweepPromotesOnHumanComment(t *testing.T) {
	task := wfParkedTask("t-backlog", "clarify", stage.ReasonBacklogSweep)
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "human", Body: "please look",
	}}
	c := newMirrorClient(t, task)
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: wfMetrics()}
	if err := r.driveUnparks(context.Background(), wfProject(), time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	if got := mdGetTask(t, c, task.Name); got.Status.Stage != tatarav1alpha1.StageTriaging {
		t.Fatalf("backlog-sweep + human comment re-entered %s, want triaging", got.Status.Stage)
	}
}

func TestDriveUnparks_BacklogSweepStaysParkedWithoutComment(t *testing.T) {
	task := wfParkedTask("t-backlog2", "clarify", stage.ReasonBacklogSweep)
	c := newMirrorClient(t, task)
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: wfMetrics()}
	if err := r.driveUnparks(context.Background(), wfProject(), time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	if got := mdGetTask(t, c, task.Name); got.Status.Stage != tatarav1alpha1.StageParked {
		t.Fatalf("backlog-sweep with NO comment re-entered %s; must stay parked", got.Status.Stage)
	}
}

// TestDriveUnparks_IdentityUnverifiedWithoutVerdictOpensConversationNeverImplementing
// was TestDriveUnparks_IdentityUnverifiedWithoutVerdictDeclines before Task 9's
// conversing widening; renamed because it no longer declines - see below.
func TestDriveUnparks_IdentityUnverifiedWithoutVerdictOpensConversationNeverImplementing(t *testing.T) {
	// The reconcile loop DOES drive identity-unverified now. Its one owned Issue
	// is deliberately seeded LIVE-APPROVED: without that, this test could not
	// tell "correctly never reaches implementing because no grammar verdict
	// feeds stage.Unpark" apart from "never reaches implementing for the
	// unrelated reason no Issues are owned" - it would pass either way and catch
	// nothing. With the Issue approved, anything that put a true into
	// UnparkInput.GrammarPassed WOULD re-enter the Task straight into
	// implementing; this asserts that specifically never happens. As of step B
	// no builder sets that field at all, which is what makes it impossible
	// rather than merely unlikely.
	//
	// Task 9: a GrammarPassed=false comment on identity-unverified now opens a
	// conversation (conversing) instead of only declining - so this Task DOES
	// move, but the conversing branch never consults Issue approval state at
	// all (it fires before that check). The property this test proves - a
	// live-approved Issue with no fresh grammar pass must never authorize
	// implementing - is enforced downstream, at restapi's verifyApprovalScope,
	// which reruns the LIVE C.6 grammar on every decision=implement from
	// conversing and never reads status.approvalVerdict; it is NOT LegalFor's
	// kind guard, which keys on Task.Spec.Kind == "review" and this Task is
	// kind=clarify.
	task := wfParkedTask("t-ident", "clarify", stage.ReasonIdentityUnverified)
	task.Status.IssueRefs = []string{"iss-ident"}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "human", Body: "go ahead",
	}}
	iss := &tatarav1alpha1.Issue{ObjectMeta: metav1.ObjectMeta{Name: "iss-ident", Namespace: mdNS}}
	iss.Status.State = "open"
	iss.Status.Status = "approved"
	c := newMirrorClient(t, task, iss)
	metrics := wfMetrics()
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: metrics}
	if err := r.driveUnparks(context.Background(), wfProject(), time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.Stage != tatarav1alpha1.StageConversing {
		t.Fatalf("stage = %s(%s), want conversing: a live-approved Issue with NO fresh grammar verdict must never reach implementing",
			got.Status.Stage, got.Status.StageReason)
	}
	// The original (pre-Task-9) version of this test asserted
	// UnparkDeclinedCounter(identity-unverified, grammar-not-passed) == 1 - "the
	// SPECIFIC decline, not just stayed parked". That assertion is gone, not
	// merely dropped: this pass no longer declines at all (target != ""), so
	// driveUnparks never calls Metrics.UnparkDeclined for it. Asserted here as
	// 0, explicitly, so a future regression that made this decline again would
	// fail LOUDLY on the counter, not silently pass by accident.
	if got := testutil.ToFloat64(metrics.UnparkDeclinedCounter(stage.ReasonIdentityUnverified, string(DeclineGrammarNotPassed))); got != 0 {
		t.Fatalf("operator_unpark_declined_total{identity-unverified,grammar-not-passed} = %v, want 0: this pass entered conversing, it did not decline", got)
	}
}

// TestDriveUnparks_ConversingRoomBudgetCapsBulkReEntry is the CRITICAL 2
// discrimination proof (2026-07-28 final review, first half): driveUnparks
// used to hoist a single "has room" boolean ONCE per pass and reuse it,
// unconditionally, for every parked Task in the batch - nothing decremented
// it as Tasks actually entered conversing. A bulk maintainer comment pass
// (UnparkInput.ActiveTasks' own doc names exactly this scenario) could
// therefore push admissions well past the per-project conversing ceiling.
// Four parked(awaiting-human) Tasks against a ceiling of 2 must never put
// more than 2 into conversing in a single pass.
func TestDriveUnparks_ConversingRoomBudgetCapsBulkReEntry(t *testing.T) {
	proj := wfProject()
	proj.Spec.MaxConversingPods = 2

	names := []string{"a", "b", "c", "d"}
	var objs []client.Object
	for _, n := range names {
		task := wfParkedTask("t-bulk-"+n, "clarify", stage.ReasonAwaitingHuman)
		task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
			At: metav1.Now(), Kind: "issue_comment", Author: "human", Body: "go ahead",
		}}
		iss := &tatarav1alpha1.Issue{ObjectMeta: metav1.ObjectMeta{Name: "iss-bulk-" + n, Namespace: mdNS}}
		iss.Status.State = "open"
		iss.Status.Status = "new" // NOT approved: allApproved must stay false so this Task's
		// re-entry consults ConversingHasRoom rather than jumping straight to implementing.
		task.Status.IssueRefs = []string{iss.Name}
		objs = append(objs, task, iss)
	}
	c := newMirrorClient(t, objs...)
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: wfMetrics()}
	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}

	conversing := 0
	for _, n := range names {
		got := mdGetTask(t, c, "t-bulk-"+n)
		if got.Status.Stage == tatarav1alpha1.StageConversing {
			conversing++
		}
	}
	if conversing > proj.Spec.MaxConversingPods {
		t.Fatalf("driveUnparks put %d Tasks into conversing in one pass against a ceiling of %d - the room budget was reused instead of spent", conversing, proj.Spec.MaxConversingPods)
	}
	if conversing == 0 {
		t.Fatalf("driveUnparks put 0 Tasks into conversing - the room budget computation itself is broken, not just the cap")
	}
}

// ---------------------------------------------------------------------------
// W1: GrammarVerifier is the PRODUCTION restapi.ApprovalVerifier. Before it was
// wired, restapi.Config.Approval was nil and verifyApprovalScope failed closed on
// every clarify decision=implement - the platform could never implement anything.
// ---------------------------------------------------------------------------

func TestGrammarVerifier_VerdictsPerIssue(t *testing.T) {
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "proj", Namespace: mdNS},
		Spec: tatarav1alpha1.ProjectSpec{
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github", BotLogin: "tatara-bot",
				MaintainerLogins: []string{"maint"},
			},
		},
	}
	approved := wfIssue("iss-ok")
	approved.Status.Comments = []tatarav1alpha1.Comment{{
		ExternalID: "c1", Author: "maint", Body: "go ahead", CreatedAt: metav1.Now(),
	}}
	noPhrase := wfIssue("iss-nophrase")
	noPhrase.Status.Comments = []tatarav1alpha1.Comment{{
		ExternalID: "c2", Author: "maint", Body: "thanks, will look", CreatedAt: metav1.Now(),
	}}
	nonMaint := wfIssue("iss-nonmaint")
	nonMaint.Status.Comments = []tatarav1alpha1.Comment{{
		ExternalID: "c3", Author: "randomuser", Body: "go ahead", CreatedAt: metav1.Now(),
	}}

	c := newMirrorClient(t, approved, noPhrase, nonMaint)
	g := &GrammarVerifier{Client: c}

	if ev, ok := g.VerifyApproval(context.Background(), proj, approved); !ok || ev == nil || ev.Login != "maint" {
		t.Fatalf("valid maintainer approval refused: ok=%v ev=%+v", ok, ev)
	}
	if _, ok := g.VerifyApproval(context.Background(), proj, noPhrase); ok {
		t.Fatalf("a non-approval-phrase comment granted approval")
	}
	if _, ok := g.VerifyApproval(context.Background(), proj, nonMaint); ok {
		t.Fatalf("a non-maintainer comment granted approval")
	}
}

// ---------------------------------------------------------------------------
// I10: a 410 Gone on SubmitTurn routes into the G.7 stop/handoff, NOT the hard
// reconcile-error/backoff path. With no TTL configured ttlStop returns cleanly,
// so the handler returns a nil error (not "submit turn0 turn: 410").
// ---------------------------------------------------------------------------

func TestHandleTurnSubmitFailure_410RoutesToTTLStop(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t410", Namespace: mdNS, UID: types.UID("uid-t410")},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "implement", ProjectRef: "proj", Goal: "g"},
		Status:     tatarav1alpha1.TaskStatus{Stage: tatarav1alpha1.StageImplementing},
	}
	c := newMirrorClient(t, task)
	r := tsReconciler(c)
	gone := &agent.HTTPError{Status: 410}
	_, err := r.handleTurnSubmitFailure(context.Background(), tsProject(3), task, gone, 0.01, "turn0")
	if err != nil {
		t.Fatalf("410 SubmitTurn returned a hard error %v; it must route to the G.7 stop/handoff", err)
	}
}

func wfIssue(name string) *tatarav1alpha1.Issue {
	return &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mdNS, UID: types.UID("uid-" + name)},
		Status:     tatarav1alpha1.IssueStatus{State: "open"},
	}
}
