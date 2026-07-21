package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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

func TestDriveUnparks_SkipsIdentityUnverified(t *testing.T) {
	// identity-unverified is webhook+grammar-driven; the reconcile loop must not
	// touch it (driving it with GrammarPassed=false would strand it).
	task := wfParkedTask("t-ident", "clarify", stage.ReasonIdentityUnverified)
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "human", Body: "go ahead",
	}}
	c := newMirrorClient(t, task)
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: wfMetrics()}
	if err := r.driveUnparks(context.Background(), wfProject(), time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.Stage != tatarav1alpha1.StageParked || got.Status.StageReason != stage.ReasonIdentityUnverified {
		t.Fatalf("driveUnparks touched identity-unverified: now %s(%s)", got.Status.Stage, got.Status.StageReason)
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
