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
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

func wfMetrics() *obs.OperatorMetrics { return obs.NewOperatorMetrics(prometheus.NewRegistry()) }

// ---------------------------------------------------------------------------
// C5: the create-edge honors Spec.InitialState, so a backlog-swept issue lands
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
		{"backlog-sweep", tatarav1alpha1.StateNew, stage.ReasonBacklogSweep, tatarav1alpha1.StateNew, stage.ReasonBacklogSweep},
		{"active-sweep-triaging", tatarav1alpha1.StateNew, "", tatarav1alpha1.StateNew, ""},
		{"empty-defaults-triaging", "", "", tatarav1alpha1.StateNew, ""},
		{"doc-batch-documenting", tatarav1alpha1.StateUnderImplementation, "", tatarav1alpha1.StateUnderImplementation, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "t-" + tc.name, Namespace: mdNS, UID: types.UID("uid-" + tc.name)},
				Spec: tatarav1alpha1.TaskSpec{
					Kind: "clarify", ProjectRef: "proj", Goal: "g",
					InitialState: tc.initStage, InitialParkReason: tc.initReason,
				},
			}
			c := newMirrorClient(t, task)
			r := tsReconciler(c)
			got := tsReconcile(t, r, tsProject(3), task, time.Now())
			if got.Status.State != tc.wantStage || got.Status.ParkReason != tc.wantReason {
				t.Fatalf("create-edge stamped %s(%s), want %s(%s)",
					got.Status.State, got.Status.ParkReason, tc.wantStage, tc.wantReason)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// THE MINT AND ITS PARK ARE ONE WRITE.
//
// A Task minted with Spec.InitialParkReason was minted TO BE PARKED. Applying
// the state and the park reason as two separate status writes puts a window
// between them in which the Task is LIVE with no park reason - and a crash, a
// conflict or an eviction inside that window leaves it there permanently. It is
// then picked up and worked: the sweep alone mints 52 parked(backlog-sweep)
// owners through this path, and every one of them is an agent pod that was
// never supposed to spawn.
//
// This is the exact non-atomicity annotations were rejected for. Park and its
// reason must be ONE write on the status subresource, and #521's create edge
// reintroduced the split it removed everywhere else.
// ---------------------------------------------------------------------------

func TestCreateEdge_MintsParkedInOneStatusWrite(t *testing.T) {
	for _, tc := range []struct{ name, initState, initReason string }{
		{"the sweep's backlog mint", tatarav1alpha1.StateNew, stage.ReasonBacklogSweep},
		{"a review mint held for a human", tatarav1alpha1.StateNew, stage.ReasonAwaitingHuman},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: "t-mint", Namespace: mdNS, UID: types.UID("uid-t-mint")},
				Spec: tatarav1alpha1.TaskSpec{
					Kind: "refine", ProjectRef: "proj", Goal: "g",
					InitialState: tc.initState, InitialParkReason: tc.initReason,
				},
			}

			// EVERY status write the mint makes, in order. The status subresource
			// is the unit of atomicity, so this is what "one write" means.
			var writes []tatarav1alpha1.TaskStatus
			c := newMirrorClientIntercepted(t, interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string,
					obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if tk, ok := obj.(*tatarav1alpha1.Task); ok && tk.Name == "t-mint" {
						writes = append(writes, *tk.Status.DeepCopy())
					}
					return cl.SubResource(sub).Update(ctx, obj, opts...)
				},
			}, task)

			r := tsReconciler(c)
			r.Metrics = wfMetrics()
			if _, err := r.reconcileStage(context.Background(), tsProject(3), task, time.Now()); err != nil {
				t.Fatalf("reconcileStage (mint): %v", err)
			}

			// THE INVARIANT, and it is stronger than the end state: no INTERMEDIATE
			// status the API server ever held may be live-with-no-park-reason. An
			// end-state-only assertion passes on the two-write version, because the
			// second write does arrive when nothing interrupts it.
			for i, st := range writes {
				if st.State != "" && st.ParkReason == "" {
					t.Fatalf("status write %d of %d left the Task LIVE at %q with NO park reason: "+
						"a crash here mints an unparked Task that gets worked", i+1, len(writes), st.State)
				}
			}
			if len(writes) != 1 {
				t.Fatalf("the mint made %d status writes, want 1: the state and the park must land together",
					len(writes))
			}

			got := mdGetTask(t, c, "t-mint")
			if got.Status.State != tc.initState || got.Status.ParkReason != tc.initReason {
				t.Fatalf("mint landed %s(%s), want %s(%s)",
					got.Status.State, got.Status.ParkReason, tc.initState, tc.initReason)
			}
			if got.Status.ParkedFromState != tc.initState {
				t.Fatalf("parkedFromState = %q, want %q: a Task parks WHERE IT IS",
					got.Status.ParkedFromState, tc.initState)
			}
			if got.Status.ParkedAt == nil || got.Status.StateEnteredAt == nil {
				t.Fatalf("the mint must stamp the whole tuple: %+v", got.Status)
			}

			// A MINT IS NOT AN OUTCOME (D1), and folding the two writes into one
			// must not resurrect the park counter the split forced parkAtMint to
			// suppress by hand.
			if v := testutil.ToFloat64(
				r.Metrics.TaskParkedCounter(tc.initState, tc.initReason)); v != 0 {
				t.Fatalf("operator_task_parked_total{%s,%s} = %v after a mint, want 0",
					tc.initState, tc.initReason, v)
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

// wfParkedTask builds a Task parked for reason. The signature is kept at
// (name, kind, reason) - not (name, kind, state, reason) - on purpose: it is
// shared with unpark_test.go/unpark_decline_test.go outside this file's
// ownership, so wfParkedStateFor infers the state this reason is actually
// reachable from off (kind, reason) instead of taking it as a parameter. Park
// does not move state, so the inferred state IS both the pre-park and the
// (still-parked or successfully-re-entered) post-drive state.
func wfParkedTask(name, kind, reason string) *tatarav1alpha1.Task {
	state := wfParkedStateFor(kind, reason)
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mdNS, UID: types.UID("uid-" + name)},
		Spec:       tatarav1alpha1.TaskSpec{Kind: kind, ProjectRef: "proj", Goal: "g"},
		Status: tatarav1alpha1.TaskStatus{
			State:           state,
			ParkReason:      reason,
			ParkedFromState: state,
			StateEnteredAt:  &metav1.Time{Time: time.Now().Add(-time.Hour)},
		},
	}
}

// wfParkedStateFor infers the state a (kind, reason) park fixture is built
// at. Some reasons name their own state regardless of kind (backlog-sweep is
// always a fresh triage; merge/deploy-timeout are always the operator-driven
// state they time out in); everything else falls back to the state its kind
// normally runs in.
func wfParkedStateFor(kind, reason string) string {
	switch reason {
	case stage.ReasonBacklogSweep:
		return tatarav1alpha1.StateNew
	case stage.ReasonMergeTimeout:
		return tatarav1alpha1.StateMerged
	case stage.ReasonDeployTimeout:
		return tatarav1alpha1.StateDeployed
	}
	switch kind {
	case "review":
		return tatarav1alpha1.StateAwaitingReview
	case "clarify", "brainstorm":
		return tatarav1alpha1.StateRefined
	default: // implement, incident, takeover, documentation, doc-batch
		return tatarav1alpha1.StateUnderImplementation
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
		{name: "merge-timeout", reason: stage.ReasonMergeTimeout, withMR: true, mrState: "open", wantStage: tatarav1alpha1.StateMerged},
		{name: "deploy-timeout", reason: stage.ReasonDeployTimeout, withMR: true, mrState: "merged", wantStage: tatarav1alpha1.StateDeployed},
		{name: "no-outcome", reason: stage.ReasonNoOutcome, withMR: false, wantStage: tatarav1alpha1.StateUnderImplementation},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Unpark never moves state, so the pre-park state IS the
			// post-re-entry wantStage (#406: no-outcome only re-drives when
			// parked FROM implementing or reviewing - a real pod ran a turn -
			// which under-implementation, wfParkedTask's state here, is).
			task := wfParkedTask("t-"+tc.name, "implement", tc.reason)
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
			if got.Status.State != tc.wantStage {
				t.Fatalf("park(%s) re-entered %s, want %s", tc.reason, got.Status.State, tc.wantStage)
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
	if got := mdGetTask(t, c, task.Name); got.Status.State != tatarav1alpha1.StateNew {
		t.Fatalf("backlog-sweep + human comment re-entered %s, want triaging", got.Status.State)
	}
}

func TestDriveUnparks_BacklogSweepStaysParkedWithoutComment(t *testing.T) {
	task := wfParkedTask("t-backlog2", "clarify", stage.ReasonBacklogSweep)
	c := newMirrorClient(t, task)
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: wfMetrics()}
	if err := r.driveUnparks(context.Background(), wfProject(), time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	if got := mdGetTask(t, c, task.Name); got.Status.State != tatarav1alpha1.StateNew || !tatarav1alpha1.Parked(got) {
		t.Fatalf("backlog-sweep with NO comment re-entered %s/parked=%v; must stay parked", got.Status.State, tatarav1alpha1.Parked(got))
	}
}

// The identity-unverified driveUnparks coverage that used to live here
// (TestDriveUnparks_IdentityUnverifiedWithoutVerdictOpensConversationNeverImplementing)
// moved to unpark_backstop_test.go in step C. Step B had already grown a
// same-named, same-shaped test there - parked(identity-unverified), a human
// comment, one live-approved owned Issue, driveUnparks, assert conversing - and
// keeping two copies of one assertion is exactly the duplication that lets the
// weaker one rot. The single surviving copy carries this one's extra
// decline-counter assertion, and the D1 no-conversing-room half sits beside it.

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
	proj.Spec.MaxLivePods = 2

	names := []string{"a", "b", "c", "d"}
	var objs []client.Object
	for _, n := range names {
		// Parked FROM refined (the live, conversation-bearing state the old
		// `conversing` pseudo-stage bucketed into, #521): unpark never moves
		// state, so a successful re-entry leaves State exactly here and only
		// clears the park flag.
		task := wfParkedTask("t-bulk-"+n, "clarify", stage.ReasonAwaitingHuman)
		task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
			At: metav1.Now(), Kind: "issue_comment", Author: "human", Body: "go ahead",
		}}
		iss := &tatarav1alpha1.Issue{ObjectMeta: metav1.ObjectMeta{Name: "iss-bulk-" + n, Namespace: mdNS}}
		iss.Status.State = "open"
		iss.Status.Status = "new" // NOT approved: allApproved must stay false so this Task's
		// re-entry consults LiveHasRoom rather than jumping straight to implementing.
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
		if !tatarav1alpha1.Parked(got) {
			conversing++
		}
	}
	if conversing > proj.Spec.MaxLivePods {
		t.Fatalf("driveUnparks put %d Tasks into conversing in one pass against a ceiling of %d - the room budget was reused instead of spent", conversing, proj.Spec.MaxLivePods)
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
		ExternalID: "c1", Author: "maint", Body: "sure, go ahead", CreatedAt: metav1.Now(),
	}}
	fabricated := wfIssue("iss-fabricated")
	fabricated.Status.Comments = []tatarav1alpha1.Comment{{
		ExternalID: "c2", Author: "maint", Body: "thanks, will look", CreatedAt: metav1.Now(),
	}}
	nonMaint := wfIssue("iss-nonmaint")
	nonMaint.Status.Comments = []tatarav1alpha1.Comment{
		{ExternalID: "c3", Author: "randomuser", Body: "go ahead", CreatedAt: metav1.Now()},
		{ExternalID: "c4", Author: "maint", Body: "let me look", CreatedAt: metav1.Now()},
	}

	c := newMirrorClient(t, approved, fabricated, nonMaint)
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	g := &GrammarVerifier{Client: c, Metrics: metrics}
	ctx := context.Background()

	if ev, ok, _ := g.VerifyApprovalDeclared(ctx, proj, approved, cites("c1", "go ahead"), ""); !ok || ev == nil || ev.Login != "maint" {
		t.Fatalf("valid cited maintainer approval refused: ok=%v ev=%+v", ok, ev)
	}
	if _, ok, _ := g.VerifyApprovalDeclared(ctx, proj, fabricated, cites("c2", "go ahead"), ""); ok {
		t.Fatalf("a FABRICATED quote granted approval")
	}
	if _, ok, _ := g.VerifyApprovalDeclared(ctx, proj, nonMaint, cites("c3", "go ahead"), ""); ok {
		t.Fatalf("a non-maintainer comment granted approval")
	}
	// Hard rule 13: every refusal path is queryable without log-scraping.
	if got := testutil.ToFloat64(metrics.ApprovalRefusedCounter(ApprovalRefusedQuoteAbsent)); got != 1 {
		t.Fatalf("operator_approval_refused_total{reason=quote-not-in-comment} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ApprovalRefusedCounter(ApprovalRefusedCitationNotMaintainer)); got != 1 {
		t.Fatalf("operator_approval_refused_total{reason=citation-not-maintainer} = %v, want 1", got)
	}

	// A nil Metrics must not panic: the seam is wired with metrics in production
	// but constructed bare in several tests.
	bare := &GrammarVerifier{Client: c}
	if _, ok, _ := bare.VerifyApprovalDeclared(ctx, proj, nonMaint, nil, ""); ok {
		t.Fatal("a nil-metrics verifier granted approval with no citation")
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
		Status:     tatarav1alpha1.TaskStatus{State: tatarav1alpha1.StateUnderImplementation},
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
