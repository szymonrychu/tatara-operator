package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// newUnparkTestReconciler builds a ProjectReconciler backed by a fake client
// seeded with objs, with APIReader pointed at the SAME fake client (the
// uncached-reader idiom ApplyUnpark relies on - see unpark.go's ApplyUnpark
// doc comment) and a fresh metrics registry so UnparkDeclined calls do not
// panic on a nil Metrics.
func newUnparkTestReconciler(t *testing.T, objs ...client.Object) *ProjectReconciler {
	t.Helper()
	c := newMirrorClient(t, objs...)
	return &ProjectReconciler{
		Client:    c,
		Scheme:    c.Scheme(),
		APIReader: c,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
}

// objectKeyOf is client.ObjectKeyFromObject, named for readability at the call
// site in this file's tests (fresh := &Task{}; r.Get(ctx, objectKeyOf(task), fresh)).
func objectKeyOf(o client.Object) client.ObjectKey {
	return client.ObjectKeyFromObject(o)
}

// THE STALL, RETRIED. The webhook fast path lost the cache race and declined
// with not-all-approved, but it persisted the verdict. The next driveUnparks
// pass re-derives the owned-Issue half from LIVE state - which by now shows the
// approval - and re-enters implementing. Without this the Task is stranded
// forever with the approval label visible on the forge.
func TestDriveUnparks_RetriesIdentityUnverifiedFromThePersistedVerdict(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "infrastructure-clarify-2026-07-27-gtwgp"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonIdentityUnverified
	// StageEnteredAt and the verdict's At are given a REALISTIC gap (minutes, not
	// microseconds): metav1.Time round-trips through the (fake or real) apiserver
	// at whole-second precision, so two same-test time.Now() calls a few
	// microseconds apart - which is what production never produces, since a human
	// must physically comment between the park and the approval - would collapse
	// to the SAME second and defeat grammarPassedFor's strict After() check.
	parkedAt := time.Now().Add(-10 * time.Minute)
	task.Status.StageEnteredAt = &metav1.Time{Time: parkedAt}
	task.Status.IssueRefs = []string{"iss-helmfile-26"}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Repo: "helmfile", Number: 26,
		Author: "szymonrychu", Body: "go ahead",
	}}
	task.Status.ApprovalVerdict = &tatarav1alpha1.ApprovalVerdict{
		At:                metav1.Time{Time: parkedAt.Add(5 * time.Minute)},
		IssueRef:          "iss-helmfile-26",
		CommentExternalID: "3606943691",
		Author:            "szymonrychu",
		Phrase:            "go ahead",
	}

	iss := &tatarav1alpha1.Issue{}
	iss.Namespace = "tatara"
	iss.Name = "iss-helmfile-26"
	iss.Status.State = "open"
	iss.Status.Status = "approved" // the write the fast path's cached read missed

	r := newUnparkTestReconciler(t, proj, task, iss)

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}

	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if fresh.Status.Stage != tatarav1alpha1.StageImplementing {
		t.Fatalf("stage = %s(%s), want implementing: the backstop did not retry the identity-unverified park",
			fresh.Status.Stage, fresh.Status.StageReason)
	}
}

// No verdict means the grammar has NEVER passed for this Task, and the backstop
// cannot manufacture one: it must never drive a grammar-less re-entry into
// implementing - that is the fully autonomous hallucinated-approval-to-prod
// path, and this is what stays impossible. But a human DID comment ("go
// ahead" here has no verdict behind it - the grammar could not read it as
// approval), and Task 9 reads that as a conversation, not a dead end: the
// Task lands in conversing. The REAL guarantee against an unverified
// implement is restapi's verifyApprovalScope (internal/restapi/outcome.go),
// which runs the LIVE C.6 grammar over every owned Issue on every
// submit_outcome(decision=implement) and never reads status.approvalVerdict
// - NOT LegalFor's kind guard, which keys on Task.Spec.Kind == "review" and
// has nothing to say about a kind=clarify Task like this one. A clarify agent
// standing in conversing CAN still move this Task toward implementing, via a
// GENUINE decision=implement that passes the live grammar - see
// TestOutcome_Conversing_ApprovalVerdictIsNeverConsulted (internal/restapi).
func TestDriveUnparks_IdentityUnverifiedWithNoVerdictOpensConversationNeverImplementing(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "t"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonIdentityUnverified
	task.Status.StageEnteredAt = &metav1.Time{Time: time.Now()}
	task.Status.IssueRefs = []string{"iss-helmfile-26"}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "szymonrychu", Body: "go ahead",
	}}

	iss := &tatarav1alpha1.Issue{}
	iss.Namespace = "tatara"
	iss.Name = "iss-helmfile-26"
	iss.Status.State = "open"
	iss.Status.Status = "approved"

	r := newUnparkTestReconciler(t, proj, task, iss)

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}

	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if fresh.Status.Stage != tatarav1alpha1.StageConversing {
		t.Fatalf("stage = %s, want conversing: a comment with no grammar verdict behind it opens a conversation, never implementing", fresh.Status.Stage)
	}
}

// THE SECOND SECURITY BOUNDARY. A verdict on record proves the grammar passed
// ONCE, against the thread state at THAT time. It says nothing about whether
// the Task's owned Issues are STILL all approved right now - an Issue can be
// reopened, a new owned Issue can be added and not yet approved, or the
// approval can simply not (yet) be visible on this one. The backstop must
// re-derive that half from LIVE state on every pass, not treat the stored
// verdict as a blanket licence: a verdict plus a currently-unapproved owned
// Issue must still decline (DeclineNotAllApproved) and leave the Task parked.
func TestDriveUnparks_IdentityUnverifiedWithVerdictButNotAllApprovedStaysParked(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "infrastructure-clarify-2026-07-27-notapproved"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonIdentityUnverified
	// See the sibling test above for why the gap is minutes, not microseconds:
	// metav1.Time round-trips at whole-second precision.
	parkedAt := time.Now().Add(-10 * time.Minute)
	task.Status.StageEnteredAt = &metav1.Time{Time: parkedAt}
	task.Status.IssueRefs = []string{"iss-helmfile-27"}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Repo: "helmfile", Number: 27,
		Author: "szymonrychu", Body: "go ahead",
	}}
	// A verdict IS on record, and it postdates this park - the grammar passed
	// for THIS park - but the live owned Issue is still open and NOT approved:
	// the other half of the F.6 rule the backstop must re-derive from live
	// state, not assume from the verdict.
	task.Status.ApprovalVerdict = &tatarav1alpha1.ApprovalVerdict{
		At:                metav1.Time{Time: parkedAt.Add(5 * time.Minute)},
		IssueRef:          "iss-helmfile-27",
		CommentExternalID: "3606943700",
		Author:            "szymonrychu",
		Phrase:            "go ahead",
	}

	iss := &tatarav1alpha1.Issue{}
	iss.Namespace = "tatara"
	iss.Name = "iss-helmfile-27"
	iss.Status.State = "open"
	iss.Status.Status = "" // NOT approved live

	r := newUnparkTestReconciler(t, proj, task, iss)

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}

	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if fresh.Status.Stage != tatarav1alpha1.StageParked {
		t.Fatalf("stage = %s, want parked: a stored verdict re-entered the Task despite live state showing "+
			"NOT all owned Issues approved", fresh.Status.Stage)
	}
	if got := testutil.ToFloat64(r.Metrics.UnparkDeclinedCounter(stage.ReasonIdentityUnverified, string(DeclineNotAllApproved))); got != 1 {
		t.Fatalf("operator_unpark_declined_total{identity-unverified,not-all-approved} = %v, want 1", got)
	}
}

// THE SECURITY REVIEW FINDING (2026-07-27): ApprovalVerdict is never cleared by
// stage.Enter (it resets Stage, StageReason, AgentKind, PodStartedAt,
// StageWorkStartedAt, Stats.PodRecreations - stage.go's Enter - but not
// ApprovalVerdict), and a NEW one is only written on an approval, leaving a
// refused approval's OLD verdict in place. So a verdict stamped for an
// EARLIER, already-consumed park can still be sitting on the Task when it
// re-parks identity-unverified for an UNRELATED later reason.
//
// The verified attack chain: Issue A is approved (sticky - verifyOneIssue,
// approval_grammar.go, deliberately never revokes an approval already given)
// and the Task implements. It later acquires an unapproved Issue B and re-parks
// identity-unverified. Issue B closes on the forge with no approval - openIssues
// drops it, so allApproved([A]) is vacuously satisfied by the OLD approval.
// ANY later non-bot comment (even "thanks", not an approval phrase) satisfies
// hasNonBotEvent. Without scoping, the months-old verdict from Issue A would
// authorize entry with NO fresh C.6 pass for THIS park. The fix: a verdict only
// counts if it was stamped strictly AFTER the CURRENT park began
// (StageEnteredAt), since stage.Enter re-stamps that on every transition.
//
// Task 9 widens the F.6 rule so a GrammarPassed=false comment opens a
// conversation instead of only declining - but that branch never looks at any
// verdict, stale or otherwise, only at hasNonBotEvent and the conversing
// ceiling, so the security property this test proves (a predating verdict
// must never authorize entry to IMPLEMENTING) is untouched: the guarantee is
// restapi's verifyApprovalScope re-running the LIVE C.6 grammar on every
// decision=implement from conversing, never status.approvalVerdict - see
// TestOutcome_Conversing_ApprovalVerdictIsNeverConsulted
// (internal/restapi/outcome_test.go), which proves this exact predating-verdict
// shape is refused there too.
func TestDriveUnparks_VerdictPredatingCurrentParkOpensConversationNeverImplementing(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	parkedAt := time.Now().Add(-time.Hour)
	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "infrastructure-clarify-2026-07-27-staleverdict"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonIdentityUnverified
	task.Status.StageEnteredAt = &metav1.Time{Time: parkedAt}
	task.Status.IssueRefs = []string{"iss-helmfile-28"}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Repo: "helmfile", Number: 28,
		Author: "szymonrychu", Body: "thanks",
	}}
	// Stamped for a DIFFERENT, EARLIER park: two hours before the CURRENT one
	// (parkedAt) began.
	task.Status.ApprovalVerdict = &tatarav1alpha1.ApprovalVerdict{
		At:                metav1.Time{Time: parkedAt.Add(-2 * time.Hour)},
		IssueRef:          "iss-helmfile-old",
		CommentExternalID: "1111111111",
		Author:            "szymonrychu",
		Phrase:            "go ahead",
	}

	// The currently owned Issue happens to ALSO read approved live (sticky from
	// the old approval): if grammarPassed were derived from the stale verdict's
	// mere presence, this Task would wrongly re-enter.
	iss := &tatarav1alpha1.Issue{}
	iss.Namespace = "tatara"
	iss.Name = "iss-helmfile-28"
	iss.Status.State = "open"
	iss.Status.Status = "approved"

	r := newUnparkTestReconciler(t, proj, task, iss)

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}

	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if fresh.Status.Stage != tatarav1alpha1.StageConversing {
		t.Fatalf("stage = %s, want conversing: the comment opens a conversation, but NEVER via the predating verdict's stale approval", fresh.Status.Stage)
	}
	if fresh.Status.ApprovalVerdict.CommentExternalID != "1111111111" {
		t.Fatalf("verdict = %+v, want the STALE verdict left untouched - conversing entry must not manufacture a fresh one",
			fresh.Status.ApprovalVerdict)
	}
}

// MINOR 6: the CRD leaves author/commentExternalId optional (schema-evolution
// headroom), so a hand-written {"at": "..."} verdict with every other field
// empty is schema-valid on the wire, and its At can trivially postdate
// StageEnteredAt. It must still never authorize entry: an empty Author is not
// evidence of ANY approval, maintainer or auto. The zero-value verdict must be
// unreachable-by-construction at the read site, not merely discouraged by CRD
// validation (which a fake client - or a hand-edited CR bypassing admission -
// does not enforce).
//
// As in the sibling tests above, Task 9's conversing branch does not consult
// the verdict at all - so the zero-value verdict now opens a conversation
// (same as no verdict would) rather than staying parked, but it still never
// authorizes IMPLEMENTING, which is the property this test exists to prove.
func TestDriveUnparks_ZeroValueVerdictNeverAuthorizes(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	parkedAt := time.Now().Add(-time.Hour)
	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "t-zero-verdict"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonIdentityUnverified
	task.Status.StageEnteredAt = &metav1.Time{Time: parkedAt}
	task.Status.IssueRefs = []string{"iss-helmfile-30"}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "szymonrychu", Body: "go ahead",
	}}
	// Only At is set (and it postdates the current park, so the TIMING check
	// alone would let this through): no Author, no IssueRef, no CommentExternalID.
	task.Status.ApprovalVerdict = &tatarav1alpha1.ApprovalVerdict{
		At: metav1.Time{Time: parkedAt.Add(time.Minute)},
	}

	iss := &tatarav1alpha1.Issue{}
	iss.Namespace = "tatara"
	iss.Name = "iss-helmfile-30"
	iss.Status.State = "open"
	iss.Status.Status = "approved"

	r := newUnparkTestReconciler(t, proj, task, iss)

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}

	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if fresh.Status.Stage != tatarav1alpha1.StageConversing {
		t.Fatalf("stage = %s, want conversing: the comment opens a conversation, but the no-Author verdict must never grant implementing", fresh.Status.Stage)
	}
}

// IMPORTANT 3: reapParked's whole invariant is "a parked Task that can still
// come back is never reaped" (reaper.go), checked by unparkFires BEFORE the
// age-based collection. unparkFires used to build UnparkInput with no
// GrammarPassed field at all (an unset bool defaults to false), so an
// identity-unverified Task carrying a genuine, in-scope, freshly-scoped verdict
// read as non-re-entryable to the reaper even though driveUnparks would re-enter
// it on its very next pass - the reaper could collect a Task the driver was
// about to save out from under it. The fix makes both call the SAME
// grammarPassedFor helper on the same Task, so they cannot disagree by
// construction.
func TestUnparkFires_AgreesWithDriveUnparksOnIdentityUnverifiedVerdict(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	parkedAt := time.Now().Add(-time.Hour)
	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "infrastructure-clarify-2026-07-27-reaperagree"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonIdentityUnverified
	task.Status.StageEnteredAt = &metav1.Time{Time: parkedAt}
	task.Status.IssueRefs = []string{"iss-helmfile-29"}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Repo: "helmfile", Number: 29,
		Author: "szymonrychu", Body: "go ahead",
	}}
	task.Status.ApprovalVerdict = &tatarav1alpha1.ApprovalVerdict{
		At:                metav1.Time{Time: parkedAt.Add(time.Minute)}, // AFTER this park began
		IssueRef:          "iss-helmfile-29",
		CommentExternalID: "2222222222",
		Author:            "szymonrychu",
		Phrase:            "go ahead",
	}

	iss := &tatarav1alpha1.Issue{}
	iss.Namespace = "tatara"
	iss.Name = "iss-helmfile-29"
	iss.Status.State = "open"
	iss.Status.Status = "approved"

	r := newUnparkTestReconciler(t, proj, task, iss)

	fires, err := r.unparkFires(context.Background(), proj, task, time.Now(), false)
	if err != nil {
		t.Fatalf("unparkFires: %v", err)
	}
	if !fires {
		t.Fatalf("unparkFires = false: the reaper disagrees with driveUnparks, which would re-enter this Task; " +
			"a Task the driver is about to save must never read as collectible")
	}
}

// 2026-07-28 security review CRITICAL 1: unparkFires is a THIRD UnparkInput
// builder (after ApplyUnpark and stage.UnparkDetailed's own callers), and it
// used to leave ConversingHasRoom at its zero value (false)
// unconditionally. The window: parked(identity-unverified), a non-bot event,
// NO valid verdict (grammarPassed=false), and the conversing ceiling has
// room. driveUnparks' ApplyUnpark, called with room=true, sends the Task to
// conversing and saves it; unparkFires, called with room=false (the bug),
// returns DeclineGrammarNotPassed and fires=false - so reapParked, once past
// ParkRetention, would delete a Task the driver was about to save out from
// under it. driveUnparksPaced and ReapTerminalPaced are paced
// INDEPENDENTLY, so a pass where the driver is throttled and the reaper is
// not is a real production window.
//
// This proves the probe and the driver reach the SAME verdict given the SAME
// room=true answer: unparkFires(room=true) must return fires=true on exactly
// the input where ApplyUnpark's own stage.UnparkDetailed call would enter
// conversing (proven by TestUnpark_IdentityUnverifiedWithoutGrammarConversesWhenThereIsRoom
// directly against the pure function).
func TestUnparkFires_AgreesWithApplyUnparkOnConversingHasRoom(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "t-crit1-conversing-window"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonIdentityUnverified
	task.Status.StageEnteredAt = &metav1.Time{Time: time.Now().Add(-time.Hour)}
	// No ApprovalVerdict at all: GrammarPassed resolves false via grammarPassedFor.
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "szymonrychu", Body: "not the magic phrase",
	}}

	r := newUnparkTestReconciler(t, proj, task)

	firesNoRoom, err := r.unparkFires(context.Background(), proj, task, time.Now(), false)
	if err != nil {
		t.Fatalf("unparkFires(room=false): %v", err)
	}
	if firesNoRoom {
		t.Fatal("unparkFires(room=false) = true: a full ceiling must fall back to the SAME non-firing decline as before Task 9, not invent a new re-entry")
	}

	firesRoom, err := r.unparkFires(context.Background(), proj, task, time.Now(), true)
	if err != nil {
		t.Fatalf("unparkFires(room=true): %v", err)
	}
	if !firesRoom {
		t.Fatal("unparkFires(room=true) = false: the reaper disagrees with what ApplyUnpark would do with room - " +
			"exactly the CRITICAL 1 window, where the reaper would delete a Task the driver was about to save into conversing")
	}
}

// 2026-07-28 security review NEW-1: unparkFires never set
// UnparkInput.MaxTurnsPerTask, so for a parked(no-outcome) Task,
// ReasonNoOutcome's Stats.Turns >= in.MaxTurnsPerTask read Turns >= 0, which
// is true for ANY real turn count. The probe always declined turns-exhausted
// regardless of how far under its actual cap the Task was, so reapParked -
// once past ParkRetention - deleted every parked(no-outcome) Task whether or
// not driveUnparks' ApplyUnpark (which threads taskMaxTurns correctly) would
// have re-entered it. Identical failure class to CRITICAL 1, on
// MaxTurnsPerTask instead of ConversingHasRoom.
//
// This Task is parked(no-outcome) from implementing, with Turns(10) far below
// the default cap (300, since neither the Task nor the Project overrides it) -
// exactly the shape ApplyUnpark would re-enter into implementing. The probe
// must agree.
func TestUnparkFires_AgreesWithApplyUnparkOnMaxTurnsPerTask(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "t-new1-no-outcome-under-cap"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonNoOutcome
	task.Status.ParkedFromStage = tatarav1alpha1.StageImplementing
	task.Status.StageEnteredAt = &metav1.Time{Time: time.Now().Add(-time.Hour)}
	task.Status.Stats.Turns = 10 // far below the default cap of 300

	r := newUnparkTestReconciler(t, proj, task)

	fires, err := r.unparkFires(context.Background(), proj, task, time.Now(), false)
	if err != nil {
		t.Fatalf("unparkFires: %v", err)
	}
	if !fires {
		t.Fatal("unparkFires = false: a parked(no-outcome) Task at 10/300 turns must agree with ApplyUnpark that it re-enters - " +
			"MaxTurnsPerTask was left at its zero value, so Turns >= 0 always declined turns-exhausted regardless of the real count")
	}
}
