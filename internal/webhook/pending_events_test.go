package webhook

// Task 18 (contract E.3 / Section I "pendingEvents") coverage for
// deliverPendingEvent and its wiring into the shared comment unpark plus the
// D2 on-demand issue mirror sync (fix M11).
// These are white-box tests (package webhook) because they call the
// unexported deliverPendingEvent directly, bypassing handleIssueComment's own
// (redundant) bot/reporter gates - the point is to prove pending_events.go's
// OWN belt-and-suspenders bot filter holds even when called directly.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

const peNS = "tatara"

// stubSpiller is a no-op objbudget.Spiller: these tests never exceed the byte
// budget, so a spill would itself be a failure signal.
type stubSpiller struct{ calls int }

func (s *stubSpiller) Spill(context.Context, string, string, any) (string, error) {
	s.calls++
	return "track-1", nil
}

// fakeApprovalReader is a minimal scm.SCMReader stub: only ListIssueComments
// is exercised by SyncIssueOnDemand, everything else panics if called (there
// is no other forge read on this path).
type fakeApprovalReader struct {
	scm.SCMReader
	comments []scm.IssueComment
	calls    int
}

func (r *fakeApprovalReader) ListIssueComments(context.Context, string, string, int) ([]scm.IssueComment, error) {
	r.calls++
	return r.comments, nil
}

func peScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	sch := runtime.NewScheme()
	if err := corev1.AddToScheme(sch); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := tatarav1.AddToScheme(sch); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return sch
}

// peClient builds a fake client carrying the field index SyncIssueOnDemand
// needs (controller.IssueKeyIndex) - without it the on-demand issue sync 500s
// on List, not because the property under test is wrong.
func peClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(peScheme(t)).WithObjects(objs...).
		WithStatusSubresource(&tatarav1.Project{}, &tatarav1.Repository{}, &tatarav1.Task{}, &tatarav1.Issue{}, &tatarav1.MergeRequest{}).
		WithIndex(&tatarav1.Issue{}, controller.IssueKeyIndex, controller.IssueKeyIndexer).
		Build()
}

func peProject(botLogin string, maintainers ...string) *tatarav1.Project {
	return &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "pe-proj", Namespace: peNS},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: "pe-proj-scm",
			Scm: &tatarav1.ScmSpec{
				Provider:         "github",
				Owner:            "o",
				BotLogin:         botLogin,
				MaintainerLogins: maintainers,
			},
		},
	}
}

func peSecret(name, token string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: peNS},
		Data:       map[string][]byte{"token": []byte(token)},
	}
}

func peRepo() *tatarav1.Repository {
	return &tatarav1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "pe-repo", Namespace: peNS},
		Spec:       tatarav1.RepositorySpec{ProjectRef: "pe-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main"},
	}
}

// peIssue builds an Issue CR owned (controller=true) by task, with the given
// mirrored comments (possibly none - a stale mirror).
func peIssue(number int, task *tatarav1.Task, comments ...tatarav1.Comment) *tatarav1.Issue {
	iss := &tatarav1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1.IssueName("pe-repo", number), Namespace: peNS},
		Spec: tatarav1.IssueSpec{
			RepositoryRef: "pe-repo", Number: number, ProjectRef: "pe-proj",
			URL: "https://github.com/o/r/issues/7",
		},
		Status: tatarav1.IssueStatus{State: "open", Status: "new", Comments: comments},
	}
	own.AddPlainOwner(iss, task)
	if err := own.HandOverController(iss, nil, task); err != nil {
		panic(err)
	}
	return iss
}

// peTask builds a live (non-parked) Task fixture at state, with stateReason -
// meaningful only on the two terminals (done/rejected); every other caller
// passes "". #521 replaced Spec.Kind's clarify with implement, so the default
// kind here is implement, not the deleted clarify - AgentKindFor and
// ReactingAgentKind resolve off originAgentKinds, which has no "clarify" entry
// and fails closed on one.
func peTask(name, state, stateReason string, issueRefs ...string) *tatarav1.Task {
	now := metav1.Now()
	return &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: peNS},
		Spec:       tatarav1.TaskSpec{Kind: "implement", ProjectRef: "pe-proj", Goal: "g"},
		Status: tatarav1.TaskStatus{
			State:          state,
			StateReason:    stateReason,
			StateEnteredAt: &now,
			IssueRefs:      issueRefs,
		},
	}
}

// peTaskKind is peTask with an explicit Spec.Kind, for the tests (2026-07-28
// security review NEW-2) that need a kind=review Task - peTask itself
// hardcodes "implement" and every other caller relies on that.
func peTaskKind(name, kind, state, stateReason string) *tatarav1.Task {
	task := peTask(name, state, stateReason)
	task.Spec.Kind = kind
	return task
}

// peParkedTask builds a Task fixture already parked at reason, via stage.Park -
// the ONE way #521 parks a Task (stamps ParkReason/ParkedAt/ParkedFromState
// together). state is the state the Task parks WHERE IT IS: park never moves
// state, so callers whose old fixture carried an implicit "conversing" or
// "clarifying" precursor pass v1alpha1.StateRefined, matching the mapping
// table those old stages folded into.
func peParkedTask(name, state, parkReason string, issueRefs ...string) *tatarav1.Task {
	task := peTask(name, state, "", issueRefs...)
	if err := stage.Park(task, parkReason, time.Now()); err != nil {
		panic(err)
	}
	return task
}

func getPETask(t *testing.T, c client.Client, name string) *tatarav1.Task {
	t.Helper()
	var task tatarav1.Task
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: peNS, Name: name}, &task); err != nil {
		t.Fatalf("get task %s: %v", name, err)
	}
	return &task
}

func getPEIssue(t *testing.T, c client.Client, name string) *tatarav1.Issue {
	t.Helper()
	var iss tatarav1.Issue
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: peNS, Name: name}, &iss); err != nil {
		t.Fatalf("get issue %s: %v", name, err)
	}
	return &iss
}

func peServer(c client.Client, sp *stubSpiller, readerFor func(provider, token string) (scm.SCMReader, error)) *Server {
	seq := &queue.SeqSource{Client: c, Namespace: peNS}
	return NewServer(Config{
		Client:    c,
		Namespace: peNS,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Seq:       seq,
		Spiller:   sp,
		ReaderFor: readerFor,
	})
}

// TestDeliverPendingEvent_BotEvent_MirroredButNotEnqueued is the E.3 enqueue
// filter: without it the operator's own park comment would land in
// pendingEvents and un-park the Task it just parked. The comment still
// mirrors (the webhook drives the mirror unconditionally), but the enqueue
// step is never reached for a bot actor.
func TestDeliverPendingEvent_BotEvent_MirroredButNotEnqueued(t *testing.T) {
	task := peTask("t-bot", tatarav1.StateRefined, "")
	iss := peIssue(7, task)
	proj := peProject("tatara-bot", "maintainer")
	c := peClient(t, proj, peRepo(), task, iss)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{
		IsComment: true, IssueRef: "o/r#7", Number: 7,
		ActorLogin: "tatara-bot", CommentID: 55, CommentBody: "tatara: parked, missing X",
	}
	s.deliverPendingEvent(context.Background(), *proj, peRepo(), ev)

	got := getPEIssue(t, c, iss.Name)
	if len(got.Status.Comments) != 1 || !got.Status.Comments[0].IsBot {
		t.Fatalf("bot comment must still land in the mirror with isBot=true, got %+v", got.Status.Comments)
	}
	gotTask := getPETask(t, c, task.Name)
	if len(gotTask.Status.PendingEvents) != 0 {
		t.Fatalf("bot event must NEVER be enqueued, got %d pendingEvents", len(gotTask.Status.PendingEvents))
	}
}

// TestDeliverPendingEvent_NonBotEvent_MirroredAndEnqueuedImmediately proves
// the webhook drives the mirror and the queue synchronously - no sweep
// involved (there is none running in this test at all).
func TestDeliverPendingEvent_NonBotEvent_MirroredAndEnqueuedImmediately(t *testing.T) {
	task := peTask("t-live", tatarav1.StateRefined, "")
	iss := peIssue(7, task)
	proj := peProject("tatara-bot", "maintainer")
	c := peClient(t, proj, peRepo(), task, iss)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{
		IsComment: true, IssueRef: "o/r#7", Number: 7,
		ActorLogin: "maintainer", CommentID: 100, CommentBody: "any update?",
	}
	s.deliverPendingEvent(context.Background(), *proj, peRepo(), ev)

	gotIssue := getPEIssue(t, c, iss.Name)
	if len(gotIssue.Status.Comments) != 1 || gotIssue.Status.Comments[0].Body != "any update?" {
		t.Fatalf("comment not mirrored immediately: %+v", gotIssue.Status.Comments)
	}
	gotTask := getPETask(t, c, task.Name)
	if len(gotTask.Status.PendingEvents) != 1 {
		t.Fatalf("pendingEvents = %d, want 1", len(gotTask.Status.PendingEvents))
	}
	pe := gotTask.Status.PendingEvents[0]
	if pe.Kind != "issue_comment" || pe.Repo != "pe-repo" || pe.Number != 7 || pe.Author != "maintainer" {
		t.Fatalf("unexpected pendingEvent: %+v", pe)
	}
}

// TestDeliverPendingEvent_NoOwningTask_MirrorsOnlyNoEnqueue: an Issue with no
// controller owner yet (the sweep has not minted a Task) still gets the
// comment mirrored; there is nothing to enqueue onto.
func TestDeliverPendingEvent_NoOwningTask_MirrorsOnlyNoEnqueue(t *testing.T) {
	iss := &tatarav1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1.IssueName("pe-repo", 8), Namespace: peNS},
		Spec:       tatarav1.IssueSpec{RepositoryRef: "pe-repo", Number: 8, ProjectRef: "pe-proj", URL: "https://github.com/o/r/issues/8"},
		Status:     tatarav1.IssueStatus{State: "open", Status: "new"},
	}
	proj := peProject("tatara-bot", "maintainer")
	c := peClient(t, proj, peRepo(), iss)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{IsComment: true, IssueRef: "o/r#8", Number: 8, ActorLogin: "maintainer", CommentID: 1, CommentBody: "hello"}
	s.deliverPendingEvent(context.Background(), *proj, peRepo(), ev)

	got := getPEIssue(t, c, iss.Name)
	if len(got.Status.Comments) != 1 {
		t.Fatalf("comment must still mirror onto an unowned Issue, got %+v", got.Status.Comments)
	}
}

// TestDeliverPendingEvent_IdentityUnverifiedUsesCommentUnpark is the Step A
// behaviour: a non-bot comment on a parked(identity-unverified) Task no longer
// runs a bespoke re-verify limb; it joins awaiting-human and backlog-sweep on
// the ONE shared driveCommentUnpark, which MUST compute conversing room for it
// (a gate on == ReasonAwaitingHuman would make the conversing edge structurally
// inert on the webhook fast path).
//
// The body is the exact literal the DELETED C.6 wordlist used to accept, on
// purpose: that is what the deleted limb turned into an un-park straight to
// implementing. The shared path feeds stage.Unpark no approval verdict at all
// and the wordlist itself is gone, so the same literal is now just a human
// saying something - a conversation, not a decision.
func TestDeliverPendingEvent_IdentityUnverifiedUsesCommentUnpark(t *testing.T) {
	task := peParkedTask("t-parked-shared", tatarav1.StateRefined, stage.ReasonIdentityUnverified)
	iss := peIssue(7, task)
	task.Status.IssueRefs = []string{iss.Name}
	proj := peProject("tatara-bot", "maintainer")
	sec := peSecret("pe-proj-scm", "pat")
	c := peClient(t, proj, peRepo(), task, iss, sec)

	rd := &fakeApprovalReader{comments: []scm.IssueComment{
		{ExternalID: "c-900", Author: "maintainer", Body: "go ahead", CreatedAt: time.Now().UTC()},
	}}
	s := peServer(c, &stubSpiller{}, func(string, string) (scm.SCMReader, error) { return rd, nil })

	ev := scm.WebhookEvent{
		IsComment: true, IssueRef: "o/r#7", Number: 7,
		ActorLogin: "maintainer", CommentID: 900, CommentBody: "go ahead",
	}
	s.deliverPendingEvent(context.Background(), *proj, peRepo(), ev)

	// Un-park never moves state any more (#521): the assertion that used to be
	// "stage became conversing" is now "the park flag cleared and the Task is
	// still exactly where it was" - which is what proves the comment reached
	// the ONE shared unpark and that unpark computed live-pod room for
	// identity-unverified, rather than declining or short-circuiting.
	gotTask := getPETask(t, c, task.Name)
	if tatarav1.Parked(gotTask) {
		t.Fatalf("still parked(%s) - the comment must reach an agent through the ONE shared unpark", gotTask.Status.ParkReason)
	}
	if gotTask.Status.State != tatarav1.StateRefined {
		t.Fatalf("state = %q, want unchanged refined (un-park never moves state)", gotTask.Status.State)
	}
}

// TestDeliverPendingEvent_CommentSyncsIssueMirror is D2. The operator verifies
// the agent's citation against Issue.Status.Comments, and the agent reads the
// external_id it cites out of the turn-0 bundle, which is rendered from the
// SAME field. Nothing else on the webhook comment path writes that field
// (mirror_refresh.go only touches Body/Title, and the parked cadence is daily),
// so without this sync the forge thread the agent must cite from is in neither,
// and every clarify refuses forever.
//
// The Task here is LIVE (clarifying), not parked: that is the whole delta. The
// deleted limb bought this forge read for parked(identity-unverified) ONLY, so
// the one stage that actually renders turn-0 bundles never got it.
func TestDeliverPendingEvent_CommentSyncsIssueMirror(t *testing.T) {
	task := peTask("t-live-sync", tatarav1.StateRefined, "")
	iss := peIssue(7, task) // mirror carries ZERO comments
	task.Status.IssueRefs = []string{iss.Name}
	proj := peProject("tatara-bot", "maintainer")
	sec := peSecret("pe-proj-scm", "pat")
	c := peClient(t, proj, peRepo(), task, iss, sec)

	// c-900 is an EARLIER maintainer comment the stale mirror never saw. It is
	// distinguishable from the webhook's own append (external id "901"), so its
	// presence proves the on-demand sync ran, not the append.
	rd := &fakeApprovalReader{comments: []scm.IssueComment{
		{ExternalID: "c-900", Author: "maintainer", Body: "use the starred option",
			CreatedAt: time.Now().UTC().Add(-time.Hour)},
	}}
	s := peServer(c, &stubSpiller{}, func(string, string) (scm.SCMReader, error) { return rd, nil })

	ev := scm.WebhookEvent{
		IsComment: true, IssueRef: "o/r#7", Number: 7,
		ActorLogin: "maintainer", CommentID: 901, CommentBody: "go ahead, I approve!",
	}
	s.deliverPendingEvent(context.Background(), *proj, peRepo(), ev)

	if rd.calls != 1 {
		t.Fatalf("forge reads = %d, want EXACTLY 1 (the on-demand issue sync must run on a LIVE Task's comment too)", rd.calls)
	}
	gotIssue := getPEIssue(t, c, iss.Name)
	var found bool
	for _, cm := range gotIssue.Status.Comments {
		if cm.ExternalID == "c-900" {
			found = true
		}
	}
	if !found {
		t.Fatalf("mirror comments = %#v, want the forge thread synced on demand (c-900 present)", gotIssue.Status.Comments)
	}
}

// TestDeliverPendingEvent_ParkedIdentityUnverified_NotYet_OpensConversation: a
// maintainer comment with no durable verdict behind it is a live conversation
// rather than a dead end. The Task moves to conversing with its idle clock
// armed, instead of sitting parked for up to 7 days. Reaching implementing from
// there still requires a genuine decision=implement that passes restapi's LIVE
// approval check - the conversing edge grants nothing on its own.
func TestDeliverPendingEvent_ParkedIdentityUnverified_NotYet_OpensConversation(t *testing.T) {
	task := peParkedTask("t-parked-no", tatarav1.StateRefined, stage.ReasonIdentityUnverified)
	iss := peIssue(7, task)
	task.Status.IssueRefs = []string{iss.Name}
	proj := peProject("tatara-bot", "maintainer")
	sec := peSecret("pe-proj-scm", "pat")
	c := peClient(t, proj, peRepo(), task, iss, sec)

	rd := &fakeApprovalReader{comments: []scm.IssueComment{
		{ExternalID: "c9", Author: "maintainer", Body: "not yet", CreatedAt: time.Now().UTC()},
	}}
	s := peServer(c, &stubSpiller{}, func(string, string) (scm.SCMReader, error) { return rd, nil })

	ev := scm.WebhookEvent{
		IsComment: true, IssueRef: "o/r#7", Number: 7,
		ActorLogin: "maintainer", CommentID: 100, CommentBody: "not yet",
	}
	s.deliverPendingEvent(context.Background(), *proj, peRepo(), ev)

	// "not yet" must not stay parked, but un-park never moves state (#521): the
	// live conversation it opens is just the Task's own state (refined),
	// un-parked with its idle clock armed - not a distinct "conversing" state.
	gotTask := getPETask(t, c, task.Name)
	if tatarav1.Parked(gotTask) {
		t.Fatalf("still parked(%s) - 'not yet' must not un-park to implementing, but it must un-park to a live conversation", gotTask.Status.ParkReason)
	}
	if gotTask.Status.State != tatarav1.StateRefined {
		t.Fatalf("state = %q, want unchanged refined", gotTask.Status.State)
	}
	if gotTask.Status.ConversationLastEventAt == nil {
		t.Fatal("ConversationLastEventAt is nil: the idle clock was never armed on entry")
	}
	if len(gotTask.Status.PendingEvents) != 1 {
		t.Fatalf("pendingEvents = %d, want 1 RETAINED (the comment rides into the live pod's turn-0 bundle, not dropped here)", len(gotTask.Status.PendingEvents))
	}
}

// TestDeliverPendingEvent_ParkedIdentityUnverified_ReviewKindMergedMR_NoConversation
// is 2026-07-28 security review NEW-2, now guaranteed structurally: the
// merged-MR guard (anyMerged(in.MRs)) can only fire if the caller actually
// LOADS the owned MRs, and the bespoke identity-unverified limb that did not is
// gone - every comment-driven re-entry is ApplyUnpark, which always loads them.
// A kind=review Task parked(identity-unverified) whose owned MR is already
// merged must NOT open a conversing pod on a stray comment - not a security
// bypass (GUARD 1 still blocks review-kind from implementing/merging/approved
// from conversing), but exactly the "one pod per human comment" waste the guard
// exists to prevent.
func TestDeliverPendingEvent_ParkedIdentityUnverified_ReviewKindMergedMR_NoConversation(t *testing.T) {
	task := peTaskKind("t-parked-review-merged", "review", tatarav1.StateAwaitingReview, "")
	if err := stage.Park(task, stage.ReasonIdentityUnverified, time.Now()); err != nil {
		t.Fatalf("park: %v", err)
	}
	mergedAt := metav1.Now()
	mr := peMR(88, task, tatarav1.MergeRequestStatus{State: "merged", MergedAt: &mergedAt})
	task.Status.MRRefs = []string{mr.Name}
	proj := peProject("tatara-bot", "maintainer")
	sec := peSecret("pe-proj-scm", "pat")
	c := peClient(t, proj, peRepo(), task, mr, sec)

	rd := &fakeApprovalReader{comments: []scm.IssueComment{
		{ExternalID: "c10", Author: "maintainer", Body: "any update?", CreatedAt: time.Now().UTC()},
	}}
	s := peServer(c, &stubSpiller{}, func(string, string) (scm.SCMReader, error) { return rd, nil })

	ev := scm.WebhookEvent{
		IsComment: true, IsPR: true, Number: 88,
		ActorLogin: "maintainer", CommentID: 101, CommentBody: "any update?",
	}
	s.deliverPendingEvent(context.Background(), *proj, peRepo(), ev)

	gotTask := getPETask(t, c, task.Name)
	if !tatarav1.Parked(gotTask) {
		t.Fatalf("state = %q, want still parked - a kind=review Task with a merged owned MR must never open a live conversation", gotTask.Status.State)
	}
}

// TestDeliverPendingEvent_ParkedIdentityUnverified_BotComment_NeverSyncsOrUnparks:
// a bot comment on a parked(identity-unverified) Task is dropped by the E.3
// filter before the owning Task is even looked up, so neither the on-demand
// issue sync (and its forge read) nor the comment unpark is reached at all.
func TestDeliverPendingEvent_ParkedIdentityUnverified_BotComment_NeverSyncsOrUnparks(t *testing.T) {
	task := peParkedTask("t-parked-bot", tatarav1.StateRefined, stage.ReasonIdentityUnverified)
	iss := peIssue(7, task)
	task.Status.IssueRefs = []string{iss.Name}
	proj := peProject("tatara-bot", "maintainer")
	sec := peSecret("pe-proj-scm", "pat")
	c := peClient(t, proj, peRepo(), task, iss, sec)

	rd := &fakeApprovalReader{}
	s := peServer(c, &stubSpiller{}, func(string, string) (scm.SCMReader, error) { return rd, nil })

	ev := scm.WebhookEvent{
		IsComment: true, IssueRef: "o/r#7", Number: 7,
		ActorLogin: "tatara-bot", CommentID: 101, CommentBody: "tatara: still working on it",
	}
	s.deliverPendingEvent(context.Background(), *proj, peRepo(), ev)

	if rd.calls != 0 {
		t.Fatalf("forge reads = %d, want 0 - a bot event must never even cost an on-demand forge read", rd.calls)
	}
	gotTask := getPETask(t, c, task.Name)
	if !tatarav1.Parked(gotTask) || len(gotTask.Status.PendingEvents) != 0 {
		t.Fatalf("bot event must change nothing: parked=%v pendingEvents=%d", tatarav1.Parked(gotTask), len(gotTask.Status.PendingEvents))
	}
}
