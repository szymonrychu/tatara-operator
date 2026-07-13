package webhook

// Task 18 (contract E.3 / Section I "pendingEvents") coverage for
// deliverPendingEvent and its wiring into reverifyParked (fix C3-3 / M11).
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
// needs (controller.IssueKeyIndex) - without it the C3-3 re-verify path 500s
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

func peTask(name, stageName, stageReason string, issueRefs ...string) *tatarav1.Task {
	return &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: peNS},
		Spec:       tatarav1.TaskSpec{Kind: "clarify", ProjectRef: "pe-proj", Goal: "g"},
		Status: tatarav1.TaskStatus{
			Stage:       stageName,
			StageReason: stageReason,
			IssueRefs:   issueRefs,
		},
	}
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
	task := peTask("t-bot", tatarav1.StageClarifying, "")
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
	task := peTask("t-live", tatarav1.StageClarifying, "")
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

// TestDeliverPendingEvent_ParkedIdentityUnverified_GoAhead_UnparksInOneComment
// is fix C3-3 + M11's webhook wiring: a maintainer "go ahead" on a parked
// Task, with the LOCAL mirror stale (no comments at all), completes in ONE
// webhook delivery - reverifyParked runs the C3-3 on-demand sync (proven by
// rd.calls==1) and the grammar pass un-parks straight to implementing,
// without waiting for any cron sweep.
func TestDeliverPendingEvent_ParkedIdentityUnverified_GoAhead_UnparksInOneComment(t *testing.T) {
	task := peTask("t-parked-ok", tatarav1.StageParked, stage.ReasonIdentityUnverified)
	iss := peIssue(7, task) // stale mirror: zero comments locally
	task.Status.IssueRefs = []string{iss.Name}
	proj := peProject("tatara-bot", "maintainer")
	sec := peSecret("pe-proj-scm", "pat")
	c := peClient(t, proj, peRepo(), task, iss, sec)

	rd := &fakeApprovalReader{comments: []scm.IssueComment{
		{ExternalID: "c9", Author: "maintainer", Body: "go ahead", CreatedAt: time.Now().UTC()},
	}}
	s := peServer(c, &stubSpiller{}, func(string, string) (scm.SCMReader, error) { return rd, nil })

	ev := scm.WebhookEvent{
		IsComment: true, IssueRef: "o/r#7", Number: 7,
		ActorLogin: "maintainer", CommentID: 99, CommentBody: "go ahead",
	}
	s.deliverPendingEvent(context.Background(), *proj, peRepo(), ev)

	if rd.calls != 1 {
		t.Fatalf("forge reads = %d, want EXACTLY 1 (the C3-3 on-demand sync must run)", rd.calls)
	}
	gotTask := getPETask(t, c, task.Name)
	if gotTask.Status.Stage != tatarav1.StageImplementing {
		t.Fatalf("stage = %q, want implementing - a maintainer 'go ahead' must un-park in ONE webhook delivery, not a 7-day cron wait", gotTask.Status.Stage)
	}
}

// TestDeliverPendingEvent_ParkedIdentityUnverified_NotYet_StaysParked: a
// non-approving maintainer comment re-runs the grammar and it fails, so the
// Task stays parked and its pendingEvents are RETAINED, never dropped.
func TestDeliverPendingEvent_ParkedIdentityUnverified_NotYet_StaysParked(t *testing.T) {
	task := peTask("t-parked-no", tatarav1.StageParked, stage.ReasonIdentityUnverified)
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

	gotTask := getPETask(t, c, task.Name)
	if gotTask.Status.Stage != tatarav1.StageParked || gotTask.Status.StageReason != stage.ReasonIdentityUnverified {
		t.Fatalf("stage = (%q,%q), want (parked, identity-unverified) - 'not yet' must not un-park", gotTask.Status.Stage, gotTask.Status.StageReason)
	}
	if len(gotTask.Status.PendingEvents) != 1 {
		t.Fatalf("pendingEvents = %d, want 1 RETAINED (never dropped on a failed re-verify)", len(gotTask.Status.PendingEvents))
	}
}

// TestDeliverPendingEvent_ParkedIdentityUnverified_BotComment_NeverReverifies:
// a bot comment on a parked(identity-unverified) Task is dropped by the E.3
// filter before the owning Task is even looked up, so reverifyParked (and its
// forge read) is never reached at all.
func TestDeliverPendingEvent_ParkedIdentityUnverified_BotComment_NeverReverifies(t *testing.T) {
	task := peTask("t-parked-bot", tatarav1.StageParked, stage.ReasonIdentityUnverified)
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
		t.Fatalf("forge reads = %d, want 0 - a bot event must never even cost a re-verify attempt", rd.calls)
	}
	gotTask := getPETask(t, c, task.Name)
	if gotTask.Status.Stage != tatarav1.StageParked || len(gotTask.Status.PendingEvents) != 0 {
		t.Fatalf("bot event must change nothing: stage=%q pendingEvents=%d", gotTask.Status.Stage, len(gotTask.Status.PendingEvents))
	}
}
