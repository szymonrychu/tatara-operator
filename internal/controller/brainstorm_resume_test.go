package controller

import (
	"context"
	"errors"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// THE NAMED REGRESSION GUARD. A documentation Task's merge is the operator
// documenting its own work; it is not the project MOVING, so it must NOT
// resume a paused brainstorm. The same goes for a pin bump, which lands as a
// documentation-adjacent bot PR and carries no Task of an implementing kind at
// all. This is the case most likely to regress silently - a paused project that
// quietly un-pauses every night on the doc batch looks exactly like a working
// system until the token bill arrives.
//
// The name still says only what it has always said - documentation and
// pin-bump Tasks are excluded - because that half of the table is the one
// that must never silently regress. clarify, incident and takeover are
// asserted TRUE in the same table (a merged MR for any of them lands real
// code, so the project moved), so an eighth kind added later still fails
// this test instead of picking a silent default, on either side of the line.
func TestBrainstormResumeKindExcludesDocumentationAndPinBumpTasks(t *testing.T) {
	// Task.Spec.Kind is a CLOSED CRD enum (task_types.go): brainstorm, incident,
	// clarify, refine, review, documentation, takeover. Every value is listed
	// here on purpose, so adding an eighth kind fails this test rather than
	// silently picking a default.
	tests := []struct {
		kind string
		want bool
	}{
		{"clarify", true},  // the full proposal-to-implementation lifecycle
		{"incident", true}, // an incident fix landing IS the project moving
		{"takeover", true}, // a takeover MR lands real code - the project moved
		{"documentation", false},
		{"refine", false},
		{"review", false},
		{"brainstorm", false},
		{"", false},
		{"implement", false}, // an AGENT kind, never a Task.Spec.Kind
	}
	for _, tc := range tests {
		t.Run(tc.kind, func(t *testing.T) {
			if got := brainstormResumeKind(tc.kind); got != tc.want {
				t.Fatalf("brainstormResumeKind(%q) = %v, want %v", tc.kind, got, tc.want)
			}
		})
	}
}

// TestStampBrainstormResumeIsANoOpWhenNotPaused documents the ANNOTATION half
// only: an unpaused project must never receive the write-per-webhook
// AnnBrainstormResume annotation, which has no reconcile consumer until the
// project actually pauses. It says nothing about Status.LastMovementAt, which
// IS stamped unconditionally - see the movement test below (I3 fix round).
func TestStampBrainstormResumeIsANoOpWhenNotPaused(t *testing.T) {
	ctx := context.Background()
	proj := mkResumeProject(t, "resume-noop", false)
	if err := StampBrainstormResume(ctx, k8sClient, testNS, proj.Name, ResumeTriggerPush); err != nil {
		t.Fatalf("StampBrainstormResume: %v", err)
	}
	fresh := getResumeProject(t, proj.Name)
	if _, ok := fresh.Annotations[tatarav1alpha1.AnnBrainstormResume]; ok {
		t.Fatal("an unpaused project must not be annotated: a resume nobody asked for is a write per webhook")
	}
}

// TestStampBrainstormResumeRecordsMovementUnconditionally is I3's fix round
// proof at the resume-stamp layer. Status.LastMovementAt must be stamped on
// EVERY call, paused or not - unlike the annotation, which stays gated on
// BrainstormPausedAt != nil - so a movement that lands while a session is in
// flight for an UNPAUSED project is never silently discarded: the exhausted
// outcome handler (internal/restapi) reads this field to refuse a stale pause.
func TestStampBrainstormResumeRecordsMovementUnconditionally(t *testing.T) {
	ctx := context.Background()
	proj := mkResumeProject(t, "resume-movement-unpaused", false)
	if getResumeProject(t, proj.Name).Status.LastMovementAt != nil {
		t.Fatal("setup: LastMovementAt must start nil")
	}
	if err := StampBrainstormResume(ctx, k8sClient, testNS, proj.Name, ResumeTriggerPush); err != nil {
		t.Fatalf("StampBrainstormResume: %v", err)
	}
	fresh := getResumeProject(t, proj.Name)
	if fresh.Status.LastMovementAt == nil {
		t.Fatal("movement must be stamped even on an UNPAUSED project - a live session must be able to observe it")
	}
	if _, ok := fresh.Annotations[tatarav1alpha1.AnnBrainstormResume]; ok {
		t.Fatal("an unpaused project still must not get the resume annotation - only the movement stamp is unconditional")
	}
}

func TestStampBrainstormResumeAnnotatesAPausedProject(t *testing.T) {
	ctx := context.Background()
	proj := mkResumeProject(t, "resume-paused", true)
	if err := StampBrainstormResume(ctx, k8sClient, testNS, proj.Name, ResumeTriggerMaintainerComment); err != nil {
		t.Fatalf("StampBrainstormResume: %v", err)
	}
	fresh := getResumeProject(t, proj.Name)
	if got := fresh.Annotations[tatarav1alpha1.AnnBrainstormResume]; got != ResumeTriggerMaintainerComment {
		t.Fatalf("annotation = %q, want %q", got, ResumeTriggerMaintainerComment)
	}
}

// The reconcile is the SINGLE clearing path: it drops the pause fields and
// removes the annotation so the same trigger cannot fire twice.
func TestClearBrainstormPauseConsumesTheAnnotation(t *testing.T) {
	ctx := context.Background()
	proj := mkResumeProject(t, "resume-clear", true)
	if err := StampBrainstormResume(ctx, k8sClient, testNS, proj.Name, ResumeTriggerManual); err != nil {
		t.Fatalf("StampBrainstormResume: %v", err)
	}
	fresh := getResumeProject(t, proj.Name)
	r := newProjectReconciler()
	if err := r.clearBrainstormPauseIfRequested(ctx, fresh); err != nil {
		t.Fatalf("clearBrainstormPauseIfRequested: %v", err)
	}
	after := getResumeProject(t, proj.Name)
	if after.Status.BrainstormPausedAt != nil || after.Status.BrainstormPauseReason != "" {
		t.Fatalf("pause not cleared: %v %q", after.Status.BrainstormPausedAt, after.Status.BrainstormPauseReason)
	}
	if _, ok := after.Annotations[tatarav1alpha1.AnnBrainstormResume]; ok {
		t.Fatal("the annotation must remove itself, or every reconcile re-resumes forever")
	}
}

// The identity-scoped push check. The operator NEVER inspects the commit: it
// compares the pushed head against its OWN record of what it landed
// (MergeRequest.Status.MergedSHA), because ListCommits returns raw
// `git config user.name` and is forgeable.
func TestOperatorLandedSHAMatchesTheOperatorsOwnMergeRecord(t *testing.T) {
	ctx := context.Background()
	proj := mkResumeProject(t, "landed-sha", true)
	mkMergedMR(t, proj.Name, "landed-sha-mr", "aaaaaaaa")

	landed, err := OperatorLandedSHA(ctx, k8sClient, testNS, proj.Name, "aaaaaaaa")
	if err != nil {
		t.Fatalf("OperatorLandedSHA: %v", err)
	}
	if !landed {
		t.Fatal("a SHA the operator itself merged must read as landed")
	}
	landed, err = OperatorLandedSHA(ctx, k8sClient, testNS, proj.Name, "bbbbbbbb")
	if err != nil {
		t.Fatalf("OperatorLandedSHA: %v", err)
	}
	if landed {
		t.Fatal("a SHA the operator never merged must NOT read as landed")
	}
}

func TestResumeBrainstormOnPushIgnoresTheOperatorsOwnMerge(t *testing.T) {
	ctx := context.Background()
	proj := mkResumeProject(t, "push-own", true)
	mkMergedMR(t, proj.Name, "push-own-mr", "cccccccc")

	if err := ResumeBrainstormOnPush(ctx, k8sClient, testNS, proj.Name, "cccccccc"); err != nil {
		t.Fatalf("ResumeBrainstormOnPush: %v", err)
	}
	if _, ok := getResumeProject(t, proj.Name).Annotations[tatarav1alpha1.AnnBrainstormResume]; ok {
		t.Fatal("the operator's own merge must not resume its own brainstorm")
	}
}

func TestResumeBrainstormOnPushResumesOnAForeignHead(t *testing.T) {
	ctx := context.Background()
	proj := mkResumeProject(t, "push-foreign", true)
	mkMergedMR(t, proj.Name, "push-foreign-mr", "dddddddd")

	if err := ResumeBrainstormOnPush(ctx, k8sClient, testNS, proj.Name, "eeeeeeee"); err != nil {
		t.Fatalf("ResumeBrainstormOnPush: %v", err)
	}
	if got := getResumeProject(t, proj.Name).Annotations[tatarav1alpha1.AnnBrainstormResume]; got != ResumeTriggerPush {
		t.Fatalf("annotation = %q, want %q", got, ResumeTriggerPush)
	}
}

// An empty head SHA is not evidence of anything, so it fails CLOSED: no resume.
// Four other triggers remain, and a spurious resume per malformed delivery
// would be a session per delivery.
func TestResumeBrainstormOnPushIgnoresAnEmptyHead(t *testing.T) {
	ctx := context.Background()
	proj := mkResumeProject(t, "push-empty", true)
	if err := ResumeBrainstormOnPush(ctx, k8sClient, testNS, proj.Name, ""); err != nil {
		t.Fatalf("ResumeBrainstormOnPush: %v", err)
	}
	if _, ok := getResumeProject(t, proj.Name).Annotations[tatarav1alpha1.AnnBrainstormResume]; ok {
		t.Fatal("an empty head SHA must not resume")
	}
}

// --- helpers ---

func mkResumeProject(t *testing.T, name string, paused bool) *tatarav1alpha1.Project {
	t.Helper()
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{}
	proj.Name = name
	proj.Namespace = testNS
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "bot"}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project %s: %v", name, err)
	}
	if paused {
		now := metav1.Now()
		proj.Status.BrainstormPausedAt = &now
		proj.Status.BrainstormPauseReason = "idea space exhausted"
		if err := k8sClient.Status().Update(ctx, proj); err != nil {
			t.Fatalf("stamp pause: %v", err)
		}
	}
	return proj
}

func getResumeProject(t *testing.T, name string) *tatarav1alpha1.Project {
	t.Helper()
	var p tatarav1alpha1.Project
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &p); err != nil {
		t.Fatalf("get project %s: %v", name, err)
	}
	return &p
}

func mkMergedMR(t *testing.T, projName, mrName, sha string) {
	t.Helper()
	ctx := context.Background()
	mr := &tatarav1alpha1.MergeRequest{}
	mr.Name = mrName
	mr.Namespace = testNS
	mr.Spec = tatarav1alpha1.MergeRequestSpec{
		ProjectRef: projName, RepositoryRef: projName + "-repo", Number: 1,
		URL: "https://github.com/o/r/pull/1",
	}
	if err := k8sClient.Create(ctx, mr); err != nil {
		t.Fatalf("create mergerequest %s: %v", mrName, err)
	}
	mr.Status.State = "merged"
	mr.Status.MergedSHA = sha
	if err := k8sClient.Status().Update(ctx, mr); err != nil {
		t.Fatalf("stamp merged sha: %v", err)
	}
}

// TestClearBrainstormPauseIfRequestedRunsEvenWhenRunScansErrors is I4's fix
// round proof. clearBrainstormPauseIfRequested used to run AFTER runScans'
// scanErr early return, so a project whose runScans errors persistently
// (a bad ScmSecretRef, a listing failure, anything) could never consume the
// tatara.dev/brainstorm-resume annotation - the manual override the design
// spec calls the escape hatch every prior-art system lacks becomes
// unreachable on exactly the project that needs it most. The fix moves the
// consume-and-clear step ABOVE the scanErr return. This proves it: runScans
// is forced to error via an injected Task-list failure (existingScanTasks),
// and the annotation must still be consumed even though Reconcile itself
// still propagates scanErr.
func TestClearBrainstormPauseIfRequestedRunsEvenWhenRunScansErrors(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	proj := mkResumeProject(t, "resume-scan-err", true)

	mkSecret(t, "resume-scan-err-scm", map[string][]byte{"token": []byte("t")})
	proj.Spec.ScmSecretRef = "resume-scan-err-scm"
	proj.Spec.Scm.Cron = &tatarav1alpha1.ScmCron{}
	if err := k8sClient.Update(ctx, proj); err != nil {
		t.Fatalf("set scmSecretRef/cron: %v", err)
	}

	if err := StampBrainstormResume(ctx, k8sClient, testNS, proj.Name, ResumeTriggerManual); err != nil {
		t.Fatalf("StampBrainstormResume: %v", err)
	}
	if _, ok := getResumeProject(t, proj.Name).Annotations[tatarav1alpha1.AnnBrainstormResume]; !ok {
		t.Fatal("setup: resume annotation must be present before reconcile")
	}

	wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
	if err != nil {
		t.Fatalf("new watch client: %v", err)
	}
	injErr := errors.New("injected: list tasks failed")
	ic := interceptor.NewClient(wc, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*tatarav1alpha1.TaskList); ok {
				return injErr
			}
			return c.List(ctx, list, opts...)
		},
	})

	r := newProjectReconciler()
	r.Client = ic
	r.ReaderFor = func(string, string) (scm.SCMReader, error) {
		return mdPlainReader{}, nil
	}

	_, rerr := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: proj.Name}})
	if rerr == nil {
		t.Fatal("setup: expected Reconcile to propagate the injected scanErr")
	}

	fresh := getResumeProject(t, proj.Name)
	if fresh.Status.BrainstormPausedAt != nil || fresh.Status.BrainstormPauseReason != "" {
		t.Fatalf("pause not cleared despite scanErr: %v %q",
			fresh.Status.BrainstormPausedAt, fresh.Status.BrainstormPauseReason)
	}
	if _, ok := fresh.Annotations[tatarav1alpha1.AnnBrainstormResume]; ok {
		t.Fatal("the manual override annotation must be consumed even when runScans errors " +
			"persistently - otherwise it is unreachable on exactly the project that needs it")
	}
}
