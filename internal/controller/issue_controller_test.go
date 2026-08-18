package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// mirrorWriter records the label writes the one-way projection makes.
type mirrorWriter struct {
	scm.SCMWriter
	added   []string
	removed []string
}

func (m *mirrorWriter) AddLabel(_ context.Context, _, _, label string) error {
	m.added = append(m.added, label)
	return nil
}

func (m *mirrorWriter) RemoveLabel(_ context.Context, _, _, label string) error {
	m.removed = append(m.removed, label)
	return nil
}

func scmSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "scm-secret", Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat")},
	}
}

// ownedIssue returns an Issue CR owned (controller=true) by task.
func ownedIssue(name string, number int, task *tatarav1alpha1.Task, status tatarav1alpha1.IssueStatus) *tatarav1alpha1.Issue {
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: "tatara-operator", Number: number, ProjectRef: "proj",
			URL: "https://github.com/szymonrychu/tatara-operator/issues/1",
		},
		Status: status,
	}
	if task != nil {
		yes := true
		iss.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: tatarav1alpha1.GroupVersion.String(),
			Kind:       "Task",
			Name:       task.Name,
			UID:        task.UID,
			Controller: &yes,
		}}
	}
	return iss
}

func newIssueReconciler(c client.Client, w scm.SCMWriter, rd scm.SCMReader) *IssueReconciler {
	r := &IssueReconciler{
		Client: c,
		SCMFor: func(string) (scm.SCMWriter, error) { return w, nil },
		SpillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller {
			return &mirrorSpiller{}
		},
	}
	if rd != nil {
		r.ReaderFor = func(string, string) (scm.SCMReader, error) { return rd, nil }
	}
	return r
}

func reconcileIssue(t *testing.T, r *IssueReconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: name},
	})
	if err != nil {
		t.Fatalf("reconcile issue %s: %v", name, err)
	}
	return res
}

// TestIssueControllerNeverWritesStatusFromLabel is fix 16, as a GENUINE FAULT
// INJECTION: a label appears on the mirror with no webhook and no citation check,
// and status.status MUST NOT move. Labels are a ONE-WAY PROJECTION of
// status.status (C.6). There is NO label -> status path at all. The only label
// READ anywhere in the control path is tatara-parked (B.4), and it decides COST,
// never AUTHORITY.
func TestIssueControllerNeverWritesStatusFromLabel(t *testing.T) {
	ctx := context.Background()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StateRefined, "")
	iss := ownedIssue(tatarav1alpha1.IssueName(repo.Name, 1), 1, task, tatarav1alpha1.IssueStatus{
		State:  "open",
		Status: "new",
	})
	c := newMirrorClient(t, proj, repo, task, iss, scmSecret())
	r := newIssueReconciler(c, &mirrorWriter{}, nil)

	// FAULT INJECTION: the approval label lands on the mirror (a forged label, a
	// human mislabel, a replayed webhook - the source does not matter).
	live := getIssueCR(t, c, iss.Name)
	live.Status.Labels = []string{"tatara-approved", "tatara-declined"}
	if err := c.Status().Update(ctx, live); err != nil {
		t.Fatalf("inject labels: %v", err)
	}

	reconcileIssue(t, r, iss.Name)

	got := getIssueCR(t, c, iss.Name)
	if got.Status.Status != "new" {
		t.Fatalf("a LABEL drove status.status to %q; labels are a one-way projection and are NEVER read to produce status", got.Status.Status)
	}
	if got.Status.Approval != nil {
		t.Fatalf("a LABEL produced approval evidence: %+v", got.Status.Approval)
	}
}

// TestIssueControllerRepairsZeroController is contract B.2 rule 5: an
// Issue/MergeRequest must NEVER have zero controller owners - it is worked by
// nobody and re-minted by nobody, because the sweep's orphan predicate sees an
// OWNED Issue.
func TestIssueControllerRepairsZeroController(t *testing.T) {
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StateRefined, "")
	task.UID = "task-uid"

	iss := ownedIssue(tatarav1alpha1.IssueName(repo.Name, 2), 2, nil, tatarav1alpha1.IssueStatus{State: "open"})
	no := false
	iss.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: tatarav1alpha1.GroupVersion.String(),
		Kind:       "Task",
		Name:       task.Name,
		UID:        task.UID,
		Controller: &no,
	}}

	c := newMirrorClient(t, proj, repo, task, iss, scmSecret())
	r := newIssueReconciler(c, &mirrorWriter{}, nil)
	reconcileIssue(t, r, iss.Name)

	got := getIssueCR(t, c, iss.Name)
	ctrlOwner := ""
	for _, o := range got.OwnerReferences {
		if o.Controller != nil && *o.Controller {
			ctrlOwner = o.Name
		}
	}
	if ctrlOwner != task.Name {
		t.Fatalf("zero-controller repair guard did not run: controller owner = %q, want %q", ctrlOwner, task.Name)
	}
}

// TestIssueControllerProjectsLabels asserts the ONE-WAY projection (C.6):
// status=approved -> +approvedLabel, status=rejected -> +declinedLabel,
// status=done -> labels stripped.
func TestIssueControllerProjectsLabels(t *testing.T) {
	tests := []struct {
		name        string
		status      string
		labels      []string // what the mirror says the forge currently carries
		wantAdded   []string
		wantRemoved []string
	}{
		{
			name: "approved projects the approved label", status: "approved",
			labels:    []string{"tatara-declined"},
			wantAdded: []string{"tatara-approved"}, wantRemoved: []string{"tatara-declined"},
		},
		{
			name: "rejected projects the declined label", status: "rejected",
			labels:    []string{"tatara-approved"},
			wantAdded: []string{"tatara-declined"}, wantRemoved: []string{"tatara-approved"},
		},
		{
			name: "done strips the labels", status: "done",
			labels:      []string{"tatara-approved", "tatara-declined"},
			wantRemoved: []string{"tatara-approved", "tatara-declined"},
		},
		{name: "new projects nothing", status: "new"},
		{
			// The label is already correct: the projection is idempotent and
			// issues NO forge write.
			name: "approved with the label already present writes nothing", status: "approved",
			labels: []string{"tatara-approved"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
			task := taskAtStage(tatarav1alpha1.StateRefined, "")
			iss := ownedIssue(tatarav1alpha1.IssueName(repo.Name, 3), 3, task, tatarav1alpha1.IssueStatus{
				State:  "open",
				Status: tc.status,
				Labels: tc.labels,
			})
			c := newMirrorClient(t, proj, repo, task, iss, scmSecret())
			w := &mirrorWriter{}
			reconcileIssue(t, newIssueReconciler(c, w, nil), iss.Name)

			if len(w.added) != len(tc.wantAdded) {
				t.Fatalf("added = %v, want %v", w.added, tc.wantAdded)
			}
			for i, label := range tc.wantAdded {
				if w.added[i] != label {
					t.Fatalf("added[%d] = %q, want %q", i, w.added[i], label)
				}
			}
			if len(w.removed) != len(tc.wantRemoved) {
				t.Fatalf("removed = %v, want %v", w.removed, tc.wantRemoved)
			}
		})
	}
}

// TestIssueControllerSyncsAtCadence asserts the B.4 cadence: an ACTIVE Task's
// Issues sync hourly; EVERY parked Task's Issues sync DAILY. A backlog issue
// nobody is working does not need an hourly re-read.
func TestIssueControllerSyncsAtCadence(t *testing.T) {
	tests := []struct {
		name        string
		stage       string
		reason      string
		lastSynced  time.Duration // age of status.lastSyncedAt
		wantRequeue time.Duration
		wantReads   int
	}{
		{
			name:  "active task, mirror fresh: no read, hourly requeue",
			stage: tatarav1alpha1.StateUnderImplementation, lastSynced: 10 * time.Minute,
			wantRequeue: MirrorCadenceActive, wantReads: 0,
		},
		{
			name:  "active task, mirror an hour stale: one read",
			stage: tatarav1alpha1.StateUnderImplementation, lastSynced: 90 * time.Minute,
			wantRequeue: MirrorCadenceActive, wantReads: 1,
		},
		{
			name:  "parked task, mirror an hour stale: NO read (daily cadence)",
			stage: tatarav1alpha1.StateNew, reason: "backlog-sweep", lastSynced: 90 * time.Minute,
			wantRequeue: MirrorCadenceParked, wantReads: 0,
		},
		{
			name:  "parked task, mirror a day stale: one read",
			stage: tatarav1alpha1.StateRefined, reason: "identity-unverified", lastSynced: 25 * time.Hour,
			wantRequeue: MirrorCadenceParked, wantReads: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
			task := taskAtStage(tc.stage, tc.reason)
			last := metav1.NewTime(time.Now().Add(-tc.lastSynced))
			iss := ownedIssue(tatarav1alpha1.IssueName(repo.Name, 4), 4, task, tatarav1alpha1.IssueStatus{
				State:        "open",
				LastSyncedAt: &last,
			})
			c := newMirrorClient(t, proj, repo, task, iss, scmSecret())
			rd := &mirrorReader{comments: []scm.IssueComment{
				{ExternalID: "5", Author: "szymonrychu", Body: "go ahead", CreatedAt: time.Now()},
			}}
			res := reconcileIssue(t, newIssueReconciler(c, &mirrorWriter{}, rd), iss.Name)

			if rd.calls != tc.wantReads {
				t.Fatalf("forge thread reads = %d, want %d", rd.calls, tc.wantReads)
			}
			if res.RequeueAfter != tc.wantRequeue {
				t.Fatalf("RequeueAfter = %v, want %v", res.RequeueAfter, tc.wantRequeue)
			}
		})
	}
}

// TestMergeRequestControllerReconciles asserts the MergeRequest reconciler runs
// the same repair guard and requeues at the same cadence. It writes NO label:
// the label vocabulary is an Issue-only projection.
func TestMergeRequestControllerReconciles(t *testing.T) {
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StateAwaitingReview, "")
	task.UID = "task-uid"

	no := false
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.MergeRequestName(repo.Name, 42),
			Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: tatarav1alpha1.GroupVersion.String(),
				Kind:       "Task",
				Name:       task.Name,
				UID:        task.UID,
				Controller: &no,
			}},
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo.Name, Number: 42, ProjectRef: proj.Name,
			URL: "https://github.com/szymonrychu/tatara-operator/pull/42",
		},
		Status: tatarav1alpha1.MergeRequestStatus{State: "open"},
	}
	c := newMirrorClient(t, proj, repo, task, mr, scmSecret())
	rd := &mirrorReader{prComments: []scm.IssueComment{
		{ExternalID: "9", Author: "tatara-bot", Body: "## Review: changes requested", CreatedAt: time.Now()},
	}}
	r := &MergeRequestReconciler{
		Client:     c,
		SpillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return &mirrorSpiller{} },
		ReaderFor:  func(string, string) (scm.SCMReader, error) { return rd, nil },
	}
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: mr.Name},
	})
	if err != nil {
		t.Fatalf("reconcile mr: %v", err)
	}
	// The repair guard above has just promoted `task` to CONTROLLER owner, and
	// awaiting-review is neither terminal nor parked, so this MR is one an agent
	// is actively working: it requeues on the tightened CI backstop cadence, not
	// the hourly mirror one. The THREAD sync still runs hourly - only the CI
	// re-read is tightened.
	if res.RequeueAfter != CIRefreshCadenceActive {
		t.Fatalf("RequeueAfter = %v, want %v", res.RequeueAfter, CIRefreshCadenceActive)
	}
	// Never synced -> the first reconcile syncs the thread.
	if rd.prCalls != 1 {
		t.Fatalf("PR thread reads = %d, want 1", rd.prCalls)
	}

	var got tatarav1alpha1.MergeRequest
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(mr), &got); err != nil {
		t.Fatalf("get mr: %v", err)
	}
	ctrlOwner := ""
	for _, o := range got.OwnerReferences {
		if o.Controller != nil && *o.Controller {
			ctrlOwner = o.Name
		}
	}
	if ctrlOwner != task.Name {
		t.Fatalf("zero-controller repair guard did not run on the MergeRequest: controller owner = %q", ctrlOwner)
	}
	if len(got.Status.Comments) != 1 || got.Status.CommentCount != 1 {
		t.Fatalf("mr thread not mirrored: %d comments (count %d)", len(got.Status.Comments), got.Status.CommentCount)
	}
	if got.Status.Status != "" {
		t.Fatalf("the reconciler wrote status.status = %q; only an ACCEPTED review outcome writes it", got.Status.Status)
	}
}

// TestIssueReconcile_MirrorReadPermanent404SkipsSyncAndContinues is the Issue
// half of tatara-operator#621. The MergeRequest side closes the mirror; this
// side deliberately does NOT, because Issue.Status.State="closed" drives
// handleIssueClosed and the WS3-I3 stop edge - a failed READ is not entitled to
// make a Task-lifecycle decision. It logs the read as terminal, skips the sync
// and lets the rest of the reconcile run.
func TestIssueReconcile_MirrorReadPermanent404SkipsSyncAndContinues(t *testing.T) {
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StateRefined, "")
	// status=approved so the label projection has real work to do: it is the LAST
	// step of doReconcile, and a label write is the only OBSERVABLE proof that the
	// reconcile continued instead of returning early with a cadence requeue - the
	// requeue value alone cannot tell the two apart.
	iss := ownedIssue(tatarav1alpha1.IssueName(repo.Name, 62), 62, task,
		tatarav1alpha1.IssueStatus{State: "open", Status: "approved"})
	c := newMirrorClient(t, proj, repo, task, iss, scmSecret())
	rd := &mirrorReader{commentsErr: &scm.HTTPError{
		Status: 410, Path: "/repos/szymonrychu/tatara-operator/issues/62/comments", Body: `{"message":"Gone"}`,
	}}
	w := &mirrorWriter{}

	res := reconcileIssue(t, newIssueReconciler(c, w, rd), iss.Name)

	if res.RequeueAfter != MirrorCadence(task) {
		t.Fatalf("RequeueAfter = %v, want the mirror cadence %v", res.RequeueAfter, MirrorCadence(task))
	}
	if len(w.added) == 0 {
		t.Fatal("the reconcile stopped at the failed read: the label projection never ran")
	}
	if rd.headSHAArgs != "szymonrychu|tatara-operator" {
		t.Fatalf("probe addressed %q, want szymonrychu|tatara-operator", rd.headSHAArgs)
	}
	if rd.headSHACalls != 1 {
		t.Fatalf("repo-readable probe calls = %d, want exactly 1", rd.headSHACalls)
	}
	got := getIssueCR(t, c, iss.Name)
	if got.Status.State != "open" {
		t.Fatalf("a failed READ must not move Issue.Status.State, got %q", got.Status.State)
	}
	// mirrorSyncDue (mirror.go:742) keys ONLY on LastSyncedAt: leaving it unstamped
	// means the read is due on EVERY reconcile, and since this reconcile returns
	// nil, controller-runtime's exponential backoff no longer rate-limits it
	// either. The stamp is what actually bounds the cost to once per cadence.
	// Status.State is deliberately still untouched (a failed READ is not entitled
	// to move it) - the stamp only says "the mirror sync ran", not "it succeeded".
	// Without it, any watch-triggered reconcile (a webhook comment append, the
	// sweep, /outcome) would pay ListIssueComments plus the repo probe every time.
	if got.Status.LastSyncedAt == nil {
		t.Fatal("LastSyncedAt was not stamped: the gone thread will be re-read on every reconcile")
	}
}

// TestIssueReconcile_GoneThreadStopsRereadingUntilTheNextCadence is the Issue
// twin of TestReconcile_MirrorReadGoneStopsRereadingUntilTheNextCadence: the
// SECOND reconcile must make no forge call at all, now that the stamp from the
// first reconcile has moved mirrorSyncDue to false.
func TestIssueReconcile_GoneThreadStopsRereadingUntilTheNextCadence(t *testing.T) {
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StateRefined, "")
	iss := ownedIssue(tatarav1alpha1.IssueName(repo.Name, 62), 62, task,
		tatarav1alpha1.IssueStatus{State: "open"})
	c := newMirrorClient(t, proj, repo, task, iss, scmSecret())
	rd := &mirrorReader{commentsErr: &scm.HTTPError{Status: 404, Path: "/x", Body: `{"message":"Not Found"}`}}
	r := newIssueReconciler(c, &mirrorWriter{}, rd)

	reconcileIssue(t, r, iss.Name)
	reconcileIssue(t, r, iss.Name)

	if rd.calls != 1 || rd.headSHACalls != 1 {
		t.Fatalf("second reconcile re-read the gone thread: calls=%d headSHACalls=%d, want 1 and 1",
			rd.calls, rd.headSHACalls)
	}
}

// TestIssueReconcile_GoneDrainIsVisibleInPrometheus: the drain guard swallows
// 404/410 out of listThreadComments, CloseIssue, Comment, AddLabel and
// EditIssue, and the drain returns at the FIRST failing intent, so every later
// intent on that Issue is head-of-line blocked with no ERROR line and no
// condition. The counter is the only signal left that a repo is silently
// refusing every forge write.
func TestIssueReconcile_GoneDrainIsVisibleInPrometheus(t *testing.T) {
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StateRefined, "")
	iss := ownedIssue(tatarav1alpha1.IssueName(repo.Name, 62), 62, task,
		tatarav1alpha1.IssueStatus{
			State:           "open",
			PendingComments: []tatarav1alpha1.PendingComment{{RequestID: "req-1", Body: "hello"}},
		})
	c := newMirrorClient(t, proj, repo, task, iss, scmSecret())
	gone := &scm.HTTPError{Status: 404, Path: "/x", Body: `{"message":"Not Found"}`}
	rd := &mirrorReader{commentsErr: gone}
	r := newIssueReconciler(c, &mirrorWriter{}, rd)
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.Driver = mdNewDriverWithReader(t, newFakeForge(t), c, rd)
	r.Driver.Metrics = m

	reconcileIssue(t, r, iss.Name)

	if got := testutil.ToFloat64(m.SCMRequestErrorByStatusCounter("github", "drain_pending_comments", "404")); got != 1 {
		t.Fatalf("operator_scm_request_errors_by_status_total{verb=\"drain_pending_comments\",status=\"404\"} = %v, want 1", got)
	}
}

// TestIssueReconcile_MirrorRead5xxPropagates: same call site, retryable status.
func TestIssueReconcile_MirrorRead5xxPropagates(t *testing.T) {
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StateRefined, "")
	iss := ownedIssue(tatarav1alpha1.IssueName(repo.Name, 62), 62, task,
		tatarav1alpha1.IssueStatus{State: "open"})
	c := newMirrorClient(t, proj, repo, task, iss, scmSecret())
	rd := &mirrorReader{commentsErr: &scm.HTTPError{Status: 500, Path: "/x", Body: "boom"}}
	r := newIssueReconciler(c, &mirrorWriter{}, rd)
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.Driver = &StageDriver{Client: c, Metrics: m}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: iss.Name},
	})
	if err == nil {
		t.Fatal("a 500 on the mirror read must propagate as a retryable error")
	}
	if rd.headSHACalls != 0 {
		t.Fatalf("the repo probe must run ONLY on the 404/410 path, got %d calls", rd.headSHACalls)
	}
	if got := testutil.ToFloat64(m.SCMRequestErrorByStatusCounter("github", "list_issue_comments", "500")); got != 1 {
		t.Fatalf("the Issue read path recorded no status series: %v, want 1", got)
	}
}

// TestIssueReconcile_GoneThreadDoesNotWedgeThePendingDrain: every arm of
// DrainPendingComments re-reads the thread to dedup its own marker, so on a
// permanently-gone issue the drain 404s exactly where the mirror sync did.
// Without a guard there, #621's loop survives the fix for any Issue carrying a
// pending intent - it just moves twenty lines down and costs an extra call.
func TestIssueReconcile_GoneThreadDoesNotWedgeThePendingDrain(t *testing.T) {
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StateRefined, "")
	iss := ownedIssue(tatarav1alpha1.IssueName(repo.Name, 62), 62, task,
		tatarav1alpha1.IssueStatus{
			State:           "open",
			PendingComments: []tatarav1alpha1.PendingComment{{RequestID: "req-1", Body: "hello"}},
		})
	c := newMirrorClient(t, proj, repo, task, iss, scmSecret())
	gone := &scm.HTTPError{Status: 404, Path: "/x", Body: `{"message":"Not Found"}`}
	rd := &mirrorReader{commentsErr: gone}
	r := newIssueReconciler(c, &mirrorWriter{}, rd)
	r.Driver = mdNewDriverWithReader(t, newFakeForge(t), c, rd)

	res := reconcileIssue(t, r, iss.Name)

	if res.RequeueAfter != MirrorCadence(task) {
		t.Fatalf("RequeueAfter = %v, want the mirror cadence %v", res.RequeueAfter, MirrorCadence(task))
	}
	// The intent is retried, never discarded: a failed forge READ is not entitled
	// to throw away an agent's durable intent.
	if got := getIssueCR(t, c, iss.Name); len(got.Status.PendingComments) != 1 {
		t.Fatalf("pending intents = %d, want the intent retained for the next cadence", len(got.Status.PendingComments))
	}
}

// TestProjectLabels_StrippingAHumanAddedLabelIsLogged: the mirror's labels are
// refreshed on the Issue cadence now (syncIssueThread), so this removal - which
// used to be all but unreachable, status.labels being frozen at mint - fires on
// a label a maintainer added by hand within one MirrorCadence. The projection
// stays strictly one-way and the label is stripped; what must not happen is it
// happening SILENTLY, with an outbound forge write contradicting a human action
// and nothing in the log to find afterwards.
func TestProjectLabels_StrippingAHumanAddedLabelIsLogged(t *testing.T) {
	ctx, entries := kvLoggingCtx()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StateRefined, "")
	iss := ownedIssue(tatarav1alpha1.IssueName(repo.Name, 1), 1, task, tatarav1alpha1.IssueStatus{
		State: "open", Status: "rejected",
		// A maintainer added tatara-approved on the forge and the cadence sync
		// mirrored it.
		Labels: []string{"tatara-approved", "tatara-declined"},
	})
	c := newMirrorClient(t, proj, repo, task, iss, scmSecret())
	w := &mirrorWriter{}
	r := newIssueReconciler(c, w, nil)

	if err := r.projectLabels(ctx, proj, repo, iss); err != nil {
		t.Fatalf("projectLabels: %v", err)
	}

	if len(w.removed) != 1 || w.removed[0] != "tatara-approved" {
		t.Fatalf("removed = %v, want [tatara-approved]: the projection is one-way", w.removed)
	}
	got := oneLoggedAction(t, *entries, "issue_label_stripped")
	if got.field("label") != "tatara-approved" {
		t.Fatalf("issue_label_stripped label = %v, want tatara-approved", got.field("label"))
	}
}
