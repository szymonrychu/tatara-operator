package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
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
// INJECTION: a label appears on the mirror with no webhook and no grammar run,
// and status.status MUST NOT move. Labels are a ONE-WAY PROJECTION of
// status.status (C.6). There is NO label -> status path at all. The only label
// READ anywhere in the control path is tatara-parked (B.4), and it decides COST,
// never AUTHORITY.
func TestIssueControllerNeverWritesStatusFromLabel(t *testing.T) {
	ctx := context.Background()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StageClarifying, "")
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
	task := taskAtStage(tatarav1alpha1.StageClarifying, "")
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
			task := taskAtStage(tatarav1alpha1.StageClarifying, "")
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
			stage: tatarav1alpha1.StageImplementing, lastSynced: 10 * time.Minute,
			wantRequeue: MirrorCadenceActive, wantReads: 0,
		},
		{
			name:  "active task, mirror an hour stale: one read",
			stage: tatarav1alpha1.StageImplementing, lastSynced: 90 * time.Minute,
			wantRequeue: MirrorCadenceActive, wantReads: 1,
		},
		{
			name:  "parked task, mirror an hour stale: NO read (daily cadence)",
			stage: tatarav1alpha1.StageParked, reason: "backlog-sweep", lastSynced: 90 * time.Minute,
			wantRequeue: MirrorCadenceParked, wantReads: 0,
		},
		{
			name:  "parked task, mirror a day stale: one read",
			stage: tatarav1alpha1.StageParked, reason: "identity-unverified", lastSynced: 25 * time.Hour,
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
	task := taskAtStage(tatarav1alpha1.StageReviewing, "")
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
	if res.RequeueAfter != MirrorCadenceActive {
		t.Fatalf("RequeueAfter = %v, want %v", res.RequeueAfter, MirrorCadenceActive)
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
