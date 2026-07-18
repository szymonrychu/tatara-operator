package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// sweepReader is the fake forge the sweep tests run against. Every method the
// sweep calls is served from these maps; everything else on scm.SCMReader is
// nil-embedded and panics if the sweep ever reaches for it (which it must not).
type sweepReader struct {
	scm.SCMReader
	issues   []scm.IssueRef
	prs      []scm.PRRef
	comments map[int][]scm.IssueComment
	content  map[int]scm.IssueContent

	issueCalls int
	prCalls    int
}

func (s *sweepReader) ListOpenIssues(context.Context, string, string) ([]scm.IssueRef, error) {
	s.issueCalls++
	return s.issues, nil
}

func (s *sweepReader) ListOpenPRs(context.Context, string, string) ([]scm.PRRef, error) {
	s.prCalls++
	return s.prs, nil
}

func (s *sweepReader) ListIssueComments(_ context.Context, _, _ string, number int) ([]scm.IssueComment, error) {
	return s.comments[number], nil
}

func (s *sweepReader) GetIssue(_ context.Context, _, _ string, number int) (scm.IssueContent, error) {
	if c, ok := s.content[number]; ok {
		return c, nil
	}
	return scm.IssueContent{}, nil
}

func sweepProject(name string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   testNS,
			Annotations: map[string]string{SweepAnnotation: SweepEnabledValue},
		},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef:        "scm-secret",
			MaxNewTasksPerSweep: 5,
			MaxOpenTasks:        6,
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github",
				BotLogin: "tatara-bot",
			},
		},
	}
}

func sweepRepo(proj string) *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "tatara-operator", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: proj,
			URL:        "https://github.com/szymonrychu/tatara-operator.git",
		},
	}
}

func humanComment(id, author, body string, at time.Time) scm.IssueComment {
	return scm.IssueComment{ExternalID: id, Author: author, Body: body, CreatedAt: at}
}

// runSweep drives one full SweepProject pass against the fake forge.
func runSweep(t *testing.T, c client.Client, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, rd scm.SCMReader) {
	t.Helper()
	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	if err := r.SweepProject(context.Background(), proj, rd, []tatarav1alpha1.Repository{*repo}, nil, SweepActivity); err != nil {
		t.Fatalf("SweepProject: %v", err)
	}
}

func sweepTasks(t *testing.T, c client.Client, proj string) []tatarav1alpha1.Task {
	t.Helper()
	var tl tatarav1alpha1.TaskList
	if err := c.List(context.Background(), &tl, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	out := make([]tatarav1alpha1.Task, 0, len(tl.Items))
	for i := range tl.Items {
		if tl.Items[i].Spec.ProjectRef == proj {
			out = append(out, tl.Items[i])
		}
	}
	return out
}

// TestOrphanIssuePredicate pins the THREE clauses of B.4's ONE orphan predicate.
// Clause (c) is the reporter intake gate (issue #102): v3 deleted it by
// omission, and its entire purpose is that an INJECTED issue never becomes a
// Task.
func TestOrphanIssuePredicate(t *testing.T) {
	proj := sweepProject("orphan-proj")
	repo := sweepRepo("orphan-proj")

	owned := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.IssueName(repo.Name, 1), Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: tatarav1alpha1.GroupVersion.String(), Kind: "Task",
				Name: "owner-task", UID: types.UID("u1"), Controller: boolp(true),
			}},
		},
	}
	ownerless := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.IssueName(repo.Name, 1), Namespace: testNS},
	}
	plainOnly := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.IssueName(repo.Name, 1), Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: tatarav1alpha1.GroupVersion.String(), Kind: "Task",
				Name: "plain-task", UID: types.UID("u2"),
			}},
		},
	}

	gated := sweepProject("orphan-proj")
	gated.Spec.Scm.ReporterLogins = []string{"alice"}

	tests := map[string]struct {
		proj *tatarav1alpha1.Project
		iss  scm.Issue
		cr   *tatarav1alpha1.Issue
		want bool
	}{
		"open, no CR, open allowlist": {
			proj: proj, iss: scm.Issue{Number: 1, State: "open", Author: "carol"}, want: true,
		},
		"clause a: closed on the forge": {
			proj: proj, iss: scm.Issue{Number: 1, State: "closed", Author: "carol"}, want: false,
		},
		"clause b: an Issue CR with a controller owner": {
			proj: proj, iss: scm.Issue{Number: 1, State: "open", Author: "carol"}, cr: owned, want: false,
		},
		"clause b: the ownerless survivor of a failed Task IS an orphan": {
			proj: proj, iss: scm.Issue{Number: 1, State: "open", Author: "carol"}, cr: ownerless, want: true,
		},
		"clause b: plain owners only is still an orphan": {
			proj: proj, iss: scm.Issue{Number: 1, State: "open", Author: "carol"}, cr: plainOnly, want: true,
		},
		"clause c: a non-allowlisted author mints NOTHING": {
			proj: gated, iss: scm.Issue{Number: 1, State: "open", Author: "mallory"}, want: false,
		},
		"clause c: an allowlisted author passes": {
			proj: gated, iss: scm.Issue{Number: 1, State: "open", Author: "alice"}, want: true,
		},
		"clause c: an empty author fails CLOSED under an active gate": {
			proj: gated, iss: scm.Issue{Number: 1, State: "open", Author: ""}, want: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := IsOrphanIssue(tc.proj, repo, tc.iss, tc.cr); got != tc.want {
				t.Fatalf("IsOrphanIssue = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestMintStage pins the TWO mint stages.
//
// The tatara-parked LABEL READ is safe where fix 16's forbidden one is not: it
// decides COST (do we spend a pod on this issue now?), never AUTHORITY (may this
// issue be implemented?). Forging the label buys an attacker a Task that stays
// PARKED - it fails SAFE. Forging an approval label would buy them prod. Do not
// generalise one rule into the other.
func TestMintStage(t *testing.T) {
	proj := sweepProject("mint-proj")
	t0 := time.Now().Add(-time.Hour)

	tests := map[string]struct {
		iss        scm.Issue
		webhook    bool
		wantStage  string
		wantReason string
	}{
		"webhook-originated mints ACTIVE": {
			iss:       scm.Issue{Number: 1, State: "open", Author: "alice"},
			webhook:   true,
			wantStage: tatarav1alpha1.StageTriaging,
		},
		"a human has the last word: ACTIVE": {
			iss: scm.Issue{Number: 1, State: "open", Author: "alice", Comments: []scm.IssueComment{
				humanComment("1", "tatara-bot", "on it", t0),
				humanComment("2", "alice", "any update?", t0.Add(time.Minute)),
			}},
			wantStage: tatarav1alpha1.StageTriaging,
		},
		"the bot has the last word: PARKED": {
			iss: scm.Issue{Number: 1, State: "open", Author: "alice", Comments: []scm.IssueComment{
				humanComment("1", "alice", "please fix", t0),
				humanComment("2", "tatara-bot", "parked", t0.Add(time.Minute)),
			}},
			wantStage:  tatarav1alpha1.StageParked,
			wantReason: stage.ReasonBacklogSweep,
		},
		"an untouched backlog issue: PARKED": {
			iss:        scm.Issue{Number: 1, State: "open", Author: "alice"},
			wantStage:  tatarav1alpha1.StageParked,
			wantReason: stage.ReasonBacklogSweep,
		},
		"tatara-parked beats a human last word: PARKED": {
			iss: scm.Issue{Number: 1, State: "open", Author: "alice", Labels: []string{TataraParkedLabel},
				Comments: []scm.IssueComment{humanComment("1", "alice", "ping", t0)}},
			wantStage:  tatarav1alpha1.StageParked,
			wantReason: stage.ReasonBacklogSweep,
		},
		// THE ORDERING. The label is checked BEFORE the marker, and that ordering IS
		// fix M25. The marker is DURABLE now (an annotation on the Issue CR), not an
		// in-process bool on the delivery being handled, so it can outlive the event
		// and meet a label the operator or a human stamped afterwards. If the marker
		// won, an uncleared one would re-open the M25 re-mint loop: mint ACTIVE ->
		// the pod re-triages -> it parks -> the reaper stamps tatara-parked -> the
		// sweep sees the marker again -> ACTIVE, forever. The label is the operator's
		// durable "this issue costs nothing" record and it is the OUTERMOST gate.
		"tatara-parked beats a WEBHOOK MARKER: PARKED": {
			iss:        scm.Issue{Number: 1, State: "open", Author: "alice", Labels: []string{TataraParkedLabel}},
			webhook:    true,
			wantStage:  tatarav1alpha1.StageParked,
			wantReason: stage.ReasonBacklogSweep,
		},
		"an empty comment author is never the bot": {
			iss: scm.Issue{Number: 1, State: "open", Author: "alice", Comments: []scm.IssueComment{
				humanComment("1", "", "deleted account", t0),
			}},
			wantStage: tatarav1alpha1.StageParked, wantReason: stage.ReasonBacklogSweep,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			gotStage, gotReason := MintStage(proj, tc.iss, tc.webhook)
			if gotStage != tc.wantStage || gotReason != tc.wantReason {
				t.Fatalf("MintStage = (%q, %q), want (%q, %q)", gotStage, gotReason, tc.wantStage, tc.wantReason)
			}
		})
	}
}

// TestSweepBacklogIssueMintsParkedTaskWithNoPod: a backlog issue mints a
// parked(backlog-sweep) Task that spawns NO pod and enqueues NO QueuedEvent.
func TestSweepBacklogIssueMintsParkedTaskWithNoPod(t *testing.T) {
	proj := sweepProject("backlog-proj")
	repo := sweepRepo("backlog-proj")
	c := newMirrorClient(t, proj, repo)
	rd := &sweepReader{
		issues:  []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 7, Author: "alice", Title: "slow query", State: "open"}},
		content: map[int]scm.IssueContent{7: {Title: "slow query", Body: "it is slow"}},
	}

	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	tk := tasks[0]
	// The mint sets the IMMUTABLE Spec.InitialStage (fix C5); Status.Stage is
	// applied later by the TaskReconciler create-edge, which this test does not
	// run.
	if tk.Spec.InitialStage != tatarav1alpha1.StageParked || tk.Spec.InitialStageReason != stage.ReasonBacklogSweep {
		t.Fatalf("initialStage = %q/%q, want parked/backlog-sweep", tk.Spec.InitialStage, tk.Spec.InitialStageReason)
	}
	if tk.Status.PodName != "" {
		t.Fatalf("parked(backlog-sweep) spawned a pod: %q", tk.Status.PodName)
	}
	var qel tatarav1alpha1.QueuedEventList
	if err := c.List(context.Background(), &qel, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list queuedevents: %v", err)
	}
	if len(qel.Items) != 0 {
		t.Fatalf("queuedevents = %d, want 0 (a parked(backlog-sweep) Task enqueues NOTHING)", len(qel.Items))
	}

	// The Task OWNS the Issue CR: on the next sweep the issue is no longer an
	// orphan, which is what breaks the re-mint loop (ownership, not a heuristic).
	var iss tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: testNS, Name: tatarav1alpha1.IssueName(repo.Name, 7)}, &iss); err != nil {
		t.Fatalf("get issue CR: %v", err)
	}
	ownerName, ok := own.ControllerOwner(&iss)
	if !ok || ownerName != tk.Name {
		t.Fatalf("issue controller owner = %q/%v, want %q", ownerName, ok, tk.Name)
	}
	if len(tk.Status.IssueRefs) != 1 || tk.Status.IssueRefs[0] != iss.Name {
		t.Fatalf("task issueRefs = %v, want [%s]", tk.Status.IssueRefs, iss.Name)
	}
}

// Contract K.1: operator_orphan_adopted_total increments once per orphan work
// item the sweep mints a Task for, by kind - the issue mint (mintTaskForIssue)
// and the PR mint (mintReviewTaskForPR) each carry their own kind label.
func TestSweepMintsFireOrphanAdopted(t *testing.T) {
	t.Run("orphan issue mint", func(t *testing.T) {
		proj := sweepProject("oa-issue-proj")
		repo := sweepRepo("oa-issue-proj")
		c := newMirrorClient(t, proj, repo)
		rd := &sweepReader{
			issues:  []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 9, Author: "alice", Title: "slow query", State: "open"}},
			content: map[int]scm.IssueContent{9: {Title: "slow query", Body: "it is slow"}},
		}
		reg := prometheus.NewRegistry()
		r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(reg)}
		if err := r.SweepProject(context.Background(), proj, rd, []tatarav1alpha1.Repository{*repo}, nil, SweepActivity); err != nil {
			t.Fatalf("SweepProject: %v", err)
		}
		if got := testutil.ToFloat64(r.Metrics.OrphanAdoptedCounter(SweepIssueKind)); got != 1 {
			t.Fatalf("operator_orphan_adopted_total{%s} = %v, want 1", SweepIssueKind, got)
		}
	})

	t.Run("orphan PR mint", func(t *testing.T) {
		base := "szymonrychu/tatara-operator"
		proj := sweepProject("oa-pr-proj")
		proj.Spec.TriggerLabel = "tatara"
		proj.Spec.Scm.PRReactionScope = "labeledOrMentioned"
		repo := sweepRepo("oa-pr-proj")
		c := newMirrorClient(t, proj, repo)
		rd := &sweepReader{prs: []scm.PRRef{{
			Repo: base, HeadRepo: base, Number: 31, Author: "contributor",
			HeadBranch: "feat/oa", Labels: []string{"tatara"},
		}}}
		reg := prometheus.NewRegistry()
		r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(reg)}
		if err := r.SweepProject(context.Background(), proj, rd, []tatarav1alpha1.Repository{*repo}, nil, SweepActivity); err != nil {
			t.Fatalf("SweepProject: %v", err)
		}
		if got := testutil.ToFloat64(r.Metrics.OrphanAdoptedCounter(SweepReviewKind)); got != 1 {
			t.Fatalf("operator_orphan_adopted_total{%s} = %v, want 1", SweepReviewKind, got)
		}
	})
}

// TestSweepHumanCommentMintsTriaging: a human comment on a backlog issue mints
// an ACTIVE Task.
func TestSweepHumanCommentMintsTriaging(t *testing.T) {
	proj := sweepProject("human-proj")
	repo := sweepRepo("human-proj")
	c := newMirrorClient(t, proj, repo)
	now := time.Now()
	rd := &sweepReader{
		issues:  []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 7, Author: "alice", State: "open"}},
		content: map[int]scm.IssueContent{7: {Title: "slow query", Body: "it is slow"}},
		comments: map[int][]scm.IssueComment{7: {
			humanComment("1", "tatara-bot", "triaged", now.Add(-time.Hour)),
			humanComment("2", "alice", "still broken", now),
		}},
	}

	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	if tasks[0].Spec.InitialStage != tatarav1alpha1.StageTriaging {
		t.Fatalf("initialStage = %q, want triaging", tasks[0].Spec.InitialStage)
	}
	if tasks[0].Spec.InitialStageReason != "" {
		t.Fatalf("initialStageReason = %q, want empty on an ACTIVE mint", tasks[0].Spec.InitialStageReason)
	}
}

// TestSweepUnauthorizedReporterMintsNoTask is clause (c) end to end: an open,
// unowned issue from a NON-allowlisted author mints NO Task; with an empty
// allowlist the same issue mints one.
func TestSweepUnauthorizedReporterMintsNoTask(t *testing.T) {
	repo := sweepRepo("gate-proj")
	injected := []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 9, Author: "mallory", State: "open"}}

	gated := sweepProject("gate-proj")
	gated.Spec.Scm.ReporterLogins = []string{"alice"}
	c := newMirrorClient(t, gated, repo)
	runSweep(t, c, gated, repo, &sweepReader{issues: injected})
	if n := len(sweepTasks(t, c, gated.Name)); n != 0 {
		t.Fatalf("tasks = %d, want 0 (an injected issue never becomes a Task)", n)
	}

	open := sweepProject("open-proj")
	repo2 := sweepRepo("open-proj")
	c2 := newMirrorClient(t, open, repo2)
	runSweep(t, c2, open, repo2, &sweepReader{issues: injected})
	if n := len(sweepTasks(t, c2, open.Name)); n != 1 {
		t.Fatalf("tasks = %d, want 1 (an empty allowlist preserves the open default)", n)
	}
}

// TestSweepReapLoopNeverGoesActive is THE LOOP TEST.
//
// The reaper collected a parked(identity-unverified) Task and stamped the SCM
// issue with tatara-parked. Its bot park COMMENT is not the last word (the M25
// scenario: a 403 on a secondary limit). Keying "active vs parked" on "does the
// bot have the last word" would mint ACTIVE here, the pod would re-triage, it
// would park again - the exact loop this exists to kill. The predicate reads
// TASK HISTORY (the durable label), not a comment.
//
// Three passes against the same fake forge: the Task count does not grow.
func TestSweepReapLoopNeverGoesActive(t *testing.T) {
	proj := sweepProject("loop-proj")
	repo := sweepRepo("loop-proj")
	// The ownerless survivor of the reaped Task: fix H13 drops the ownerRef, and
	// per B.1 a zero-owner object is NEVER garbage collected. It is still there.
	survivor := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.IssueName(repo.Name, 42), Namespace: testNS},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo.Name, Number: 42, ProjectRef: proj.Name,
			URL: "https://github.com/szymonrychu/tatara-operator/issues/42",
		},
	}
	c := newMirrorClient(t, proj, repo, survivor)
	now := time.Now()
	rd := &sweepReader{
		issues: []scm.IssueRef{{
			Repo: "szymonrychu/tatara-operator", Number: 42, Author: "alice", State: "open",
			Labels: []string{TataraParkedLabel},
		}},
		content: map[int]scm.IssueContent{42: {Title: "needs approval", Body: "please"}},
		comments: map[int][]scm.IssueComment{42: {
			humanComment("1", "tatara-bot", "I cannot verify the approver", now.Add(-2*time.Hour)),
			humanComment("2", "alice", "go ahead", now.Add(-time.Hour)),
		}},
	}

	for pass := 1; pass <= 3; pass++ {
		runSweep(t, c, proj, repo, rd)
		tasks := sweepTasks(t, c, proj.Name)
		if len(tasks) != 1 {
			t.Fatalf("pass %d: tasks = %d, want 1 (the sweep must not re-mint an owned issue)", pass, len(tasks))
		}
		tk := tasks[0]
		if tk.Spec.InitialStage != tatarav1alpha1.StageParked || tk.Spec.InitialStageReason != stage.ReasonBacklogSweep {
			t.Fatalf("pass %d: initialStage = %q/%q, want parked/backlog-sweep (NEVER active)",
				pass, tk.Spec.InitialStage, tk.Spec.InitialStageReason)
		}
	}

	// The mint ADOPTED the ownerless survivor: a blind Create would have
	// AlreadyExists'd on every re-mint of every previously-failed Task.
	var iss tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: testNS, Name: tatarav1alpha1.IssueName(repo.Name, 42)}, &iss); err != nil {
		t.Fatalf("get issue CR: %v", err)
	}
	if _, ok := own.ControllerOwner(&iss); !ok {
		t.Fatal("the adopted Issue CR has no controller owner")
	}
	if iss.Status.Title != "needs approval" {
		t.Fatalf("issue mirror title = %q, want the synced forge title", iss.Status.Title)
	}
}

// TestMintAdoptsOwnerlessIssueCR: the mint is ADOPT-OR-CREATE on the Issue CR,
// never a blind Create. A failed Task RELEASES its controller-ownership and
// drops the ownerRef, leaving a zero-owner Issue CR that is never GC'd.
func TestMintAdoptsOwnerlessIssueCR(t *testing.T) {
	proj := sweepProject("adopt-proj")
	repo := sweepRepo("adopt-proj")
	survivor := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.IssueName(repo.Name, 3), Namespace: testNS},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo.Name, Number: 3, ProjectRef: proj.Name,
			URL: "https://github.com/szymonrychu/tatara-operator/issues/3",
		},
	}
	survivor.Status.Comments = []tatarav1alpha1.Comment{{
		ExternalID: "old", Author: "alice", Body: "the original report",
		CreatedAt: metav1.NewTime(time.Now().Add(-48 * time.Hour)),
	}}
	c := newMirrorClient(t, proj, repo, survivor)
	rd := &sweepReader{
		issues:  []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 3, Author: "alice", State: "open"}},
		content: map[int]scm.IssueContent{3: {Title: "t", Body: "b"}},
	}

	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	var iss tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: testNS, Name: survivor.Name}, &iss); err != nil {
		t.Fatalf("get issue CR: %v", err)
	}
	owner, ok := own.ControllerOwner(&iss)
	if !ok || owner != tasks[0].Name {
		t.Fatalf("controller owner = %q/%v, want %q", owner, ok, tasks[0].Name)
	}
	if len(iss.Status.Comments) != 1 || iss.Status.Comments[0].ExternalID != "old" {
		t.Fatalf("the adopted CR lost its mirrored thread: %+v", iss.Status.Comments)
	}
}

// TestAdoptPR pins the THREE adoption clauses. Clause (c) is what stops an
// outside contributor from injecting an MR into a trusted Task's merge stream: a
// fork PR may name its head branch anything, INCLUDING task/<a-real-task>.
func TestAdoptPR(t *testing.T) {
	proj := sweepProject("adopt-pr-proj")
	task := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "t-1", Namespace: testNS}}
	base := "szymonrychu/tatara-operator"

	tests := map[string]struct {
		pr   scm.PRRef
		task *tatarav1alpha1.Task
		want bool
	}{
		"bot, task branch, same repo": {
			pr:   scm.PRRef{Repo: base, HeadRepo: base, Author: "tatara-bot", HeadBranch: "task/t-1"},
			task: task, want: true,
		},
		"clause a: a human on the Task's own branch is NOT adopted": {
			pr:   scm.PRRef{Repo: base, HeadRepo: base, Author: "mallory", HeadBranch: "task/t-1"},
			task: task, want: false,
		},
		"clause b: the bot on some other branch is NOT adopted": {
			pr:   scm.PRRef{Repo: base, HeadRepo: base, Author: "tatara-bot", HeadBranch: "chore/bump"},
			task: task, want: false,
		},
		"clause c: a FORK PR on a task/* branch is NOT adopted": {
			pr:   scm.PRRef{Repo: base, HeadRepo: "mallory/tatara-operator", Author: "tatara-bot", HeadBranch: "task/t-1"},
			task: task, want: false,
		},
		"clause c: an UNKNOWN head repo fails CLOSED": {
			pr:   scm.PRRef{Repo: base, HeadRepo: "", Author: "tatara-bot", HeadBranch: "task/t-1"},
			task: task, want: false,
		},
		"no owning Task": {
			pr:   scm.PRRef{Repo: base, HeadRepo: base, Author: "tatara-bot", HeadBranch: "task/t-1"},
			task: nil, want: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := AdoptPR(proj, tc.task, tc.pr); got != tc.want {
				t.Fatalf("AdoptPR = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSweepAdoptsBotPRIntoOwningTask: a bot PR on the Task's own branch is
// adopted into that Task's mrRefs and the MergeRequest CR is owned by it.
func TestSweepAdoptsBotPRIntoOwningTask(t *testing.T) {
	proj := sweepProject("pradopt-proj")
	repo := sweepRepo("pradopt-proj")
	owner := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "pradopt-proj-clarify-x", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g"},
	}
	owner.Status.Stage = tatarav1alpha1.StageImplementing
	c := newMirrorClient(t, proj, repo, owner)
	rd := &sweepReader{prs: []scm.PRRef{{
		Repo: "szymonrychu/tatara-operator", HeadRepo: "szymonrychu/tatara-operator",
		Number: 11, Author: "tatara-bot", HeadBranch: "task/" + owner.Name, HeadSHA: "deadbeef",
	}}}

	runSweep(t, c, proj, repo, rd)

	if n := len(sweepTasks(t, c, proj.Name)); n != 1 {
		t.Fatalf("tasks = %d, want 1 (adoption mints NOTHING)", n)
	}
	var mr tatarav1alpha1.MergeRequest
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: testNS, Name: tatarav1alpha1.MergeRequestName(repo.Name, 11)}, &mr); err != nil {
		t.Fatalf("get mergerequest CR: %v", err)
	}
	if got, ok := own.ControllerOwner(&mr); !ok || got != owner.Name {
		t.Fatalf("MR controller owner = %q/%v, want %q", got, ok, owner.Name)
	}
	var fresh tatarav1alpha1.Task
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: owner.Name}, &fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if len(fresh.Status.MRRefs) != 1 || fresh.Status.MRRefs[0] != mr.Name {
		t.Fatalf("task mrRefs = %v, want [%s]", fresh.Status.MRRefs, mr.Name)
	}
}

// TestSweepForkPROnTaskBranchIsNotAdopted: end to end, a fork PR naming a real
// Task's branch is neither adopted nor turned into anything else.
func TestSweepForkPROnTaskBranchIsNotAdopted(t *testing.T) {
	proj := sweepProject("fork-proj")
	repo := sweepRepo("fork-proj")
	owner := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "fork-proj-clarify-y", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g"},
	}
	owner.Status.Stage = tatarav1alpha1.StageImplementing
	c := newMirrorClient(t, proj, repo, owner)
	rd := &sweepReader{prs: []scm.PRRef{{
		Repo: "szymonrychu/tatara-operator", HeadRepo: "mallory/tatara-operator",
		Number: 12, Author: "tatara-bot", HeadBranch: "task/" + owner.Name,
	}}}

	runSweep(t, c, proj, repo, rd)

	var fresh tatarav1alpha1.Task
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: owner.Name}, &fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if len(fresh.Status.MRRefs) != 0 {
		t.Fatalf("a FORK PR was injected into a trusted Task's merge stream: %v", fresh.Status.MRRefs)
	}
	if n := len(sweepTasks(t, c, proj.Name)); n != 1 {
		t.Fatalf("tasks = %d, want 1 (the fork PR minted something)", n)
	}
}

// TestSweepBotPRNotAdoptableIsIgnored is clause 2: BOT-AUTHORED and NOT
// ADOPTABLE -> IGNORE. FULL STOP. No Task, no pod, no tokens, NEVER a review
// Task. prInReactionScope does NOT close this hole: it returns true immediately
// for IsTrustedAuthor, and the bot IS a trusted author.
//
// Two real populations: (a) ORPHANED AGENT PRs, whose review Task the flaky
// review agent approves - and the author check PASSES, because the author IS the
// bot - shipping an abandoned, never-approved change through push-CD; and (b) CI
// PIN-BUMP PRs (tatara-helmfile's cd-release bump PR on every release, the
// wrapper's daily refresh-claude-code PR), each of which would eat the
// maxOpenTasks budget and RACE GitHub's own auto-merge.
func TestSweepBotPRNotAdoptableIsIgnored(t *testing.T) {
	tests := map[string]scm.PRRef{
		"an orphaned agent PR whose Task is gone": {
			Repo: "szymonrychu/tatara-operator", HeadRepo: "szymonrychu/tatara-operator",
			Number: 20, Author: "tatara-bot", HeadBranch: "task/a-task-that-no-longer-exists",
		},
		"a CI pin-bump PR": {
			Repo: "szymonrychu/tatara-operator", HeadRepo: "szymonrychu/tatara-operator",
			Number: 21, Author: "tatara-bot", HeadBranch: "chore/bump-tatara-cli-v1.2.3",
			Body: "Automated pin bump", Labels: []string{"semver:patch"},
		},
	}
	for name, pr := range tests {
		t.Run(name, func(t *testing.T) {
			proj := sweepProject("botpr-proj")
			repo := sweepRepo("botpr-proj")
			c := newMirrorClient(t, proj, repo)

			runSweep(t, c, proj, repo, &sweepReader{prs: []scm.PRRef{pr}})

			if n := len(sweepTasks(t, c, proj.Name)); n != 0 {
				t.Fatalf("tasks = %d, want 0 (a bot-authored non-adoptable PR is IGNORED, FULL STOP)", n)
			}
			// Not touched at all: no MergeRequest CR, so nothing owns it, nothing
			// reviews it, nothing merges it, and nothing races the forge's own
			// auto-merge on it.
			var mrl tatarav1alpha1.MergeRequestList
			if err := c.List(context.Background(), &mrl, client.InNamespace(testNS)); err != nil {
				t.Fatalf("list mergerequests: %v", err)
			}
			if len(mrl.Items) != 0 {
				t.Fatalf("mergerequest CRs = %d, want 0", len(mrl.Items))
			}
		})
	}
}

// TestSweepHumanPRReactionScope: a HUMAN-authored PR mints a review-kind Task
// iff prInReactionScope. The predicate's doc-comment names the incident it was
// written for - the !1090 token-burn loop, where the bot re-reviewed every
// unlabeled, un-mentioned MR on every scan cycle.
func TestSweepHumanPRReactionScope(t *testing.T) {
	base := "szymonrychu/tatara-operator"
	tests := map[string]struct {
		labels    []string
		body      string
		wantTasks int
	}{
		"outside the reaction scope: NO review Task": {wantTasks: 0},
		"carries the trigger label":                  {labels: []string{"tatara"}, wantTasks: 1},
		"@-mentions the bot":                         {body: "@tatara-bot please look", wantTasks: 1},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			proj := sweepProject("scope-proj")
			proj.Spec.TriggerLabel = "tatara"
			proj.Spec.Scm.PRReactionScope = "labeledOrMentioned"
			repo := sweepRepo("scope-proj")
			c := newMirrorClient(t, proj, repo)
			rd := &sweepReader{prs: []scm.PRRef{{
				Repo: base, HeadRepo: base, Number: 30, Author: "contributor",
				HeadBranch: "feat/x", Labels: tc.labels, Body: tc.body,
			}}}

			runSweep(t, c, proj, repo, rd)

			tasks := sweepTasks(t, c, proj.Name)
			if len(tasks) != tc.wantTasks {
				t.Fatalf("tasks = %d, want %d", len(tasks), tc.wantTasks)
			}
			if tc.wantTasks == 1 {
				if tasks[0].Spec.Kind != SweepReviewKind {
					t.Fatalf("kind = %q, want review", tasks[0].Spec.Kind)
				}
				if tasks[0].Spec.InitialStage != tatarav1alpha1.StageTriaging {
					t.Fatalf("initialStage = %q, want triaging", tasks[0].Spec.InitialStage)
				}
				if len(tasks[0].Status.MRRefs) != 1 {
					t.Fatalf("mrRefs = %v, want the reviewed MR", tasks[0].Status.MRRefs)
				}
			}
		})
	}
}

// ============================================================================
// The MERGEREQUEST orphan / re-mint gap. The reaper now leaves a human's
// still-open PR its mirror (OWNERLESS, so it is never GC'd and IS re-mintable);
// these are the sweep's half of that contract.
// ============================================================================

// TestMintReviewStage pins the re-mint disposition, and it turns on ONE
// question: HAS A REVIEW ALREADY BEEN POSTED ON THIS PR?
//
// status.status is OPERATOR-OWNED and written only when a review LANDED on the
// forge (C.5.3 clearPendingReview), so it is the durable record of "we already
// said our piece". "new" is the head-moved reset: a fresh review IS owed.
func TestMintReviewStage(t *testing.T) {
	withStatus := func(s string) *tatarav1alpha1.MergeRequest {
		mr := &tatarav1alpha1.MergeRequest{}
		mr.Status.Status = s
		return mr
	}
	tests := map[string]struct {
		cr         *tatarav1alpha1.MergeRequest
		wantStage  string
		wantReason string
	}{
		"no CR at all: a PR we have never seen mints ACTIVE": {
			cr: nil, wantStage: tatarav1alpha1.StageTriaging,
		},
		"a CR with no posted verdict: the review never landed, so RUN it": {
			cr: withStatus(""), wantStage: tatarav1alpha1.StageTriaging,
		},
		"status=new: the head MOVED and a FRESH review is owed": {
			cr: withStatus("new"), wantStage: tatarav1alpha1.StageTriaging,
		},
		"status=needs-changes: the review WAS posted -> PARKED, no re-review": {
			cr: withStatus("needs-changes"), wantStage: tatarav1alpha1.StageParked,
			wantReason: stage.ReasonAwaitingHuman,
		},
		"status=approved: the review WAS posted -> PARKED, no re-review": {
			cr: withStatus("approved"), wantStage: tatarav1alpha1.StageParked,
			wantReason: stage.ReasonAwaitingHuman,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			gotStage, gotReason := MintReviewStage(tc.cr)
			if gotStage != tc.wantStage || gotReason != tc.wantReason {
				t.Fatalf("MintReviewStage = (%q, %q), want (%q, %q)",
					gotStage, gotReason, tc.wantStage, tc.wantReason)
			}
		})
	}
}

// humanPRMirror is the ownerless MergeRequest CR the reaper leaves behind: the
// human's PR is still OPEN on the forge and a review has already been posted on
// it.
func humanPRMirror(proj, repo string, number int, rounds string) *tatarav1alpha1.MergeRequest {
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.MergeRequestName(repo, number), Namespace: testNS,
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo, Number: number, ProjectRef: proj,
			URL: "https://github.com/szymonrychu/tatara-operator/pull/50",
		},
	}
	if rounds != "" {
		mr.Annotations = map[string]string{AnnHumanReviewRounds: rounds}
	}
	mr.Status.Author = "contributor"
	mr.Status.HeadBranch = "fix/their-branch"
	mr.Status.State = "open"
	mr.Status.Status = "needs-changes"
	mr.Status.ReviewRounds = 2
	mr.Status.Comments = []tatarav1alpha1.Comment{{
		ExternalID: "old-review", Author: "tatara-bot", Body: "## Review: needs-changes",
		CreatedAt: metav1.NewTime(time.Now().Add(-8 * 24 * time.Hour)),
	}}
	return mr
}

func humanPR(number int) scm.PRRef {
	base := "szymonrychu/tatara-operator"
	return scm.PRRef{
		Repo: base, HeadRepo: "contributor/tatara-operator", Number: number,
		Author: "contributor", HeadBranch: "fix/their-branch", HeadSHA: "cafe",
	}
}

// TestSweepAdoptsOwnerlessMergeRequestCR is the MR analogue of
// TestMintAdoptsOwnerlessIssueCR: the re-mint ADOPTS the surviving mirror rather
// than creating a duplicate, and it comes back PARKED - a review was already
// posted on this PR, so re-running one would just re-post it.
func TestSweepAdoptsOwnerlessMergeRequestCR(t *testing.T) {
	proj := sweepProject("mradopt-proj")
	repo := sweepRepo("mradopt-proj")
	survivor := humanPRMirror(proj.Name, repo.Name, 50, "2")
	c := newMirrorClient(t, proj, repo, survivor)

	runSweep(t, c, proj, repo, &sweepReader{prs: []scm.PRRef{humanPR(50)}})

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	tk := tasks[0]
	if tk.Spec.Kind != SweepReviewKind {
		t.Fatalf("kind = %q, want review", tk.Spec.Kind)
	}
	if tk.Spec.InitialStage != tatarav1alpha1.StageParked || tk.Spec.InitialStageReason != stage.ReasonAwaitingHuman {
		t.Fatalf("initialStage = %q/%q, want parked/awaiting-human (the review was ALREADY posted)",
			tk.Spec.InitialStage, tk.Spec.InitialStageReason)
	}
	if tk.Status.PodName != "" {
		t.Fatalf("the re-minted review Task spawned a pod: %q", tk.Status.PodName)
	}
	// The V7-9 counter survived the reap: 3 rounds left, not 5.
	if tk.Status.HumanReviewRounds != 2 {
		t.Fatalf("humanReviewRounds = %d, want 2 carried from the surviving mirror", tk.Status.HumanReviewRounds)
	}

	// ADOPT, never a blind Create: ONE MergeRequest CR, and it is the SAME one -
	// the mirrored review thread is still on it.
	var mrl tatarav1alpha1.MergeRequestList
	if err := c.List(context.Background(), &mrl, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list mergerequests: %v", err)
	}
	if len(mrl.Items) != 1 {
		t.Fatalf("mergerequest CRs = %d, want exactly 1 (the mint DOUBLE-MINTED)", len(mrl.Items))
	}
	got := mrl.Items[0]
	owner, ok := own.ControllerOwner(&got)
	if !ok || owner != tk.Name {
		t.Fatalf("the adopted MR CR's controller owner = %q/%v, want %q", owner, ok, tk.Name)
	}
	if len(got.Status.Comments) != 1 || got.Status.Comments[0].ExternalID != "old-review" {
		t.Fatalf("the adopted CR lost its mirrored review thread: %+v", got.Status.Comments)
	}
	if len(tk.Status.MRRefs) != 1 || tk.Status.MRRefs[0] != got.Name {
		t.Fatalf("task mrRefs = %v, want [%s]", tk.Status.MRRefs, got.Name)
	}
}

// TestSweepNeverReReviewsAHumanPR IS THE LOOP TEST, and it is the MergeRequest
// twin of TestSweepReapLoopNeverGoesActive.
//
// The cycle it must NOT create: a contributor opens a PR, the review agent
// requests changes, the Task parks at awaiting-human, seven days pass, B.6 reaps
// the Task, the sweep re-mints - and the new review pod RE-POSTS the same review
// on a PR nobody touched. Every seven days. Forever.
//
// Three full reap -> sweep cycles against the same fake forge. The Task count
// never grows, the MergeRequest CR is never duplicated, no minted Task is ever
// ACTIVE, and the forge is never written to (the reaper's writer panics on every
// method).
func TestSweepNeverReReviewsAHumanPR(t *testing.T) {
	ctx := context.Background()
	proj := sweepProject("noreloop")
	proj.Spec.Scm.BotLogin = "tatara-bot"
	repo := sweepRepo("noreloop")
	survivor := humanPRMirror(proj.Name, repo.Name, 50, "2")
	c := newMirrorClient(t, proj, repo, reapSecret(), survivor)

	w := &reapWriter{} // ZERO forge writes across every cycle
	r := &ProjectReconciler{
		Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor: func(string) (scm.SCMWriter, error) { return w, nil },
	}
	rd := &sweepReader{prs: []scm.PRRef{humanPR(50)}}

	for cycle := 1; cycle <= 3; cycle++ {
		if err := r.SweepProject(ctx, proj, rd, []tatarav1alpha1.Repository{*repo}, nil, SweepActivity); err != nil {
			t.Fatalf("cycle %d: SweepProject: %v", cycle, err)
		}
		tasks := sweepTasks(t, c, proj.Name)
		if len(tasks) != 1 {
			t.Fatalf("cycle %d: tasks = %d, want 1 (the sweep re-minted on top of an owned PR)", cycle, len(tasks))
		}
		tk := tasks[0]
		if tk.Spec.InitialStage != tatarav1alpha1.StageParked || tk.Spec.InitialStageReason != stage.ReasonAwaitingHuman {
			t.Fatalf("cycle %d: initialStage = %q/%q, want parked/awaiting-human: a re-minted review Task must NEVER be ACTIVE - it would re-post the review",
				cycle, tk.Spec.InitialStage, tk.Spec.InitialStageReason)
		}
		// Project what the create-edge would apply from Spec.InitialStage (fix C5:
		// the mint itself never stamps Status.Stage) and check THAT is not active -
		// the exact question this assertion has always asked.
		projected := tk.DeepCopy()
		projected.Status.Stage = tk.Spec.InitialStage
		if StageActive(projected) {
			t.Fatalf("cycle %d: the re-minted review Task is ACTIVE; it will spawn a review pod on a PR nobody touched", cycle)
		}
		if tk.Status.HumanReviewRounds != 2 {
			t.Fatalf("cycle %d: humanReviewRounds = %d, want 2: the V7-9 cap must survive the reap, or the PR gets 5 MORE review pods every 7 days",
				cycle, tk.Status.HumanReviewRounds)
		}

		// A second sweep pass inside the same cycle must be a total no-op: the PR is
		// now OWNED, so it is not an orphan and it mints NOTHING.
		if err := r.SweepProject(ctx, proj, rd, []tatarav1alpha1.Repository{*repo}, nil, SweepActivity); err != nil {
			t.Fatalf("cycle %d: SweepProject (second pass): %v", cycle, err)
		}
		if n := len(sweepTasks(t, c, proj.Name)); n != 1 {
			t.Fatalf("cycle %d: tasks after the second pass = %d, want 1 (an OWNED PR is not an orphan)", cycle, n)
		}

		// Drive the create-edge (fix C5) so the reaper - which reads Status.Stage,
		// not Spec.InitialStage - sees this review Task as parked before it ages
		// out. In production the reconciler applies Spec.InitialStage long before
		// seven days pass; this mirrors that sequencing.
		live := getSweepTask(t, c, tk.Name)
		tr := &TaskReconciler{Client: c, Metrics: r.Metrics}
		if _, err := tr.reconcileStage(ctx, proj, live, time.Now()); err != nil {
			t.Fatalf("cycle %d: drive create-edge: %v", cycle, err)
		}

		// Seven days pass. B.6 reaps the park.
		aged := getSweepTask(t, c, tk.Name)
		rewound := metav1.NewTime(time.Now().Add(-8 * 24 * time.Hour))
		aged.Status.StageEnteredAt = &rewound
		if err := c.Status().Update(ctx, aged); err != nil {
			t.Fatalf("cycle %d: rewind stageEnteredAt: %v", cycle, err)
		}
		if err := r.ReapTerminal(ctx, proj); err != nil {
			t.Fatalf("cycle %d: ReapTerminal: %v", cycle, err)
		}
		if n := len(sweepTasks(t, c, proj.Name)); n != 0 {
			t.Fatalf("cycle %d: tasks after the reap = %d, want 0", cycle, n)
		}
	}

	// ZERO forge writes, in every cycle. The human's PR was never closed, never
	// commented on, never re-reviewed, and their branch was never deleted.
	if len(w.closed) != 0 || len(w.deleted) != 0 || len(w.comments) != 0 || len(w.labels) != 0 {
		t.Fatalf("the forge was written to: closed=%v deleted=%v comments=%v labels=%v",
			w.closed, w.deleted, w.comments, w.labels)
	}
	// And EXACTLY ONE MergeRequest CR survived all three cycles.
	var mrl tatarav1alpha1.MergeRequestList
	if err := c.List(ctx, &mrl, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list mergerequests: %v", err)
	}
	if len(mrl.Items) != 1 {
		t.Fatalf("mergerequest CRs = %d, want exactly 1 after three reap/sweep cycles", len(mrl.Items))
	}
}

// TestSweepDoesNotReMintOverAnOwnedHumanPR is the MR analogue of IsOrphanIssue's
// clause (b), and it closes a bug that fired on EVERY hourly pass: a human's PR
// never has a task/<name> head branch, so taskForBranch always returns nil for
// it, and ClassifyPR looked at nothing else. A PR under ACTIVE review therefore
// re-classified as PRReview on the very next pass - minting a second review Task
// whose ownMergeRequest then failed ("already has controller owner"), leaving a
// stage-less junk Task behind AND failing the whole sweep pass (which suppresses
// the heartbeat gauge the sweep-stalled alert reads).
func TestSweepDoesNotReMintOverAnOwnedHumanPR(t *testing.T) {
	proj := sweepProject("owned-pr-proj")
	repo := sweepRepo("owned-pr-proj")
	c := newMirrorClient(t, proj, repo)
	rd := &sweepReader{prs: []scm.PRRef{humanPR(60)}}

	// Pass 1 mints the review Task and takes ownership of the MR CR.
	runSweep(t, c, proj, repo, rd)
	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 || tasks[0].Spec.InitialStage != tatarav1alpha1.StageTriaging {
		t.Fatalf("pass 1: tasks = %+v, want one triaging review Task", tasks)
	}

	// Passes 2 and 3 mint NOTHING and must not error: the MR CR now has a
	// controller owner, so the PR is not an orphan.
	for pass := 2; pass <= 3; pass++ {
		runSweep(t, c, proj, repo, rd)
		if n := len(sweepTasks(t, c, proj.Name)); n != 1 {
			t.Fatalf("pass %d: tasks = %d, want 1 (the sweep re-minted over a PR it is already reviewing)", pass, n)
		}
		var mrl tatarav1alpha1.MergeRequestList
		if err := c.List(context.Background(), &mrl, client.InNamespace(testNS)); err != nil {
			t.Fatalf("list mergerequests: %v", err)
		}
		if len(mrl.Items) != 1 {
			t.Fatalf("pass %d: mergerequest CRs = %d, want 1", pass, len(mrl.Items))
		}
	}
}

func getSweepTask(t *testing.T, c client.Client, name string) *tatarav1alpha1.Task {
	t.Helper()
	var tk tatarav1alpha1.Task
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &tk); err != nil {
		t.Fatalf("get task %s: %v", name, err)
	}
	return &tk
}

// TestSweepMintCapsBind: both creation budgets bind. maxNewTasksPerSweep caps
// the pass; maxOpenTasks caps ACTIVE Tasks, and parked(backlog-sweep) Tasks are
// NOT active so they do not count against it. Remaining orphans are minted on
// the next pass - the predicate is stateless, nothing is lost.
func TestSweepMintCapsBind(t *testing.T) {
	t.Run("maxNewTasksPerSweep", func(t *testing.T) {
		proj := sweepProject("cap-new-proj")
		proj.Spec.MaxNewTasksPerSweep = 2
		repo := sweepRepo("cap-new-proj")
		c := newMirrorClient(t, proj, repo)
		rd := &sweepReader{}
		for n := 1; n <= 5; n++ {
			rd.issues = append(rd.issues, scm.IssueRef{
				Repo: "szymonrychu/tatara-operator", Number: n, Author: "alice", State: "open"})
		}
		before := testutil.ToFloat64(obs.SweepMintCapHitTotal.WithLabelValues(proj.Name, obs.SweepCapMaxNewTasksPerSweep))

		runSweep(t, c, proj, repo, rd)

		if n := len(sweepTasks(t, c, proj.Name)); n != 2 {
			t.Fatalf("tasks = %d, want 2 (maxNewTasksPerSweep)", n)
		}
		after := testutil.ToFloat64(obs.SweepMintCapHitTotal.WithLabelValues(proj.Name, obs.SweepCapMaxNewTasksPerSweep))
		if after <= before {
			t.Fatalf("operator_sweep_mint_cap_hit_total{cap=maxNewTasksPerSweep} did not increment (%v -> %v)", before, after)
		}

		// The remaining orphans are minted on the NEXT pass.
		runSweep(t, c, proj, repo, rd)
		if n := len(sweepTasks(t, c, proj.Name)); n != 4 {
			t.Fatalf("tasks after pass 2 = %d, want 4", n)
		}
	})

	t.Run("maxOpenTasks caps ACTIVE mints only", func(t *testing.T) {
		proj := sweepProject("cap-open-proj")
		proj.Spec.MaxOpenTasks = 1
		repo := sweepRepo("cap-open-proj")
		// One ACTIVE Task already: the budget is spent.
		live := &tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: "cap-open-proj-live", Namespace: testNS},
			Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g"},
		}
		live.Status.Stage = tatarav1alpha1.StageImplementing
		c := newMirrorClient(t, proj, repo, live)
		now := time.Now()
		rd := &sweepReader{
			issues: []scm.IssueRef{
				{Repo: "szymonrychu/tatara-operator", Number: 1, Author: "alice", State: "open"},
				{Repo: "szymonrychu/tatara-operator", Number: 2, Author: "alice", State: "open"},
			},
			comments: map[int][]scm.IssueComment{
				// #1 wants an ACTIVE mint (human last word) and must be REFUSED.
				1: {humanComment("c1", "alice", "still broken", now)},
			},
		}
		before := testutil.ToFloat64(obs.SweepMintCapHitTotal.WithLabelValues(proj.Name, obs.SweepCapMaxOpenTasks))

		runSweep(t, c, proj, repo, rd)

		tasks := sweepTasks(t, c, proj.Name)
		if len(tasks) != 2 {
			t.Fatalf("tasks = %d, want 2 (the live one + the parked backlog mint)", len(tasks))
		}
		for i := range tasks {
			if tasks[i].Name == live.Name {
				continue
			}
			if tasks[i].Spec.InitialStage != tatarav1alpha1.StageParked {
				t.Fatalf("minted initialStage = %q, want parked (maxOpenTasks is spent)", tasks[i].Spec.InitialStage)
			}
		}
		after := testutil.ToFloat64(obs.SweepMintCapHitTotal.WithLabelValues(proj.Name, obs.SweepCapMaxOpenTasks))
		if after <= before {
			t.Fatalf("operator_sweep_mint_cap_hit_total{cap=maxOpenTasks} did not increment (%v -> %v)", before, after)
		}
	})
}

// TestSweepHeartbeat: a clean pass stamps the heartbeat gauge. For a heartbeat,
// NoData IS the failure (the alert sets noDataState: Alerting), so the gauge is
// only ever stamped by a pass that actually completed.
func TestSweepHeartbeat(t *testing.T) {
	proj := sweepProject("hb-proj")
	repo := sweepRepo("hb-proj")
	c := newMirrorClient(t, proj, repo)

	obs.SweepLastSuccessTimestamp.WithLabelValues(SweepActivity).Set(0)
	runSweep(t, c, proj, repo, &sweepReader{})

	if got := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues(SweepActivity)); got <= 0 {
		t.Fatalf("operator_sweep_last_success_timestamp_seconds{activity=sweep} = %v, want a stamped timestamp", got)
	}
}

// TestSeedSweepHeartbeat: on leader restart the process-local heartbeat gauge is
// re-seeded from the durable Project status to the NEWEST successful pass across
// all projects (the gauge is one series, not per-project), so a rollover does not
// blank it into NoData until the next 4h sweep (issue #342).
func TestSeedSweepHeartbeat(t *testing.T) {
	newer := metav1.Time{Time: time.Unix(1784347207, 0)} // 2026-07-18T04:00:07Z
	older := metav1.Time{Time: time.Unix(1784246408, 0)} // 2026-07-17T00:00:08Z
	p1 := sweepProject("seed-newer")
	p1.Status.LastSweepSuccess = &newer
	p2 := sweepProject("seed-older")
	p2.Status.LastSweepSuccess = &older
	p3 := sweepProject("seed-never") // never swept: no stamp, must be ignored
	c := newMirrorClient(t, p1, p2, p3)

	obs.SweepLastSuccessTimestamp.WithLabelValues(SweepActivity).Set(0)
	got, err := SeedSweepHeartbeat(context.Background(), c, testNS)
	if err != nil {
		t.Fatalf("SeedSweepHeartbeat: %v", err)
	}
	if got == nil || got.Unix() != 1784347207 {
		t.Fatalf("seeded time = %v, want the newest pass (unix 1784347207)", got)
	}
	if v := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues(SweepActivity)); v != 1784347207 {
		t.Fatalf("gauge = %v, want 1784347207 (newest across projects, not the older stamp or 0)", v)
	}
}

// TestSeedSweepHeartbeatNoHistory: with no persisted success the gauge is left
// unset so the alert's NoData path still reports a never-swept operator rather
// than a fabricated heartbeat.
func TestSeedSweepHeartbeatNoHistory(t *testing.T) {
	c := newMirrorClient(t, sweepProject("seed-fresh"))

	obs.SweepLastSuccessTimestamp.WithLabelValues(SweepActivity).Set(0)
	got, err := SeedSweepHeartbeat(context.Background(), c, testNS)
	if err != nil {
		t.Fatalf("SeedSweepHeartbeat: %v", err)
	}
	if got != nil {
		t.Fatalf("seeded time = %v, want nil when no project has ever swept", got)
	}
	if v := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues(SweepActivity)); v != 0 {
		t.Fatalf("gauge = %v, want it left at 0 (no history to seed from)", v)
	}
}

// TestStampScanSweepPersistsLastSweepSuccess: a clean sweep pass stamps the
// durable heartbeat field (and only it) so the seed above has something to read.
func TestStampScanSweepPersistsLastSweepSuccess(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "sweep-stamp-proj", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "sweep-stamp-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "bot"},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	r := &ProjectReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	if err := r.stampScan(ctx, proj, "sweep"); err != nil {
		t.Fatalf("stampScan sweep: %v", err)
	}
	if proj.Status.LastSweepSuccess == nil {
		t.Fatal("stampScan(sweep) must set the in-memory proj.Status.LastSweepSuccess")
	}
	var got tatarav1alpha1.Project
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "sweep-stamp-proj"}, &got); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if got.Status.LastSweepSuccess == nil {
		t.Fatal("stampScan(sweep) must persist Status.LastSweepSuccess")
	}
	if got.Status.LastIssueScan != nil {
		t.Fatal("stampScan(sweep) must not stamp LastIssueScan (the two heartbeats are independent)")
	}
}

// TestSweepEnabledByDefault: the cutover deleted issueScan/mrScan/backstop, so
// the sweep is the ONLY intake and an ABSENT annotation cannot mean "no intake".
// The annotation survives only as an explicit per-project break-glass OFF.
func TestSweepEnabledByDefault(t *testing.T) {
	proj := sweepProject("default-proj")
	proj.Annotations = nil
	if !SweepEnabled(proj) {
		t.Fatal("the sweep is OFF without the annotation; it must be ON by default")
	}
	on := sweepProject("on-proj")
	if !SweepEnabled(on) {
		t.Fatal("the sweep is OFF with the annotation explicitly enabled")
	}
	off := sweepProject("off-proj")
	off.Annotations = map[string]string{SweepAnnotation: SweepDisabledValue}
	if SweepEnabled(off) {
		t.Fatal("the sweep is ON with the break-glass annotation set to disabled")
	}
}

func boolp(b bool) *bool { return &b }

// ---------------------------------------------------------------------------
// THE WEBHOOK-ORIGINATED MARKER. THE PLATFORM'S FRONT DOOR.
//
// A human opens an issue and the platform DOES SOMETHING. Without the marker a
// freshly-opened human issue (open, human-authored, ZERO comments, no
// tatara-parked label) is byte-for-byte indistinguishable from a three-year-old
// untouched backlog issue, and the sweep parks BOTH: the loop in the README
// ("SCM webhook -> the operator turns a labelled issue into a Task -> it spawns
// an agent pod") is dead at step one, and the issue sits there until the human
// comments a SECOND time on their own issue.
//
// The distinguishing signal is LIVENESS, not thread shape, and it can only come
// from the webhook: a live, HMAC-verified, attributable delivery. It must NOT
// come from re-reading "zero comments" as "a human has the last word" - that
// would mint the whole cutover backlog ACTIVE, which is USER DECISION B2's
// 17-to-100-pod-hour re-triage storm.
// ---------------------------------------------------------------------------

// markedIssueCR is the ownerless mirror the webhook leaves behind: contract B.2
// permits an ownerless Issue CR, and the sweep's adopt-or-create path (fix
// M3-10) ADOPTS it rather than colliding with it.
func markedIssueCR(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) *tatarav1alpha1.Issue {
	return &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.IssueName(repo.Name, number), Namespace: testNS,
			Annotations: map[string]string{AnnWebhookOriginated: time.Now().UTC().Format(time.RFC3339)},
		},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo.Name, Number: number, ProjectRef: proj.Name,
			URL: "https://github.com/szymonrychu/tatara-operator/issues/7",
		},
	}
}

func sweptIssueCR(t *testing.T, c client.Client, repo *tatarav1alpha1.Repository, number int) *tatarav1alpha1.Issue {
	t.Helper()
	var iss tatarav1alpha1.Issue
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: testNS, Name: tatarav1alpha1.IssueName(repo.Name, number)}, &iss); err != nil {
		t.Fatalf("get issue CR %d: %v", number, err)
	}
	return &iss
}

// TestSweepWebhookOriginatedIssueMintsTriaging IS THE TEST THAT PROVES THE
// PLATFORM WORKS AT ALL: a human opens a brand-new issue (ZERO comments), a
// webhook lands and marks the mirror, and the very next sweep mints an ACTIVE
// (triaging) Task - not parked(backlog-sweep).
func TestSweepWebhookOriginatedIssueMintsTriaging(t *testing.T) {
	proj := sweepProject("webhook-proj")
	repo := sweepRepo("webhook-proj")
	c := newMirrorClient(t, proj, repo, markedIssueCR(proj, repo, 7))
	rd := &sweepReader{
		issues:  []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 7, Author: "alice", State: "open"}},
		content: map[int]scm.IssueContent{7: {Title: "the login page 500s", Body: "steps to reproduce"}},
		// ZERO comments. A brand-new issue has none, and humanHasLastWord is false on
		// an empty thread. The marker is the ONLY thing that makes this ACTIVE.
	}

	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	if tasks[0].Spec.InitialStage != tatarav1alpha1.StageTriaging {
		t.Fatalf("initialStage = %q/%q, want triaging: a human OPENED this issue and the webhook said so",
			tasks[0].Spec.InitialStage, tasks[0].Spec.InitialStageReason)
	}
	if tasks[0].Spec.InitialStageReason != "" {
		t.Fatalf("initialStageReason = %q, want empty on an ACTIVE mint", tasks[0].Spec.InitialStageReason)
	}

	// CONSUMED. The marker is a one-shot: it must not survive to re-activate a
	// LATER park.
	iss := sweptIssueCR(t, c, repo, 7)
	if _, still := iss.Annotations[AnnWebhookOriginated]; still {
		t.Fatal("the webhook marker survived the mint; it must be consumed exactly once")
	}
	// And the mint ADOPTED the ownerless CR the webhook created.
	owner, ok := own.ControllerOwner(iss)
	if !ok || owner != tasks[0].Name {
		t.Fatalf("issue controller owner = %q/%v, want %q", owner, ok, tasks[0].Name)
	}
}

// TestSweepCutoverBacklogStillParksWithZeroPods is the OTHER half, and it is the
// one the marker must not break. FORTY open, zero-comment, human-authored
// backlog issues with NO webhook and NO marker mint FORTY parked(backlog-sweep)
// Tasks: zero pods, zero QueuedEvents, zero tokens.
func TestSweepCutoverBacklogStillParksWithZeroPods(t *testing.T) {
	const backlog = 40
	proj := sweepProject("cutover-proj")
	proj.Spec.MaxNewTasksPerSweep = backlog // one pass, whole backlog
	repo := sweepRepo("cutover-proj")
	c := newMirrorClient(t, proj, repo, mdSecret())

	rd := &sweepReader{content: map[int]scm.IssueContent{}}
	for n := 1; n <= backlog; n++ {
		rd.issues = append(rd.issues, scm.IssueRef{
			Repo: "szymonrychu/tatara-operator", Number: n, Author: "alice", State: "open",
		})
		rd.content[n] = scm.IssueContent{Title: "old bug", Body: "from before the cutover"}
	}

	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != backlog {
		t.Fatalf("tasks = %d, want %d", len(tasks), backlog)
	}
	for i := range tasks {
		if tasks[i].Spec.InitialStage != tatarav1alpha1.StageParked ||
			tasks[i].Spec.InitialStageReason != stage.ReasonBacklogSweep {
			t.Fatalf("task %s initialStage = %q/%q, want parked/backlog-sweep: the cutover backlog must NEVER re-triage",
				tasks[i].Name, tasks[i].Spec.InitialStage, tasks[i].Spec.InitialStageReason)
		}
	}

	var qel tatarav1alpha1.QueuedEventList
	if err := c.List(context.Background(), &qel, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list queuedevents: %v", err)
	}
	if len(qel.Items) != 0 {
		t.Fatalf("queuedevents = %d, want 0", len(qel.Items))
	}

	// AND THE RECONCILER AGREES. Driving every one of the 40 Tasks through the
	// stage machine creates ZERO pods: parked is terminal, and the reconciler
	// returns before it can ever reach the pod path.
	r := tsReconciler(c)
	for i := range tasks {
		if _, err := r.reconcileStage(context.Background(), proj, &tasks[i], time.Now()); err != nil {
			t.Fatalf("reconcileStage %s: %v", tasks[i].Name, err)
		}
	}
	var pods corev1.PodList
	if err := c.List(context.Background(), &pods, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("pods = %d, want 0: a %d-issue cutover backlog must cost ZERO pod-hours",
			len(pods.Items), backlog)
	}
}

// TestWebhookMarkerIsConsumedExactlyOnce: the marker is spent by the mint that
// reads it. If it survived, the reap/re-mint cycle would re-activate the issue
// forever - the M25 loop by another door.
//
// The reap here is the M25 scenario: the reaper's tatara-parked label write
// FAILED (a 403 on a secondary rate limit), so there is NO label on the forge
// issue to fall back on. The marker being GONE is the only thing keeping pass 2
// parked.
func TestWebhookMarkerIsConsumedExactlyOnce(t *testing.T) {
	proj := sweepProject("once-proj")
	repo := sweepRepo("once-proj")
	c := newMirrorClient(t, proj, repo, markedIssueCR(proj, repo, 7))
	rd := &sweepReader{
		issues:  []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 7, Author: "alice", State: "open"}},
		content: map[int]scm.IssueContent{7: {Title: "t", Body: "b"}},
	}

	runSweep(t, c, proj, repo, rd)
	first := sweepTasks(t, c, proj.Name)
	if len(first) != 1 || first[0].Spec.InitialStage != tatarav1alpha1.StageTriaging {
		t.Fatalf("pass 1: tasks = %d, initialStage = %q, want 1 triaging", len(first), first[0].Spec.InitialStage)
	}

	// The Task parked and the reaper collected it: the Issue CR is RELEASED
	// (ownerless, never GC'd per B.1) and the Task is gone. No tatara-parked label
	// made it to the forge.
	iss := sweptIssueCR(t, c, repo, 7)
	iss.OwnerReferences = nil
	if err := c.Update(context.Background(), iss); err != nil {
		t.Fatalf("release issue ownership: %v", err)
	}
	if err := c.Delete(context.Background(), &first[0]); err != nil {
		t.Fatalf("delete reaped task: %v", err)
	}

	runSweep(t, c, proj, repo, rd)

	second := sweepTasks(t, c, proj.Name)
	if len(second) != 1 {
		t.Fatalf("pass 2: tasks = %d, want 1", len(second))
	}
	if second[0].Spec.InitialStage != tatarav1alpha1.StageParked ||
		second[0].Spec.InitialStageReason != stage.ReasonBacklogSweep {
		t.Fatalf("pass 2: initialStage = %q/%q, want parked/backlog-sweep: a SPENT marker must never re-activate a Task that has since parked",
			second[0].Spec.InitialStage, second[0].Spec.InitialStageReason)
	}
}

// TestSweepBudgetsBindOnWebhookOriginatedMints: the marker buys an ACTIVE stage,
// not a bypass. maxNewTasksPerSweep and maxOpenTasks BOTH still bind (fix B1),
// and an issue the budget deferred KEEPS its marker so the next pass still mints
// it ACTIVE - the deferral must not silently downgrade it to the backlog.
func TestSweepBudgetsBindOnWebhookOriginatedMints(t *testing.T) {
	t.Run("maxNewTasksPerSweep binds and the deferred marker survives", func(t *testing.T) {
		proj := sweepProject("cap-new-proj")
		proj.Spec.MaxNewTasksPerSweep = 2
		repo := sweepRepo("cap-new-proj")
		c := newMirrorClient(t, proj, repo,
			markedIssueCR(proj, repo, 1), markedIssueCR(proj, repo, 2), markedIssueCR(proj, repo, 3))
		rd := &sweepReader{content: map[int]scm.IssueContent{}}
		for n := 1; n <= 3; n++ {
			rd.issues = append(rd.issues, scm.IssueRef{
				Repo: "szymonrychu/tatara-operator", Number: n, Author: "alice", State: "open"})
			rd.content[n] = scm.IssueContent{Title: "t", Body: "b"}
		}

		runSweep(t, c, proj, repo, rd)

		tasks := sweepTasks(t, c, proj.Name)
		if len(tasks) != 2 {
			t.Fatalf("tasks = %d, want 2 (maxNewTasksPerSweep binds on marked issues too)", len(tasks))
		}
		if v := sweptIssueCR(t, c, repo, 3).Annotations[AnnWebhookOriginated]; v == "" {
			t.Fatal("the budget-deferred issue LOST its marker; the next pass would park a live human issue")
		}
	})

	t.Run("maxOpenTasks binds: a marked mint is ACTIVE and counts", func(t *testing.T) {
		proj := sweepProject("cap-open-proj")
		proj.Spec.MaxOpenTasks = 1
		repo := sweepRepo("cap-open-proj")
		c := newMirrorClient(t, proj, repo, markedIssueCR(proj, repo, 1), markedIssueCR(proj, repo, 2))
		rd := &sweepReader{content: map[int]scm.IssueContent{}}
		for n := 1; n <= 2; n++ {
			rd.issues = append(rd.issues, scm.IssueRef{
				Repo: "szymonrychu/tatara-operator", Number: n, Author: "alice", State: "open"})
			rd.content[n] = scm.IssueContent{Title: "t", Body: "b"}
		}

		runSweep(t, c, proj, repo, rd)

		tasks := sweepTasks(t, c, proj.Name)
		if len(tasks) != 1 {
			t.Fatalf("tasks = %d, want 1 (maxOpenTasks=1 binds: an ACTIVE mint counts against it)", len(tasks))
		}
		if tasks[0].Spec.InitialStage != tatarav1alpha1.StageTriaging {
			t.Fatalf("initialStage = %q, want triaging", tasks[0].Spec.InitialStage)
		}
	})
}

// TestWebhookOpenedIssueTriagesAndSpawnsAPod IS THE END-TO-END PROOF, from the
// webhook's marker to a running agent pod:
//
//	marker -> sweep mints triaging -> reconcile: triaging mints the Issue CR and
//	          routes to clarifying -> reconcile: the clarify POD is created.
//
// Nothing about the platform works if this does not.
func TestWebhookOpenedIssueTriagesAndSpawnsAPod(t *testing.T) {
	proj := sweepProject("e2e-proj")
	proj.Spec.MaxConcurrentAgents = 3
	readySince := metav1.NewTime(time.Now().Add(-time.Hour))
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{
		Phase: "Ready", Endpoint: "http://mem", ReadySince: &readySince,
	}
	repo := sweepRepo("e2e-proj")
	c := newMirrorClient(t, proj, repo, mdSecret(), markedIssueCR(proj, repo, 7))
	rd := &sweepReader{
		issues:  []scm.IssueRef{{Repo: "szymonrychu/tatara-operator", Number: 7, Author: "alice", State: "open"}},
		content: map[int]scm.IssueContent{7: {Title: "the login page 500s", Body: "steps to reproduce"}},
	}

	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 || tasks[0].Spec.InitialStage != tatarav1alpha1.StageTriaging {
		t.Fatalf("sweep minted %d tasks at initialStage %q, want 1 at triaging", len(tasks), tasks[0].Spec.InitialStage)
	}

	r := tsReconciler(c)
	r.PodConfig = agent.PodConfig{
		Namespace: testNS, AnthropicSecretName: "anthropic", CLIOIDCSecretName: "tatara-cli-oidc",
	}
	task := &tasks[0]
	now := time.Now()

	// PASS 0: the create-edge (fix C5). The mint only set Spec.InitialStage; this
	// pass applies it to Status.Stage and requeues - it does not yet drive
	// triaging's own routing.
	task = tsReconcile(t, r, proj, task, now)
	if task.Status.Stage != tatarav1alpha1.StageTriaging {
		t.Fatalf("stage after the create-edge = %q, want triaging", task.Status.Stage)
	}

	// PASS 1: triaging is POD-LESS. It mints the Issue CRs (F.2) and routes on
	// spec.kind - clarify -> clarifying, where the C.6 approval gate lives.
	task = tsReconcile(t, r, proj, task, now)
	if task.Status.Stage != tatarav1alpha1.StageClarifying {
		t.Fatalf("stage after triage = %q, want clarifying", task.Status.Stage)
	}

	// PASS 2: clarifying is a POD stage. The pod is created.
	task = tsReconcile(t, r, proj, task, now)
	var pod corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{
		Namespace: testNS, Name: agent.PodName(task)}, &pod); err != nil {
		t.Fatalf("THE AGENT POD WAS NEVER CREATED. A human opened an issue and the platform did nothing: %v", err)
	}
	if pod.Annotations[annPodStage] != tatarav1alpha1.StageClarifying {
		t.Fatalf("pod stage annotation = %q, want clarifying", pod.Annotations[annPodStage])
	}
}
