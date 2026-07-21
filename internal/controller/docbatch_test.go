package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// docsRepoURL is the docs repo reapProject() enrols. The batch's
// spec.repositoryRef is the Repository CR that mirrors it.
const docsRepoURL = "https://github.com/szymonrychu/tatara-documentation.git"

// deliveredWithMergedMR builds a delivered Task whose single owned MR is MERGED:
// the exact shape the nightly batch covers.
func deliveredWithMergedMR(t *testing.T, proj, repo, name string, number int, at time.Time) (*tatarav1alpha1.Task, *tatarav1alpha1.MergeRequest) {
	t.Helper()
	tk := reapTask(proj, name, "clarify", tatarav1alpha1.StageDelivered, "", at)
	stamp := metav1.NewTime(at)
	tk.Status.DeliveredAt = &stamp
	tk.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName(repo, number)}

	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.MergeRequestName(repo, number), Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{reapOwnerRef(name, true)},
		},
		Spec: tatarav1alpha1.MergeRequestSpec{RepositoryRef: repo, Number: number, ProjectRef: proj},
	}
	mr.Status.Author = "tatara-bot"
	mr.Status.HeadBranch = agent.TaskBranch(tk)
	mr.Status.State = "merged"
	return tk, mr
}

func docBatches(t *testing.T, c client.Client, proj string) []tatarav1alpha1.Task {
	t.Helper()
	var tl tatarav1alpha1.TaskList
	if err := c.List(context.Background(), &tl, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	var out []tatarav1alpha1.Task
	for i := range tl.Items {
		if tl.Items[i].Spec.ProjectRef == proj && tl.Items[i].Spec.Kind == DocBatchKind &&
			len(tl.Items[i].Spec.DocumentsTasks) > 0 {
			out = append(out, tl.Items[i])
		}
	}
	return out
}

// TestDocBatchMintsOneTaskForNDelivered: ONE nightly Task covers N delivered
// Tasks. One doc pod, one docs MR, one review, one tatara-documentation release
// per NIGHT - not one of each per delivered one-line fix.
func TestDocBatchMintsOneTaskForNDelivered(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("docmint")
	src := reapRepo("docmint", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	docs := reapRepo("docmint", "tatara-documentation", docsRepoURL)

	t1, m1 := deliveredWithMergedMR(t, "docmint", src.Name, "task-a", 1, time.Now().Add(-3*time.Hour))
	t2, m2 := deliveredWithMergedMR(t, "docmint", src.Name, "task-b", 2, time.Now().Add(-2*time.Hour))
	// NEVER covered: a brainstorm skip has zero MRs. A docs PR about nothing, a
	// review, a merge and a release, every day, is exactly what fix 25 kills.
	skip := reapTask("docmint", "task-skip", "brainstorm", tatarav1alpha1.StageDelivered, "", time.Now())
	skipAt := metav1.NewTime(time.Now())
	skip.Status.DeliveredAt = &skipAt
	// NEVER covered: an incident false_positive is REJECTED, not delivered.
	fp := reapTask("docmint", "task-fp", "incident", tatarav1alpha1.StageRejected, stage.ReasonFalsePositive, time.Now())

	c := newMirrorClient(t, proj, src, docs, reapSecret(), t1, t2, skip, fp, m1, m2)
	r := reapReconciler(c, &reapWriter{})

	if err := r.MintDocBatch(ctx, proj); err != nil {
		t.Fatalf("MintDocBatch: %v", err)
	}

	batches := docBatches(t, c, "docmint")
	if len(batches) != 1 {
		t.Fatalf("minted %d documentation batches, want exactly ONE", len(batches))
	}
	b := batches[0]
	if got := b.Spec.DocumentsTasks; len(got) != 2 || got[0] != "task-a" || got[1] != "task-b" {
		t.Fatalf("spec.documentsTasks = %v, want [task-a task-b] (the skip and the false_positive are NEVER covered)", got)
	}
	if b.Spec.RepositoryRef != docs.Name {
		t.Fatalf("spec.repositoryRef = %q, want the docs repo %q", b.Spec.RepositoryRef, docs.Name)
	}
	// MintDocBatch sets the IMMUTABLE Spec.InitialStage (fix C5); Status.Stage is
	// applied later by the TaskReconciler create-edge, which this test does not
	// run.
	if b.Spec.InitialStage != tatarav1alpha1.StageDocumenting {
		t.Fatalf("initialStage = %q, want documenting", b.Spec.InitialStage)
	}

	// Drive the create-edge (fix C5) so the in-flight guard - which reads
	// Status.Stage, not Spec.InitialStage - sees this batch as live before the
	// second pass. In production the reconciler applies Spec.InitialStage long
	// before the next night's mint tick; this mirrors that sequencing.
	live, _ := mustGetTask(t, c, b.Name)
	tr := &TaskReconciler{Client: c, Metrics: r.Metrics}
	if _, err := tr.reconcileStage(ctx, proj, live, time.Now()); err != nil {
		t.Fatalf("drive create-edge: %v", err)
	}

	// An empty covered set mints NOTHING (the batch above now holds them, but
	// they are still documentedBy="" - the in-flight guard is what stops a second
	// batch racing the first over the same parents).
	if err := r.MintDocBatch(ctx, proj); err != nil {
		t.Fatalf("MintDocBatch (second pass): %v", err)
	}
	if got := len(docBatches(t, c, "docmint")); got != 1 {
		t.Fatalf("a second batch was minted while one was in flight: %d batches", got)
	}
}

// TestDocBatchStampsDocumentedByOnDelivered: on the batch reaching delivered,
// status.documentedBy is stamped on EVERY covered Task - which is what finally
// lets the reaper collect them at their 48h TTL.
func TestDocBatchStampsDocumentedByOnDelivered(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("docstamp")
	src := reapRepo("docstamp", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	docs := reapRepo("docstamp", "tatara-documentation", docsRepoURL)

	t1, m1 := deliveredWithMergedMR(t, "docstamp", src.Name, "task-a", 1, time.Now())
	batch := reapTask("docstamp", "doc-batch", DocBatchKind, tatarav1alpha1.StageDelivered, "", time.Now())
	batch.Spec.DocumentsTasks = []string{"task-a"}
	batch.Spec.RepositoryRef = docs.Name
	batch.Status.Stats.PodRuns = 3

	c := newMirrorClient(t, proj, src, docs, reapSecret(), t1, m1, batch)
	r := reapReconciler(c, &reapWriter{})

	if err := r.ResolveDocBatch(ctx, batch); err != nil {
		t.Fatalf("ResolveDocBatch: %v", err)
	}
	got, _ := mustGetTask(t, c, "task-a")
	if got.Status.DocumentedBy != "doc-batch" {
		t.Fatalf("documentedBy = %q, want doc-batch", got.Status.DocumentedBy)
	}
}

// TestReapTerminalStampsDocumentedByOnNormalDelivery is the wiring test for the
// COMMON case: a documentation batch that runs, its docs PR merges, and it
// reaches delivered THROUGH THE NORMAL review/merge/deploy path (merge.go), not
// through forceDocTimeout. ResolveDocBatch must fire from THE REAPER'S regular
// pass over stage=delivered Tasks, or every successful nightly batch - the common
// case - leaves its parents documentedBy="" forever and MintDocBatch re-covers
// them EVERY subsequent night.
func TestReapTerminalStampsDocumentedByOnNormalDelivery(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("docnormal")
	src := reapRepo("docnormal", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	docs := reapRepo("docnormal", "tatara-documentation", docsRepoURL)

	t1, m1 := deliveredWithMergedMR(t, "docnormal", src.Name, "task-a", 1, time.Now())
	// The batch reached delivered with reason="" - the normal merge.go path, NOT
	// doc-timeout. It ran (podRuns > 0) and is fresh (well within its 48h TTL), so
	// the ONLY thing left to verify is whether the reap pass resolves it at all.
	batch := reapTask("docnormal", "doc-batch", DocBatchKind, tatarav1alpha1.StageDelivered, "", time.Now())
	batch.Spec.DocumentsTasks = []string{"task-a"}
	batch.Spec.RepositoryRef = docs.Name
	batch.Status.Stats.PodRuns = 3

	c := newMirrorClient(t, proj, src, docs, reapSecret(), t1, m1, batch)
	r := reapReconciler(c, &reapWriter{})

	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}
	got, _ := mustGetTask(t, c, "task-a")
	if got.Status.DocumentedBy != "doc-batch" {
		t.Fatalf("documentedBy = %q, want doc-batch: a normally-delivered doc batch must resolve on the reaper's regular pass, not only via forceDocTimeout", got.Status.DocumentedBy)
	}
}

// TestReapTerminalStampsDocumentedByOnParked covers the other terminal a doc
// batch can reach through the normal pod-stage path: parked (e.g. a merge/deploy
// timeout), never having gone through StageDocumenting's own doc-timeout edge.
// ResolveDocBatch's podRuns==0 carve-out must still apply here.
func TestReapTerminalStampsDocumentedByOnParked(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("docparked")
	src := reapRepo("docparked", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	docs := reapRepo("docparked", "tatara-documentation", docsRepoURL)

	t1, m1 := deliveredWithMergedMR(t, "docparked", src.Name, "task-a", 1, time.Now())
	batch := reapTask("docparked", "doc-batch", DocBatchKind,
		tatarav1alpha1.StageParked, stage.ReasonMergeTimeout, time.Now())
	batch.Spec.DocumentsTasks = []string{"task-a"}
	batch.Spec.RepositoryRef = docs.Name
	batch.Status.Stats.PodRuns = 2 // it ran

	c := newMirrorClient(t, proj, src, docs, reapSecret(), t1, m1, batch)
	w := &reapWriter{
		comment:  func(string, string) error { return nil },
		addLabel: func(string, string) error { return nil },
	}
	r := reapReconciler(c, w)

	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}
	got, _ := mustGetTask(t, c, "task-a")
	if got.Status.DocumentedBy != "doc-batch" {
		t.Fatalf("documentedBy = %q, want doc-batch: a parked doc batch that ran must resolve on the reaper's regular pass", got.Status.DocumentedBy)
	}
}

// TestDocBatchNeverRanIsPickedUpTheNextNight IS fix M21, and it is not cosmetic.
//
// v3 stamped documentedBy the moment the doc Task hit doc-timeout. A priority-2
// doc batch on a busy project STARVES, never gets an agent slot, times out at 2h
// having run ZERO pods, and stamps every member as documented. Result: on a busy
// project the docs are NEVER WRITTEN, every night, silently.
//
// TWO CONSECUTIVE NIGHTS. The second batch must cover the SAME Tasks.
func TestDocBatchNeverRanIsPickedUpTheNextNight(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("docstarve")
	src := reapRepo("docstarve", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	docs := reapRepo("docstarve", "tatara-documentation", docsRepoURL)

	t1, m1 := deliveredWithMergedMR(t, "docstarve", src.Name, "task-a", 1, time.Now().Add(-20*time.Hour))
	t2, m2 := deliveredWithMergedMR(t, "docstarve", src.Name, "task-b", 2, time.Now().Add(-19*time.Hour))

	c := newMirrorClient(t, proj, src, docs, reapSecret(), t1, t2, m1, m2)
	r := reapReconciler(c, &reapWriter{})

	// ---- NIGHT ONE ----
	if err := r.MintDocBatch(ctx, proj); err != nil {
		t.Fatalf("night 1 MintDocBatch: %v", err)
	}
	night1 := docBatches(t, c, "docstarve")
	if len(night1) != 1 {
		t.Fatalf("night 1 minted %d batches, want 1", len(night1))
	}
	b1 := night1[0]
	if len(b1.Spec.DocumentsTasks) != 2 {
		t.Fatalf("night 1 covered %v, want both tasks", b1.Spec.DocumentsTasks)
	}

	// Drive the create-edge (fix C5): MintDocBatch only sets the immutable
	// Spec.InitialStage; the TaskReconciler create-edge applies it to
	// Status.Stage, which forceDocTimeout below switches on.
	b1live, _ := mustGetTask(t, c, b1.Name)
	tr := &TaskReconciler{Client: c, Metrics: r.Metrics}
	if _, err := tr.reconcileStage(ctx, proj, b1live, time.Now()); err != nil {
		t.Fatalf("night 1 create-edge: %v", err)
	}

	// It STARVES: 2h+ in documenting, and stats.podRuns is ZERO. It NEVER RAN.
	live, _ := mustGetTask(t, c, b1.Name)
	if live.Status.Stage != tatarav1alpha1.StageDocumenting {
		t.Fatalf("create-edge stamped %q, want documenting", live.Status.Stage)
	}
	entered := metav1.NewTime(time.Now().Add(-3 * time.Hour))
	live.Status.StageEnteredAt = &entered
	live.Status.Stats.PodRuns = 0
	if err := c.Status().Update(ctx, live); err != nil {
		t.Fatalf("age the batch: %v", err)
	}

	before := testutil.ToFloat64(obs.DocTaskAbandonedTotal.WithLabelValues(obs.DocAbandonedNeverRan))
	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal (doc timeout): %v", err)
	}

	// The stuck batch is force-moved to delivered(doc-timeout) - no parent is ever
	// pinned by a stuck doc batch.
	forced, ok := mustGetTask(t, c, b1.Name)
	if !ok {
		t.Fatal("the stuck batch vanished")
	}
	if forced.Status.Stage != tatarav1alpha1.StageDelivered || forced.Status.StageReason != stage.ReasonDocTimeout {
		t.Fatalf("stuck batch is %s(%s), want delivered(doc-timeout)", forced.Status.Stage, forced.Status.StageReason)
	}
	if got := testutil.ToFloat64(obs.DocTaskAbandonedTotal.WithLabelValues(obs.DocAbandonedNeverRan)); got <= before {
		t.Fatalf("operator_doc_task_abandoned_total{reason=never_ran} = %v, want > %v", got, before)
	}

	// STAMP NOTHING. The parents stay documentedBy="".
	for _, name := range []string{"task-a", "task-b"} {
		got, _ := mustGetTask(t, c, name)
		if got.Status.DocumentedBy != "" {
			t.Fatalf("%s.documentedBy = %q; a batch that NEVER RAN must stamp NOTHING",
				name, got.Status.DocumentedBy)
		}
	}

	// ---- NIGHT TWO ----
	if err := r.MintDocBatch(ctx, proj); err != nil {
		t.Fatalf("night 2 MintDocBatch: %v", err)
	}
	var b2 *tatarav1alpha1.Task
	for _, b := range docBatches(t, c, "docstarve") {
		if b.Name != b1.Name {
			b2 = b.DeepCopy()
		}
	}
	if b2 == nil {
		t.Fatal("night 2 minted NO batch: the starved parents are never documented, silently - fix M21 is not implemented")
	}
	if got := b2.Spec.DocumentsTasks; len(got) != 2 || got[0] != "task-a" || got[1] != "task-b" {
		t.Fatalf("night 2 covered %v, want the SAME [task-a task-b]", got)
	}
}

// TestDocBatchTimeoutStampsWhenItRan: the other side of the M21 carve-out. A
// batch that RAN and timed out stamps documentedBy anyway - the work was
// attempted, and we do not retry it forever.
func TestDocBatchTimeoutStampsWhenItRan(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("docran")
	src := reapRepo("docran", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	docs := reapRepo("docran", "tatara-documentation", docsRepoURL)

	t1, m1 := deliveredWithMergedMR(t, "docran", src.Name, "task-a", 1, time.Now())
	batch := reapTask("docran", "doc-batch", DocBatchKind,
		tatarav1alpha1.StageDocumenting, "", time.Now().Add(-3*time.Hour))
	batch.Spec.DocumentsTasks = []string{"task-a"}
	batch.Spec.RepositoryRef = docs.Name
	batch.Status.Stats.PodRuns = 2 // IT RAN.

	c := newMirrorClient(t, proj, src, docs, reapSecret(), t1, m1, batch)
	r := reapReconciler(c, &reapWriter{})

	before := testutil.ToFloat64(obs.DocTaskAbandonedTotal.WithLabelValues(obs.DocAbandonedTimeout))
	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}
	got, _ := mustGetTask(t, c, "task-a")
	if got.Status.DocumentedBy != "doc-batch" {
		t.Fatalf("documentedBy = %q; a batch that RAN and timed out stamps anyway", got.Status.DocumentedBy)
	}
	if v := testutil.ToFloat64(obs.DocTaskAbandonedTotal.WithLabelValues(obs.DocAbandonedTimeout)); v <= before {
		t.Fatalf("operator_doc_task_abandoned_total{reason=timeout} = %v, want > %v", v, before)
	}

	// Idempotent: a second pass must not double-count the abandonment.
	after := testutil.ToFloat64(obs.DocTaskAbandonedTotal.WithLabelValues(obs.DocAbandonedTimeout))
	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal (second pass): %v", err)
	}
	if v := testutil.ToFloat64(obs.DocTaskAbandonedTotal.WithLabelValues(obs.DocAbandonedTimeout)); v != after {
		t.Fatalf("the doc-abandoned counter moved on a re-pass: %v -> %v", after, v)
	}
}

// TestDocBatchIsNeverItsOwnParent: a documentation batch has merged MRs of its
// own. If it were coverable, every night's batch would document the previous
// night's batch, forever - and the delivered-reap gate (documentedBy != "" OR
// zero merged MRs) would pin every batch in the cluster permanently.
func TestDocBatchIsNeverItsOwnParent(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("docself")
	src := reapRepo("docself", "tatara-documentation", docsRepoURL)
	deliveredAt := time.Now().Add(-72 * time.Hour)

	batch, mr := deliveredWithMergedMR(t, "docself", src.Name, "old-batch", 1, deliveredAt)
	batch.Spec.Kind = DocBatchKind
	batch.Spec.DocumentsTasks = []string{"long-gone-task"}
	batch.Spec.RepositoryRef = src.Name

	c := newMirrorClient(t, proj, src, reapSecret(), batch, mr)
	r := reapReconciler(c, &reapWriter{})

	if err := r.MintDocBatch(ctx, proj); err != nil {
		t.Fatalf("MintDocBatch: %v", err)
	}
	for _, b := range docBatches(t, c, "docself") {
		if b.Name != "old-batch" {
			t.Fatalf("a documentation batch was minted to document a documentation batch: %s covers %v",
				b.Name, b.Spec.DocumentsTasks)
		}
	}
	// And it is NOT pinned: a delivered batch past its TTL reaps with documentedBy
	// permanently empty.
	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}
	if _, ok := mustGetTask(t, c, "old-batch"); ok {
		t.Fatal("a delivered documentation batch past its 48h TTL was pinned by its own documentedBy gate")
	}
}

// TestNeedsDocumentingRequiresConfiguredCron is issue #392: a project with
// documentation.enabled but NO cron schedule structurally never mints a nightly
// batch, so without this exemption a delivered Task with all its MRs merged is
// pinned by the doc_reference GC block forever. needsDocumenting must agree with
// docsRepository/MintDocBatch on what "doc batching is actually configured" means,
// symmetric to the existing zero-merged-MR exemption.
func TestNeedsDocumentingRequiresConfiguredCron(t *testing.T) {
	ctx := context.Background()

	newProj := func(mutate func(p *tatarav1alpha1.Project)) *tatarav1alpha1.Project {
		p := reapProject("cronreq")
		if mutate != nil {
			mutate(p)
		}
		return p
	}

	cases := []struct {
		name string
		proj *tatarav1alpha1.Project
		noMR bool
		want bool
	}{
		{
			name: "docs disabled: nil Documentation",
			proj: newProj(func(p *tatarav1alpha1.Project) { p.Spec.Documentation = nil }),
			want: false,
		},
		{
			name: "docs disabled: Enabled false",
			proj: newProj(func(p *tatarav1alpha1.Project) { p.Spec.Documentation.Enabled = false }),
			want: false,
		},
		{
			name: "docs disabled: empty Repo",
			proj: newProj(func(p *tatarav1alpha1.Project) { p.Spec.Documentation.Repo = "" }),
			want: false,
		},
		{
			name: "docs enabled + repo but no cron schedule (issue #392)",
			proj: newProj(func(p *tatarav1alpha1.Project) { p.Spec.Scm.Cron.Documentation.Schedule = "" }),
			want: false,
		},
		{
			name: "docs enabled + repo but nil Cron",
			proj: newProj(func(p *tatarav1alpha1.Project) { p.Spec.Scm.Cron = nil }),
			want: false,
		},
		{
			name: "fully configured: still blocked (regression)",
			proj: newProj(nil),
			want: true,
		},
		{
			name: "fully configured but zero merged MRs: unaffected",
			proj: newProj(nil),
			noMR: true,
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := reapRepo(tc.proj.Name, "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
			var c client.Client
			var tk *tatarav1alpha1.Task
			if tc.noMR {
				tk = reapTask(tc.proj.Name, "task-a", "brainstorm", tatarav1alpha1.StageDelivered, "", time.Now())
				stamp := metav1.NewTime(time.Now())
				tk.Status.DeliveredAt = &stamp
				c = newMirrorClient(t, tc.proj, src, reapSecret(), tk)
			} else {
				var mr *tatarav1alpha1.MergeRequest
				tk, mr = deliveredWithMergedMR(t, tc.proj.Name, src.Name, "task-a", 1, time.Now())
				c = newMirrorClient(t, tc.proj, src, reapSecret(), tk, mr)
			}
			r := reapReconciler(c, &reapWriter{})

			got, err := r.needsDocumenting(ctx, tc.proj, tk)
			if err != nil {
				t.Fatalf("needsDocumenting: %v", err)
			}
			if got != tc.want {
				t.Fatalf("needsDocumenting = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestMintDocBatchSkipsUnconfiguredProject covers the MintDocBatch caller: a
// project whose documentation.enabled+repo are set but whose cron schedule is
// empty must never cover a delivered Task, even though docsRepository (which
// only checks enabled+repo) resolves the docs repo fine.
func TestMintDocBatchSkipsUnconfiguredProject(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("cronmint")
	proj.Spec.Scm.Cron.Documentation.Schedule = ""
	src := reapRepo("cronmint", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	docs := reapRepo("cronmint", "tatara-documentation", docsRepoURL)

	t1, m1 := deliveredWithMergedMR(t, "cronmint", src.Name, "task-a", 1, time.Now())

	c := newMirrorClient(t, proj, src, docs, reapSecret(), t1, m1)
	r := reapReconciler(c, &reapWriter{})

	if err := r.MintDocBatch(ctx, proj); err != nil {
		t.Fatalf("MintDocBatch: %v", err)
	}
	if got := len(docBatches(t, c, "cronmint")); got != 0 {
		t.Fatalf("minted %d documentation batches for an unconfigured (no cron schedule) project, want 0", got)
	}
}

// TestReapDeliveredNotBlockedWhenDocBatchingNotConfigured covers the reapDelivered
// caller: on a project whose doc batching never runs, a delivered Task with all
// its MRs merged must reap on its 48h TTL, not sit pinned by doc_reference
// forever (issue #392).
func TestReapDeliveredNotBlockedWhenDocBatchingNotConfigured(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("cronreap")
	proj.Spec.Scm.Cron.Documentation.Schedule = ""
	src := reapRepo("cronreap", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	deliveredAt := time.Now().Add(-72 * time.Hour) // past the 48h TTL

	tk, mr := deliveredWithMergedMR(t, "cronreap", src.Name, "task-a", 1, deliveredAt)

	c := newMirrorClient(t, proj, src, reapSecret(), tk, mr)
	r := reapReconciler(c, &reapWriter{})

	before := testutil.ToFloat64(obs.GCBlockedTotal.WithLabelValues(obs.GCBlockedDocReference))
	if err := r.ReapTerminal(ctx, proj); err != nil {
		t.Fatalf("ReapTerminal: %v", err)
	}
	if got := testutil.ToFloat64(obs.GCBlockedTotal.WithLabelValues(obs.GCBlockedDocReference)); got != before {
		t.Fatalf("operator_gc_blocked_total{reason=doc_reference} = %v, want unchanged at %v: an unconfigured project must never be reported as blocked", got, before)
	}
	if _, ok := mustGetTask(t, c, "task-a"); ok {
		t.Fatal("a delivered task on a project with no doc-batch cron schedule was pinned by the doc_reference GC block instead of reaping at its 48h TTL")
	}
}

// TestMintDocBatchRecordsOutcome: every MintDocBatch tick records its outcome on
// operator_doc_batch_mint_total, so a stalled mint is visible directly instead of
// only via the downstream GC-block counter (issue #423). A firing-but-empty tick
// (the common quiet night) must be distinguishable from a cron that never fires.
func TestMintDocBatchRecordsOutcome(t *testing.T) {
	ctx := context.Background()

	// minted: one delivered+merged uncovered Task.
	proj := reapProject("mintobs")
	src := reapRepo("mintobs", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	docs := reapRepo("mintobs", "tatara-documentation", docsRepoURL)
	t1, m1 := deliveredWithMergedMR(t, "mintobs", src.Name, "task-a", 1, time.Now().Add(-3*time.Hour))
	c := newMirrorClient(t, proj, src, docs, reapSecret(), t1, m1)
	r := reapReconciler(c, &reapWriter{})
	beforeMint := testutil.ToFloat64(obs.DocBatchMintTotal.WithLabelValues(obs.DocMintMinted))
	if err := r.MintDocBatch(ctx, proj); err != nil {
		t.Fatalf("MintDocBatch (minted): %v", err)
	}
	if got := testutil.ToFloat64(obs.DocBatchMintTotal.WithLabelValues(obs.DocMintMinted)); got <= beforeMint {
		t.Fatalf("result=minted = %v, want > %v", got, beforeMint)
	}

	// empty: nothing delivered needs documenting.
	proj2 := reapProject("mintempty")
	src2 := reapRepo("mintempty", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	docs2 := reapRepo("mintempty", "tatara-documentation", docsRepoURL)
	c2 := newMirrorClient(t, proj2, src2, docs2, reapSecret())
	r2 := reapReconciler(c2, &reapWriter{})
	beforeEmpty := testutil.ToFloat64(obs.DocBatchMintTotal.WithLabelValues(obs.DocMintEmpty))
	if err := r2.MintDocBatch(ctx, proj2); err != nil {
		t.Fatalf("MintDocBatch (empty): %v", err)
	}
	if got := testutil.ToFloat64(obs.DocBatchMintTotal.WithLabelValues(obs.DocMintEmpty)); got <= beforeEmpty {
		t.Fatalf("result=empty = %v, want > %v", got, beforeEmpty)
	}

	// deferred: a batch is already in flight over the same parents.
	proj3 := reapProject("mintdefer")
	src3 := reapRepo("mintdefer", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")
	docs3 := reapRepo("mintdefer", "tatara-documentation", docsRepoURL)
	t3, m3 := deliveredWithMergedMR(t, "mintdefer", src3.Name, "task-a", 1, time.Now().Add(-3*time.Hour))
	inflight := reapTask("mintdefer", "doc-inflight", DocBatchKind, tatarav1alpha1.StageDocumenting, "", time.Now())
	inflight.Spec.DocumentsTasks = []string{"task-a"}
	inflight.Spec.RepositoryRef = "tatara-documentation"
	c3 := newMirrorClient(t, proj3, src3, docs3, reapSecret(), t3, m3, inflight)
	r3 := reapReconciler(c3, &reapWriter{})
	beforeDefer := testutil.ToFloat64(obs.DocBatchMintTotal.WithLabelValues(obs.DocMintDeferred))
	if err := r3.MintDocBatch(ctx, proj3); err != nil {
		t.Fatalf("MintDocBatch (deferred): %v", err)
	}
	if got := testutil.ToFloat64(obs.DocBatchMintTotal.WithLabelValues(obs.DocMintDeferred)); got <= beforeDefer {
		t.Fatalf("result=deferred = %v, want > %v", got, beforeDefer)
	}
}
