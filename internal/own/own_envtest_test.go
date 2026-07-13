package own

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// THE GC CAVEAT, stated plainly.
//
// envtest's control plane is kube-apiserver + etcd ONLY. It does NOT run
// kube-controller-manager, so it does NOT run the garbage collector, and
// setup-envtest does not ship a kcm binary to enable one. A dependent whose
// owners have all been deleted is therefore NEVER actually collected here.
//
// So the three GC-semantics cases below (multi-owner survival, cascade on
// delivery, zero owners never collected) assert the OWNER-REFERENCE STATE the
// real GC keys on, computed by solidOwners() exactly as
// garbagecollector.attemptToDeleteItem does: an owner ref is "solid" when the
// owner it names still resolves. The GC deletes the dependent iff
// len(solid) == 0 and keeps it iff len(solid) > 0. Asserting the predicate
// input against a REAL API server (real ownerRef admission, real deletes) is
// the honest half of the test; faking a collection would not be.
//
// Case 2's "the API server rejects two controller=true refs" is an
// ADMISSION-time check (metav1 validation, not the GC), so it shows the real
// 422 from the real API server and is unaffected by the caveat.

var (
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client
)

const (
	testNS = "tatara"

	envtestStartAttempts = 3
	envtestStartBackoff  = 3 * time.Second
)

func TestMain(m *testing.M) {
	logf.SetLogger(zap.New(zap.UseDevMode(true)))

	code := func() int {
		var err error
		for attempt := 1; ; attempt++ {
			testEnv = &envtest.Environment{
				CRDDirectoryPaths: []string{
					filepath.Join("..", "..", "charts", "tatara-operator", "crd-bases"),
				},
				ErrorIfCRDPathMissing: true,
			}
			if cfg, err = testEnv.Start(); err == nil {
				break
			}
			_ = testEnv.Stop()
			if attempt >= envtestStartAttempts {
				panic("start envtest: " + err.Error())
			}
			time.Sleep(envtestStartBackoff)
		}
		defer func() { _ = testEnv.Stop() }()

		if err := tataradevv1alpha1.AddToScheme(scheme.Scheme); err != nil {
			panic("add scheme: " + err.Error())
		}
		k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
		if err != nil {
			panic("new client: " + err.Error())
		}

		ns := &corev1.Namespace{}
		ns.Name = testNS
		if err := k8sClient.Create(context.Background(), ns); err != nil {
			panic("create namespace: " + err.Error())
		}

		return m.Run()
	}()
	os.Exit(code)
}

// mkTask creates a real Task in the cluster and returns it (with its UID).
func mkTask(t *testing.T, ctx context.Context, name string) *tataradevv1alpha1.Task {
	t.Helper()
	tk := &tataradevv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tataradevv1alpha1.TaskSpec{
			ProjectRef: "p-test",
			Goal:       "own package test task " + name,
		},
	}
	if err := k8sClient.Create(ctx, tk); err != nil {
		t.Fatalf("create Task %s: %v", name, err)
	}
	t.Cleanup(func() {
		_ = k8sClient.Delete(context.Background(), tk)
	})
	return tk
}

// mkIssue creates a real Issue in the cluster with the given owner refs.
func mkIssue(t *testing.T, ctx context.Context, name string, refs []metav1.OwnerReference) *tataradevv1alpha1.Issue {
	t.Helper()
	iss := &tataradevv1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       testNS,
			OwnerReferences: refs,
		},
		Spec: tataradevv1alpha1.IssueSpec{
			RepositoryRef: "repo-test",
			Number:        7,
			URL:           "https://example.invalid/issues/7",
			ProjectRef:    "p-test",
		},
	}
	if err := k8sClient.Create(ctx, iss); err != nil {
		t.Fatalf("create Issue %s: %v", name, err)
	}
	t.Cleanup(func() {
		_ = k8sClient.Delete(context.Background(), iss)
	})
	return iss
}

func getIssue(t *testing.T, ctx context.Context, name string) *tataradevv1alpha1.Issue {
	t.Helper()
	var iss tataradevv1alpha1.Issue
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &iss); err != nil {
		t.Fatalf("get Issue %s: %v", name, err)
	}
	return &iss
}

// solidOwners is garbagecollector.attemptToDeleteItem's predicate: the owner
// refs of obj whose owner still RESOLVES. The real GC deletes obj iff this is
// empty. envtest runs no GC (see the file header), so the GC-semantics cases
// assert on this.
func solidOwners(t *testing.T, ctx context.Context, obj client.Object) []string {
	t.Helper()
	var solid []string
	for _, r := range obj.GetOwnerReferences() {
		if r.Kind != "Task" {
			continue
		}
		var tk tataradevv1alpha1.Task
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: r.Name}, &tk)
		switch {
		case err == nil:
			// The GC treats an owner whose UID no longer matches as gone.
			if tk.UID == r.UID {
				solid = append(solid, r.Name)
			}
		case apierrors.IsNotFound(err):
		default:
			t.Fatalf("resolve owner %s: %v", r.Name, err)
		}
	}
	return solid
}

// deleteTaskAndWait deletes a Task and blocks until the API server reports it
// gone, so solidOwners() sees the post-delete truth.
func deleteTaskAndWait(t *testing.T, ctx context.Context, tk *tataradevv1alpha1.Task) {
	t.Helper()
	if err := k8sClient.Delete(ctx, tk); err != nil {
		t.Fatalf("delete Task %s: %v", tk.Name, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var got tataradevv1alpha1.Task
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: tk.Namespace, Name: tk.Name}, &got)
		if apierrors.IsNotFound(err) {
			return
		}
		if err != nil {
			t.Fatalf("await Task %s deletion: %v", tk.Name, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("Task %s still present after delete", tk.Name)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Case 1: MULTI-OWNER SURVIVAL (B.1). An Issue owned by Task A (controller)
// and Task B (plain). A is deleted. Because B still resolves, the GC's solid
// set is non-empty, so the Issue is NOT collected. Then the reaper's B.5
// handover promotes B to controller=true and the API server accepts it.
func TestMultiOwnerSurvival(t *testing.T) {
	ctx := context.Background()
	a := mkTask(t, ctx, "own-surv-a")
	b := mkTask(t, ctx, "own-surv-b")

	iss := &tataradevv1alpha1.Issue{}
	AddPlainOwner(iss, a)
	AddPlainOwner(iss, b)
	if err := HandOverController(iss, nil, a); err != nil {
		t.Fatalf("seed controller owner: %v", err)
	}
	created := mkIssue(t, ctx, "iss-own-surv", iss.GetOwnerReferences())

	if name, ok := ControllerOwner(created); !ok || name != a.Name {
		t.Fatalf("controller owner = (%q, %v), want (%s, true)", name, ok, a.Name)
	}

	// B.5: hand the flag over to the surviving plain owner BEFORE deleting A.
	live := getIssue(t, ctx, "iss-own-surv")
	if err := HandOverController(live, a, b); err != nil {
		t.Fatalf("HandOverController: %v", err)
	}
	if err := k8sClient.Update(ctx, live); err != nil {
		t.Fatalf("update Issue after handover: %v", err)
	}

	deleteTaskAndWait(t, ctx, a)

	after := getIssue(t, ctx, "iss-own-surv")
	if got := solidOwners(t, ctx, after); len(got) != 1 || got[0] != b.Name {
		t.Fatalf("solid owners = %v, want [%s]; a non-empty solid set is exactly why the GC keeps the Issue", got, b.Name)
	}
	if name, ok := ControllerOwner(after); !ok || name != b.Name {
		t.Fatalf("controller owner after handover = (%q, %v), want (%s, true)", name, ok, b.Name)
	}
	if len(after.GetOwnerReferences()) != 2 {
		t.Fatalf("owner refs = %d, want 2 (the dead owner's ref stays until the GC prunes it)", len(after.GetOwnerReferences()))
	}
}

// Case 2: CONTROLLER HANDOVER ON FOLD (B.3). The umbrella adopts a member's
// Issue in ONE Update that appends U (controller=false), rewrites M to
// controller=false and rewrites U to controller=true. The API server ACCEPTS
// that (exactly one controller) and REJECTS a hand-rolled two-controller
// object with a 422. Then M is deleted and the Issue survives under U.
func TestControllerHandoverOnFold(t *testing.T) {
	ctx := context.Background()
	member := mkTask(t, ctx, "own-fold-m")
	umbrella := mkTask(t, ctx, "own-fold-u")

	seed := &tataradevv1alpha1.Issue{}
	AddPlainOwner(seed, member)
	if err := HandOverController(seed, nil, member); err != nil {
		t.Fatalf("seed controller owner: %v", err)
	}
	mkIssue(t, ctx, "iss-own-fold", seed.GetOwnerReferences())

	// The API server REJECTS two controller=true refs: 422 Invalid.
	bad := getIssue(t, ctx, "iss-own-fold")
	badRefs := append([]metav1.OwnerReference{}, bad.GetOwnerReferences()...)
	badRefs = append(badRefs, metav1.OwnerReference{
		APIVersion:         tataradevv1alpha1.GroupVersion.String(),
		Kind:               "Task",
		Name:               umbrella.Name,
		UID:                umbrella.UID,
		Controller:         boolPtr(true),
		BlockOwnerDeletion: boolPtr(true),
	})
	bad.SetOwnerReferences(badRefs)
	err := k8sClient.Update(ctx, bad)
	if err == nil {
		t.Fatalf("API server ACCEPTED two controller=true owner refs; the single-PUT swap's whole premise is that it does not")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("two-controller update error = %v (want 422 Invalid)", err)
	}
	var status apierrors.APIStatus
	if !errors.As(err, &status) || status.Status().Code != 422 {
		t.Fatalf("two-controller update status = %v, want HTTP 422 from the API server", err)
	}

	// The fold's ONE Update: append U plain, then swap the flag M -> U.
	fold := getIssue(t, ctx, "iss-own-fold")
	if added := AddPlainOwner(fold, umbrella); !added {
		t.Fatalf("AddPlainOwner(umbrella) reported no change")
	}
	if err := HandOverController(fold, member, umbrella); err != nil {
		t.Fatalf("HandOverController: %v", err)
	}
	if err := k8sClient.Update(ctx, fold); err != nil {
		t.Fatalf("API server REJECTED the single-PUT fold swap: %v", err)
	}

	adopted := getIssue(t, ctx, "iss-own-fold")
	if name, ok := ControllerOwner(adopted); !ok || name != umbrella.Name {
		t.Fatalf("controller owner = (%q, %v), want (%s, true)", name, ok, umbrella.Name)
	}
	if got := len(adopted.GetOwnerReferences()); got != 2 {
		t.Fatalf("owner refs = %d, want 2", got)
	}

	// Adopt, verify, THEN delete: the Issue survives the member's deletion.
	deleteTaskAndWait(t, ctx, member)
	survivor := getIssue(t, ctx, "iss-own-fold")
	if got := solidOwners(t, ctx, survivor); len(got) != 1 || got[0] != umbrella.Name {
		t.Fatalf("solid owners after member delete = %v, want [%s]", got, umbrella.Name)
	}
}

// Case 3: CASCADE ON DELIVERY. An Issue owned ONLY by Task T. T is deleted, so
// EVERY owner ref resolves to gone: len(solid) == 0 and the real GC collects
// the Issue (see the file header on why envtest cannot run that collection).
func TestCascadeOnDelivery(t *testing.T) {
	ctx := context.Background()
	tk := mkTask(t, ctx, "own-cascade-t")

	seed := &tataradevv1alpha1.Issue{}
	AddPlainOwner(seed, tk)
	if err := HandOverController(seed, nil, tk); err != nil {
		t.Fatalf("seed controller owner: %v", err)
	}
	mkIssue(t, ctx, "iss-own-cascade", seed.GetOwnerReferences())

	deleteTaskAndWait(t, ctx, tk)

	iss := getIssue(t, ctx, "iss-own-cascade")
	if got := solidOwners(t, ctx, iss); len(got) != 0 {
		t.Fatalf("solid owners = %v, want none: the GC collects the Issue iff every owner ref resolves to gone", got)
	}
	if len(iss.GetOwnerReferences()) == 0 {
		t.Fatalf("Issue has no owner refs at all; it would LEAK, not cascade")
	}
}

// Case 4: ZERO OWNERS ARE NEVER COLLECTED. This half of B.1 is what makes "no
// SCM artifact without a Task" structural: an Issue with NO ownerRefs is never
// GC'd, so it is never silently lost - the sweep's orphan predicate is what
// picks it back up.
func TestZeroOwnersNeverCollected(t *testing.T) {
	ctx := context.Background()
	mkIssue(t, ctx, "iss-own-zero", nil)

	iss := getIssue(t, ctx, "iss-own-zero")
	if got := len(iss.GetOwnerReferences()); got != 0 {
		t.Fatalf("owner refs = %d, want 0", got)
	}
	if got := solidOwners(t, ctx, iss); len(got) != 0 {
		t.Fatalf("solid owners = %v, want none", got)
	}
	if _, ok := ControllerOwner(iss); ok {
		t.Fatalf("an unowned Issue reports a controller owner")
	}

	// The GC never enqueues a dependent with zero owner refs at all, so the
	// object stays. Re-read to show it is still there.
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "iss-own-zero"}, &tataradevv1alpha1.Issue{}); err != nil {
		t.Fatalf("zero-owner Issue disappeared: %v", err)
	}
}

// Case 5: ZERO-CONTROLLER-OWNER REPAIR (B.2 rule 5). An Issue with two plain
// owners and no controller owner is worked by nobody and re-minted by nobody
// (the orphan predicate sees an OWNED Issue). RepairZeroController promotes
// the OLDEST surviving owner, logs at ERROR, and increments
// operator_orphan_no_controller_total.
func TestRepairZeroController(t *testing.T) {
	ctx := context.Background()
	a := mkTask(t, ctx, "own-repair-a")
	b := mkTask(t, ctx, "own-repair-b")

	seed := &tataradevv1alpha1.Issue{}
	AddPlainOwner(seed, a)
	AddPlainOwner(seed, b)
	mkIssue(t, ctx, "iss-own-repair", seed.GetOwnerReferences())

	iss := getIssue(t, ctx, "iss-own-repair")
	if _, ok := ControllerOwner(iss); ok {
		t.Fatalf("hand-crafted Issue already has a controller owner")
	}

	sink := &recordingSink{}
	lctx := logf.IntoContext(ctx, logr.New(sink))
	before := testutil.ToFloat64(obs.OrphanNoControllerTotal)

	repaired, err := RepairZeroController(lctx, k8sClient, iss)
	if err != nil {
		t.Fatalf("RepairZeroController: %v", err)
	}
	if !repaired {
		t.Fatalf("RepairZeroController reported no repair on a zero-controller Issue")
	}

	fixed := getIssue(t, ctx, "iss-own-repair")
	name, ok := ControllerOwner(fixed)
	if !ok || name != a.Name {
		t.Fatalf("controller owner after repair = (%q, %v), want the OLDEST surviving owner (%s, true)", name, ok, a.Name)
	}
	if got := len(fixed.GetOwnerReferences()); got != 2 {
		t.Fatalf("owner refs after repair = %d, want 2", got)
	}
	if after := testutil.ToFloat64(obs.OrphanNoControllerTotal); after != before+1 {
		t.Fatalf("operator_orphan_no_controller_total = %v, want %v", after, before+1)
	}
	if sink.errors != 1 {
		t.Fatalf("ERROR log lines = %d, want 1", sink.errors)
	}

	// Idempotent: a second pass sees a healthy object and does nothing.
	repaired, err = RepairZeroController(lctx, k8sClient, fixed)
	if err != nil {
		t.Fatalf("second RepairZeroController: %v", err)
	}
	if repaired {
		t.Fatalf("RepairZeroController repaired an already-healthy Issue")
	}
	if after := testutil.ToFloat64(obs.OrphanNoControllerTotal); after != before+1 {
		t.Fatalf("guard fired again on a healthy Issue: counter = %v", after)
	}

	// A dead owner is skipped: delete A, strip the flag, repair promotes B.
	stripped := getIssue(t, ctx, "iss-own-repair")
	refs := stripped.GetOwnerReferences()
	for i := range refs {
		refs[i].Controller = nil
	}
	stripped.SetOwnerReferences(refs)
	if err := k8sClient.Update(ctx, stripped); err != nil {
		t.Fatalf("strip controller flag: %v", err)
	}
	deleteTaskAndWait(t, ctx, a)

	iss = getIssue(t, ctx, "iss-own-repair")
	repaired, err = RepairZeroController(lctx, k8sClient, iss)
	if err != nil {
		t.Fatalf("RepairZeroController after owner death: %v", err)
	}
	if !repaired {
		t.Fatalf("RepairZeroController reported no repair with a dead oldest owner")
	}
	fixed = getIssue(t, ctx, "iss-own-repair")
	if name, ok := ControllerOwner(fixed); !ok || name != b.Name {
		t.Fatalf("controller owner = (%q, %v), want the oldest SURVIVING owner (%s, true)", name, ok, b.Name)
	}
}

// recordingSink counts ERROR-level log calls so the repair guard's mandated
// ERROR log is asserted, not assumed.
type recordingSink struct {
	errors int
}

func (s *recordingSink) Init(logr.RuntimeInfo)          {}
func (s *recordingSink) Enabled(int) bool               { return true }
func (s *recordingSink) Info(int, string, ...any)       {}
func (s *recordingSink) Error(error, string, ...any)    { s.errors++ }
func (s *recordingSink) WithValues(...any) logr.LogSink { return s }
func (s *recordingSink) WithName(string) logr.LogSink   { return s }
