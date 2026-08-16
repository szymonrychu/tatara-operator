package controller

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// A FRESH TAKEOVER IS MINTED STRAIGHT INTO THE WORK (#604), not into the gate.
// It owns zero Issue CRs by construction - Source.IsPR is always true, so
// mintIssueCRs bails - and `refined`'s only forward edge is the approval gate,
// which refuses no-live-issue for a Task owning zero Issues. Minted there it
// burned a pod and parked awaiting-human, forever. under-implementation is also
// exactly where the re-take un-park below already landed, so the two takeover
// entry paths now agree instead of one of them being unreachable.
func TestMintOrUnparkTakeoverTask_MintsBoundIntoUnderImplementation(t *testing.T) {
	ctx := context.Background()
	proj, repo := seedProjectRepo(t, ctx)
	mr := seedOpenExternalMR(t, ctx, proj, repo, 7, "renovate/foo", "octocat") // author != bot

	m := newTestMinter(t)
	task, err := m.MintOrUnparkTakeoverTask(ctx, proj, repo, mr, "alice", "please take over and fix conflicts", testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	if task.Spec.Kind != "takeover" {
		t.Fatalf("kind = %q", task.Spec.Kind)
	}
	if task.Spec.InitialState != tatarav1alpha1.StateUnderImplementation {
		t.Fatalf("initial state = %q, want under-implementation: a takeover owns zero Issues, so a "+
			"mint into refined can never pass the gate", task.Spec.InitialState)
	}
	if task.Annotations[tatarav1alpha1.AnnTakeoverHeadBranch] != "renovate/foo" {
		t.Fatalf("push branch annotation = %q", task.Annotations[tatarav1alpha1.AnnTakeoverHeadBranch])
	}
	if task.Spec.Source == nil || !task.Spec.Source.IsPR || task.Spec.Source.Number != 7 {
		t.Fatalf("source not bound to the MR: %+v", task.Spec.Source)
	}
	// The takeover Task controller-owns the MR mirror after mint.
	got := getMR(t, ctx, proj, repo, 7)
	if ctrl, ok := ownerControllerName(got); !ok || ctrl != task.Name {
		t.Fatalf("takeover Task must controller-own the MR; owner=%q", ctrl)
	}
}

func TestMintOrUnparkTakeoverTask_UnparksExisting(t *testing.T) {
	ctx := context.Background()
	proj, repo := seedProjectRepo(t, ctx)
	mr := seedOpenExternalMR(t, ctx, proj, repo, 8, "renovate/bar", "octocat")
	m := newTestMinter(t)

	first, err := m.MintOrUnparkTakeoverTask(ctx, proj, repo, mr, "alice", "take over", testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a stand-down: park it ownership-lost.
	parkTaskOwnershipLost(t, ctx, first)

	second, err := m.MintOrUnparkTakeoverTask(ctx, proj, repo, mr, "alice", "take over again", testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	if second.Name != first.Name {
		t.Fatalf("re-take must reuse the same Task: %q vs %q", first.Name, second.Name)
	}
	got := getTask(t, second.Name)
	if got.Status.State != tatarav1alpha1.StateUnderImplementation {
		t.Fatalf("re-take must re-enter under-implementation, got %q", got.Status.State)
	}
	if tatarav1alpha1.Parked(got) {
		t.Fatalf("re-take must clear the park flag, still parked: %q", got.Status.ParkReason)
	}
}

// TestMintOrUnparkTakeoverTask_ExistingNotOwnershipLostPark_ReturnedUnchanged
// covers the fall-through branch (takeover_mint.go's `return &existing, nil`
// with no re-enter): a Task already exists for this MR and is EITHER live
// (any non-parked stage) OR parked for some OTHER reason than ownership-lost.
// OP9's takeover endpoint relies on this being a pure no-op - repeat calls
// (e.g. a maintainer posting "take over" twice, or once while the Task is
// mid-flight) must never re-enter, re-mint, or otherwise mutate the Task.
func TestMintOrUnparkTakeoverTask_ExistingNotOwnershipLostPark_ReturnedUnchanged(t *testing.T) {
	cases := []struct {
		name   string
		stg    string
		reason string
	}{
		{"live task mid-review is returned unchanged", tatarav1alpha1.StateAwaitingReview, ""},
		{"parked for a non-ownership-lost reason is returned unchanged", tatarav1alpha1.StateUnderImplementation, stage.ReasonAwaitingHuman},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			proj, repo := seedProjectRepo(t, ctx)
			mr := seedOpenExternalMR(t, ctx, proj, repo, 20+i, "renovate/qux", "octocat")
			m := newTestMinter(t)

			first, err := m.MintOrUnparkTakeoverTask(ctx, proj, repo, mr, "alice", "take over", testSpiller(t))
			if err != nil {
				t.Fatal(err)
			}
			stampTaskStatus(t, ctx, first, tc.stg, tc.reason)

			second, err := m.MintOrUnparkTakeoverTask(ctx, proj, repo, mr, "alice", "take over again", testSpiller(t))
			if err != nil {
				t.Fatal(err)
			}
			if second.Name != first.Name || second.UID != first.UID {
				t.Fatalf("fall-through must return the SAME task: first=%s/%s second=%s/%s",
					first.Name, first.UID, second.Name, second.UID)
			}

			got := getTask(t, second.Name)
			if got.Status.State != tc.stg {
				t.Fatalf("stage mutated: got %q, want unchanged %q", got.Status.State, tc.stg)
			}
			if got.Status.ParkReason != tc.reason {
				t.Fatalf("stage reason mutated: got %q, want unchanged %q", got.Status.ParkReason, tc.reason)
			}

			// No second takeover Task was minted for this MR.
			var list tatarav1alpha1.TaskList
			if err := k8sClient.List(ctx, &list); err != nil {
				t.Fatalf("list tasks: %v", err)
			}
			count := 0
			for j := range list.Items {
				if list.Items[j].Spec.ProjectRef == proj.Name && list.Items[j].Spec.Kind == takeoverKind {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("expected exactly 1 takeover task for %s, got %d", proj.Name, count)
			}
		})
	}
}

// AN INTERRUPTED MINT MUST BACKFILL mrRefs ON THE ENDPOINT RETRY (#604).
//
// createTaskRaceSafe, bindMRToTask and stampMintStatus are three writes, not
// one: a mint that errors between the first and the last leaves a Task with MR
// ownership already flipped and status.mrRefs EMPTY. At `refined` that was
// merely another way to be stuck. At `under-implementation` it is worse, because
// mrRefs is load-bearing in two places:
//
//   - submit_outcome(action=submitted) is gated on >= 1 owned MR being open, so
//     the agent's turn 400s no-open-mr and it can never leave.
//   - mr_write(action=open)'s idempotency loop reads status.mrRefs, so an agent
//     working around that opens a DUPLICATE merge request against the human's PR.
//
// reconcileMRBindingBackstop does not cover it: repairMRBinding returned nil for
// any MR CR that already had a controller owner, and a takeover's always does.
// So the endpoint retry - the maintainer simply commenting again - is the repair
// path, and it has to fall through its own idempotent early return to run.
func TestMintOrUnparkTakeoverTask_BackfillsMRRefsAfterAnInterruptedMint(t *testing.T) {
	ctx := context.Background()
	proj, repo := seedProjectRepo(t, ctx)
	mr := seedOpenExternalMR(t, ctx, proj, repo, 31, "renovate/interrupted", "octocat")
	m := newTestMinter(t)

	first, err := m.MintOrUnparkTakeoverTask(ctx, proj, repo, mr, "alice", "take over", testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	wantRef := tatarav1alpha1.MergeRequestName(repo.Name, 31)

	// Reproduce the interrupted mint: the Task and the MR binding exist, the
	// mrRefs stamp never landed.
	clearTaskMRRefs(t, ctx, first)

	second, err := m.MintOrUnparkTakeoverTask(ctx, proj, repo, mr, "alice", "take over again", testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	if second.Name != first.Name {
		t.Fatalf("the retry must reuse the same Task: %q vs %q", first.Name, second.Name)
	}
	got := getTask(t, second.Name)
	if !slices.Contains(got.Status.MRRefs, wantRef) {
		t.Fatalf("mrRefs = %v, want the backfilled %q: without it submit_outcome(submitted) 400s "+
			"no-open-mr and mr_write(open) opens a duplicate PR", got.Status.MRRefs, wantRef)
	}
	// Still exactly one ref: the backfill must not duplicate on a third call.
	third, err := m.MintOrUnparkTakeoverTask(ctx, proj, repo, mr, "alice", "and again", testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	got = getTask(t, third.Name)
	if len(got.Status.MRRefs) != 1 {
		t.Fatalf("mrRefs = %v, want exactly one entry after a repeat call", got.Status.MRRefs)
	}
}

// repairMRBinding's own half of the same hole: an MR CR whose controller owner
// IS this Task, with the Task's mrRefs empty, must get the ref stamped rather
// than being skipped as "already bound". The never-steal rule is unchanged for
// a CR owned by anyone ELSE - that is the next assertion.
func TestRepairMRBinding_StampsRefWhenTheBindingIsAlreadyOurs(t *testing.T) {
	ctx := context.Background()
	proj, repo := seedProjectRepo(t, ctx)
	mr := seedOpenExternalMR(t, ctx, proj, repo, 32, "renovate/ours", "octocat")
	m := newTestMinter(t)

	task, err := m.MintOrUnparkTakeoverTask(ctx, proj, repo, mr, "alice", "take over", testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	clearTaskMRRefs(t, ctx, task)

	if err := m.repairMRBinding(ctx, proj, repo, mrExtFromMR(mr), task, testSpiller(t)); err != nil {
		t.Fatalf("repair: %v", err)
	}
	got := getTask(t, task.Name)
	want := tatarav1alpha1.MergeRequestName(repo.Name, 32)
	if !slices.Contains(got.Status.MRRefs, want) {
		t.Fatalf("mrRefs = %v, want %q stamped: the CR is controller-owned by THIS task, so "+
			"'already bound' is only half true - the ref stamp is what was interrupted", got.Status.MRRefs, want)
	}
}

// THE NEVER-STEAL RULE IS UNCHANGED. A CR controller-owned by a DIFFERENT Task
// is left completely alone, and this Task's mrRefs stay empty: stamping a ref to
// an MR we do not own would give this Task a merge request another Task drives.
func TestRepairMRBinding_LeavesAForeignOwnedMRAlone(t *testing.T) {
	ctx := context.Background()
	proj, repo := seedProjectRepo(t, ctx)
	mr := seedOpenExternalMR(t, ctx, proj, repo, 33, "renovate/theirs", "octocat")
	m := newTestMinter(t)

	owner, err := m.MintOrUnparkTakeoverTask(ctx, proj, repo, mr, "alice", "take over", testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}

	// A second, unrelated Task tries to repair the SAME merge request.
	stranger := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: owner.Name + "-stranger", Namespace: proj.Namespace},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: repo.Name, Kind: "review", Goal: "not mine",
		},
	}
	if err := k8sClient.Create(ctx, stranger); err != nil {
		t.Fatalf("create stranger task: %v", err)
	}
	if err := m.repairMRBinding(ctx, proj, repo, mrExtFromMR(mr), stranger, testSpiller(t)); err != nil {
		t.Fatalf("repair must be a silent no-op, got: %v", err)
	}
	if got := getTask(t, stranger.Name); len(got.Status.MRRefs) != 0 {
		t.Fatalf("a foreign-owned MR must never be stamped onto another Task, mrRefs = %v", got.Status.MRRefs)
	}
	if ctrl, ok := ownerControllerName(getMR(t, ctx, proj, repo, 33)); !ok || ctrl != owner.Name {
		t.Fatalf("controller ownership moved to %q; never steal", ctrl)
	}
}

// --- envtest fixtures local to the takeover minter tests ---

// clearTaskMRRefs wipes status.mrRefs, reproducing a mint that errored between
// createTaskRaceSafe/bindMRToTask and stampMintStatus.
func clearTaskMRRefs(t *testing.T, ctx context.Context, task *tatarav1alpha1.Task) {
	t.Helper()
	var fresh tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(task), &fresh); err != nil {
		t.Fatalf("get task %s: %v", task.Name, err)
	}
	fresh.Status.MRRefs = nil
	if err := k8sClient.Status().Update(ctx, &fresh); err != nil {
		t.Fatalf("clear mrRefs on %s: %v", task.Name, err)
	}
}

// takeoverTestSlug derives a short, valid k8s name segment from the running
// test's name, so parallel tests sharing the ONE envtest control plane (see
// suite_test.go's package-wide k8sClient) never collide on a Project/Repository
// name.
func takeoverTestSlug(t *testing.T) string {
	t.Helper()
	s := strings.ToLower(t.Name())
	s = regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if len(s) > 40 {
		s = s[len(s)-40:]
	}
	// Trim AGAIN: the truncation above cuts at a fixed offset, so a long enough
	// test name lands it mid-separator and yields a leading "-", which is not a
	// legal RFC 1123 name and fails the Secret create with an error that names
	// the fixture rather than the test.
	return strings.Trim(s, "-")
}

// seedProjectRepo creates a minimal live Project+Repository pair for the
// takeover minter tests, uniquely named per test (see takeoverTestSlug).
func seedProjectRepo(t *testing.T, ctx context.Context) (*tatarav1alpha1.Project, *tatarav1alpha1.Repository) {
	t.Helper()
	name := takeoverTestSlug(t)
	mkSecret(t, name+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: name + "-scm",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-repo", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: name, URL: "https://github.com/o/r.git", DefaultBranch: "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}
	return proj, repo
}

// seedOpenExternalMR builds the MergeRequest CR value the minter is handed, as
// if it were freshly read by the caller (OP9's takeover endpoint) from a live
// MR. It is NOT persisted here: MintOrUnparkTakeoverTask's own bindMRToTask is
// what creates/upserts the mirror, and the tests assert that write happened.
func seedOpenExternalMR(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, headBranch, author string) *tatarav1alpha1.MergeRequest {
	t.Helper()
	_ = ctx
	return &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.MergeRequestName(repo.Name, number),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo.Name,
			ProjectRef:    proj.Name,
			Number:        number,
			URL:           fmt.Sprintf("https://github.com/o/r/pull/%d", number),
		},
		Status: tatarav1alpha1.MergeRequestStatus{
			Title:      fmt.Sprintf("external change #%d", number),
			Author:     author,
			State:      "open",
			HeadBranch: headBranch,
			HeadSHA:    fmt.Sprintf("sha-%d", number),
		},
	}
}

// newTestMinter builds a Minter bound to the package's shared envtest client.
func newTestMinter(t *testing.T) *Minter {
	t.Helper()
	return &Minter{Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme()}
}

// testSpiller is a Spiller that fails the test if ever actually called: none
// of these fixtures approach the A.7 byte budget, so a spill here means the
// test built something unexpectedly huge.
func testSpiller(t *testing.T) objbudget.Spiller {
	t.Helper()
	return &mirrorSpiller{}
}

// getMR fetches the live MergeRequest CR mirror for (repo, number).
func getMR(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int) *tatarav1alpha1.MergeRequest {
	t.Helper()
	var mr tatarav1alpha1.MergeRequest
	key := client.ObjectKey{Namespace: proj.Namespace, Name: tatarav1alpha1.MergeRequestName(repo.Name, number)}
	if err := k8sClient.Get(ctx, key, &mr); err != nil {
		t.Fatalf("get mergerequest %s: %v", key.Name, err)
	}
	return &mr
}

// ownerControllerName is own.ControllerOwner, named for readability at the
// call site of a test assertion.
func ownerControllerName(obj client.Object) (string, bool) {
	return own.ControllerOwner(obj)
}

// parkTaskOwnershipLost stamps task directly into parked(ownership-lost),
// simulating an external-push stand-down (OP3) without driving the full
// under-implementation->parked(ownership-lost) transition sequence: that
// sequence is OP3's own coverage, and re-deriving it here would just be
// exercising the same edges a second time under a different test's name.
func parkTaskOwnershipLost(t *testing.T, ctx context.Context, task *tatarav1alpha1.Task) {
	t.Helper()
	stampTaskStatus(t, ctx, task, tatarav1alpha1.StateUnderImplementation, stage.ReasonOwnershipLost)
}

// stampTaskStatus writes state/reason straight onto task's status, bypassing
// stage.Enter's legality checks, for tests that need to PLACE a Task in some
// state (optionally parked) without re-deriving how it got there (that
// derivation is each state's own edge's coverage elsewhere). reason lands on
// status.parkReason (a park is a flag orthogonal to state, #521) unless state
// is done/rejected, where it lands on status.stateReason instead.
func stampTaskStatus(t *testing.T, ctx context.Context, task *tatarav1alpha1.Task, state, reason string) {
	t.Helper()
	var fresh tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(task), &fresh); err != nil {
		t.Fatalf("get task %s: %v", task.Name, err)
	}
	now := metav1.Now()
	fresh.Status.State = state
	fresh.Status.ParkReason = ""
	fresh.Status.StateReason = ""
	if reason != "" {
		if state == tatarav1alpha1.StateDone || state == tatarav1alpha1.StateRejected {
			fresh.Status.StateReason = reason
		} else {
			fresh.Status.ParkReason = reason
			fresh.Status.ParkedFromState = state
		}
	}
	fresh.Status.StateEnteredAt = &now
	fresh.Status.PodStartedAt = nil
	if err := k8sClient.Status().Update(ctx, &fresh); err != nil {
		t.Fatalf("stamp task %s state=%s reason=%s: %v", task.Name, state, reason, err)
	}
	*task = fresh
}

// The takeover mint is a direct Task create too, and carried the same #517
// pod-name gap as the intake and docbatch mints. The envtest project/repo names
// are derived from the test name and are deliberately long, so this also covers
// BuildPodName's 63-char trim: the type and id segments survive intact, the
// project and repo segments are what give.
func TestMintOrUnparkTakeoverTask_StampsPodName(t *testing.T) {
	ctx := context.Background()
	proj, repo := seedProjectRepo(t, ctx)
	mr := seedOpenExternalMR(t, ctx, proj, repo, 11, "renovate/baz", "octocat")

	m := newTestMinter(t)
	task, err := m.MintOrUnparkTakeoverTask(ctx, proj, repo, mr, "alice", "take over", testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	got := task.Annotations[agent.PodNameAnnotation]
	if got == "" {
		t.Fatalf("takeover mint stamped no pod name; it would fall back to wrapper-%s", task.Name)
	}
	if len(got) > 63 {
		t.Fatalf("pod name %q is %d chars, over the DNS-1123 budget", got, len(got))
	}
	if !strings.HasPrefix(got, "tko-") || !strings.HasSuffix(got, "-p11") {
		t.Fatalf("pod name = %q, want tko-<project>-<repo>-p11", got)
	}
	if got != agent.PodName(task) {
		t.Fatalf("PodName = %q, want the stamped %q", agent.PodName(task), got)
	}
	// The takeover head-branch annotation the literal already carried must
	// survive the stamp (StampPodName adds to the map, never replaces it).
	if task.Annotations[tatarav1alpha1.AnnTakeoverHeadBranch] != "renovate/baz" {
		t.Fatalf("stamping clobbered the takeover head-branch annotation: %+v", task.Annotations)
	}
}

// THE TAKEOVER BRANCH DERIVATION IS PINNED, AND THE MIRROR TITLE IT COMES FROM
// IS NOT INERT ANY MORE.
//
// mirror.go assigns mr.Status.Title unconditionally, and the sweep's mrSnapshot
// used to leave it empty - so on a live project every sweep pass BLANKED it. It
// now carries the real title (the adopted merge request's body/title are review
// inputs), which makes this chain live where it used to be dead:
//
//	mr.Status.Title -> Source.Title (frozen at mint) -> agent.TaskBranch
//	  -> ourMR (reaper.go), the gate on CLOSING a merge request and DELETING its
//	     head branch.
//
// Source.Title is IMMUTABLE spec, so a title the maintainer edits afterwards
// cannot move a live Task's verdict - that is the property this pins, and it is
// the reason freezing at mint is the right design rather than reading the mirror
// at reap time. Low probability, terminal consequence: keep it pinned.
func TestMintOrUnparkTakeoverTask_FreezesTheBranchAtTheTitleItSaw(t *testing.T) {
	ctx := context.Background()
	proj, repo := seedProjectRepo(t, ctx)
	m := newTestMinter(t)
	mr := seedOpenExternalMR(t, ctx, proj, repo, 61, "contrib/fix", "octocat")
	mr.Status.Title = "Fix the flaky reaper test"

	task, err := m.MintOrUnparkTakeoverTask(ctx, proj, repo, mr, "alice", "take over", testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	if task.Spec.Source.Title != "Fix the flaky reaper test" {
		t.Fatalf("Source.Title = %q, want the title the mint SAW", task.Spec.Source.Title)
	}
	const want = "tatara/feat-61-fix-the-flaky-reaper-test"
	if got := agent.TaskBranch(task); got != want {
		t.Fatalf("TaskBranch = %q, want %q: ourMR keys on this exact string", got, want)
	}

	// The forge title changes (a maintainer edits it, or the engine retargets a
	// bump). The mirror follows; the Task's branch derivation must NOT.
	mr.Status.Title = "Fix the flaky reaper test (v2)"
	same := getTask(t, task.Name)
	if got := agent.TaskBranch(same); got != want {
		t.Fatalf("TaskBranch moved to %q after a mirror title change: a live Task's ourMR verdict "+
			"is not stable, and ourMR gates closing a merge request and deleting its head branch", got)
	}
}
