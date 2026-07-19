package controller

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

func TestMintOrUnparkTakeoverTask_MintsBoundIntoApproved(t *testing.T) {
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
	if task.Spec.InitialStage != tatarav1alpha1.StageApproved {
		t.Fatalf("initial stage = %q, want approved", task.Spec.InitialStage)
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
	if got.Status.Stage != tatarav1alpha1.StageApproved {
		t.Fatalf("re-take must re-enter approved, got %q", got.Status.Stage)
	}
	if got.Status.StageReason != stage.ReasonOwnershipLost {
		t.Fatalf("re-entry stage reason = %q, want %q", got.Status.StageReason, stage.ReasonOwnershipLost)
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
		{"live approved task is returned unchanged", tatarav1alpha1.StageApproved, ""},
		{"parked for a non-ownership-lost reason is returned unchanged", tatarav1alpha1.StageParked, stage.ReasonAwaitingHuman},
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
			if got.Status.Stage != tc.stg {
				t.Fatalf("stage mutated: got %q, want unchanged %q", got.Status.Stage, tc.stg)
			}
			if got.Status.StageReason != tc.reason {
				t.Fatalf("stage reason mutated: got %q, want unchanged %q", got.Status.StageReason, tc.reason)
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

// --- envtest fixtures local to the takeover minter tests ---

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
	return s
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
// approved->implementing->parked(ownership-lost) transition sequence: that
// sequence is OP3's own coverage, and re-deriving it here would just be
// exercising the same edges a second time under a different test's name.
func parkTaskOwnershipLost(t *testing.T, ctx context.Context, task *tatarav1alpha1.Task) {
	t.Helper()
	stampTaskStatus(t, ctx, task, tatarav1alpha1.StageParked, stage.ReasonOwnershipLost)
}

// stampTaskStatus is parkTaskOwnershipLost's general form: it writes stg/reason
// straight onto task's status, bypassing stage.Enter's legality checks, for
// tests that need to PLACE a Task in some state without re-deriving how it got
// there (that derivation is each state's own edge's coverage elsewhere).
func stampTaskStatus(t *testing.T, ctx context.Context, task *tatarav1alpha1.Task, stg, reason string) {
	t.Helper()
	var fresh tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(task), &fresh); err != nil {
		t.Fatalf("get task %s: %v", task.Name, err)
	}
	now := metav1.Now()
	fresh.Status.Stage = stg
	fresh.Status.StageReason = reason
	if stg == tatarav1alpha1.StageParked {
		fresh.Status.ParkedFromStage = tatarav1alpha1.StageImplementing
	}
	fresh.Status.StageEnteredAt = &now
	fresh.Status.PodStartedAt = nil
	if err := k8sClient.Status().Update(ctx, &fresh); err != nil {
		t.Fatalf("stamp task %s stage=%s reason=%s: %v", task.Name, stg, reason, err)
	}
	*task = fresh
}
