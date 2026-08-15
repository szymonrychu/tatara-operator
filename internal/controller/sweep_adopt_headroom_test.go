package controller

import (
	"context"
	"strconv"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// headroomProject arms sweepProject for adoption with an explicit lane cap.
func headroomProject(name string, maxOpen int) *tatarav1alpha1.Project {
	p := sweepProject(name)
	p.Spec.UpgradePolicy = &tatarav1alpha1.UpgradePolicySpec{AdoptBranchPrefix: "renovate/"}
	p.Spec.Scm.Cron = &tatarav1alpha1.ScmCron{
		Upgrade: tatarav1alpha1.UpgradeActivity{MaxOpenUpgrades: maxOpen},
	}
	return p
}

// enginePR is a listing row from the dependency engine, authored by the bot.
func enginePR(repo *tatarav1alpha1.Repository, number int) scm.PRRef {
	return scm.PRRef{
		Repo:       "szymonrychu/tatara-operator",
		HeadRepo:   "szymonrychu/tatara-operator",
		Number:     number,
		Author:     "tatara-bot",
		Title:      "chore(deps): bump " + strconv.Itoa(number),
		HeadBranch: "renovate/dep-" + strconv.Itoa(number),
		HeadSHA:    "sha-" + strconv.Itoa(number),
	}
}

// seedAdoptedLane persists an already-adopted, still-live upgrade Task and the
// MergeRequest mirror it controller-owns: the steady state of a merge request
// this project adopted on an earlier pass and has not merged yet.
func seedAdoptedLane(t *testing.T, ctx context.Context, c client.Client,
	proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) {
	t.Helper()

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      AdoptedUpgradeTaskName(proj.Name, repo.Name, number),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Kind:          "upgrade",
			Goal:          "already adopted",
			MergeOrder:    []string{repo.Name},
			Source: &tatarav1alpha1.TaskSource{
				Number: number, IsPR: true, Title: "chore(deps): bump " + strconv.Itoa(number),
			},
		},
	}
	if err := c.Create(ctx, task); err != nil {
		t.Fatalf("seed adopted task %d: %v", number, err)
	}
	task.Status.State = tatarav1alpha1.StateAwaitingReview
	if err := c.Status().Update(ctx, task); err != nil {
		t.Fatalf("stamp adopted task %d: %v", number, err)
	}

	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.MergeRequestName(repo.Name, number),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo.Name, ProjectRef: proj.Name, Number: number,
			URL: "https://github.com/szymonrychu/tatara-operator/pull/" + strconv.Itoa(number),
		},
	}
	own.AddPlainOwner(mr, task)
	if err := own.HandOverController(mr, nil, task); err != nil {
		t.Fatalf("hand over controller for %d: %v", number, err)
	}
	if err := c.Create(ctx, mr); err != nil {
		t.Fatalf("seed adopted mirror %d: %v", number, err)
	}
	mr.Status.State = "open"
	mr.Status.Author = "tatara-bot"
	mr.Status.HeadBranch = "renovate/dep-" + strconv.Itoa(number)
	mr.Status.HeadSHA = "sha-" + strconv.Itoa(number)
	if err := c.Status().Update(ctx, mr); err != nil {
		t.Fatalf("stamp adopted mirror %d: %v", number, err)
	}
}

// THE HEADROOM WINDOW MUST BE COMPUTED OVER WHAT THIS PASS CAN ACTUALLY ADOPT.
//
// The measured shape: maxOpenUpgrades=3, merge requests 42 and 43 already
// adopted and still open, 44 unowned. The lane count is 2, so the headroom is 1
// - correctly - but the oldest-first WINDOW was selected by a shape-only
// predicate that ignores ownership, so it contained {42}: the two merge requests
// this pass can do nothing about consumed the slot, and 44 was skipped
// upgrade_headroom_bound. Forever, on every pass, because 42 and 43 are always
// the lowest-numbered. Effective utilisation caps near maxOpenUpgrades/2.
func TestSweepPRs_AlreadyAdoptedMergeRequestsDoNotConsumeTheAdoptionWindow(t *testing.T) {
	ctx := context.Background()
	proj := headroomProject("headroom-proj", 3)
	repo := sweepRepo("headroom-proj")
	c := newMirrorClient(t, proj, repo)

	seedAdoptedLane(t, ctx, c, proj, repo, 42)
	seedAdoptedLane(t, ctx, c, proj, repo, 43)

	// The forge lists newest-first, which is what makes the ordering matter.
	rd := &sweepReader{prs: []scm.PRRef{enginePR(repo, 44), enginePR(repo, 43), enginePR(repo, 42)}}
	runSweep(t, c, proj, repo, rd)

	want := AdoptedUpgradeTaskName(proj.Name, repo.Name, 44)
	var got tatarav1alpha1.Task
	if err := c.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: want}, &got); err != nil {
		t.Fatalf("merge request 44 was NOT adopted although one lane of three is free: %v.\n"+
			"The headroom window was spent on 42 and 43, which this pass could never adopt: "+
			"they are already owned.", err)
	}
}

// THE CAP ITSELF STILL BINDS, and it still binds OLDEST-FIRST. One free lane and
// three unowned merge requests means exactly one adoption this pass, and it must
// be the oldest of the three even though the forge listed it last.
func TestSweepPRs_TheFreeLaneGoesToTheOldestUnownedMergeRequest(t *testing.T) {
	ctx := context.Background()
	proj := headroomProject("headroom-order-proj", 1)
	repo := sweepRepo("headroom-order-proj")
	c := newMirrorClient(t, proj, repo)

	rd := &sweepReader{prs: []scm.PRRef{enginePR(repo, 71), enginePR(repo, 70), enginePR(repo, 69)}}
	runSweep(t, c, proj, repo, rd)

	for _, tc := range []struct {
		number int
		want   bool
	}{{69, true}, {70, false}, {71, false}} {
		name := AdoptedUpgradeTaskName(proj.Name, repo.Name, tc.number)
		err := c.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: name}, &tatarav1alpha1.Task{})
		if tc.want && err != nil {
			t.Fatalf("merge request %d (the oldest) was not adopted into the one free lane: %v", tc.number, err)
		}
		if !tc.want && err == nil {
			t.Fatalf("merge request %d adopted past a maxOpenUpgrades of 1", tc.number)
		}
	}
}

// INERTNESS ON RELEASE. Every behavioural difference this branch introduces is
// gated on adoptBranchPrefix being non-empty (default) AND the author being one
// the project owns. With the prefix empty NOTHING may change: no adoption, no
// headroom, no window, and the merge requests the engine already has open must
// classify exactly as they did before.
func TestSweepAdoption_IsInertWithNoAdoptBranchPrefix(t *testing.T) {
	proj := headroomProject("inert-proj", 3)
	proj.Spec.UpgradePolicy.AdoptBranchPrefix = "" // the shipped default
	proj.Spec.Scm.PRReactionScope = "labeledOrMentioned"
	repo := sweepRepo("inert-proj")
	c := newMirrorClient(t, proj, repo)

	engine := enginePR(repo, 44)
	human := enginePR(repo, 45)
	human.Author = "alice"
	human.HeadBranch = "feat/human"

	// Classification is byte-identical to the pre-adoption dispositions.
	if got := ClassifyPR(proj, repo, engine, nil, "", nil); got != PRIgnore {
		t.Fatalf("bot-authored engine MR = %v, want PRIgnore (clause 2)", got)
	}
	if got := ClassifyPR(proj, repo, human, nil, "", nil); got != PRIgnore {
		t.Fatalf("unlabelled human MR = %v, want PRIgnore (out of reaction scope)", got)
	}

	runSweep(t, c, proj, repo, &sweepReader{prs: []scm.PRRef{engine, human}})

	if n := len(sweepTasks(t, c, proj.Name)); n != 0 {
		t.Fatalf("an inert project minted %d Tasks, want 0", n)
	}
	// And the author gate is the SECOND lock: even with the prefix armed, the
	// engine running under a human's token adopts nothing.
	armed := headroomProject("inert-authorgate-proj", 3)
	armed.Spec.Scm.PRReactionScope = "labeledOrMentioned"
	armedRepo := sweepRepo("inert-authorgate-proj")
	armedC := newMirrorClient(t, armed, armedRepo)
	preToken := enginePR(armedRepo, 46)
	preToken.Author = "szymonrychu" // pre-cutover: Renovate ran with the human's token
	runSweep(t, armedC, armed, armedRepo, &sweepReader{prs: []scm.PRRef{preToken}})
	for _, tk := range sweepTasks(t, armedC, armed.Name) {
		if tk.Spec.Kind == "upgrade" {
			t.Fatalf("adopted %s although the author is not the bot or an allowlisted engine", tk.Name)
		}
	}
}
