package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// A HUMAN HELPING TATARA'S OWN MERGE REQUEST IS NOT A TAKEOVER OF IT.
//
// Live on tatara-operator#622: the implement Task mt-i-tatara-operator-622
// pushed two green merge requests (tatara-helmfile#424, tatara-operator#625) and
// a human then pushed to BOTH head branches to unblock them - a rebase on one, a
// credential fix on the other. Each push moved the live head off
// Status.LastBotHeadSHA, ReconcileOwnership read that as an external push, and
// flipToExternal parked the implement Task ownership-lost and handed the mirrors'
// controller ownership to freshly minted review Tasks. The implement Task then
// controller-owned ZERO merge requests, so submit_outcome(action=submitted)
// answered `400 this task owns no open MR` on three consecutive pods: the work
// was pushed and green, and the Task that did it could not deliver it.
//
// The predicate conflated two different things. "This is a human's merge request
// and tatara is only reviewing it" is what `external` protects, and it must keep
// working. "A human contributed a commit to the merge request TATARA ITSELF
// opened" is help, and severing the Task from its own work is the exact opposite
// of what helping should do. The discriminator is Status.Author: the merge
// request's forge author is the platform bot, which nothing about a later push
// changes.
func TestReconcileOwnership_HumanAssistOnTataraOwnMRDoesNotFlip(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithNormalTask(t, ctx, proj, repo, 622, normalTaskWorkBranch(proj, repo, 622), "bot-head")
	setMRAuthor(t, ctx, mr, proj.Spec.Scm.BotLogin)

	implName := tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, 622)
	beforeFlip := testutil.ToFloat64(obs.OwnershipFlipCounter("to-external", "external-push"))
	beforeAssist := testutil.ToFloat64(obs.OwnershipHumanAssistCounter(repo.Name, 622))

	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if flipped {
		t.Fatalf("a human commit on tatara's OWN merge request must not flip ownership")
	}

	got := getMR(t, ctx, proj, repo, 622)
	if got.Status.Ownership != tatarav1alpha1.OwnershipTatara {
		t.Fatalf("ownership = %q, want tatara", got.Status.Ownership)
	}
	if got.Status.OwnershipReason != "seed" || got.Status.OwnershipChangedAt != nil {
		t.Fatalf("no flip happened, so no flip audit trail may be written: reason=%q changedAt=%v",
			got.Status.OwnershipReason, got.Status.OwnershipChangedAt)
	}
	// The baseline advances, so the assist converges instead of being re-judged
	// on every reconcile of the mirror for as long as the merge request is open.
	if got.Status.LastBotHeadSHA != "human-head" {
		t.Fatalf("lastBotHeadSHA = %q, want the assisted head", got.Status.LastBotHeadSHA)
	}
	// ADVANCING THE BASELINE MUST NOT ERASE THE FACT. Once lastBotHeadSHA names a
	// commit the bot did not push, the mirror carries no other trace that a third
	// party's work is on this branch - and the review agent reads exactly these
	// two fields. See TestReconcileOwnership_HumanAssistIsDurablyRecorded.
	if got.Status.LastExternalAssistSHA != "human-head" {
		t.Fatalf("lastExternalAssistSHA = %q, want the foreign head", got.Status.LastExternalAssistSHA)
	}

	// THE THING THE 400 WAS ABOUT: the implement Task still controller-owns its
	// own merge request, which is what restapi.ownedMRs reads.
	if ctrl, ok := ownerControllerName(got); !ok || ctrl != implName {
		t.Fatalf("controller owner = %q (ok=%v), want the implement task %q", ctrl, ok, implName)
	}
	var impl tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: implName}, &impl); err != nil {
		t.Fatalf("get implement task: %v", err)
	}
	if tatarav1alpha1.Parked(&impl) {
		t.Fatalf("implement task parked %q: a human helping must not stop the task delivering", impl.Status.ParkReason)
	}

	// No review Task is minted for a bot-authored merge request: ClassifyPR's
	// invariant is that every review-kind Task is non-bot-authored BY
	// CONSTRUCTION, and the hand-back was the one path that broke it.
	reviewName := tatarav1alpha1.IntakeTaskName(proj.Name, SweepReviewKind, repo.Name, 622)
	var review tatarav1alpha1.Task
	err = k8sClient.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: reviewName}, &review)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("a review task was minted for tatara's own merge request (err=%v)", err)
	}

	if after := testutil.ToFloat64(obs.OwnershipFlipCounter("to-external", "external-push")); after != beforeFlip {
		t.Fatalf("flip counter moved on a non-flip")
	}
	if after := testutil.ToFloat64(obs.OwnershipHumanAssistCounter(repo.Name, 622)); after-beforeAssist != 1 {
		t.Fatalf("the assist must be COUNTED, not silent: %v -> %v", beforeAssist, after)
	}
}

// The assist is idempotent and silent once the baseline has caught up: the
// second reconcile of the same head is an ordinary no-drift pass, so it must
// neither write nor count again.
func TestReconcileOwnership_HumanAssistConvergesAfterOnePass(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithNormalTask(t, ctx, proj, repo, 623, normalTaskWorkBranch(proj, repo, 623), "bot-head")
	setMRAuthor(t, ctx, mr, proj.Spec.Scm.BotLogin)

	if _, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	before := testutil.ToFloat64(obs.OwnershipHumanAssistCounter(repo.Name, 623))
	rv := getMR(t, ctx, proj, repo, 623).ResourceVersion

	if _, err := d.ReconcileOwnership(ctx, proj, repo, getMR(t, ctx, proj, repo, 623), "human-head", nil); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if got := getMR(t, ctx, proj, repo, 623).ResourceVersion; got != rv {
		t.Fatalf("second pass wrote the mirror again: rv %s -> %s", rv, got)
	}
	if after := testutil.ToFloat64(obs.OwnershipHumanAssistCounter(repo.Name, 623)); after != before {
		t.Fatalf("second pass counted another assist")
	}
}

// FINDING 1. THE ASSIST LAUNDERED THE BASELINE AND ERASED THE ONLY SIGNAL.
// retainOnHumanAssist advances LastBotHeadSHA to a head the bot did NOT push and
// deliberately writes no OwnershipReason and no OwnershipChangedAt, so once it
// has run NOTHING on the mirror separates "this branch is entirely tatara's
// commits" from "a third party's commit is sitting in it". The review agent reads
// exactly last_bot_head_sha and head_sha out of prompt/bundle.go's
// <merge_request>, sees them equal, approves - and mergeAllowedForOwnership
// returns true unconditionally for `tatara`, so the operator merges the foreign
// commit.
//
// Advancing the baseline for convergence is correct; erasing the fact is not. The
// assist head is recorded DURABLY in its own field, and a SECOND assist is judged
// against the platform's baseline, not silently against the first assister's head
// as if it were tatara's.
func TestReconcileOwnership_HumanAssistIsDurablyRecorded(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithNormalTask(t, ctx, proj, repo, 627, normalTaskWorkBranch(proj, repo, 627), "bot-head")
	setMRAuthor(t, ctx, mr, proj.Spec.Scm.BotLogin)

	if _, err := d.ReconcileOwnership(ctx, proj, repo, mr, "assist-1", nil); err != nil {
		t.Fatalf("first assist: %v", err)
	}
	got := getMR(t, ctx, proj, repo, 627)
	if got.Status.LastExternalAssistSHA != "assist-1" {
		t.Fatalf("first assist not recorded: lastExternalAssistSHA = %q", got.Status.LastExternalAssistSHA)
	}

	before := testutil.ToFloat64(obs.OwnershipHumanAssistCounter(repo.Name, 627))
	if _, err := d.ReconcileOwnership(ctx, proj, repo, got, "assist-2", nil); err != nil {
		t.Fatalf("second assist: %v", err)
	}
	got = getMR(t, ctx, proj, repo, 627)
	if got.Status.LastExternalAssistSHA != "assist-2" {
		t.Fatalf("the SECOND assist must be visible too: lastExternalAssistSHA = %q", got.Status.LastExternalAssistSHA)
	}
	if got.Status.LastBotHeadSHA != "assist-2" {
		t.Fatalf("baseline must still converge: lastBotHeadSHA = %q", got.Status.LastBotHeadSHA)
	}
	if after := testutil.ToFloat64(obs.OwnershipHumanAssistCounter(repo.Name, 627)); after-before != 1 {
		t.Fatalf("the second assist must be counted on its own (repo, number) series: %v -> %v", before, after)
	}
	// The counter must be able to name the merge request it is about, or it
	// cannot drive an investigation. A label-less counter answers "some assist
	// happened somewhere" and nothing else.
	if testutil.ToFloat64(obs.OwnershipHumanAssistCounter(repo.Name, 999)) != 0 {
		t.Fatalf("assists must be per-merge-request, not smeared across one series")
	}
}

// FINDING 2. A LAGGING CACHE MUST NOT DECIDE WHETHER THE STAND-DOWN HAPPENS.
// The owner read is what tells an ADOPTED dependency-upgrade merge request (which
// must stand down) apart from one tatara opened to deliver a Task's own work
// (which must not). Before the carve-out existed a stale/NotFound owner read only
// picked the log-reason prefix and the flip happened either way - fail-safe. With
// the carve-out gated on `!adopted`, a stale NotFound on a freshly-minted adopted
// Task turns an adopted engine merge request a human pushed to into an "assist",
// and finding 1's baseline advance then erases the drift forever.
//
// The read therefore goes through the UNCACHED APIReader, exactly as merge.go and
// TaskTakenOver already do for this class of decision. Here the CACHED client
// cannot see the adopted owner at all and the APIReader can: the flip must still
// happen, and it must carry the ADOPTED prefix, which is only reachable from the
// uncached answer.
func TestReconcileOwnership_AdoptedOwnerMissingFromTheCacheStillStandsDown(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedAdoptedUpgradeMR(t, ctx, proj, repo, 628, "engine-head")

	ownerName := AdoptedUpgradeTaskName(proj.Name, repo.Name, 628)
	d.Client = &taskBlindClient{Client: k8sClient, hide: ownerName}

	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "a-humans-commit", nil)
	if err != nil || !flipped {
		t.Fatalf("a lagging cache must not suppress the stand-down: flipped=%v err=%v", flipped, err)
	}
	got := getMR(t, ctx, proj, repo, 628)
	if got.Status.OwnershipReason != adoptedPushReasonPrefix+"a-humans-commit" {
		t.Fatalf("reason = %q, want the ADOPTED prefix (only the UNCACHED owner read yields it)",
			got.Status.OwnershipReason)
	}
}

// FINDING 2, second half: AN UNRESOLVABLE OWNER FAILS CLOSED INTO THE FLIP.
// The mirror still carries a controller ownerRef but the Task behind it is gone
// (mid-handover, RepairZeroController's window, a reap that has not cleared the
// ref yet). The carve-out's justification is "the Task that opened it must keep
// being able to deliver it"; with no such Task there is nothing to protect, so
// the drift is a stand-down.
func TestReconcileOwnership_UnresolvableOwnerFailsClosed(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithNormalTask(t, ctx, proj, repo, 629, normalTaskWorkBranch(proj, repo, 629), "bot-head")
	setMRAuthor(t, ctx, mr, proj.Spec.Scm.BotLogin)

	ownerName := tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, 629)
	d.Client = &taskBlindClient{Client: k8sClient, hide: ownerName}
	d.APIReader = &taskBlindClient{Client: k8sClient, hide: ownerName}

	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil)
	if err != nil || !flipped {
		t.Fatalf("an unresolvable owner must fail CLOSED into the flip: flipped=%v err=%v", flipped, err)
	}
	if got := getMR(t, ctx, proj, repo, 629); got.Status.Ownership != tatarav1alpha1.OwnershipExternal {
		t.Fatalf("ownership = %q, want external", got.Status.Ownership)
	}
}

// FINDING 3. THE CARVE-OUT MUST NAME THE OWNER IT IS PROTECTING.
// An ORPHANED bot-authored mirror - zero controller Task refs, the state a reap
// or a mid-handover leaves behind - satisfied `bot != "" && author == bot` with
// nothing else to check, so a human push produced no flip, no park, no review
// Task, and the mirror stayed `ownership: tatara`. AdoptUpgradeMR clause (h) only
// refuses re-adoption of an `external` mirror, so that orphan was still
// re-adoptable and a fresh pod would be told "approving MERGES it" for a branch
// carrying unattributed commits.
func TestReconcileOwnership_OrphanedBotAuthoredMRStillFlips(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedOpenMR(t, ctx, proj, repo, 630, "tatara/clarify-630", proj.Spec.Scm.BotLogin, "bot-head")
	stampOwnedTataraNoOwner(t, ctx, mr, "bot-head")

	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil)
	if err != nil || !flipped {
		t.Fatalf("an ORPHANED bot-authored mirror must still flip: flipped=%v err=%v", flipped, err)
	}
	if got := getMR(t, ctx, proj, repo, 630); got.Status.Ownership != tatarav1alpha1.OwnershipExternal {
		t.Fatalf("ownership = %q, want external", got.Status.Ownership)
	}
}

// FINDING 3, second clause: the head branch must be the owner's own work branch,
// the same pairing every sibling bot-authorship gate makes (reaper.go's
// mr.Status.HeadBranch == agent.TaskBranch(t), AdoptPR's clause (b)). A
// bot-authored mirror sitting on a branch the owning Task does not push to is not
// the Task's own work, so the carve-out's premise does not hold.
func TestReconcileOwnership_BotAuthoredMROffTheTaskBranchStillFlips(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithNormalTask(t, ctx, proj, repo, 631, "somebody/elses-branch", "bot-head")
	setMRAuthor(t, ctx, mr, proj.Spec.Scm.BotLogin)

	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil)
	if err != nil || !flipped {
		t.Fatalf("a bot-authored mirror off the owner's work branch must flip: flipped=%v err=%v", flipped, err)
	}
	if got := getMR(t, ctx, proj, repo, 631); got.Status.Ownership != tatarav1alpha1.OwnershipExternal {
		t.Fatalf("ownership = %q, want external", got.Status.Ownership)
	}
}

// normalTaskWorkBranch is the head branch a seedTataraOwnedMRWithNormalTask
// fixture must carry for the carve-out to recognize the merge request as the
// owning Task's OWN work. It is derived from agent.TaskBranch rather than
// spelled out, so the fixture cannot drift away from the predicate.
func normalTaskWorkBranch(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) string {
	return agent.TaskBranch(&tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, number),
			Namespace: proj.Namespace,
		},
	})
}

// bindNormalOwnerTask creates the full-lifecycle (kind=clarify) Task a
// seedOpenMR mirror would have had and makes it the mirror's controller owner,
// leaving Status untouched so a classification pass still sees ownership="". It
// is what turns an ORPHAN mirror into one the carve-out can protect: the
// carve-out has to be able to name the Task whose delivery it is preserving.
func bindNormalOwnerTask(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, number int) {
	t.Helper()
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, number),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Kind:          SweepIssueKind,
			Goal:          "push to the MR",
			MergeOrder:    []string{repo.Name},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create owner task: %v", err)
	}
	stampTaskStatus(t, ctx, task, tatarav1alpha1.StateUnderImplementation, "")

	var fresh tatarav1alpha1.MergeRequest
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(mr), &fresh); err != nil {
		t.Fatalf("get mergerequest %s: %v", mr.Name, err)
	}
	if err := controllerutil.SetControllerReference(task, &fresh, k8sClient.Scheme()); err != nil {
		t.Fatalf("set controller ref: %v", err)
	}
	if err := k8sClient.Update(ctx, &fresh); err != nil {
		t.Fatalf("bind owner to %s: %v", mr.Name, err)
	}
	*mr = fresh
}

// stampOwnedTataraNoOwner makes a seedOpenMR mirror tatara-owned with a baseline
// but NO controller owner - the orphan shape a reap or a mid-handover leaves.
func stampOwnedTataraNoOwner(t *testing.T, ctx context.Context, mr *tatarav1alpha1.MergeRequest, botHead string) {
	t.Helper()
	var fresh tatarav1alpha1.MergeRequest
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(mr), &fresh); err != nil {
		t.Fatalf("get mergerequest %s: %v", mr.Name, err)
	}
	fresh.Status.Ownership = tatarav1alpha1.OwnershipTatara
	fresh.Status.OwnershipReason = "seed"
	fresh.Status.LastBotHeadSHA = botHead
	if err := k8sClient.Status().Update(ctx, &fresh); err != nil {
		t.Fatalf("stamp %s: %v", mr.Name, err)
	}
	*mr = fresh
}

// taskBlindClient is k8sClient with ONE Task made invisible, so a test can put
// the cached reader and the uncached APIReader out of step on the exact read the
// ownership decision turns on.
type taskBlindClient struct {
	client.Client
	hide string
}

func (c *taskBlindClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object,
	opts ...client.GetOption) error {

	if _, isTask := obj.(*tatarav1alpha1.Task); isTask && key.Name == c.hide {
		return apierrors.NewNotFound(schema.GroupResource{Group: tatarav1alpha1.GroupVersion.Group,
			Resource: "tasks"}, key.Name)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// THE PROTECTION SURVIVES, and this is the pin for it: a merge request a HUMAN
// authored - one tatara only ever reviews, or took over on request - still
// stands down on an unattributable push, parks its pushing owner, and hands the
// mirror to the review Task. Only the bot-authored case is carved out.
func TestReconcileOwnership_HumanAuthoredMRStillFlips(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithNormalTask(t, ctx, proj, repo, 624, "feature/human", "bot-head")
	setMRAuthor(t, ctx, mr, "octocat")

	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil)
	if err != nil || !flipped {
		t.Fatalf("a human-authored MR must still flip: flipped=%v err=%v", flipped, err)
	}
	if got := getMR(t, ctx, proj, repo, 624); got.Status.Ownership != tatarav1alpha1.OwnershipExternal {
		t.Fatalf("ownership = %q, want external", got.Status.Ownership)
	}
}

// THE SHARP EDGE OF AN AUTHOR-BASED CARVE-OUT: a dependency-upgrade engine may
// run with the BOT'S OWN TOKEN, so an adopted merge request is bot-authored too.
// Nobody asks the platform to adopt one - the sweep does it automatically - so a
// human's commit there is not help with tatara's own work, and it must keep the
// adopted stand-down (adoptedPushReasonPrefix, human-merged-only) it has always
// had. The owner, not the author, is what separates the two.
func TestReconcileOwnership_AdoptedUpgradeMROnTheBotTokenStillFlips(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedAdoptedUpgradeMR(t, ctx, proj, repo, 626, "engine-head")
	if mr.Status.Author != proj.Spec.Scm.BotLogin {
		t.Fatalf("fixture must be bot-authored to prove anything; author = %q", mr.Status.Author)
	}

	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "a-humans-commit", nil)
	if err != nil || !flipped {
		t.Fatalf("an adopted engine MR must still stand down: flipped=%v err=%v", flipped, err)
	}
	got := getMR(t, ctx, proj, repo, 626)
	if got.Status.Ownership != tatarav1alpha1.OwnershipExternal {
		t.Fatalf("ownership = %q, want external", got.Status.Ownership)
	}
	if got.Status.OwnershipReason != adoptedPushReasonPrefix+"a-humans-commit" {
		t.Fatalf("reason = %q, want the ADOPTED prefix", got.Status.OwnershipReason)
	}
}

// setMRAuthor rewrites a seeded mirror's forge author in place. The seeds hand
// out "octocat"; the whole discriminator this file pins is Status.Author, so
// every case here states its own.
func setMRAuthor(t *testing.T, ctx context.Context, mr *tatarav1alpha1.MergeRequest, author string) {
	t.Helper()
	var fresh tatarav1alpha1.MergeRequest
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(mr), &fresh); err != nil {
		t.Fatalf("get mergerequest %s: %v", mr.Name, err)
	}
	fresh.Status.Author = author
	if err := k8sClient.Status().Update(ctx, &fresh); err != nil {
		t.Fatalf("set author on %s: %v", mr.Name, err)
	}
	*mr = fresh
}
