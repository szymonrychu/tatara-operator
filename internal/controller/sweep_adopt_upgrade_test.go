package controller

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// projectWithAdoptPrefix is sweepProject armed for adoption. The trigger label
// and the prod reaction scope are set because two of the tests below turn on
// them.
func projectWithAdoptPrefix(bot, prefix string) *tatarav1alpha1.Project {
	p := sweepProject("adopt-upgrade-proj")
	p.Spec.Scm.BotLogin = bot
	p.Spec.Scm.PRReactionScope = "labeledOrMentioned"
	p.Spec.TriggerLabel = "tatara"
	p.Spec.UpgradePolicy = &tatarav1alpha1.UpgradePolicySpec{AdoptBranchPrefix: prefix}
	return p
}

func adoptRepo() *tatarav1alpha1.Repository {
	return &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "charts", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: "adopt-upgrade-proj",
			URL:        "https://gitlab.com/szymonrychu/charts.git",
		},
	}
}

// The POST-token-change shape: Renovate runs with the platform bot's token, so
// it authors as the bot.
func renovatePR() scm.PRRef {
	return scm.PRRef{
		Repo:       "charts",
		HeadRepo:   "charts",
		Number:     41,
		Author:     "szymonrychu-bot",
		Title:      "chore(deps): update cilium to v1.17.0",
		HeadBranch: "renovate/cilium",
	}
}

// The PRE-token-change shape, kept as a named helper because two tests turn on
// it: Renovate ran with the human maintainer's token and authored as him.
func renovatePRAuthoredByTheHuman() scm.PRRef {
	pr := renovatePR()
	pr.Author = "szymonrychu"
	return pr
}

// The headline case. Branch prefix AND an adoptable author, together.
func TestClassifyPR_RenovateMRWithNoOwnerAdoptsAsUpgrade(t *testing.T) {
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	if got := ClassifyPR(proj, adoptRepo(), renovatePR(), nil, ""); got != PRAdoptUpgrade {
		t.Fatalf("ClassifyPR = %v, want PRAdoptUpgrade", got)
	}
}

// PLACEMENT PROOF 1, AND THE ONE THAT CAN LOSE COVERAGE SILENTLY. Clause 2 is
// `pr.Author == bot -> PRIgnore`. A bot-authored Renovate merge request reaches
// it the moment the token changes, and PRIgnore mints nothing, logs nothing
// above V(1) and counts nothing. The new clause MUST sit ahead of clause 2.
func TestClassifyPR_AdoptionSitsAheadOfTheBotAuthorIgnore(t *testing.T) {
	repo := adoptRepo()

	// Without adoption configured, a bot-authored MR on a branch no Task owns
	// is exactly what clause 2 exists to swallow.
	off := projectWithAdoptPrefix("szymonrychu-bot", "")
	if got := ClassifyPR(off, repo, renovatePR(), nil, ""); got != PRIgnore {
		t.Fatalf("baseline ClassifyPR = %v, want PRIgnore (clause 2)", got)
	}
	// With it configured, the same merge request adopts instead.
	on := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	if got := ClassifyPR(on, repo, renovatePR(), nil, ""); got != PRAdoptUpgrade {
		t.Fatalf("ClassifyPR = %v, want PRAdoptUpgrade", got)
	}
}

// PLACEMENT PROOF 2, and the proof that the DORMANT window behaves. Before the
// token change Renovate authors as szymonrychu, who is this project's sole
// maintainer AND sole reporter, so IsTrustedAuthor short-circuits
// prInReactionScope's first line and the merge request mints a review Task
// (deviation 2). Adoption must NOT fire on it - the author is not adoptable -
// and the pre-token behaviour must be byte-identical to today's.
func TestClassifyPR_AHumanAuthoredRenovateMRStillMintsAReviewTask(t *testing.T) {
	repo := adoptRepo()
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	proj.Spec.Scm.MaintainerLogins = []string{"szymonrychu"}
	proj.Spec.Scm.ReporterLogins = []string{"szymonrychu"}

	if got := ClassifyPR(proj, repo, renovatePRAuthoredByTheHuman(), nil, ""); got != PRReview {
		t.Fatalf("ClassifyPR = %v, want PRReview: adoption must stay dormant until the "+
			"token change moves the author (sequencing constraint 7)", got)
	}
	// And it is identical with adoption switched off, which is the definition
	// of "the operator release changes nothing".
	off := projectWithAdoptPrefix("szymonrychu-bot", "")
	off.Spec.Scm.MaintainerLogins = []string{"szymonrychu"}
	off.Spec.Scm.ReporterLogins = []string{"szymonrychu"}
	if got := ClassifyPR(off, repo, renovatePRAuthoredByTheHuman(), nil, ""); got != PRReview {
		t.Fatalf("baseline ClassifyPR = %v, want PRReview (see plan deviation 2)", got)
	}
}

// The allowlist arm: an engine running under its OWN account, not the bot's.
func TestAdoptUpgradeMR_AcceptsAnAllowlistedEngineLogin(t *testing.T) {
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	proj.Spec.UpgradePolicy.UpgradeEngineLogins = []string{"renovate-bot"}
	pr := renovatePR()
	pr.Author = "renovate-bot"
	if !AdoptUpgradeMR(proj, pr, nil, "") {
		t.Fatal("an allowlisted engine login must be adoptable")
	}
}

// A genuine human merge request must NEVER become an upgrade Task. Each guard
// tested alone.
func TestAdoptUpgradeMR_RefusesEveryNonRenovateShape(t *testing.T) {
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	cases := []struct {
		name string
		mut  func(*scm.PRRef)
		proj *tatarav1alpha1.Project
	}{
		{"a human feature branch", func(p *scm.PRRef) { p.HeadBranch = "feat/new-thing" }, proj},
		{"a branch that only looks like the prefix", func(p *scm.PRRef) { p.HeadBranch = "renovate-experiment" }, proj},
		{"a fork", func(p *scm.PRRef) { p.HeadRepo = "someone-else/charts" }, proj},
		{"an unknown head repo fails CLOSED", func(p *scm.PRRef) { p.HeadRepo = "" }, proj},
		{"the trigger label is an explicit human ask and wins", func(p *scm.PRRef) { p.Labels = []string{"tatara"} }, proj},
		{"adoption not configured", func(p *scm.PRRef) {}, projectWithAdoptPrefix("szymonrychu-bot", "")},
		// THE NEW GUARD. Anyone with push access can name a branch renovate/x.
		// Under branch-only recognition tatara would adopt it, own it, and
		// merge it on an approve.
		{"a HUMAN opened it on a prefixed branch", func(p *scm.PRRef) { p.Author = "szymonrychu" }, proj},
		{"an unknown author", func(p *scm.PRRef) { p.Author = "some-contributor" }, proj},
		{"an EMPTY author fails CLOSED", func(p *scm.PRRef) { p.Author = "" }, proj},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pr := renovatePR()
			tc.mut(&pr)
			if AdoptUpgradeMR(tc.proj, pr, nil, "") {
				t.Errorf("%s: AdoptUpgradeMR = true, want false", tc.name)
			}
		})
	}
}

// The maintainer must never be allowlistable into the engine set by accident.
// This is a guard on the OPERATOR, not on the values file: if somebody does put
// a human there, adoption follows the config - so the test documents the
// consequence rather than preventing it, and the CRD doc comment carries the
// prohibition.
func TestAdoptableAuthor_IsExactMatchOnly(t *testing.T) {
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	proj.Spec.UpgradePolicy.UpgradeEngineLogins = []string{"renovate-bot"}
	for _, author := range []string{"", "renovate", "renovate-bot-2", "Renovate-Bot", "szymonrychu"} {
		if adoptableAuthor(proj, author) {
			t.Errorf("adoptableAuthor(%q) = true, want false: exact match only", author)
		}
	}
	for _, author := range []string{"szymonrychu-bot", "renovate-bot"} {
		if !adoptableAuthor(proj, author) {
			t.Errorf("adoptableAuthor(%q) = false, want true", author)
		}
	}
}

// Never steal. A merge request with a live owning Task, or whose mirror CR is
// controller-owned by anybody, is not adoptable - which is also what stops the
// adopted merge request from being re-adopted on every subsequent pass:
// taskForBranch cannot see a renovate/* branch, but liveOwner sees the CR.
func TestAdoptUpgradeMR_NeverStealsAndNeverReAdopts(t *testing.T) {
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	owned := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "mt-u-charts-41-abc"}}
	if AdoptUpgradeMR(proj, renovatePR(), owned, "") {
		t.Error("must not adopt a merge request whose branch already has an owning Task")
	}
	if AdoptUpgradeMR(proj, renovatePR(), nil, "mt-u-charts-41-abc") {
		t.Error("must not re-adopt: a controller-owned mirror CR means it is already adopted")
	}
}

// THE TWO PREDICATES ARE ONE FACT AND MUST NOT DRIFT. AdoptUpgradeMR decides
// which merge requests the platform TAKES; ownershipForAuthor decides which it
// is ALLOWED TO MERGE, and mergeAllowedForOwnership refuses external/"initial"
// outright. An author accepted by the first and refused by the second produces
// the worst possible shape: the review agent approves, the Task walks to
// merged, and the merge driver refuses it - after the verdict is already posted
// on the forge. Retiring the mint-time ownership flip is only sound while these
// two agree, so they share adoptableAuthor and this test says so.
func TestAdoptedAuthorsAreExactlyTheOwnershipTataraAuthors(t *testing.T) {
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	proj.Spec.UpgradePolicy.UpgradeEngineLogins = []string{"renovate-bot"}

	for _, author := range []string{"szymonrychu-bot", "renovate-bot"} {
		pr := renovatePR()
		pr.Author = author
		if !AdoptUpgradeMR(proj, pr, nil, "") {
			t.Fatalf("%s must be adoptable", author)
		}
		if got := ownershipForAuthor(proj, author); got != tatarav1alpha1.OwnershipTatara {
			t.Errorf("ownershipForAuthor(%q) = %q, want tatara: an adopted merge request the "+
				"corridor cannot merge is worse than one never adopted", author, got)
		}
		mr := &tatarav1alpha1.MergeRequest{}
		mr.Status.Ownership = ownershipForAuthor(proj, author)
		mr.Status.OwnershipReason = "initial"
		if !mergeAllowedForOwnership(mr) {
			t.Errorf("%s: mergeAllowedForOwnership = false on the backfill classification", author)
		}
	}

	// And the negative that gives it meaning: a human stays external, so the
	// corridor still refuses to merge a human's merge request.
	if got := ownershipForAuthor(proj, "szymonrychu"); got != tatarav1alpha1.OwnershipExternal {
		t.Errorf("ownershipForAuthor(human) = %q, want external", got)
	}
	if got := ownershipForAuthor(proj, ""); got != tatarav1alpha1.OwnershipExternal {
		t.Errorf("ownershipForAuthor(empty) = %q, want external", got)
	}
}

// After adoption, the existing clause sends it to PRIgnore forever. No new
// clause is needed for the steady state and none may be added.
func TestClassifyPR_AnAlreadyAdoptedMRIsIgnoredNotReAdopted(t *testing.T) {
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	if got := ClassifyPR(proj, adoptRepo(), renovatePR(), nil, "mt-u-charts-41-abc"); got != PRIgnore {
		t.Fatalf("ClassifyPR = %v, want PRIgnore", got)
	}
}

// A bot-authored merge request on the task branch still adopts into its Task.
// The new clause must not shadow clause 1.
func TestClassifyPR_ClauseOneStillWins(t *testing.T) {
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "upgrade-abc123", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "upgrade"},
	}
	pr := scm.PRRef{
		Author: "szymonrychu-bot", HeadBranch: agent.TaskBranch(task),
		Repo: "charts", HeadRepo: "charts",
	}
	if got := ClassifyPR(proj, adoptRepo(), pr, task, ""); got != PRAdopt {
		t.Fatalf("ClassifyPR = %v, want PRAdopt", got)
	}
}
