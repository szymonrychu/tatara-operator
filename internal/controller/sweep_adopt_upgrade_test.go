package controller

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	"sigs.k8s.io/controller-runtime/pkg/client"

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
	if got := ClassifyPR(proj, adoptRepo(), renovatePR(), nil, "", nil); got != PRAdoptUpgrade {
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
	if got := ClassifyPR(off, repo, renovatePR(), nil, "", nil); got != PRIgnore {
		t.Fatalf("baseline ClassifyPR = %v, want PRIgnore (clause 2)", got)
	}
	// With it configured, the same merge request adopts instead.
	on := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	if got := ClassifyPR(on, repo, renovatePR(), nil, "", nil); got != PRAdoptUpgrade {
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

	if got := ClassifyPR(proj, repo, renovatePRAuthoredByTheHuman(), nil, "", nil); got != PRReview {
		t.Fatalf("ClassifyPR = %v, want PRReview: adoption must stay dormant until the "+
			"token change moves the author (sequencing constraint 7)", got)
	}
	// And it is identical with adoption switched off, which is the definition
	// of "the operator release changes nothing".
	off := projectWithAdoptPrefix("szymonrychu-bot", "")
	off.Spec.Scm.MaintainerLogins = []string{"szymonrychu"}
	off.Spec.Scm.ReporterLogins = []string{"szymonrychu"}
	if got := ClassifyPR(off, repo, renovatePRAuthoredByTheHuman(), nil, "", nil); got != PRReview {
		t.Fatalf("baseline ClassifyPR = %v, want PRReview (see plan deviation 2)", got)
	}
}

// The allowlist arm: an engine running under its OWN account, not the bot's.
func TestAdoptUpgradeMR_AcceptsAnAllowlistedEngineLogin(t *testing.T) {
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	proj.Spec.UpgradePolicy.UpgradeEngineLogins = []string{"renovate-bot"}
	pr := renovatePR()
	pr.Author = "renovate-bot"
	if !AdoptUpgradeMR(proj, pr, nil, "", nil) {
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
			if AdoptUpgradeMR(tc.proj, pr, nil, "", nil) {
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
	if AdoptUpgradeMR(proj, renovatePR(), owned, "", nil) {
		t.Error("must not adopt a merge request whose branch already has an owning Task")
	}
	if AdoptUpgradeMR(proj, renovatePR(), nil, "mt-u-charts-41-abc", nil) {
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
		if !AdoptUpgradeMR(proj, pr, nil, "", nil) {
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
	if got := ClassifyPR(proj, adoptRepo(), renovatePR(), nil, "mt-u-charts-41-abc", nil); got != PRIgnore {
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
	if got := ClassifyPR(proj, adoptRepo(), pr, task, "", nil); got != PRAdopt {
		t.Fatalf("ClassifyPR = %v, want PRAdopt", got)
	}
}

// sweepAdoptProject is the sweep-harness project ARMED for adoption: the
// prefix, plus an upgrade cron. Design D1 retired per-pass adoption headroom
// entirely (Task 8), so nothing on this path reads MaxOpenUpgrades any more -
// it is a fixed 1 here, not a parameter, purely so the cron shape matches a
// project whose upgrade activity is configured, same as prod.
func sweepAdoptProject(name string) *tatarav1alpha1.Project {
	p := sweepProject(name)
	p.Spec.UpgradePolicy = &tatarav1alpha1.UpgradePolicySpec{AdoptBranchPrefix: "renovate/"}
	p.Spec.Scm.Cron = &tatarav1alpha1.ScmCron{
		Upgrade: tatarav1alpha1.UpgradeActivity{Schedule: "0 */4 * * *", MaxOpenUpgrades: 1},
	}
	return p
}

// sweepRenovatePR is a listing row shaped like the sweep's own repo fixture.
func sweepRenovatePR(number int, branch string) scm.PRRef {
	return scm.PRRef{
		Repo: "szymonrychu/tatara-operator", HeadRepo: "szymonrychu/tatara-operator",
		Number: number, Author: "tatara-bot", HeadBranch: branch,
		Title: "chore(deps): update something", HeadSHA: "sha", Body: "notes",
	}
}

// enginePR is a listing row from the dependency engine, authored by the bot,
// on repo's own slug - not a hardcoded one, so a test seeding a differently
// named repo gets a PRRef that actually matches it. Moved from the
// now-deleted sweep_adopt_headroom_test.go.
func enginePR(t *testing.T, repo *tatarav1alpha1.Repository, number int) scm.PRRef {
	t.Helper()
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		t.Fatalf("owner/repo from %q: %v", repo.Spec.URL, err)
	}
	slug := owner + "/" + name
	return scm.PRRef{
		Repo:       slug,
		HeadRepo:   slug,
		Number:     number,
		Author:     "tatara-bot",
		Title:      "chore(deps): bump " + strconv.Itoa(number),
		HeadBranch: "renovate/dep-" + strconv.Itoa(number),
		HeadSHA:    "sha-" + strconv.Itoa(number),
	}
}

// seedAdoptedLane persists an already-adopted, still-live upgrade Task and the
// MergeRequest mirror it controller-owns: the steady state of a merge request
// this project adopted on an earlier pass and has not merged yet. Moved from
// the now-deleted sweep_adopt_headroom_test.go.
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

// adoptedTasksIn returns the ADOPTED upgrade Tasks in the project (the cron
// fixture is kind=upgrade too, so the discriminator is stage.AdoptedMR).
func adoptedTasksIn(t *testing.T, c client.Client, proj string) []tatarav1alpha1.Task {
	t.Helper()
	out := []tatarav1alpha1.Task{}
	for _, tk := range sweepTasks(t, c, proj) {
		if tk.Spec.Kind == "upgrade" && stage.AdoptedMR(&tk) {
			out = append(out, tk)
		}
	}
	return out
}

// sweepQueuedEvents lists the QueuedEvents ONE sweep pass left behind for a
// project, so a test can assert on the enqueue rather than on a minted Task -
// the sweep is a producer now, not a minter (Task 8).
func sweepQueuedEvents(t *testing.T, c client.Client, proj string) []tatarav1alpha1.QueuedEvent {
	t.Helper()
	var qel tatarav1alpha1.QueuedEventList
	if err := c.List(context.Background(), &qel, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list queuedevents: %v", err)
	}
	out := make([]tatarav1alpha1.QueuedEvent, 0, len(qel.Items))
	for _, qe := range qel.Items {
		if qe.Spec.ProjectRef == proj {
			out = append(out, qe)
		}
	}
	return out
}

// THE SWEEP IS A BACKSTOP NOW, NOT A PACER. It enqueues under the SAME dedup key
// the webhook uses, so a merge request the webhook already queued collides on
// AlreadyExists and burns no sequence number - and one whose delivery was lost
// while the operator was down is picked up on the next pass.
func TestSweepPRs_AdoptableMergeRequestIsEnqueuedNotMinted(t *testing.T) {
	proj := sweepAdoptProject("adopt-enqueue-proj")
	repo := sweepRepo("adopt-enqueue-proj")
	c := newMirrorClient(t, proj, repo)
	rd := &sweepReader{}
	for _, n := range []int{41, 42, 43} {
		rd.prs = append(rd.prs, sweepRenovatePR(n, fmt.Sprintf("renovate/dep-%d", n)))
	}

	runSweep(t, c, proj, repo, rd)

	events := sweepQueuedEvents(t, c, proj.Name)
	if len(events) != 3 {
		t.Fatalf("queued events = %d, want 3 (one per adoptable merge request, unbounded by "+
			"maxOpenUpgrades=1 per design D1)", len(events))
	}
	seen := map[int]bool{}
	for _, qe := range events {
		a := qe.Spec.Payload.AdoptedUpgrade
		if a == nil {
			t.Fatalf("queued event %s carries no payload.adoptedUpgrade", qe.Name)
		}
		seen[a.Number] = true
		if qe.Spec.Priority == nil || *qe.Spec.Priority != 2 {
			t.Errorf("queued event %s priority = %v, want 2", qe.Name, qe.Spec.Priority)
		}
	}
	for _, n := range []int{41, 42, 43} {
		if !seen[n] {
			t.Errorf("merge request %d was not queued for adoption", n)
		}
	}

	// The dispatcher is the minter now, not the sweep.
	if n := len(adoptedTasksIn(t, c, proj.Name)); n != 0 {
		t.Fatalf("the sweep pass minted %d Tasks directly, want 0: admission mints, the sweep only enqueues", n)
	}
}

// A SECOND PASS OVER THE SAME MERGE REQUEST IS A NO-OP. The natural key is
// deterministic, so the Create collides and EnqueueEvent reports created=false.
func TestSweepPRs_ASecondPassDoesNotDoubleEnqueue(t *testing.T) {
	proj := sweepAdoptProject("adopt-second-pass-proj")
	repo := sweepRepo("adopt-second-pass-proj")
	c := newMirrorClient(t, proj, repo)
	rd := &sweepReader{prs: []scm.PRRef{sweepRenovatePR(41, "renovate/cilium")}}

	before := testutil.ToFloat64(obs.AdoptionEnqueuedTotal.WithLabelValues(proj.Name, SweepActivity))
	runSweep(t, c, proj, repo, rd)
	runSweep(t, c, proj, repo, rd)
	after := testutil.ToFloat64(obs.AdoptionEnqueuedTotal.WithLabelValues(proj.Name, SweepActivity))

	if events := sweepQueuedEvents(t, c, proj.Name); len(events) != 1 {
		t.Fatalf("queued events after two passes = %d, want 1: the dedup key must collide", len(events))
	}
	if after-before != 1 {
		t.Errorf("operator_adoption_enqueued_total{activity=sweep} increased by %v, want 1", after-before)
	}
}

// A MERGE REQUEST A LIVE TASK ALREADY OWNS PRODUCES NO SECOND EVENT. ClassifyPR
// sends it to PRIgnore on the mirror's live controller owner, exactly as before.
func TestSweepPRs_AnAlreadyAdoptedMergeRequestEnqueuesNothing(t *testing.T) {
	ctx := context.Background()
	proj := sweepAdoptProject("adopt-already-proj")
	repo := sweepRepo("adopt-already-proj")
	c := newMirrorClient(t, proj, repo)
	seedAdoptedLane(t, ctx, c, proj, repo, 42)

	runSweep(t, c, proj, repo, &sweepReader{prs: []scm.PRRef{enginePR(t, repo, 42)}})

	if events := sweepQueuedEvents(t, c, proj.Name); len(events) != 0 {
		t.Fatalf("queued events for an already-adopted merge request = %d, want 0", len(events))
	}
}

// Adoption must not consume the sweepBudget that mints review and issue Tasks.
// They are different mechanisms entirely now: maxNewTasksPerSweep protects the
// forge and the token spend for NEW Task mints, and the adoption enqueue goes
// through EnqueueEvent, which is not gated on it at all.
func TestSweepAdoption_DoesNotConsumeTheReviewMintBudget(t *testing.T) {
	proj := sweepAdoptProject("adopt-budget-proj")
	proj.Spec.MaxNewTasksPerSweep = 1
	proj.Spec.TriggerLabel = "tatara"
	proj.Spec.Scm.PRReactionScope = "labeledOrMentioned"
	repo := sweepRepo("adopt-budget-proj")
	c := newMirrorClient(t, proj, repo)
	rd := &sweepReader{prs: []scm.PRRef{
		sweepRenovatePR(41, "renovate/cilium"),
		{Repo: "szymonrychu/tatara-operator", HeadRepo: "szymonrychu/tatara-operator",
			Number: 42, Author: "alice", HeadBranch: "feat/thing", Labels: []string{"tatara"}},
	}}

	runSweep(t, c, proj, repo, rd)

	if events := sweepQueuedEvents(t, c, proj.Name); len(events) != 1 {
		t.Errorf("queued adoption events = %d, want 1", len(events))
	}
	tasks := sweepTasks(t, c, proj.Name)
	var review int
	for i := range tasks {
		if tasks[i].Spec.Kind == SweepReviewKind {
			review++
		}
	}
	if review != 1 {
		t.Errorf("review Tasks = %d, want 1: adoption must not eat maxNewTasksPerSweep", review)
	}
}

// A project that configures the prefix but has NO upgrade cron still enqueues
// the merge request for adoption, and must not panic: adoption no longer reads
// Cron.Upgrade at all (design D1 - the mint-time lane cap it fed is gone), so a
// declared upgrade cron is no longer a precondition for adoption.
func TestSweepAdoption_NoUpgradeCronStillEnqueuesAndDoesNotPanic(t *testing.T) {
	proj := sweepProject("adopt-nocron-proj")
	proj.Spec.UpgradePolicy = &tatarav1alpha1.UpgradePolicySpec{AdoptBranchPrefix: "renovate/"}
	repo := sweepRepo("adopt-nocron-proj")
	c := newMirrorClient(t, proj, repo)
	rd := &sweepReader{prs: []scm.PRRef{sweepRenovatePR(41, "renovate/cilium")}}

	runSweep(t, c, proj, repo, rd)

	if events := sweepQueuedEvents(t, c, proj.Name); len(events) != 1 {
		t.Fatalf("queued adoption events with no upgrade cron configured = %d, want 1", len(events))
	}
}

// INERTNESS ON RELEASE. Every behavioural difference adoption introduces is
// gated on adoptBranchPrefix being non-empty (default) AND the author being one
// the project owns. With the prefix empty NOTHING may change: no adoption, no
// queued event, and the merge requests the engine already has open must
// classify exactly as they did before. Moved from the now-deleted
// sweep_adopt_headroom_test.go; headroomProject is gone with the window, so
// this now arms via sweepAdoptProject instead.
func TestSweepAdoption_IsInertWithNoAdoptBranchPrefix(t *testing.T) {
	proj := sweepAdoptProject("inert-proj")
	proj.Spec.UpgradePolicy.AdoptBranchPrefix = "" // the shipped default
	proj.Spec.Scm.PRReactionScope = "labeledOrMentioned"
	repo := sweepRepo("inert-proj")
	c := newMirrorClient(t, proj, repo)

	engine := enginePR(t, repo, 44)
	human := enginePR(t, repo, 45)
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
	if n := len(sweepQueuedEvents(t, c, proj.Name)); n != 0 {
		t.Fatalf("an inert project queued %d adoption events, want 0", n)
	}
	// And the author gate is the SECOND lock: even with the prefix armed, the
	// engine running under a human's token adopts nothing.
	armed := sweepAdoptProject("inert-authorgate-proj")
	armed.Spec.Scm.PRReactionScope = "labeledOrMentioned"
	armedRepo := sweepRepo("inert-authorgate-proj")
	armedC := newMirrorClient(t, armed, armedRepo)
	preToken := enginePR(t, armedRepo, 46)
	preToken.Author = "szymonrychu" // pre-cutover: Renovate ran with the human's token
	runSweep(t, armedC, armed, armedRepo, &sweepReader{prs: []scm.PRRef{preToken}})
	for _, tk := range sweepTasks(t, armedC, armed.Name) {
		if tk.Spec.Kind == "upgrade" {
			t.Fatalf("adopted %s although the author is not the bot or an allowlisted engine", tk.Name)
		}
	}
	if n := len(sweepQueuedEvents(t, armedC, armed.Name)); n != 0 {
		t.Fatalf("queued %d adoption events although the author is not adoptable", n)
	}
}

// A DECLINE MUST OUTLIVE THE TASK THAT MADE IT.
//
// The reaper orphans the mirror and deletes a parked or terminal adopted Task,
// which frees the deterministic Task name and leaves the mirror with no live
// owner - so AdoptUpgradeMR clause (e) stops refusing and the next sweep
// re-adopts the SAME merge request into a fresh Task with a fresh review turn.
// A bump the upgrade agent declined as unsafe therefore comes back, and the next
// reviewer sees none of that history. Clause (g) reads the durable marker the
// reap stamps.
func TestAdoptUpgradeMR_RefusesAMirrorMarkedAdoptionRefused(t *testing.T) {
	proj := projectWithAdoptPrefix("szymonrychu-bot", "renovate/")
	pr := renovatePR()

	fresh := &tatarav1alpha1.MergeRequest{}
	if !AdoptUpgradeMR(proj, pr, nil, "", fresh) {
		t.Fatal("a mirror with no marker must still be adoptable")
	}
	refused := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{AnnAdoptionRefused: "implement-declined"},
		},
	}
	if AdoptUpgradeMR(proj, pr, nil, "", refused) {
		t.Fatal("a merge request the upgrade agent already declined was re-adopted: " +
			"the decline is erased and a fresh review turn can approve and merge it")
	}
	if got := ClassifyPR(proj, adoptRepo(), pr, nil, "", refused); got != PRIgnore {
		t.Fatalf("ClassifyPR = %v, want PRIgnore for a refused mirror", got)
	}
	// The marker is scoped to the MIRROR it is stamped on: a DIFFERENT merge
	// request from the same engine is untouched, so one refusal never stops the
	// lane.
	other := renovatePR()
	other.Number = 42
	other.HeadBranch = "renovate/loki"
	if got := ClassifyPR(proj, adoptRepo(), other, nil, "", nil); got != PRAdoptUpgrade {
		t.Fatalf("ClassifyPR = %v on an unmarked sibling, want PRAdoptUpgrade", got)
	}
}
