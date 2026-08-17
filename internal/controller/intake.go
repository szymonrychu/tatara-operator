package controller

import (
	"context"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// Minter is the ONE reactive intake mint path (B.4). Both the sweep loop and
// the webhook construct one and call MintForItem, so "what Task does this forge
// item produce" has a single source of truth. The mint is a DIRECT Task create
// (it synchronously owns the Issue/MergeRequest CR at mint time, which a
// parked(backlog-sweep) Task depends on), made race-safe by a deterministic
// natural-key Task name + a live existence check + IgnoreAlreadyExists.
type Minter struct {
	Client    client.Client
	APIReader client.Reader // uncached; nil falls back to Client
	Scheme    *runtime.Scheme
	Metrics   *obs.OperatorMetrics
	// SpillerFor resolves the A.7 byte-budget spiller for a Project. Every OTHER
	// Minter mint method (MintForItem/MintReviewTask/MintIssueTask) takes sp as an
	// explicit parameter from its caller, which already has one in hand (the
	// webhook's per-request s.cfg.SpillerFor, or the sweep's per-scan spiller).
	// EnsureTaskForMRComment is the exception: its signature is fixed by the
	// general comment->task pipeline contract (the webhook fast path AND the
	// OP12 sweep backstop call the exact same 5-arg signature), so it resolves
	// its own spiller here instead. Nil-safe (see spillerFor) for callers built
	// before this field existed, or in unit tests.
	SpillerFor func(proj *tatarav1alpha1.Project) objbudget.Spiller
	// Activity is the {activity} metric label this Minter's skips and stale-owner
	// repairs are attributed to: SweepActivity for the sweep's own funnel,
	// WebhookActivity for the webhook's. Empty means SweepActivity, which is what
	// every construction did IMPLICITLY before - and it made
	// SweepStaleOwnerRepairedTotal's documented triage instruction ("a SUSTAINED
	// rate is a reap that is not handing over ownership - go read B.5") wrong for
	// the webhook half, which reaps nothing.
	Activity string
}

// WebhookActivity is the {activity} label for mints driven by a live forge
// delivery rather than by a sweep pass. See Minter.Activity.
const WebhookActivity = "webhook"

// activity resolves Minter.Activity, defaulting to the sweep.
func (m *Minter) activity() string {
	if m.Activity == "" {
		return SweepActivity
	}
	return m.Activity
}

// minter builds the ONE shared intake funnel from the reconciler's own fields.
func (r *ProjectReconciler) minter() *Minter {
	return &Minter{
		Client:     r.Client,
		APIReader:  r.APIReader,
		Scheme:     r.Scheme,
		Metrics:    r.Metrics,
		SpillerFor: r.SpillerFor,
	}
}

// driver builds a StageDriver from the reconciler's own fields, so the OP12
// sweep can call the SAME ReconcileOwnership convergence function the
// MergeRequestReconciler's webhook fast path drives - one function, two
// callers, per its own doc comment. Mirrors minter() above.
func (r *ProjectReconciler) driver() *StageDriver {
	return &StageDriver{
		Client:     r.Client,
		APIReader:  r.APIReader,
		Metrics:    r.Metrics,
		SpillerFor: r.SpillerFor,
	}
}

// spillerFor resolves the per-project spiller for the ProjectReconciler's own
// direct objbudget.Fit* calls (the doc-batch stamp), nil-safe for unit tests
// exactly like Minter.spillerFor below.
func (r *ProjectReconciler) spillerFor(proj *tatarav1alpha1.Project) objbudget.Spiller {
	if r.SpillerFor == nil || proj == nil {
		return nil
	}
	return r.SpillerFor(proj)
}

// spillerFor resolves the per-project spiller EnsureTaskForMRComment mints
// with. A nil SpillerFor yields a nil Spiller: safe for a genuinely fresh
// mint (the synced MR snapshot carries no comments to evict yet), and matches
// the nil-safe SpillerFor idiom the other reconcilers already use for unit
// tests. Production wiring (the webhook's s.minter()) always sets a real,
// never-nil resolver.
func (m *Minter) spillerFor(proj *tatarav1alpha1.Project) objbudget.Spiller {
	if m.SpillerFor == nil || proj == nil {
		return nil
	}
	return m.SpillerFor(proj)
}

func (m *Minter) reader() client.Reader {
	if m.APIReader != nil {
		return m.APIReader
	}
	return m.Client
}

// MintOutcome is the CLOSED result vocabulary of the intake funnel. It replaces
// a bare `created bool`, which squeezed four materially different states into
// one bit:
//
//	MintNotOwed           classification says no Task is owed (bot PR, ignored,
//	                      already owned by a live Task). Nothing happened.
//	MintCreated           a new Task exists. This is the ONLY outcome a caller
//	                      may describe as "minted".
//	MintExistingLive      the deterministic natural key is held by a LIVE twin.
//	                      No new Task; the twin's binding was repaired.
//	MintTombstoneDeleted  the key was held by a DEAD twin, which this call just
//	                      DELETED. The mint is still OWED and has not happened.
//	                      A caller must re-drive, not report success.
//
// The last two are why the bool was a defect: resume.go logged "re-minted the
// issue fresh" for every non-error return, including the one where a Task had
// just been destroyed and nothing replaced it. Every caller must now switch.
type MintOutcome string

const (
	// MintNotOwed: classification decided no Task is owed. Also the value
	// returned alongside a non-nil error, which callers must check FIRST.
	MintNotOwed MintOutcome = "not_owed"
	// MintCreated: a new Task exists.
	MintCreated MintOutcome = "created"
	// MintExistingLive: a live twin holds the natural key.
	MintExistingLive MintOutcome = "existing_live"
	// MintTombstoneDeleted: a dead twin held the key and was deleted. The mint
	// is OWED and has NOT happened; re-drive.
	MintTombstoneDeleted MintOutcome = "tombstone_deleted"
)

// ForgeItem is one forge work item the intake funnel classifies + mints for.
type ForgeItem struct {
	IsPR  bool
	Issue scm.Issue // when !IsPR
	PR    scm.PRRef // when IsPR
}

// MintForItem classifies item with the SAME predicates the sweep uses and mints
// the Task if one is owed, race-safe on the natural key. It returns a typed
// MintOutcome; see MintOutcome for what each member obliges the caller to do.
// It applies NO creation budget: the webhook mints a live human
// signal immediately, and downstream admission (ensureTicket -> dispatcher)
// bounds concurrency. The sweep keeps its own budget check BEFORE calling the
// per-stage mint helpers (see sweepIssues/sweepPRs).
func (m *Minter) MintForItem(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, item ForgeItem, webhookOriginated bool,
	sp objbudget.Spiller) (*tatarav1alpha1.Task, MintOutcome, error) {

	activity := m.activity()
	if item.IsPR {
		cr, err := m.mergeRequestCR(ctx, proj, repo, item.PR.Number)
		if err != nil {
			return mintErr(SweepReviewKind, err)
		}
		// LIVENESS, not presence, on this arm too (issue #521). ClassifyPR's
		// orphan check is the exact MR analogue of IsOrphanIssue clause (b) and
		// carried the identical defect: a dangling controller ref made an open
		// human PR classify PRIgnore forever, on the webhook path as well as the
		// sweep's. A human PR never carries a task/<name> head branch, so it has
		// no owning Task by branch and the MR CR owner is the only input.
		liveOwner, lerr := m.resolveLiveMROwner(ctx, proj, cr, activity)
		if lerr != nil {
			return mintErr(SweepReviewKind, lerr)
		}
		switch ClassifyPR(proj, repo, item.PR, nil, liveOwner, cr) {
		case PRReview:
			stg, reason := MintReviewStage(cr)
			return m.MintReviewTask(ctx, proj, repo, item.PR, cr, stg, reason, sp)
		default: // PRAdopt / PRClaimed / PRAdoptUpgrade (all sweep-only) / PRIgnore
			// PRAdoptUpgrade is deliberately NOT minted here. Adoption is paced by
			// the project-wide maxOpenUpgrades headroom, which is computed once per
			// SWEEP PASS; a webhook delivery has no such budget in hand and would
			// mint past the cap on every merge request the engine opens at once.
			// The sweep picks it up oldest-first within minutes.
			obs.MintOutcomeTotal.WithLabelValues(SweepReviewKind, string(MintNotOwed)).Inc()
			return nil, MintNotOwed, nil
		}
	}

	cr, err := m.issueCR(ctx, proj, repo, item.Issue.Number)
	if err != nil {
		return mintErr(SweepIssueKind, err)
	}
	// Clause (b) resolves LIVENESS, not just presence, and repairs a tombstone
	// ref in place (issue #521). The webhook path needs this exactly as much as
	// the sweep does: a dangling controller ref made the primary mint a silent
	// no-op too, so a human opening an issue got nothing either.
	liveOwner, lerr := m.resolveLiveIssueOwner(ctx, proj, cr, activity)
	if lerr != nil {
		return mintErr(SweepIssueKind, lerr)
	}
	// The REASON is read, not discarded. IsOrphanIssue's own doc says a caller
	// cannot structurally skip an issue without naming the clause that refused
	// it, and this - the first non-sweep caller - did exactly that: a human
	// opened an issue, the reporter allowlist refused it, and there was no
	// counter and no log. Same green-and-silent state as #521, on the path a
	// human notices first.
	if orphan, skipReason := IsOrphanIssue(proj, repo, item.Issue, liveOwner); !orphan {
		skipIssue(ctx, proj, repo, item.Issue.Number, activity, skipReason)
		obs.MintOutcomeTotal.WithLabelValues(SweepIssueKind, string(MintNotOwed)).Inc()
		return nil, MintNotOwed, nil
	}
	stg, reason := MintStage(proj, repo, item.Issue, webhookOriginated)
	return m.MintIssueTask(ctx, proj, repo, item.Issue, stg, reason, sp)
}

// mintErr is the ONE error return of the intake funnel, so an error is a
// COUNTED outcome instead of an invisible one. MintOutcomeTotal was incremented
// only on the two success paths inside MintIssueTask/MintReviewTask, which left
// every classify decline and every failure off the series entirely - a counter
// that only ever counts the good news.
func mintErr(kind string, err error) (*tatarav1alpha1.Task, MintOutcome, error) {
	obs.MintOutcomeTotal.WithLabelValues(kind, obs.MintOutcomeError).Inc()
	return nil, MintNotOwed, err
}

// MintIssueTask is mintTaskForIssue's race-safe body (fix M3-10's ADOPT-OR-CREATE
// on the Issue CR, unchanged): the Task name is now the DETERMINISTIC
// IntakeTaskName so a concurrent webhook + sweep mint for the same (project,
// kind, repo, number) collide on AlreadyExists rather than minting twice, and
// the create itself goes through createTaskRaceSafe.
func (m *Minter) MintIssueTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	ext scm.Issue, stg, reason string, sp objbudget.Spiller) (*tatarav1alpha1.Task, MintOutcome, error) {

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, ext.Number),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:        proj.Name,
			Kind:              SweepIssueKind,
			Goal:              issueGoal(ext),
			InitialState:      stg,
			InitialParkReason: reason,
			Source: &tatarav1alpha1.TaskSource{
				Provider:    providerOf(proj),
				IssueRef:    fmt.Sprintf("%s#%d", ext.URL, ext.Number),
				Number:      ext.Number,
				Title:       ext.Title,
				AuthorLogin: ext.Author,
			},
		},
	}
	// Issue #517's descriptive pod name, stamped ONCE here (Kind and Source are
	// set). createTaskRaceSafe is ADOPT-OR-CREATE and never writes this literal
	// on the adopt path, so an existing twin keeps whatever it was minted with -
	// including nothing at all, where agent.PodName's wrapper-<name> fallback
	// still applies.
	agent.StampPodName(task, proj.Name, repo.Name)
	if err := controllerutil.SetControllerReference(proj, task, m.Scheme); err != nil {
		return mintErr(SweepIssueKind, fmt.Errorf("intake: set task ownerref: %w", err))
	}
	// createTaskRaceSafe counts the outcome it decides (including its own
	// errors), so nothing below re-counts a created/existing/tombstone verdict.
	outcome, existing, err := m.createTaskRaceSafe(ctx, task)
	if err != nil {
		return nil, MintNotOwed, err
	}
	if outcome != MintCreated {
		// Backstop: repair an interrupted mint that left the twin's Issue CR an
		// unbound stub (the MergeRequest analogue in MintReviewTask documents why).
		if existing != nil {
			if rerr := m.repairIssueBinding(ctx, proj, repo, ext, existing, sp); rerr != nil {
				return mintErr(SweepIssueKind, rerr)
			}
		}
		return task, outcome, nil
	}
	if err := SyncIssue(ctx, m.Client, sp, proj, repo, ext); err != nil {
		return mintErr(SweepIssueKind, fmt.Errorf("intake: sync issue: %w", err))
	}
	issName := tatarav1alpha1.IssueName(repo.Name, ext.Number)
	if err := ownIssueForTask(ctx, m.Client, proj.Namespace, issName, task); err != nil {
		return mintErr(SweepIssueKind, err)
	}
	// The STAGE comes from Spec.InitialState via the TaskReconciler create-edge
	// (fix C5): NO racing post-create stage write. Only issueRefs is stamped here,
	// under RetryOnConflict, so it survives the reconciler winning the create-edge
	// race and stamping the stage first.
	if err := m.stampMintStatus(ctx, task, func(fresh *tatarav1alpha1.Task) {
		if !slices.Contains(fresh.Status.IssueRefs, issName) {
			fresh.Status.IssueRefs = append(fresh.Status.IssueRefs, issName)
		}
	}); err != nil {
		return mintErr(SweepIssueKind, err)
	}
	if m.Metrics != nil {
		m.Metrics.OrphanAdopted(SweepIssueKind)
	}
	return task, MintCreated, nil
}

// MintReviewTask is mintReviewTaskForPR's race-safe body (unchanged ADOPT-OR-
// CREATE on the MergeRequest CR): deterministic IntakeTaskName + createTaskRaceSafe
// in place of the random-suffixed TaskName + blind Create.
// expectFrom optionally names the controller owner this mint's MR bind is
// allowed to hand over FROM atomically (fix #408: reMintReviewOwner's
// captured prevOwner, threaded down to bindMRToTask -> ownMergeRequest, so
// the demote and the promote land in the SAME Update). Omit it (or pass "")
// for the ordinary fresh-mint case, where no prior controller is expected -
// every call site except reMintReviewOwner.
func (m *Minter) MintReviewTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	pr scm.PRRef, cr *tatarav1alpha1.MergeRequest, stg, reason string, sp objbudget.Spiller, expectFrom ...string) (*tatarav1alpha1.Task, MintOutcome, error) {

	ext := mrSnapshot(proj, repo, pr)
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.IntakeTaskName(proj.Name, SweepReviewKind, repo.Name, pr.Number),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:        proj.Name,
			Kind:              SweepReviewKind,
			Goal:              fmt.Sprintf("Review %s", ext.URL),
			InitialState:      stg,
			InitialParkReason: reason,
			Source: &tatarav1alpha1.TaskSource{
				Provider:    providerOf(proj),
				IssueRef:    ext.URL,
				Number:      pr.Number,
				IsPR:        true,
				HeadSHA:     pr.HeadSHA,
				AuthorLogin: pr.Author,
			},
		},
	}
	// See MintIssueTask: same stamp, same repo ref, so the two intake mints and
	// the queue path all name the same repo the same way.
	agent.StampPodName(task, proj.Name, repo.Name)
	if err := controllerutil.SetControllerReference(proj, task, m.Scheme); err != nil {
		return mintErr(SweepReviewKind, fmt.Errorf("intake: set task ownerref: %w", err))
	}
	outcome, existing, err := m.createTaskRaceSafe(ctx, task)
	if err != nil {
		return nil, MintNotOwed, err
	}
	if outcome != MintCreated {
		// Backstop: the natural-key twin already exists. An interrupted mint (a
		// restart between the Task create and the bind below) can leave that twin's
		// MergeRequest CR an UNBOUND stub - no owner, empty status - and nothing
		// else repairs it: the cadence reconciler only re-syncs the comment thread.
		// The twin then owns no open MR forever and its review agent's
		// submit_outcome 400s on every retry. Repair the bind against the existing
		// twin (a no-op once it is already bound).
		if existing != nil {
			if rerr := m.repairMRBinding(ctx, proj, repo, ext, existing, sp); rerr != nil {
				return mintErr(SweepReviewKind, rerr)
			}
		}
		return task, outcome, nil
	}
	if err := m.bindMRToTask(ctx, proj, repo, ext, task, sp, expectFrom...); err != nil {
		return mintErr(SweepReviewKind, err)
	}
	// Stage from Spec.InitialState via the create-edge (fix C5); mrRefs +
	// humanReviewRounds stamped under RetryOnConflict so they survive the reconciler
	// winning the create-edge race.
	mrName := tatarav1alpha1.MergeRequestName(repo.Name, pr.Number)
	rounds := carriedHumanReviewRounds(cr)
	if err := m.stampMintStatus(ctx, task, func(fresh *tatarav1alpha1.Task) {
		if !slices.Contains(fresh.Status.MRRefs, mrName) {
			fresh.Status.MRRefs = append(fresh.Status.MRRefs, mrName)
		}
		fresh.Status.HumanReviewRounds = rounds
	}); err != nil {
		return mintErr(SweepReviewKind, err)
	}
	if m.Metrics != nil {
		m.Metrics.OrphanAdopted(SweepReviewKind)
	}
	return task, MintCreated, nil
}

// createTaskRaceSafe creates task idempotently on its DETERMINISTIC name. On a
// natural-key collision (a concurrent webhook + sweep, or the backstop pass over
// an already-minted item) it returns MintExistingLive or MintTombstoneDeleted
// rather than a second Task.
// A collision with a DEAD (terminal/deleting) twin of the same name is the
// re-mint-after-reap case: delete the tombstone and retry, so a legitimately new
// event is never blocked by a dead name.
//
// On MintExistingLive, that live twin is returned so the caller can REPAIR an
// interrupted mint (the twin's Issue/MR mirror may still be an unbound stub -
// see MintReviewTask/MintIssueTask). It is nil on every other non-created path
// (a terminal twin just got deleted for a clean re-mint; there is nothing to
// repair).
func (m *Minter) createTaskRaceSafe(ctx context.Context, task *tatarav1alpha1.Task) (MintOutcome, *tatarav1alpha1.Task, error) {
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	// THE natural-key arbiter, so THE place the outcome is metered: counting in
	// MintIssueTask/MintReviewTask instead left the third caller
	// (MintOrUnparkTakeoverTask, which calls this directly) off the series
	// entirely, and with it every tombstone delete on the takeover path.
	count := func(o MintOutcome) MintOutcome {
		obs.MintOutcomeTotal.WithLabelValues(task.Spec.Kind, string(o)).Inc()
		return o
	}
	countErr := func() { obs.MintOutcomeTotal.WithLabelValues(task.Spec.Kind, obs.MintOutcomeError).Inc() }
	// Live (uncached) pre-check shrinks the window before the deterministic-name
	// collision even applies; the collision below is the actual arbiter.
	existing := &tatarav1alpha1.Task{}
	if err := m.reader().Get(ctx, key, existing); err == nil {
		if existing.DeletionTimestamp == nil && !tatarav1alpha1.TaskDone(existing) {
			return count(MintExistingLive), existing, nil // live twin: the backstop no-ops the create but repairs the bind
		}
	} else if !apierrors.IsNotFound(err) {
		countErr()
		return MintNotOwed, nil, fmt.Errorf("intake: pre-check task %s: %w", key.Name, err)
	}

	err := m.Client.Create(ctx, task)
	if err == nil {
		return count(MintCreated), nil, nil
	}
	if !apierrors.IsAlreadyExists(err) {
		countErr()
		return MintNotOwed, nil, fmt.Errorf("intake: create task %s: %w", key.Name, err)
	}
	// Resolve the winning twin through the UNCACHED reader (F3-1): a stale cache
	// that showed a still-LIVE twin as terminal would cascade a Delete of its
	// owned Issue/MR mirror. The Delete below stays on m.Client (writes go
	// through the writer), but the terminal/deleting decision must be live.
	if getErr := m.reader().Get(ctx, key, existing); getErr != nil {
		countErr()
		return MintNotOwed, nil, fmt.Errorf("intake: resolve existing task %s: %w", key.Name, getErr)
	}
	if existing.DeletionTimestamp != nil || tatarav1alpha1.TaskDone(existing) {
		if delErr := m.Client.Delete(ctx, existing); delErr != nil && !apierrors.IsNotFound(delErr) {
			countErr()
			return MintNotOwed, nil, fmt.Errorf("intake: delete stale terminal task %s: %w", key.Name, delErr)
		}
		log.FromContext(ctx).Info("intake: deleted stale terminal task on name collision; the mint is still OWED",
			"action", "intake_stale_delete", "resource_id", key.Name)
		return count(MintTombstoneDeleted), nil, nil // the caller re-drives; see MintOutcome
	}
	return count(MintExistingLive), existing, nil // live twin
}

// issueCR returns the Issue CR for (repo, number), or nil when none exists.
//
// Deliberately reads through m.Client (cached), NOT m.reader(). A 2026-07-28
// review round tried switching this to the uncached reader (the same idiom
// MarkWebhookOriginated uses) and reverted it. Two things justify the cached
// read on their own, neither of which depends on any one caller:
//
//   - THE PACKAGE PATTERN. Only the specific re-entrant read that genuinely
//     needs freshness is promoted to APIReader; the broader orphan/ownership
//     CLASSIFICATION read stays on the cached Client. Promoting this one would
//     make the exception the rule.
//   - THE VERDICT IS INSENSITIVE TO IT. cr=nil and cr=<a freshly-created,
//     still-ownerless CR> are equivalent for IsOrphanIssue: its cr!=nil guard
//     only ever changes the outcome when cr IS owned. So the read-after-write
//     race is closed for this read's caller by MarkWebhookOriginated's own
//     create-time stamp and reader threading, and this Get does not need to
//     duplicate that.
//
// The resume path is back (resume.go, finding H8) and it is again a caller that
// severs an Issue's ownership through m.Client and then calls MintForItem in the
// SAME pass, so this Get must keep observing that sever. That is a consequence
// of the two reasons above, not a third one: the reasoning is what pins this
// decision, not any one caller or test name.
func (m *Minter) issueCR(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) (*tatarav1alpha1.Issue, error) {
	var iss tatarav1alpha1.Issue
	key := types.NamespacedName{Namespace: proj.Namespace, Name: tatarav1alpha1.IssueName(repo.Name, number)}
	if err := m.Client.Get(ctx, key, &iss); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &iss, nil
}

// mergeRequestCR is issueCR's MergeRequest half.
func (m *Minter) mergeRequestCR(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) (*tatarav1alpha1.MergeRequest, error) {
	var mr tatarav1alpha1.MergeRequest
	key := types.NamespacedName{Namespace: proj.Namespace, Name: tatarav1alpha1.MergeRequestName(repo.Name, number)}
	if err := m.Client.Get(ctx, key, &mr); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &mr, nil
}

// stampMintStatus stamps a freshly minted Task's STATUS (issueRefs / mrRefs /
// humanReviewRounds) under RetryOnConflict, WITHOUT touching the stage - the
// stage is derived by the TaskReconciler create-edge from Spec.InitialState (fix
// C5). mutate must be idempotent (it runs on every retry against the fresh
// object).
func (m *Minter) stampMintStatus(ctx context.Context, task *tatarav1alpha1.Task, mutate func(*tatarav1alpha1.Task)) error {
	return m.stampMintStatusIfChanged(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		mutate(fresh)
		return true
	})
}

// stampMintStatusIfChanged is stampMintStatus with a mutator that REPORTS
// whether it changed anything. A false return skips the Status().Update
// entirely and still refreshes the caller's copy.
//
// The distinction is only worth having because the decision must be made
// against the FRESH object, never the caller's in-memory one. Since #604,
// repairMRBinding's owned-by-us branch reaches the mrRefs stamp on paths that
// run per open merge request per sweep pass, so an unconditional Update spent
// an apiserver round trip - and a RetryOnConflict path under contention - on
// every already-converged object, writing bytes identical to the ones there.
// Short-circuiting on the CALLER's copy instead would be a correctness bug, not
// an optimisation: a stale copy that still lists the ref is exactly the state an
// interrupted mint leaves behind, and skipping there would decline to perform
// the repair this function exists for.
func (m *Minter) stampMintStatusIfChanged(ctx context.Context, task *tatarav1alpha1.Task,
	mutate func(*tatarav1alpha1.Task) bool) error {

	key := client.ObjectKeyFromObject(task)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := m.Client.Get(ctx, key, fresh); err != nil {
			return err
		}
		if !mutate(fresh) {
			*task = *fresh
			return nil
		}
		if err := m.Client.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*task = *fresh
		return nil
	}); err != nil {
		return fmt.Errorf("intake: stamp mint status on %s: %w", task.Name, err)
	}
	return nil
}

// bindMRToTask mirrors the MR and hands the Task its controller ownership.
// expectFrom (optional, see MintReviewTask) is forwarded to ownMergeRequest
// unchanged; omitted or "" for the ordinary fresh-mint case.
func (m *Minter) bindMRToTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	ext scm.MergeRequest, task *tatarav1alpha1.Task, sp objbudget.Spiller, expectFrom ...string) error {

	if err := SyncMergeRequest(ctx, m.Client, sp, proj, repo, ext); err != nil {
		return fmt.Errorf("intake: sync mergerequest: %w", err)
	}
	ef := ""
	if len(expectFrom) > 0 {
		ef = expectFrom[0]
	}
	return m.ownMergeRequest(ctx, proj, tatarav1alpha1.MergeRequestName(repo.Name, ext.Number), task, ef)
}

// repairMRBinding heals a mint that was interrupted between the Task create and
// bindMRToTask: the Task is live but its MergeRequest CR is an UNBOUND stub (no
// controller owner, empty status), or was never created at all. It re-runs the
// SAME mirror-and-own bind against the existing twin and re-stamps mrRefs. It
// NEVER steals: a CR controller-owned by ANOTHER Task is left completely
// untouched, so the repair is safe to run on every backstop pass and race-safe
// under concurrency (ownMergeRequest is the real arbiter; this Get is a cheap
// skip for the common already-bound case).
//
// THE OWNED-BY-US CASE FALLS THROUGH TO THE REF STAMP (#604). It used to return
// nil for ANY controller-owned CR, which conflated "someone else's, hands off"
// with "ours, and the bind half already landed". The mint is THREE writes -
// createTaskRaceSafe, bindMRToTask, stampMintStatus - so an interruption between
// the second and third leaves exactly that shape: the CR is ours, and mrRefs is
// empty. Skipping it there made this backstop structurally unable to repair the
// window it exists for, and a takeover's MR ALWAYS has a controller owner, so
// the backstop was inert for that kind entirely. The bind is not re-run (it
// already happened and ownMergeRequest would no-op anyway); only the stamp is,
// and stampMintStatus is idempotent on the ref.
func (m *Minter) repairMRBinding(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	ext scm.MergeRequest, task *tatarav1alpha1.Task, sp objbudget.Spiller) error {

	name := tatarav1alpha1.MergeRequestName(repo.Name, ext.Number)
	var mr tatarav1alpha1.MergeRequest
	err := m.Client.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: name}, &mr)
	if err == nil {
		if ctrl, owned := own.ControllerOwner(&mr); owned {
			if ctrl != task.Name {
				return nil // foreign-owned: never steal, never re-mirror
			}
			// Already OURS: the bind landed, the mrRefs stamp may not have.
			return m.stampMRRef(ctx, task, name)
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("intake: repair get mergerequest %s: %w", name, err)
	}
	if err := m.bindMRToTask(ctx, proj, repo, ext, task, sp); err != nil {
		return err
	}
	log.FromContext(ctx).Info("intake: repaired interrupted mint binding",
		"action", "intake_repair_bind", "resource_id", task.Name, "mr", name,
		"kind", task.Spec.Kind)
	return m.stampMRRef(ctx, task, name)
}

// stampMRRef appends name to task.status.mrRefs, once. It is the LAST of the
// three writes a mint makes, and the one an interruption most often loses -
// mrRefs is what submit_outcome(action=submitted)'s open-MR gate and mr_write's
// open-idempotency both read, so a Task missing it can neither finish nor
// safely re-open.
//
// It writes only when the ref is genuinely absent from the FRESH object - see
// stampMintStatusIfChanged for why that distinction is load-bearing rather than
// a micro-optimisation.
func (m *Minter) stampMRRef(ctx context.Context, task *tatarav1alpha1.Task, name string) error {
	return m.stampMintStatusIfChanged(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if slices.Contains(fresh.Status.MRRefs, name) {
			return false
		}
		fresh.Status.MRRefs = append(fresh.Status.MRRefs, name)
		return true
	})
}

// repairIssueBinding is repairMRBinding's Issue-mint counterpart, and it carries
// the same OWNED-BY-US fall-through for the same reason (#604 review). The Issue
// mint is three writes too - createTaskRaceSafe, SyncIssue+ownIssueForTask,
// stampMintStatus - so an interruption between the last two leaves an Issue CR
// controller-owned by THIS Task and an EMPTY status.issueRefs, and the old
// blanket "any controller owner means hands off" skip declined to repair exactly
// that.
//
// It matters MORE here than on the merge-request side, because issueRefs is what
// verifyApprovalScope counts: a Task carrying none is refused no-live-issue at
// `refined` forever - the #604 strand itself, arrived at from the other
// direction and on the LIVE issue path rather than a dead one.
//
// The bind is not re-run when the CR is already ours (ownIssueForTask would
// no-op); only the stamp is, and it is idempotent on the ref.
func (m *Minter) repairIssueBinding(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	ext scm.Issue, task *tatarav1alpha1.Task, sp objbudget.Spiller) error {

	name := tatarav1alpha1.IssueName(repo.Name, ext.Number)
	var iss tatarav1alpha1.Issue
	err := m.Client.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: name}, &iss)
	if err == nil {
		if ctrl, owned := own.ControllerOwner(&iss); owned {
			if ctrl != task.Name {
				return nil // foreign-owned: never steal, never re-mirror
			}
			// Already OURS: the bind landed, the issueRefs stamp may not have.
			return m.stampIssueRef(ctx, task, name)
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("intake: repair get issue %s: %w", name, err)
	}
	if err := SyncIssue(ctx, m.Client, sp, proj, repo, ext); err != nil {
		return fmt.Errorf("intake: repair sync issue: %w", err)
	}
	if err := ownIssueForTask(ctx, m.Client, proj.Namespace, name, task); err != nil {
		return err
	}
	log.FromContext(ctx).Info("intake: repaired interrupted issue mint binding",
		"action", "intake_repair_bind", "resource_id", task.Name, "issue", name)
	return m.stampIssueRef(ctx, task, name)
}

// stampIssueRef is stampMRRef's Issue counterpart: it appends name to
// task.status.issueRefs, once, and only when the ref is genuinely absent from
// the FRESH object.
//
// The freshness is the whole point, exactly as it is for stampMRRef: an
// interrupted mint leaves a CALLER whose in-memory copy may still list the ref
// it never persisted, so deciding against the caller's copy would decline to
// perform the repair this function exists for.
func (m *Minter) stampIssueRef(ctx context.Context, task *tatarav1alpha1.Task, name string) error {
	return m.stampMintStatusIfChanged(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if slices.Contains(fresh.Status.IssueRefs, name) {
			return false
		}
		fresh.Status.IssueRefs = append(fresh.Status.IssueRefs, name)
		return true
	})
}

// ownMergeRequest appends task as the MergeRequest CR's controller owner. It
// NEVER steals: an artifact controller-owned by anyone other than task itself
// or expectFrom is not this call's to take, so reaching here in that state is
// a bug, not a race to paper over.
//
// expectFrom names the ONE other controller this call is allowed to hand the
// flag over FROM, atomically, in this same RetryOnConflict Update - fix #408:
// no standalone Controller=false demote Update followed by a separate
// promote, which left a zero-controller window RepairZeroController could
// race and promote the wrong owner into. Pass "" for the ordinary fresh-mint
// case, where no prior controller is expected at all.
func (m *Minter) ownMergeRequest(ctx context.Context, proj *tatarav1alpha1.Project, name string, task *tatarav1alpha1.Task, expectFrom string) error {
	key := types.NamespacedName{Namespace: proj.Namespace, Name: name}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var mr tatarav1alpha1.MergeRequest
		if err := m.Client.Get(ctx, key, &mr); err != nil {
			return err
		}
		cur, ok := own.ControllerOwner(&mr)
		if ok {
			if cur == task.Name {
				return nil // already owns it: idempotent no-op
			}
			if cur != expectFrom {
				return fmt.Errorf("mergerequest %s already has controller owner %s", name, cur)
			}
		}
		own.AddPlainOwner(&mr, task)
		var from *tatarav1alpha1.Task
		if ok {
			from = &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: cur, Namespace: proj.Namespace}}
		}
		if err := own.HandOverController(&mr, from, task); err != nil {
			return err
		}
		return m.Client.Update(ctx, &mr)
	})
	if err != nil {
		return fmt.Errorf("intake: own mergerequest %s: %w", name, err)
	}
	return nil
}
