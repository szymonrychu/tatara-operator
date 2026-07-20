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

// ForgeItem is one forge work item the intake funnel classifies + mints for.
type ForgeItem struct {
	IsPR  bool
	Issue scm.Issue // when !IsPR
	PR    scm.PRRef // when IsPR
}

// MintForItem classifies item with the SAME predicates the sweep uses and mints
// the Task if one is owed, race-safe on the natural key. created=false means
// "nothing to mint" (bot/ignored/already-owned) OR "the backstop found it
// already minted". It applies NO creation budget: the webhook mints a live human
// signal immediately, and downstream admission (ensureTicket -> dispatcher)
// bounds concurrency. The sweep keeps its own budget check BEFORE calling the
// per-stage mint helpers (see sweepIssues/sweepPRs).
func (m *Minter) MintForItem(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, item ForgeItem, webhookOriginated bool,
	sp objbudget.Spiller) (*tatarav1alpha1.Task, bool, error) {

	if item.IsPR {
		cr, err := m.mergeRequestCR(ctx, proj, repo, item.PR.Number)
		if err != nil {
			return nil, false, err
		}
		// A human PR never carries a task/<name> head branch, so it has no owning
		// Task by branch: ClassifyPR's orphan check keys on the MR CR owner only.
		switch ClassifyPR(proj, repo, item.PR, nil, cr) {
		case PRReview:
			stg, reason := MintReviewStage(cr)
			return m.MintReviewTask(ctx, proj, repo, item.PR, cr, stg, reason, sp)
		default: // PRAdopt (sweep-only) / PRIgnore
			return nil, false, nil
		}
	}

	cr, err := m.issueCR(ctx, proj, repo, item.Issue.Number)
	if err != nil {
		return nil, false, err
	}
	if !IsOrphanIssue(proj, repo, item.Issue, cr) {
		return nil, false, nil
	}
	stg, reason := MintStage(proj, item.Issue, webhookOriginated)
	return m.MintIssueTask(ctx, proj, repo, item.Issue, stg, reason, sp)
}

// MintIssueTask is mintTaskForIssue's race-safe body (fix M3-10's ADOPT-OR-CREATE
// on the Issue CR, unchanged): the Task name is now the DETERMINISTIC
// IntakeTaskName so a concurrent webhook + sweep mint for the same (project,
// kind, repo, number) collide on AlreadyExists rather than minting twice, and
// the create itself goes through createTaskRaceSafe.
func (m *Minter) MintIssueTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	ext scm.Issue, stg, reason string, sp objbudget.Spiller) (*tatarav1alpha1.Task, bool, error) {

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, ext.Number),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:         proj.Name,
			Kind:               SweepIssueKind,
			Goal:               issueGoal(ext),
			InitialStage:       stg,
			InitialStageReason: reason,
			Source: &tatarav1alpha1.TaskSource{
				Provider:    providerOf(proj),
				IssueRef:    fmt.Sprintf("%s#%d", ext.URL, ext.Number),
				Number:      ext.Number,
				Title:       ext.Title,
				AuthorLogin: ext.Author,
			},
		},
	}
	if err := controllerutil.SetControllerReference(proj, task, m.Scheme); err != nil {
		return nil, false, fmt.Errorf("intake: set task ownerref: %w", err)
	}
	created, existing, err := m.createTaskRaceSafe(ctx, task)
	if err != nil {
		return nil, false, err
	}
	if !created {
		// Backstop: repair an interrupted mint that left the twin's Issue CR an
		// unbound stub (the MergeRequest analogue in MintReviewTask documents why).
		if existing != nil {
			if rerr := m.repairIssueBinding(ctx, proj, repo, ext, existing, sp); rerr != nil {
				return nil, false, rerr
			}
		}
		return task, false, nil
	}
	if err := SyncIssue(ctx, m.Client, sp, proj, repo, ext); err != nil {
		return nil, false, fmt.Errorf("intake: sync issue: %w", err)
	}
	issName := tatarav1alpha1.IssueName(repo.Name, ext.Number)
	if err := ownIssueForTask(ctx, m.Client, proj.Namespace, issName, task); err != nil {
		return nil, false, err
	}
	// The STAGE comes from Spec.InitialStage via the TaskReconciler create-edge
	// (fix C5): NO racing post-create stage write. Only issueRefs is stamped here,
	// under RetryOnConflict, so it survives the reconciler winning the create-edge
	// race and stamping the stage first.
	if err := m.stampMintStatus(ctx, task, func(fresh *tatarav1alpha1.Task) {
		if !slices.Contains(fresh.Status.IssueRefs, issName) {
			fresh.Status.IssueRefs = append(fresh.Status.IssueRefs, issName)
		}
	}); err != nil {
		return nil, false, err
	}
	if m.Metrics != nil {
		m.Metrics.OrphanAdopted(SweepIssueKind)
	}
	return task, true, nil
}

// MintReviewTask is mintReviewTaskForPR's race-safe body (unchanged ADOPT-OR-
// CREATE on the MergeRequest CR): deterministic IntakeTaskName + createTaskRaceSafe
// in place of the random-suffixed TaskName + blind Create.
func (m *Minter) MintReviewTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	pr scm.PRRef, cr *tatarav1alpha1.MergeRequest, stg, reason string, sp objbudget.Spiller) (*tatarav1alpha1.Task, bool, error) {

	ext := mrSnapshot(proj, repo, pr)
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.IntakeTaskName(proj.Name, SweepReviewKind, repo.Name, pr.Number),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:         proj.Name,
			Kind:               SweepReviewKind,
			Goal:               fmt.Sprintf("Review %s", ext.URL),
			InitialStage:       stg,
			InitialStageReason: reason,
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
	if err := controllerutil.SetControllerReference(proj, task, m.Scheme); err != nil {
		return nil, false, fmt.Errorf("intake: set task ownerref: %w", err)
	}
	created, existing, err := m.createTaskRaceSafe(ctx, task)
	if err != nil {
		return nil, false, err
	}
	if !created {
		// Backstop: the natural-key twin already exists. An interrupted mint (a
		// restart between the Task create and the bind below) can leave that twin's
		// MergeRequest CR an UNBOUND stub - no owner, empty status - and nothing
		// else repairs it: the cadence reconciler only re-syncs the comment thread.
		// The twin then owns no open MR forever and its review agent's
		// submit_outcome 400s on every retry. Repair the bind against the existing
		// twin (a no-op once it is already bound).
		if existing != nil {
			if rerr := m.repairMRBinding(ctx, proj, repo, ext, existing, sp); rerr != nil {
				return nil, false, rerr
			}
		}
		return task, false, nil
	}
	if err := m.bindMRToTask(ctx, proj, repo, ext, task, sp); err != nil {
		return nil, false, err
	}
	// Stage from Spec.InitialStage via the create-edge (fix C5); mrRefs +
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
		return nil, false, err
	}
	if m.Metrics != nil {
		m.Metrics.OrphanAdopted(SweepReviewKind)
	}
	return task, true, nil
}

// createTaskRaceSafe creates task idempotently on its DETERMINISTIC name. On a
// natural-key collision (a concurrent webhook + sweep, or the backstop pass over
// an already-minted item) it returns created=false rather than a second Task.
// A collision with a DEAD (terminal/deleting) twin of the same name is the
// re-mint-after-reap case: delete the tombstone and retry, so a legitimately new
// event is never blocked by a dead name.
//
// When created=false because a LIVE twin already holds the name, that twin is
// returned so the caller can REPAIR an interrupted mint (the twin's Issue/MR
// mirror may still be an unbound stub - see MintReviewTask/MintIssueTask). It is
// nil on every other created=false path (a terminal twin just got deleted for a
// clean re-mint; there is nothing to repair).
func (m *Minter) createTaskRaceSafe(ctx context.Context, task *tatarav1alpha1.Task) (bool, *tatarav1alpha1.Task, error) {
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	// Live (uncached) pre-check shrinks the window before the deterministic-name
	// collision even applies; the collision below is the actual arbiter.
	existing := &tatarav1alpha1.Task{}
	if err := m.reader().Get(ctx, key, existing); err == nil {
		if existing.DeletionTimestamp == nil && !tatarav1alpha1.TaskDone(existing) {
			return false, existing, nil // live twin: the backstop no-ops the create but repairs the bind
		}
	} else if !apierrors.IsNotFound(err) {
		return false, nil, fmt.Errorf("intake: pre-check task %s: %w", key.Name, err)
	}

	err := m.Client.Create(ctx, task)
	if err == nil {
		return true, nil, nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return false, nil, fmt.Errorf("intake: create task %s: %w", key.Name, err)
	}
	// Resolve the winning twin through the UNCACHED reader (F3-1): a stale cache
	// that showed a still-LIVE twin as terminal would cascade a Delete of its
	// owned Issue/MR mirror. The Delete below stays on m.Client (writes go
	// through the writer), but the terminal/deleting decision must be live.
	if getErr := m.reader().Get(ctx, key, existing); getErr != nil {
		return false, nil, fmt.Errorf("intake: resolve existing task %s: %w", key.Name, getErr)
	}
	if existing.DeletionTimestamp != nil || tatarav1alpha1.TaskDone(existing) {
		if delErr := m.Client.Delete(ctx, existing); delErr != nil && !apierrors.IsNotFound(delErr) {
			return false, nil, fmt.Errorf("intake: delete stale terminal task %s: %w", key.Name, delErr)
		}
		log.FromContext(ctx).Info("intake: deleted stale terminal task on name collision; re-minting next pass",
			"action", "intake_stale_delete", "resource_id", key.Name)
		return false, nil, nil // re-mint on the next tick against the freed name
	}
	return false, existing, nil // live twin
}

// issueCR returns the Issue CR for (repo, number), or nil when none exists.
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
// stage is derived by the TaskReconciler create-edge from Spec.InitialStage (fix
// C5). mutate must be idempotent (it runs on every retry against the fresh
// object).
func (m *Minter) stampMintStatus(ctx context.Context, task *tatarav1alpha1.Task, mutate func(*tatarav1alpha1.Task)) error {
	key := client.ObjectKeyFromObject(task)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := m.Client.Get(ctx, key, fresh); err != nil {
			return err
		}
		mutate(fresh)
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
func (m *Minter) bindMRToTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	ext scm.MergeRequest, task *tatarav1alpha1.Task, sp objbudget.Spiller) error {

	if err := SyncMergeRequest(ctx, m.Client, sp, proj, repo, ext); err != nil {
		return fmt.Errorf("intake: sync mergerequest: %w", err)
	}
	return m.ownMergeRequest(ctx, proj, tatarav1alpha1.MergeRequestName(repo.Name, ext.Number), task)
}

// repairMRBinding heals a review mint that was interrupted between the Task
// create and bindMRToTask: the Task is live but its MergeRequest CR is an UNBOUND
// stub (no controller owner, empty status), or was never created at all. It
// re-runs the SAME mirror-and-own bind against the existing twin and re-stamps
// mrRefs. It NEVER steals: a CR already controller-owned - by the twin (the
// idempotent re-run) or, defensively, by anyone else - is left untouched, so the
// repair is safe to run on every backstop pass and race-safe under concurrency
// (ownMergeRequest is the real arbiter; this Get is a cheap skip for the common
// already-bound case).
func (m *Minter) repairMRBinding(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	ext scm.MergeRequest, task *tatarav1alpha1.Task, sp objbudget.Spiller) error {

	name := tatarav1alpha1.MergeRequestName(repo.Name, ext.Number)
	var mr tatarav1alpha1.MergeRequest
	err := m.Client.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: name}, &mr)
	if err == nil {
		if _, owned := own.ControllerOwner(&mr); owned {
			return nil // already bound (or foreign-owned): never steal, never re-mirror
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("intake: repair get mergerequest %s: %w", name, err)
	}
	if err := m.bindMRToTask(ctx, proj, repo, ext, task, sp); err != nil {
		return err
	}
	log.FromContext(ctx).Info("intake: repaired interrupted review mint binding",
		"action", "intake_repair_bind", "resource_id", task.Name, "mr", name)
	return m.stampMintStatus(ctx, task, func(fresh *tatarav1alpha1.Task) {
		if !slices.Contains(fresh.Status.MRRefs, name) {
			fresh.Status.MRRefs = append(fresh.Status.MRRefs, name)
		}
	})
}

// repairIssueBinding is repairMRBinding's Issue-mint counterpart.
func (m *Minter) repairIssueBinding(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	ext scm.Issue, task *tatarav1alpha1.Task, sp objbudget.Spiller) error {

	name := tatarav1alpha1.IssueName(repo.Name, ext.Number)
	var iss tatarav1alpha1.Issue
	err := m.Client.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: name}, &iss)
	if err == nil {
		if _, owned := own.ControllerOwner(&iss); owned {
			return nil // already bound (or foreign-owned): never steal, never re-mirror
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
	return m.stampMintStatus(ctx, task, func(fresh *tatarav1alpha1.Task) {
		if !slices.Contains(fresh.Status.IssueRefs, name) {
			fresh.Status.IssueRefs = append(fresh.Status.IssueRefs, name)
		}
	})
}

// ownMergeRequest appends task as the MergeRequest CR's controller owner. It
// NEVER steals: an artifact that already has a controller owner is not an
// orphan, so reaching here with a different controller is a bug, not a race to
// paper over.
func (m *Minter) ownMergeRequest(ctx context.Context, proj *tatarav1alpha1.Project, name string, task *tatarav1alpha1.Task) error {
	key := types.NamespacedName{Namespace: proj.Namespace, Name: name}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var mr tatarav1alpha1.MergeRequest
		if err := m.Client.Get(ctx, key, &mr); err != nil {
			return err
		}
		if cur, ok := own.ControllerOwner(&mr); ok {
			if cur != task.Name {
				return fmt.Errorf("mergerequest %s already has controller owner %s", name, cur)
			}
			return nil
		}
		own.AddPlainOwner(&mr, task)
		if err := own.HandOverController(&mr, nil, task); err != nil {
			return err
		}
		return m.Client.Update(ctx, &mr)
	})
	if err != nil {
		return fmt.Errorf("intake: own mergerequest %s: %w", name, err)
	}
	return nil
}
