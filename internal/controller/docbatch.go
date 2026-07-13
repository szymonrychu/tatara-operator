package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE NIGHTLY DOCUMENTATION BATCH (contract B.6, fix F2).
//
// ONE doc pod, ONE docs MR, ONE review, ONE tatara-documentation release per
// NIGHT - instead of one of each per delivered one-line fix. The batch covers N
// delivered Tasks; on reaching delivered it stamps status.documentedBy on every
// one of them, which is what finally lets the reaper collect them.
//
// A brainstorm `skip` is NEVER covered (a cron brainstorm that correctly says
// "nothing novel" must not spawn a docs pod, a docs PR about nothing, a review, a
// merge and a release, every day). An incident false_positive, a fold-only refine
// and a declined implement are likewise never documented and have documentedBy
// permanently empty. They all share one shape: ZERO merged MRs.

// DocBatchKind is the Task kind the nightly batch mints.
const DocBatchKind = "documentation"

// MintDocBatch is the nightly cron, once per project per night. It mints ONE
// documentation Task covering every delivered Task that still needs documenting,
// or NOTHING when there is nothing to document.
func (r *ProjectReconciler) MintDocBatch(ctx context.Context, proj *tatarav1alpha1.Project) error {
	l := log.FromContext(ctx)

	docsRepo, err := r.docsRepository(ctx, proj)
	if err != nil {
		return err
	}
	if docsRepo == nil {
		return nil // docs not enabled, or the docs repo is not enrolled: nowhere to write.
	}

	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return fmt.Errorf("docbatch: list tasks: %w", err)
	}

	var covered []string
	for i := range tl.Items {
		t := &tl.Items[i]
		if t.Spec.ProjectRef != proj.Name || t.Status.Stage != tatarav1alpha1.StageDelivered {
			continue
		}
		// A batch already in flight OWNS this night's parents. Minting a second one
		// would put the same Tasks in two batches, race two docs PRs at the same
		// docs repo, and double the release train.
		needs, err := r.needsDocumenting(ctx, t)
		if err != nil {
			return err
		}
		if needs {
			covered = append(covered, t.Name)
		}
	}
	if len(covered) == 0 {
		l.V(1).Info("docbatch: nothing delivered needs documenting; minting nothing",
			"action", "doc_batch_empty", "resource_id", proj.Name)
		return nil
	}
	if inFlight := docBatchInFlight(tl.Items, proj.Name); inFlight != "" {
		l.Info("docbatch: a batch is already in flight; deferring this night's mint",
			"action", "doc_batch_inflight", "resource_id", proj.Name, "batch", inFlight)
		return nil
	}
	sort.Strings(covered) // deterministic membership, deterministic goal

	now := time.Now()
	batch := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.TaskName(proj.Name, DocBatchKind, now, rand.String(5)),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:     proj.Name,
			Kind:           DocBatchKind,
			RepositoryRef:  docsRepo.Name,
			DocumentsTasks: covered,
			// Create -> documenting is derived by the reconciler create-edge from
			// Spec.InitialStage (fix C5): no racing post-create stage write.
			InitialStage: tatarav1alpha1.StageDocumenting,
			Goal: fmt.Sprintf(
				"Nightly documentation batch. %d task(s) were delivered and merged since the "+
					"last batch:\n\n%s\n\nUpdate the documentation repo for whichever of them are "+
					"doc-relevant, in ONE pull request. No-op on the ones that are not.",
				len(covered), "- "+strings.Join(covered, "\n- ")),
		},
	}
	if err := controllerutil.SetControllerReference(proj, batch, r.Scheme); err != nil {
		return fmt.Errorf("docbatch: set task ownerref: %w", err)
	}
	if err := r.Create(ctx, batch); err != nil {
		return fmt.Errorf("docbatch: create batch task: %w", err)
	}
	// Create -> documenting is an F.3 edge in its own right: the batch is minted
	// STRAIGHT into its agent stage, with no triage. The stage is carried in
	// Spec.InitialStage and applied by the reconciler create-edge (fix C5), so no
	// post-create status write races the reconciler.
	l.Info("minted the nightly documentation batch",
		"action", "doc_batch_mint", "resource_id", batch.Name, "project", proj.Name,
		"docs_repo", docsRepo.Name, "covers", len(covered))
	return nil
}

// ResolveDocBatch settles a documentation batch that has reached a terminal, and
// it is where fix M21 lives.
//
//	delivered (normal)              -> stamp documentedBy on every covered Task.
//	delivered(doc-timeout) / parked -> IF stats.podRuns == 0 it NEVER RAN:
//	                                      stamp NOTHING. The covered Tasks stay
//	                                      documentedBy="" and the NEXT night's
//	                                      batch picks them up.
//	                                   ELSE it ran and failed: stamp anyway. The
//	                                      work was attempted; do not retry forever.
//
// THE podRuns == 0 CARVE-OUT IS NOT COSMETIC. v3 stamped documentedBy the moment
// the doc Task hit doc-timeout - and a priority-2 doc batch on a busy project
// STARVES, never gets an agent slot, times out at 2h having run ZERO pods, and
// stamps every member as documented. Result: on a busy project the docs are never
// written, every night, silently.
func (r *ProjectReconciler) ResolveDocBatch(ctx context.Context, batch *tatarav1alpha1.Task) error {
	if batch.Spec.Kind != DocBatchKind || len(batch.Spec.DocumentsTasks) == 0 {
		return nil
	}
	if batch.Annotations[AnnDocBatchResolved] == "true" {
		return nil
	}
	l := log.FromContext(ctx)

	abandoned := batch.Status.Stage == tatarav1alpha1.StageParked ||
		(batch.Status.Stage == tatarav1alpha1.StageDelivered && batch.Status.StageReason == stage.ReasonDocTimeout)

	if abandoned && batch.Status.Stats.PodRuns == 0 {
		obs.DocTaskAbandonedTotal.WithLabelValues(obs.DocAbandonedNeverRan).Inc()
		l.Info("documentation batch NEVER RAN (zero pod runs); its members stay undocumented for the next night",
			"action", "doc_batch_abandoned", "resource_id", batch.Name,
			"reason", obs.DocAbandonedNeverRan, "covers", len(batch.Spec.DocumentsTasks),
			"stage", batch.Status.Stage, "stage_reason", batch.Status.StageReason)
		return r.annotateTask(ctx, batch, AnnDocBatchResolved, "true")
	}
	if abandoned {
		obs.DocTaskAbandonedTotal.WithLabelValues(obs.DocAbandonedTimeout).Inc()
		l.Info("documentation batch ran and did not deliver; stamping its members anyway",
			"action", "doc_batch_abandoned", "resource_id", batch.Name,
			"reason", obs.DocAbandonedTimeout, "pod_runs", batch.Status.Stats.PodRuns)
	}
	if err := r.StampDocumentedBy(ctx, batch); err != nil {
		return err
	}
	return r.annotateTask(ctx, batch, AnnDocBatchResolved, "true")
}

// StampDocumentedBy writes status.documentedBy = <batch name> on EVERY covered
// Task. It is what un-pins them from the reaper's delivered gate. An already-
// stamped Task keeps its FIRST batch: the parent belongs to the batch that
// actually covered it.
func (r *ProjectReconciler) StampDocumentedBy(ctx context.Context, batch *tatarav1alpha1.Task) error {
	l := log.FromContext(ctx)
	for _, name := range batch.Spec.DocumentsTasks {
		key := client.ObjectKey{Namespace: batch.Namespace, Name: name}
		var parent tatarav1alpha1.Task
		if err := r.Get(ctx, key, &parent); err != nil {
			if apierrors.IsNotFound(err) {
				continue // already reaped; nothing to pin
			}
			return fmt.Errorf("docbatch: get covered task %s: %w", name, err)
		}
		if parent.Status.DocumentedBy != "" {
			continue
		}
		err := objbudget.FitTask(ctx, r.Client, nil, key, func(cur *tatarav1alpha1.Task) {
			if cur.Status.DocumentedBy == "" {
				cur.Status.DocumentedBy = batch.Name
			}
		})
		if err != nil {
			return fmt.Errorf("docbatch: stamp documentedBy on %s: %w", name, err)
		}
		l.Info("stamped documentedBy on a covered task",
			"action", "doc_batch_stamp", "resource_id", name, "batch", batch.Name)
	}
	return nil
}

// forceDocTimeout is B.6's stuck-documenting row: a batch still in `documenting`
// past docStageBudget (2h) is force-moved to delivered(doc-timeout) and resolved.
// The parent-pinning window is bounded on BOTH ends by this: a delivered Task
// waits at most docStageBudget past the batch that picks it up, and the batch is
// force-terminated at exactly that budget. A documentation Task can never pin its
// parent forever.
func (r *ProjectReconciler) forceDocTimeout(ctx context.Context, batch *tatarav1alpha1.Task, now time.Time) error {
	mrs, err := r.ownedMRs(ctx, batch)
	if err != nil {
		return err
	}
	// Through the CHOKE POINT: delivered(doc-timeout) is a terminal outcome and
	// must fire operator_task_terminal_total like every other one.
	stamp := metav1.NewTime(now)
	if err := EnterStage(ctx, r.Client, nil, r.Metrics, batch, mrs,
		tatarav1alpha1.StageDelivered, stage.ReasonDocTimeout, now, func(t *tatarav1alpha1.Task) {
			t.Status.DeliveredAt = &stamp
		}); err != nil {
		return fmt.Errorf("docbatch: force %s to delivered(doc-timeout): %w", batch.Name, err)
	}
	log.FromContext(ctx).Info("documentation batch exceeded docStageBudget; forced to delivered(doc-timeout)",
		"action", "doc_batch_timeout", "resource_id", batch.Name,
		"pod_runs", batch.Status.Stats.PodRuns, "covers", len(batch.Spec.DocumentsTasks))
	return r.ResolveDocBatch(ctx, batch)
}

// needsDocumenting is THE predicate. It gates BOTH the nightly covered set AND
// the reaper's delivered TTL, and it must be ONE function or the two disagree and
// a Task falls in the gap and is pinned forever:
//
//	a. spec.kind != documentation   the batch is never its OWN parent. A batch has
//	                                merged MRs of its own, so a coverable batch
//	                                would document last night's batch forever - and
//	                                the reaper's gate would pin every batch in the
//	                                cluster permanently.
//	b. len(mrRefs) > 0              a brainstorm skip, an incident false_positive, a
//	   AND every MR merged          fold-only refine and a declined implement have
//	                                ZERO merged MRs. They are never documented and
//	                                documentedBy stays permanently empty.
//	c. documentedBy == ""           not covered yet.
func (r *ProjectReconciler) needsDocumenting(ctx context.Context, t *tatarav1alpha1.Task) (bool, error) {
	if t.Spec.Kind == DocBatchKind || t.Status.DocumentedBy != "" || len(t.Status.MRRefs) == 0 {
		return false, nil
	}
	mrs, err := r.ownedMRs(ctx, t)
	if err != nil {
		return false, err
	}
	if len(mrs) == 0 {
		return false, nil
	}
	for i := range mrs {
		if mrs[i].Status.State != "merged" {
			return false, nil
		}
	}
	return true, nil
}

// docBatchInFlight returns the name of a documentation batch that has not settled
// yet, or "".
func docBatchInFlight(tasks []tatarav1alpha1.Task, project string) string {
	for i := range tasks {
		t := &tasks[i]
		if t.Spec.ProjectRef != project || t.Spec.Kind != DocBatchKind || len(t.Spec.DocumentsTasks) == 0 {
			continue
		}
		switch t.Status.Stage {
		case "", tatarav1alpha1.StageDelivered, tatarav1alpha1.StageRejected,
			tatarav1alpha1.StageFailed, tatarav1alpha1.StageParked:
			continue
		default:
			return t.Name
		}
	}
	return ""
}

// docsRepository resolves the Repository CR mirroring Project.spec.documentation.repo.
// nil (with no error) means "no docs repo": documentation is disabled, or the repo
// is not enrolled and the bot therefore has nowhere to push.
func (r *ProjectReconciler) docsRepository(ctx context.Context, proj *tatarav1alpha1.Project) (*tatarav1alpha1.Repository, error) {
	doc := proj.Spec.Documentation
	if doc == nil || !doc.Enabled || doc.Repo == "" {
		return nil, nil
	}
	repos, err := r.projectReposForScan(ctx, proj)
	if err != nil {
		return nil, err
	}
	for i := range repos {
		if scm.SameRemote(doc.Repo, repos[i].Spec.URL) {
			return &repos[i], nil
		}
	}
	log.FromContext(ctx).Info("docbatch: docs repo not enrolled; minting nothing",
		"action", "doc_batch_no_repo", "resource_id", proj.Name, "docs_repo", doc.Repo)
	return nil, nil
}
