// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// mutateAnnotations applies mutate to a FRESHLY READ copy of obj's annotations
// and persists the result, under RetryOnConflict. mutate returns false to write
// nothing at all.
//
// THE FRESH READ IS THE POINT, TWICE OVER. Repository metadata has writers that
// never coordinate - the webhook server stamps SweepRequestedAnnotation from any
// of three replicas, the leader's sweep writes UpgradeDeferredAnnotation, and
// the lane release below stamps SweepRequestedAnnotation again - so a
// read-modify-write off a cached object silently drops whichever marker lost,
// and a dropped sweep request IS the four-hour wait this mechanism removes.
// It also makes "has this key already" a decision about the object as it IS: the
// sweep is handed its repos as a slice snapshot taken at the top of the pass, and
// a presence test against that snapshot answers for a Repository as it was
// before any earlier pass wrote to it.
func mutateAnnotations(ctx context.Context, c client.Client, obj client.Object,
	mutate func(ann map[string]string) bool) error {

	fresh, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("annotate: %T is not a client.Object", obj)
	}
	name := client.ObjectKeyFromObject(obj)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := c.Get(ctx, name, fresh); err != nil {
			return err
		}
		ann := fresh.GetAnnotations()
		if ann == nil {
			ann = map[string]string{}
		}
		if !mutate(ann) {
			return nil
		}
		fresh.SetAnnotations(ann)
		return c.Update(ctx, fresh)
	}); err != nil {
		return fmt.Errorf("annotate %s: %w", name, err)
	}
	return nil
}

// stampAnnotation writes key=value, last-writer-wins. A no-op when the value is
// already there, so a repeated stamp of the SAME instant costs no write.
func stampAnnotation(ctx context.Context, c client.Client, obj client.Object, key, value string) error {
	return mutateAnnotations(ctx, c, obj, func(ann map[string]string) bool {
		if ann[key] == value {
			return false
		}
		ann[key] = value
		return true
	})
}

// markAnnotation drives a key by PRESENCE: added (carrying value) when want is
// true and absent, removed when want is false and present, and no write in
// either steady state. The value is written once per arming and never refreshed,
// because presence is what every reader tests.
func markAnnotation(ctx context.Context, c client.Client, obj client.Object, key, value string, want bool) error {
	return mutateAnnotations(ctx, c, obj, func(ann map[string]string) bool {
		if (ann[key] != "") == want {
			return false
		}
		if want {
			ann[key] = value
		} else {
			delete(ann, key)
		}
		return true
	})
}

// recordUpgradeDeferral keeps a Repository's UpgradeDeferredAnnotation in step
// with what the pass that just swept it actually did: set when the pass deferred
// an adoptable merge request upgrade_headroom_bound, cleared when it did not.
//
// PRESENCE IS THE STATE, so a steady backlog costs ZERO writes: only the two
// EDGES - the first pass that defers and the first pass that stops deferring -
// touch the object at all. That matters because a Repository update wakes the
// RepositoryReconciler, and a per-pass rewrite of an unchanged timestamp would
// wake it on every sweep of every repo forever.
//
// The clear is the half that BOUNDS the mechanism; see UpgradeDeferredAnnotation
// for why a record that outlived its backlog turns every later freed lane into a
// forge listing of a repo with nothing to adopt.
func (r *ProjectReconciler) recordUpgradeDeferral(ctx context.Context, repo *tatarav1alpha1.Repository,
	deferred bool, now time.Time) error {

	return markAnnotation(ctx, r.Client, repo, tatarav1alpha1.UpgradeDeferredAnnotation,
		now.UTC().Format(time.RFC3339), deferred)
}

// releaseUpgradeLane is the RELEASE half of the adoption fast path: an upgrade
// Task has reached a terminal state or parked, so the lane it held against
// maxOpenUpgrades is free, and any repository that recorded a headroom deferral
// can now be swept instead of waiting out its four-hourly issueScan slot.
//
// IT DOES NOT MINT, AND THAT IS THE WHOLE DESIGN. It stamps the SAME marker the
// webhook fast path stamps and returns; reposDueForScan pulls the slot forward
// and the leader's sweep arm mints, oldest merge request first, under the ONE
// openUpgradeLaneCount check that already enforces the cap correctly against a
// freshly listed set. No second counter, no second minting path, and the cap
// stays inside a single serialized writer - which is precisely why eab78709's
// webhook half does not mint either.
//
// WHY HERE. A lane frees when an upgrade Task goes done/rejected or parks, and
// those are written by four different actors: submit_outcome on any replica, the
// queue controller, the stage driver, and this reconciler. The terminal early
// return in reconcileStage is where all four CONVERGE - the leader reconciles
// the Task itself whoever wrote its state, on the Task's own event, with no
// polling and no watch of its own. Hooking the writers instead would mean four
// hooks, one of them in an HTTP handler that runs on every replica.
//
// EXACTLY ONCE PER TASK, via AnnUpgradeLaneReleased. A terminal Task is
// reconciled many more times than it goes terminal.
//
// BEST-EFFORT, NEVER FATAL. Every failure here costs latency and nothing else:
// the merge request is still open and its ordinary slot still adopts it, exactly
// as before this path existed. Failing the reconcile of a Task that is already
// terminal would buy retries of work the reaper is about to make moot.
func (r *TaskReconciler) releaseUpgradeLane(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, now time.Time) {

	// Only an upgrade Task holds a lane - openUpgradeLaneCount counts exactly
	// this kind - so nothing else can free one.
	if task.Spec.Kind != adoptedUpgradeKind {
		return
	}
	if task.Annotations[tatarav1alpha1.AnnUpgradeLaneReleased] != "" {
		return
	}
	l := log.FromContext(ctx)

	var repos tatarav1alpha1.RepositoryList
	if err := r.List(ctx, &repos, client.InNamespace(task.Namespace)); err != nil {
		l.Error(err, "task: could not list repositories to release an upgrade lane; the ordinary slot still adopts",
			"action", "upgrade_lane_release_failed", "resource_id", task.Name, "project", proj.Name)
		return
	}
	waiting := make([]*tatarav1alpha1.Repository, 0, len(repos.Items))
	for i := range repos.Items {
		rp := &repos.Items[i]
		if rp.Spec.ProjectRef != proj.Name {
			continue
		}
		if rp.Annotations[tatarav1alpha1.UpgradeDeferredAnnotation] == "" {
			continue
		}
		waiting = append(waiting, rp)
	}
	// NOTHING DEFERRED, NOTHING WRITTEN - not even the latch. This is the inert
	// case and it is by far the common one: every project but `infrastructure`
	// has an empty adoptBranchPrefix, so no pass can ever defer and no repository
	// can ever carry the record. A latch written here would be a write per
	// terminal upgrade Task on every project, bought for nothing, and it would
	// also latch away a release that a LATER burst genuinely owes this Task.
	if len(waiting) == 0 {
		return
	}

	stamp := now.UTC().Format(time.RFC3339)
	for _, rp := range waiting {
		if err := stampAnnotation(ctx, r.Client, rp, tatarav1alpha1.SweepRequestedAnnotation, stamp); err != nil {
			l.Error(err, "task: could not pull a deferred repository's sweep slot forward; the ordinary slot still adopts",
				"action", "upgrade_lane_release_failed", "resource_id", task.Name,
				"project", proj.Name, "repository", rp.Name)
			continue
		}
		l.Info("task: upgrade lane freed; a repository that deferred an adoptable merge request is swept now",
			"action", "upgrade_lane_released", "resource_id", task.Name, "project", proj.Name,
			"repository", rp.Name, "state", task.Status.State, "park_reason", task.Status.ParkReason,
			"requested_at", stamp)
	}
	// LAST, so a failed request is retried by the next reconcile rather than
	// latched away. The cost of the reverse - a re-stamped request on a repo
	// already asked for - is one redundant listing; the cost of latching first is
	// a deferral that waits four hours.
	if err := stampAnnotation(ctx, r.Client, task, tatarav1alpha1.AnnUpgradeLaneReleased, stamp); err != nil {
		l.Error(err, "task: could not latch an upgrade lane release; a repeat request costs one extra listing",
			"action", "upgrade_lane_latch_failed", "resource_id", task.Name, "project", proj.Name)
	}
}
