package controller

import (
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// memoryApplyTransientGrace is how long applyMemoryStack may keep failing on a
// RETRYABLE dependency before the reconcile stops absorbing it and records the
// stack as Failed like any other apply error.
//
// Sized off the 2026-07-24 incident (issue #439): a cluster-wide rolling node
// reboot took the cnpg operator - and with it the mcluster.cnpg.io admission
// webhook - offline for ~6 minutes. 10 minutes clears that and an ordinary
// cnpg-operator rollout with room to spare, while still surfacing a webhook
// that is genuinely gone (uninstalled cnpg, a wedged operator) within one
// alert evaluation window rather than never.
const memoryApplyTransientGrace = 10 * time.Minute

// isTransientApplyError reports whether an applyMemoryStack failure is a
// retryable dependency problem rather than a real rejection of what we asked
// for.
//
// The distinction is load-bearing and deliberately narrow. An admission webhook
// that cannot be REACHED is reported by the apiserver as a 500 with reason
// InternalError and a message naming the webhook ("Internal error occurred:
// failed calling webhook \"mcluster.cnpg.io\": ... connect: connection
// refused"). An admission webhook that REJECTS the object answers Forbidden or
// Invalid ("admission webhook ... denied the request"), which is a genuine spec
// problem - a cnpg storage shrink, say - and MUST stay a hard failure, because
// silently retrying it forever is how a broken Project spec becomes invisible.
// Everything else here (429/503/ServerTimeout/Timeout) is "come back later" by
// definition in the apimachinery contract.
//
// Deliberately NOT covered: a raw transport failure reaching the apiserver
// itself (no APIStatus to read). A reconcile that cannot talk to the apiserver
// also cannot write status, so it fails on the status Update path, not here.
func isTransientApplyError(err error) bool {
	if err == nil {
		return false
	}
	if apierrors.IsTooManyRequests(err) ||
		apierrors.IsServiceUnavailable(err) ||
		apierrors.IsServerTimeout(err) ||
		apierrors.IsTimeout(err) {
		return true
	}
	return apierrors.IsInternalError(err) && strings.Contains(err.Error(), "failed calling webhook")
}

// memoryApplyRetryBackoff returns the requeue interval for a memory stack whose
// apply has been failing transiently for elapsed. It backs the poll off a
// dependency that is clearly not coming straight back, without ever letting the
// stack go unpolled for long enough to miss the recovery.
//
// This bounds the SELF-driven cadence only. The Project controller also Owns()
// every object in the stack, so during a node reboot the object churn drives
// Reconcile far faster than any requeue interval; what actually stops that
// storm from amplifying is the soft classification (no returned error, so no
// controller-runtime "Reconciler error" line per pass), not this backoff.
func memoryApplyRetryBackoff(elapsed time.Duration) time.Duration {
	switch {
	case elapsed < time.Minute:
		return memoryRequeue
	case elapsed < 5*time.Minute:
		return 30 * time.Second
	default:
		return time.Minute
	}
}

// noteTransientApply records a transient apply failure for a project and
// returns the requeue interval to poll on plus whether the failure has now
// outlasted memoryApplyTransientGrace and must be escalated to a hard failure.
//
// The grace is measured in WALL TIME from the first failure of the current run,
// never in reconcile passes: the Owns() watch edges make the pass rate a
// function of how much the cluster is churning, so a pass counter would
// escalate a 30-second webhook blip during a reboot storm and sit patiently
// through a genuinely dead webhook on an idle cluster - exactly backwards.
//
// Read/written only on the serialised reconcile path
// (MaxConcurrentReconciles: 1); no mutex required, same as memoryUnhealthyCycles.
func (r *ProjectReconciler) noteTransientApply(project string, now time.Time) (time.Duration, bool) {
	if r.memoryApplyTransientSince == nil {
		r.memoryApplyTransientSince = map[string]time.Time{}
	}
	since, ok := r.memoryApplyTransientSince[project]
	if !ok {
		since = now
		r.memoryApplyTransientSince[project] = since
	}
	elapsed := now.Sub(since)
	if elapsed >= memoryApplyTransientGrace {
		// Escalating: drop the mark so the NEXT run starts a fresh grace window
		// rather than escalating instantly forever once the first one expires.
		delete(r.memoryApplyTransientSince, project)
		return 0, true
	}
	return memoryApplyRetryBackoff(elapsed), false
}

// clearTransientApply ends a project's transient-failure run. Called on every
// successful apply, so a dependency that flaps in and out gets a full grace
// window each time it goes down.
func (r *ProjectReconciler) clearTransientApply(project string) {
	delete(r.memoryApplyTransientSince, project)
}
