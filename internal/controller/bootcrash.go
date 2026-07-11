package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

const annBootCrashAttempts = "tatara.dev/boot-crash-attempts"

// annBootCrashDiagnostics holds the most recent boot-crash cause captured from
// pod.Status (failing container, exit code, log tail). resetAgentRun preserves
// it (like annBootCrashAttempts) so the cause survives the per-attempt pod
// delete/recreate and can be surfaced at exhaustion; setDeployState and
// recordTurn clear it so it never leaks into the next state / run.
const annBootCrashDiagnostics = "tatara.dev/boot-crash-diagnostics"

// annBootCrashLastPodUID records the pod.UID whose boot crash last incremented
// annBootCrashAttempts, so rapid Owns(Pod) reconciles on the same stale/
// terminating pod cannot bump the budget more than once per distinct pod
// instance. resetAgentRun preserves it (like annBootCrashAttempts) across
// respawns; recordTurn and setDeployState clear it with the attempt counter.
const annBootCrashLastPodUID = "tatara.dev/boot-crash-last-pod-uid"

// bootCrashDetailCap bounds the captured diagnostic so it fits comfortably in an
// annotation and a condition message. The kubelet log tail dominates the length.
const bootCrashDetailCap = 1024

// bootCrashReason inspects a not-yet-Ready wrapper Pod and returns a non-empty
// reason when its boot has definitively failed: the Pod reached a Failed phase
// (restartPolicy=Never, so a wrapper that exits non-zero lands here), a
// container is in CrashLoopBackOff, or a container terminated non-zero before
// /readyz came up. Returns "" when the pod is merely still booting.
func bootCrashReason(pod *corev1.Pod) string {
	if pod.Status.Phase == corev1.PodFailed {
		return "PodFailed"
	}
	statuses := make([]corev1.ContainerStatus, 0, len(pod.Status.InitContainerStatuses)+len(pod.Status.ContainerStatuses))
	statuses = append(statuses, pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)
	for _, cs := range statuses {
		if w := cs.State.Waiting; w != nil && w.Reason == "CrashLoopBackOff" {
			return "CrashLoopBackOff"
		}
		if t := cs.State.Terminated; t != nil && t.ExitCode != 0 {
			return "ContainerExited"
		}
	}
	return ""
}

// bootCrashDetail extracts a bounded, human-readable diagnostic from a
// not-yet-Ready Pod's status: the failing container name, its exit code, the
// terminated/waiting reason, and the kubelet-captured log tail (populated by
// terminationMessagePolicy=FallbackToLogsOnError on a non-zero exit). It reads
// only fields already on pod.Status, so no logs API or pods/log RBAC is needed.
// Returns "" when no container status explains the failure.
func bootCrashDetail(pod *corev1.Pod) string {
	statuses := make([]corev1.ContainerStatus, 0, len(pod.Status.InitContainerStatuses)+len(pod.Status.ContainerStatuses))
	statuses = append(statuses, pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)

	// Prefer a terminated container (carries the exit code + log tail).
	for _, cs := range statuses {
		if t := cs.State.Terminated; t != nil && t.ExitCode != 0 {
			return containerCrashDetail(cs.Name, t, nil)
		}
	}
	// Otherwise a waiting container with a reason (CrashLoopBackOff carries its
	// tail on LastTerminationState; ImagePullBackOff/CreateContainerError do not).
	for _, cs := range statuses {
		if w := cs.State.Waiting; w != nil && w.Reason != "" {
			return containerCrashDetail(cs.Name, cs.LastTerminationState.Terminated, w)
		}
	}
	// No container status pinpoints it (e.g. a bare PodFailed): fall back to the
	// pod-level reason/message.
	if d := strings.TrimSpace(pod.Status.Reason + " " + pod.Status.Message); d != "" {
		return truncateDetail(d)
	}
	return ""
}

// containerCrashDetail formats a single container's failure into a compact,
// bounded "key=value" string (container, waiting, exit, reason, log).
func containerCrashDetail(name string, term *corev1.ContainerStateTerminated, wait *corev1.ContainerStateWaiting) string {
	parts := []string{"container=" + name}
	if wait != nil && wait.Reason != "" {
		parts = append(parts, "waiting="+wait.Reason)
	}
	if term != nil {
		parts = append(parts, fmt.Sprintf("exit=%d", term.ExitCode))
		if term.Reason != "" {
			parts = append(parts, "reason="+term.Reason)
		}
		if msg := strings.TrimSpace(term.Message); msg != "" {
			parts = append(parts, "log="+msg)
		}
	}
	return truncateDetail(strings.Join(parts, " "))
}

// truncateDetail trims and caps a diagnostic at bootCrashDetailCap.
func truncateDetail(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > bootCrashDetailCap {
		return s[:bootCrashDetailCap] + "...(truncated)"
	}
	return s
}

// bootDeadlineExceeded reports whether a not-yet-Ready pod has exceeded
// agentBootDeadline without becoming Ready. The deadline is anchored to
// pod.Status.StartTime (when the container runtime started the pod) so that
// image-pull and scheduling latency do not consume the readiness window.
// Falls back to CreationTimestamp only when StartTime has not been set yet
// (e.g. the pod is still being scheduled).
func bootDeadlineExceeded(pod *corev1.Pod) bool {
	if pod.Status.StartTime != nil && !pod.Status.StartTime.IsZero() {
		return time.Since(pod.Status.StartTime.Time) > agentBootDeadline
	}
	if pod.CreationTimestamp.IsZero() {
		return false
	}
	return time.Since(pod.CreationTimestamp.Time) > agentBootDeadline
}

// handleBootCrash recovers a Task whose wrapper Pod failed to boot. On a crash
// signal (Failed/CrashLoopBackOff/non-zero exit) or a pod that never became
// Ready within agentBootDeadline, it respawns the run via resetAgentRun bounded
// by maxPodRecreations boot attempts; once exhausted it fails the Task so the
// lifecycle-orphan sweep can re-pick it rather than spinning forever.
// handled=false means the pod is still legitimately booting -> caller requeues.
func (r *TaskReconciler) handleBootCrash(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error, bool) {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: agent.PodName(task)}, pod); err != nil {
		// NotFound: ensurePodAndService recreates it next reconcile. Transient
		// errors: keep waiting. Either way this is not a boot crash to act on.
		return ctrl.Result{}, nil, false
	}

	// Gate 1: a pod with DeletionTimestamp is already being torn down by a prior
	// respawn (resetAgentRun issued the Delete; the finalizer grace period keeps
	// the object visible). Do not count this dying instance again; wait for
	// ensurePodAndService to create the replacement.
	if pod.DeletionTimestamp != nil {
		return ctrl.Result{RequeueAfter: agentBootRequeue}, nil, true
	}

	reason := bootCrashReason(pod)
	if reason == "" {
		if !bootDeadlineExceeded(pod) {
			return ctrl.Result{}, nil, false
		}
		reason = "BootTimeout"
	}

	// Gate 2: count at most one boot-crash attempt per distinct pod instance.
	// If this exact pod UID already incremented the budget, its respawn was already
	// issued; requeue and wait for the replacement rather than bumping again. This
	// gives a genuinely slow boot its full maxPodRecreations x agentBootDeadline.
	if task.Annotations[annBootCrashLastPodUID] == string(pod.UID) {
		return ctrl.Result{RequeueAfter: agentBootRequeue}, nil, true
	}

	// Capture the crash cause from pod.Status BEFORE resetAgentRun / terminate
	// delete the pod, persisting it across respawns (last-write-wins) so the
	// terminal condition + issue comment can explain WHY the boot failed.
	if detail := bootCrashDetail(pod); detail != "" {
		if err := r.captureBootCrashDiagnostics(ctx, task, detail); err != nil {
			return ctrl.Result{}, err, true
		}
	}
	diag := task.Annotations[annBootCrashDiagnostics]

	l := log.FromContext(ctx)
	attempts := r.bootCrashAttempts(task) + 1
	if attempts > maxPodRecreations {
		r.Metrics.AgentBootCrash(reason, "failed")
		l.Info("agent pod boot failed; recreation budget exhausted, failing task",
			"action", "agent_boot_crash_exhausted", "resource_id", task.Name, "reason", reason,
			"attempts", maxPodRecreations, "diagnostics", diag)
		msg := fmt.Sprintf("wrapper pod failed to boot (%s) after %d attempts", reason, maxPodRecreations)
		if diag != "" {
			msg = fmt.Sprintf("wrapper pod failed to boot (%s: %s) after %d attempts", reason, diag, maxPodRecreations)
		}
		res, terr := r.terminate(ctx, task, "Failed", "BootCrashLoop", msg)
		// Post the cause once to the linked issue (survives terminal-CRD GC, #81).
		// Only after terminate commits Failed, so a terminate retry cannot double-post.
		if terr == nil {
			r.commentBootCrashDiagnostics(ctx, task, reason, diag)
		}
		return res, terr, true
	}

	r.Metrics.AgentBootCrash(reason, "respawn")
	l.Info("agent pod boot failed; respawning",
		"action", "agent_boot_crash", "resource_id", task.Name, "reason", reason, "attempt", attempts, "diagnostics", diag)
	if err := r.bumpBootCrashAttempts(ctx, task, pod.UID); err != nil {
		return ctrl.Result{}, err, true
	}
	if err := r.resetAgentRun(ctx, task); err != nil {
		return ctrl.Result{}, err, true
	}
	return ctrl.Result{RequeueAfter: agentBootRequeue}, nil, true
}

// bootCrashAttempts returns the count of boot-crash respawns for the current
// run. resetAgentRun preserves this annotation (unlike pod-recreations) so the
// budget accumulates across respawns; recordTurn clears it once a turn lands.
func (r *TaskReconciler) bootCrashAttempts(task *tatarav1alpha1.Task) int {
	n, _ := strconv.Atoi(task.Annotations[annBootCrashAttempts])
	return n
}

func (r *TaskReconciler) bumpBootCrashAttempts(ctx context.Context, task *tatarav1alpha1.Task, podUID types.UID) error {
	return r.patchTaskAnnotations(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		// Authoritative per-pod idempotency: the handleBootCrash gate reads the
		// cache, so under cache lag two reconciles can both pass it. Re-check
		// against the freshly-read Task here so the same pod instance bumps the
		// budget at most once even when a conflict-retry observes the
		// catching-up cache.
		if fresh.Annotations[annBootCrashLastPodUID] == string(podUID) {
			return false
		}
		n, _ := strconv.Atoi(fresh.Annotations[annBootCrashAttempts])
		fresh.Annotations[annBootCrashAttempts] = strconv.Itoa(n + 1)
		fresh.Annotations[annBootCrashLastPodUID] = string(podUID)
		return true
	})
}

// captureBootCrashDiagnostics records the most recent boot-crash cause in the
// annBootCrashDiagnostics annotation (last-write-wins) so it survives the
// per-attempt pod delete in resetAgentRun and reaches the terminal condition /
// issue comment at exhaustion. It also updates the in-memory task so the caller
// reads the fresh value without a re-Get.
func (r *TaskReconciler) captureBootCrashDiagnostics(ctx context.Context, task *tatarav1alpha1.Task, detail string) error {
	return r.patchTaskAnnotations(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if fresh.Annotations[annBootCrashDiagnostics] == detail {
			return false // idempotent: this exact cause is already recorded
		}
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[annBootCrashDiagnostics] = detail
		return true
	})
}

// commentBootCrashDiagnostics posts the boot-crash cause once to the Task's
// linked issue (the detail captured from pod.Status), formatted for the boot
// path. Delegates the SCM plumbing to postTerminalComment (task_controller.go).
func (r *TaskReconciler) commentBootCrashDiagnostics(ctx context.Context, task *tatarav1alpha1.Task, reason, diag string) {
	body := fmt.Sprintf("Wrapper pod failed to boot (`%s`) and the run was terminated after %d boot attempts.",
		reason, maxPodRecreations)
	if diag != "" {
		body += "\n\nLast captured cause from `pod.Status`:\n\n```\n" + diag + "\n```"
	}
	r.postTerminalComment(ctx, task, body)
}
