package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// PodWatchReconciler arms the two POD clocks of the F.4 three-clock model. They
// are the only two timestamps the model is load-bearing on, and NOTHING ELSE
// SETS THEM:
//
//	on pod CREATE   -> status.podStartedAt       = the pod's start   (arms CLOCK 2)
//	                   RE-stamped on every respawn, never left stale
//	on pod READY    -> status.stageWorkStartedAt = the Ready instant (arms CLOCK 3)
//	on a transition -> BOTH cleared (re-arms CLOCK 1) - stage.Enter already does
//	                   this; it is not duplicated here.
//
// A stale podStartedAt carried into a new pod is not a cosmetic bug: it disarms
// CLOCK 1 while the Task sits in the admission queue, and it puts G.7's
// t0 = podStartedAt + agentPodTTLSeconds ALREADY IN THE PAST, so the operator
// TTL-stops the fresh pod before its first turn.
//
// It also runs the G.10 contract handshake at pod-ready, BEFORE turn-0, and it
// enforces CLOCK 2: a pod that never becomes Ready within PodReadyTimeout
// RESPAWNS (it does NOT fail the Task). Since O3 there is NO terminal on that
// loop - maxPodRecreations is gone - so it runs to the 24h residency dead-man
// switch and is watched by the operator_pod_recreations_total alert instead.
// pod-not-ready is not a reason and never was.
//
// It is ADDITIVE: it acts only on Tasks that carry status.stage (the new model).
// A legacy phase-driven Task is left entirely to TaskReconciler.
type PodWatchReconciler struct {
	client.Client
	Session   agent.Session
	Namespace string
	// Metrics carries the D1 terminal counter. Both of this reconciler's terminal
	// entries - failed(pod-recreation-exhausted) and failed(agent-contract-mismatch)
	// - go through the EnterStage choke point, so neither can go uncounted.
	Metrics *obs.OperatorMetrics
	// SpillerFor resolves the A.7 spiller for the transition write.
	SpillerFor func(proj *tatarav1alpha1.Project) objbudget.Spiller
}

// spillerForTask resolves the A.7 spiller for a Task's project. A project that
// cannot be read (deleted mid-flight) yields nil: the transition still lands, and
// only an over-budget one would be refused.
func (r *PodWatchReconciler) spillerForTask(ctx context.Context, task *tatarav1alpha1.Task) objbudget.Spiller {
	if r.SpillerFor == nil {
		return nil
	}
	var proj tatarav1alpha1.Project
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &proj); err != nil {
		return nil
	}
	return r.SpillerFor(&proj)
}

// The pods get;list;watch;delete this needs is already granted by the
// TaskReconciler's RBAC marker; no second marker (it would only churn the
// generated role).

// wrapperPodPredicate admits only the operator's own agent Pods, so the
// reconciler never wakes on an unrelated Pod in the namespace.
func wrapperPodPredicate() predicate.Predicate {
	sel := agent.WrapperPodSelector()
	isWrapper := func(o client.Object) bool {
		l := o.GetLabels()
		for k, v := range sel {
			if l[k] != v {
				return false
			}
		}
		return l[agent.LabelTask] != ""
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return isWrapper(e.Object) },
		UpdateFunc:  func(e event.UpdateEvent) bool { return isWrapper(e.ObjectNew) },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(e event.GenericEvent) bool { return isWrapper(e.Object) },
	}
}

// podReady reports whether the Pod carries a true Ready condition, and when it
// last transitioned there.
func podReady(pod *corev1.Pod) (time.Time, bool) {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			if !c.LastTransitionTime.IsZero() {
				return c.LastTransitionTime.Time, true
			}
			return time.Now(), true
		}
	}
	return time.Time{}, false
}

// podStart is the instant the pod's clock 2 measures from: the container
// runtime's StartTime when the kubelet has stamped it, else the object's
// creation. It is the same anchor bootDeadlineExceeded uses, so the stamp and
// the deadline check can never disagree.
func podStart(pod *corev1.Pod) time.Time {
	if pod.Status.StartTime != nil && !pod.Status.StartTime.IsZero() {
		return pod.Status.StartTime.Time
	}
	return pod.CreationTimestamp.Time
}

// The body is doReconcile; this wrapper is the #538 shutdown-cancellation
// boundary (see classifyReconcileShutdown).
func (r *PodWatchReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.doReconcile(ctx, req)
	return classifyReconcileShutdown(ctx, "podclocks", req.Name, res, err)
}

func (r *PodWatchReconciler) doReconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if pod.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}
	taskName := pod.Labels[agent.LabelTask]
	if taskName == "" {
		return ctrl.Result{}, nil
	}
	task := &tatarav1alpha1.Task{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: taskName}, task); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// The greenness gate: only the state model owns these timestamps. A Task with
	// no state yet is driven entirely by TaskReconciler's create edge, and a
	// PARKED Task holds no pod worth arming a clock for.
	if task.Status.State == "" || tatarav1alpha1.TaskDone(task) || tatarav1alpha1.Parked(task) {
		return ctrl.Result{}, nil
	}

	// CLOCK 2, armed. On the first sighting of THIS pod - including every respawn -
	// podStartedAt is (re-)stamped, and any stageWorkStartedAt left over from the
	// pod it replaced is dropped.
	if err := StampPodStartedAt(ctx, r.Client, pod.Namespace, taskName, podStart(pod)); err != nil {
		return ctrl.Result{}, err
	}

	readyAt, ready := podReady(pod)
	if !ready {
		return r.handleNotReady(ctx, pod, taskName)
	}

	// Already handshook and stamped for this pod: nothing left to do.
	if err := r.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: taskName}, task); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if task.Status.StateWorkStartedAt != nil {
		return ctrl.Result{}, nil
	}

	// G.10: assert the wrapper's contract version BEFORE a single turn is
	// submitted. Zero tokens are burned on a skewed image.
	if err := r.assertContract(ctx, pod, task); err != nil {
		// A NEIGHBOURING contract version is a release train mid-flight, not a
		// broken wrapper. Wait it out (#544) - see waitOutContractSkew.
		if agent.IsContractSkew(err) {
			return r.waitOutContractSkew(ctx, pod, task, err)
		}
		if agent.IsContractMismatch(err) {
			return ctrl.Result{}, r.failContractMismatch(ctx, pod, task, err)
		}
		// The wrapper is Ready but its HTTP server has not finished booting. Retry;
		// CLOCK 2 is still armed and bounds this.
		return ctrl.Result{RequeueAfter: agentBootRequeue}, nil
	}
	// The handshake passed: retract any skew this image was carrying. The train
	// can finish by rolling EITHER side, and rolling the operator forward is what
	// actually ended #544 - the event counter could not see that at all.
	obs.AgentContractSkewResolved(podImage(pod))

	// CLOCK 3, armed.
	if err := StampStageWorkStartedAt(ctx, r.Client, pod.Namespace, taskName, readyAt); err != nil {
		return ctrl.Result{}, err
	}
	log.FromContext(ctx).Info("agent pod ready; stage work clock armed",
		"action", "stage_work_started", "resource_id", taskName,
		"state", task.Status.State, "agent_kind", task.Status.AgentKind, "pod", pod.Name)
	return ctrl.Result{}, nil
}

// handleNotReady is the CLOCK 2 breach handler: a pod that never becomes Ready
// within PodReadyTimeout RESPAWNS, burning one podRecreations. It does NOT fail
// the Task, and since O3 it has no terminal at all - maxPodRecreations is
// deleted, so a boot-crash loop runs until the 24h stage.ResidencyCapAll
// dead-man switch. THIS IS THE WORST CASE O3 accepted explicitly: at the
// PodReadyTimeout cycle that is on the order of a few hundred pods, and the
// compensating control is the operator_pod_recreations_total churn alert, not a
// cap.
//
// It is a DEADLINE, not a crash detector. A pod that dies AFTER becoming Ready
// is not this function's - the early return below hands it to the reconciler's
// podUnusable check (internal/controller/task_stage.go), which reads phase and
// container termination.
//
// The deadline check is bootDeadlineExceeded, which anchors on
// pod.Status.StartTime so image-pull and scheduling latency do not consume the
// readiness window - an ImagePullBackOff pod is exactly the case this catches.
func (r *PodWatchReconciler) handleNotReady(ctx context.Context, pod *corev1.Pod, taskName string) (ctrl.Result, error) {
	if !bootDeadlineExceeded(pod) {
		return ctrl.Result{RequeueAfter: agentBootRequeue}, nil
	}
	l := log.FromContext(ctx)
	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: taskName}, fresh); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if fresh.Status.State == "" || fresh.Status.StateWorkStartedAt != nil {
		return ctrl.Result{}, nil
	}
	stage.RecordRespawn(fresh)
	recreations := fresh.Status.Stats.PodRecreations
	// See respawnLostPod: counted at every attempt. A pod that never becomes ready
	// is the boot-crash loop the churn alert exists for, and with the cap deleted
	// this counter is the ONLY thing that makes it visible. BootTimeout is the one
	// reason this site can ever report: it is CLOCK 2 and nothing else.
	obs.PodRecreation(fresh.Spec.ProjectRef, fresh.Spec.Kind, obs.RecreationReasonBootTimeout)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: taskName}, cur); err != nil {
			return err
		}
		if cur.Status.State == "" || cur.Status.StateWorkStartedAt != nil {
			return nil
		}
		cur.Status.Stats.PodRecreations = recreations
		// The pod is going away; clear its clock so the replacement re-stamps a
		// FRESH podStartedAt rather than inheriting the dead pod's.
		cur.Status.PodStartedAt = nil
		cur.Status.StateWorkStartedAt = nil
		return r.Status().Update(ctx, cur)
	}); err != nil {
		return ctrl.Result{}, err
	}
	l.Info("agent pod never became ready; respawning",
		"action", "pod_respawn", "resource_id", taskName, "pod", pod.Name)
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("delete never-ready wrapper pod: %w", err)
	}
	return ctrl.Result{RequeueAfter: agentBootRequeue}, nil
}

// assertContract runs the G.10 handshake against the pod's wrapper.
func (r *PodWatchReconciler) assertContract(ctx context.Context, pod *corev1.Pod, task *tatarav1alpha1.Task) error {
	if r.Session == nil {
		return nil
	}
	return agent.AssertContractVersion(ctx, r.Session, agent.BaseURL(task, pod.Namespace))
}

// agentContractSkewDeadline bounds how long a Task waits out a contract skew.
//
// Sized off the thing it is waiting for: the 2026-08-08 train had the wrapper
// pin land 56 minutes before the operator pin, and the operator Deployment then
// took ~16 more minutes to roll across 3 replicas - ~72 minutes end to end. Two
// hours leaves headroom for a slower train without ever approaching a stage
// budget, and a skew still present after two hours is not a train, it is a pin
// nobody is going to fix by itself.
const agentContractSkewDeadline = 2 * time.Hour

// agentContractSkewRequeue paces the re-check while a skew is being waited out.
const agentContractSkewRequeue = 30 * time.Second

// podImage names the wrapper image a pod is running, for the skew metric.
func podImage(pod *corev1.Pod) string {
	if len(pod.Spec.Containers) == 0 {
		return ""
	}
	return pod.Spec.Containers[0].Image
}

// waitOutContractSkew is the #544 fix: a wrapper on an ADJACENT contract version
// is a release train in flight, and the Task waits for it instead of being
// destroyed by it.
//
// THE WAIT IS NOT WISHFUL. The operator and the agent image are pinned in
// different helm releases, so the skew resolves by either side rolling - and
// when it resolves by the OPERATOR rolling forward (which is what actually ended
// #544), THIS EXACT POD passes the handshake on the very next reconcile with no
// respawn and no lost turns. Nothing has been submitted yet, so nothing is at
// risk while we wait: the work clock is unarmed and zero tokens are burned.
//
// The wait is bounded by agentContractSkewDeadline measured from the pod's own
// start. Past it the skew is no longer a rollout and the pre-existing terminal
// park is the correct answer, so this degrades exactly into the old behaviour
// rather than replacing it.
func (r *PodWatchReconciler) waitOutContractSkew(ctx context.Context, pod *corev1.Pod,
	task *tatarav1alpha1.Task, cause error) (ctrl.Result, error) {

	var se *agent.ContractSkewError
	if !errors.As(cause, &se) {
		return ctrl.Result{}, cause
	}
	image := podImage(pod)
	expected, got := strconv.Itoa(se.Expected), strconv.Itoa(se.Got)
	// The gauge is the alertable signal and it stays up for the WHOLE window,
	// unlike the mismatch counter it replaces (see obs.agentContractSkew).
	obs.AgentContractSkewObserved(expected, got, image)

	waited := time.Duration(0)
	if start := podStart(pod); !start.IsZero() {
		waited = time.Since(start)
	}
	l := log.FromContext(ctx)
	if waited < agentContractSkewDeadline {
		l.Info("agent wrapper contract is skewed by one release; waiting for the train to settle",
			"action", "agent_contract_skew_wait", "resource_id", task.Name,
			"expected", se.Expected, "got", se.Got, "image", image,
			"waited_seconds", int(waited.Seconds()),
			"deadline_seconds", int(agentContractSkewDeadline.Seconds()))
		return ctrl.Result{RequeueAfter: agentContractSkewRequeue}, nil
	}
	l.Info("agent wrapper contract skew outlived the release-train window; failing the task",
		"action", "agent_contract_skew_expired", "resource_id", task.Name,
		"expected", se.Expected, "got", se.Got, "image", image,
		"waited_seconds", int(waited.Seconds()))
	return ctrl.Result{}, r.failContractMismatch(ctx, pod, task,
		&agent.ContractMismatchError{Expected: se.Expected, Got: se.Got})
}

// failContractMismatch is the G.10 terminal: the Task fails INSTANTLY, before a
// single turn is submitted. The wrapper image is pinned in a different helm
// release than the operator and helmfile applies releases concurrently, so this
// state is reachable and will happen; without the instant fail every pod in the
// skew burns its whole turn budget producing nothing.
func (r *PodWatchReconciler) failContractMismatch(ctx context.Context, pod *corev1.Pod, task *tatarav1alpha1.Task, cause error) error {
	var mm *agent.ContractMismatchError
	if !errors.As(cause, &mm) {
		return cause
	}
	image := ""
	if len(pod.Spec.Containers) > 0 {
		image = pod.Spec.Containers[0].Image
	}
	obs.AgentContractMismatch(strconv.Itoa(mm.Expected), strconv.Itoa(mm.Got), image)
	log.FromContext(ctx).Error(cause, "agent wrapper speaks an unsupported contract version; failing task before turn-0",
		"action", "agent_contract_mismatch", "resource_id", task.Name,
		"expected", mm.Expected, "got", mm.Got, "image", image)

	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: task.Name}, fresh); err != nil {
		return client.IgnoreNotFound(err)
	}
	if tatarav1alpha1.TaskDone(fresh) || tatarav1alpha1.Parked(fresh) {
		return nil
	}
	// Through the PARK CHOKE POINT: it deletes the wrapper and counts the park.
	return ParkTask(ctx, r.Client, r.spillerForTask(ctx, fresh), r.Metrics, fresh,
		stage.ReasonAgentContractMismatch, time.Now(), nil)
}

// StampPodStartedAt arms CLOCK 2: it stamps status.podStartedAt from the pod's
// own start instant, and bumps stats.podRuns. It is idempotent per pod - a
// re-reconcile of the SAME pod re-stamps the same value and does not double-count
// podRuns - and it RE-stamps on a respawn, dropping the replaced pod's
// stageWorkStartedAt so CLOCK 2 re-arms rather than CLOCK 3 staying armed on a
// pod that no longer exists.
func StampPodStartedAt(ctx context.Context, c client.Client, namespace, taskName string, at time.Time) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: taskName}, fresh); err != nil {
			return err
		}
		if fresh.Status.State == "" {
			return nil
		}
		if cur := fresh.Status.PodStartedAt; cur != nil && !cur.Time.Before(at) {
			return nil // already stamped for this pod (or a newer one)
		}
		stamp := metav1.NewTime(at)
		fresh.Status.PodStartedAt = &stamp
		fresh.Status.StateWorkStartedAt = nil
		fresh.Status.Stats.PodRuns++
		return c.Status().Update(ctx, fresh)
	})
}

// StampStageWorkStartedAt arms CLOCK 3 at pod-ready. It never runs before
// podStartedAt is set: a stageWorkStartedAt without a podStartedAt would disarm
// CLOCK 2 for a pod nothing is measuring.
func StampStageWorkStartedAt(ctx context.Context, c client.Client, namespace, taskName string, at time.Time) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: taskName}, fresh); err != nil {
			return err
		}
		if fresh.Status.State == "" || fresh.Status.PodStartedAt == nil {
			return nil
		}
		if fresh.Status.StateWorkStartedAt != nil {
			return nil
		}
		stamp := metav1.NewTime(at)
		fresh.Status.StateWorkStartedAt = &stamp
		return c.Status().Update(ctx, fresh)
	})
}

// SetupWithManager registers the pod-clock watch. It is a SECOND controller on
// Pods, deliberately: TaskReconciler's Owns(&corev1.Pod{}) must keep firing on
// every Pod event - that is what wakes the reconciler on a pod that DIED, for
// podUnusable to classify - so this watch cannot be folded into it behind a Ready
// predicate without losing those events.
func (r *PodWatchReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("podclocks").
		For(&corev1.Pod{}, builder.WithPredicates(wrapperPodPredicate())).
		Complete(r)
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
