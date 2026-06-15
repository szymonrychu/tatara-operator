package controller

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

func TestBootCrashReason(t *testing.T) {
	cases := []struct {
		name string
		pod  corev1.Pod
		want string
	}{
		{
			name: "still booting (pending, no statuses)",
			pod:  corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending}},
			want: "",
		},
		{
			name: "pod failed phase",
			pod:  corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}},
			want: "PodFailed",
		},
		{
			name: "container crashloop backoff",
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name:  "wrapper",
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
				}},
			}},
			want: "CrashLoopBackOff",
		},
		{
			name: "container terminated non-zero",
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name:  "wrapper",
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}},
				}},
			}},
			want: "ContainerExited",
		},
		{
			name: "container terminated zero exit is not a crash",
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name:  "wrapper",
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
				}},
			}},
			want: "",
		},
		{
			name: "init container crash detected",
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				InitContainerStatuses: []corev1.ContainerStatus{{
					Name:  "init",
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 2}},
				}},
			}},
			want: "ContainerExited",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := bootCrashReason(&tc.pod); got != tc.want {
				t.Fatalf("bootCrashReason = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBootDeadlineExceeded(t *testing.T) {
	zero := corev1.Pod{}
	if bootDeadlineExceeded(&zero) {
		t.Fatal("zero creation timestamp must not count as exceeded")
	}
	recent := corev1.Pod{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.Now()}}
	if bootDeadlineExceeded(&recent) {
		t.Fatal("freshly created pod must not be past the boot deadline")
	}
	old := corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * agentBootDeadline)),
	}}
	if !bootDeadlineExceeded(&old) {
		t.Fatal("pod older than the boot deadline must count as exceeded")
	}
}

// seedBootCrashTask creates a Task in Planning with a wrapper Pod of the given
// phase, returning the reconciler (with metrics registry) and task.
func seedBootCrashTask(t *testing.T, name string, podPhase corev1.PodPhase) (*TaskReconciler, *prometheus.Registry, *tatarav1alpha1.Task) {
	t.Helper()
	reg := prometheus.NewRegistry()
	r := newTaskReconciler(newFakeSession())
	r.Metrics = obs.NewOperatorMetrics(reg)

	task := &tatarav1alpha1.Task{}
	task.Name = name
	task.Namespace = testNS
	task.Spec.ProjectRef = name + "-proj"
	task.Spec.RepositoryRef = name + "-repo"
	task.Spec.Goal = "g"
	if err := k8sClient.Create(context.Background(), task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.Phase = "Planning"
	task.Status.PodName = agent.PodName(task)
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("set planning: %v", err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: agent.PodName(task), Namespace: testNS},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "wrapper", Image: "wrapper:1"}}},
	}
	if err := k8sClient.Create(context.Background(), pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	pod.Status.Phase = podPhase
	if err := k8sClient.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("set pod phase: %v", err)
	}
	return r, reg, getTask(t, name)
}

func TestHandleBootCrashRespawnsUnderBudget(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, reg, task := seedBootCrashTask(t, "bootcrash-respawn", corev1.PodFailed)

	res, err, handled := r.handleBootCrash(ctx, task)
	if err != nil {
		t.Fatalf("handleBootCrash: %v", err)
	}
	if !handled {
		t.Fatal("a Failed pod must be handled, not requeued as still-booting")
	}
	if res.RequeueAfter != agentBootRequeue {
		t.Fatalf("requeueAfter = %v, want %v", res.RequeueAfter, agentBootRequeue)
	}

	// Run was reset: phase cleared, attempts bumped, pod deleted.
	got := getTask(t, "bootcrash-respawn")
	if got.Status.Phase != "" {
		t.Fatalf("phase = %q, want reset to empty", got.Status.Phase)
	}
	if n := got.Annotations[annBootCrashAttempts]; n != "1" {
		t.Fatalf("boot-crash attempts = %q, want 1", n)
	}
	pod := &corev1.Pod{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: agent.PodName(task)}, pod); err == nil {
		t.Fatal("wrapper pod should have been deleted on respawn")
	}
	if v := counterValue(t, reg, "operator_agent_boot_crash_total", map[string]string{"reason": "PodFailed", "outcome": "respawn"}); v != 1 {
		t.Fatalf("respawn metric = %v, want 1", v)
	}
}

func TestHandleBootCrashFailsWhenBudgetExhausted(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, reg, task := seedBootCrashTask(t, "bootcrash-exhausted", corev1.PodFailed)

	// Pre-set attempts at the budget so the next detection terminates.
	task.Annotations = map[string]string{annBootCrashAttempts: strconv.Itoa(maxPodRecreations)}
	if err := k8sClient.Update(ctx, task); err != nil {
		t.Fatalf("set attempts: %v", err)
	}
	task = getTask(t, "bootcrash-exhausted")

	_, err, handled := r.handleBootCrash(ctx, task)
	if err != nil {
		t.Fatalf("handleBootCrash: %v", err)
	}
	if !handled {
		t.Fatal("exhausted budget must be handled")
	}
	got := getTask(t, "bootcrash-exhausted")
	if got.Status.Phase != "Failed" {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	cond := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
	if cond == nil || cond.Reason != "BootCrashLoop" {
		t.Fatalf("condition = %+v, want Ready/BootCrashLoop", cond)
	}
	if v := counterValue(t, reg, "operator_agent_boot_crash_total", map[string]string{"reason": "PodFailed", "outcome": "failed"}); v != 1 {
		t.Fatalf("failed metric = %v, want 1", v)
	}
}

func TestHandleBootCrashStillBooting(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, task := seedBootCrashTask(t, "bootcrash-booting", corev1.PodPending)

	_, err, handled := r.handleBootCrash(ctx, task)
	if err != nil {
		t.Fatalf("handleBootCrash: %v", err)
	}
	if handled {
		t.Fatal("a freshly-pending pod within the boot deadline must not be handled")
	}
}

func TestHandleBootCrashPodMissing(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r := newTaskReconciler(newFakeSession())
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	task := &tatarav1alpha1.Task{}
	task.Name = "bootcrash-nopod"
	task.Namespace = testNS
	task.Spec.ProjectRef = "p"
	task.Spec.RepositoryRef = "r"
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}

	_, err, handled := r.handleBootCrash(ctx, getTask(t, "bootcrash-nopod"))
	if err != nil {
		t.Fatalf("handleBootCrash: %v", err)
	}
	if handled {
		t.Fatal("missing pod must not be handled as a boot crash (ensurePod recreates it)")
	}
}
