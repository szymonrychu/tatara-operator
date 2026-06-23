package controller

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
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

func TestBootCrashDetail(t *testing.T) {
	cases := []struct {
		name string
		pod  corev1.Pod
		want []string // substrings that must all appear; nil => want ""
	}{
		{
			name: "still booting -> empty",
			pod:  corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending}},
			want: nil,
		},
		{
			name: "terminated non-zero carries exit code, reason, and log tail",
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name: "wrapper",
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 1, Reason: "Error", Message: "panic: boom\ngoroutine 1",
					}},
				}},
			}},
			want: []string{"container=wrapper", "exit=1", "reason=Error", "log=panic: boom"},
		},
		{
			name: "crashloopbackoff uses the last-termination tail",
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name:  "wrapper",
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
					LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 2, Message: "fatal: missing env",
					}},
				}},
			}},
			want: []string{"container=wrapper", "waiting=CrashLoopBackOff", "exit=2", "log=fatal: missing env"},
		},
		{
			name: "waiting reason only (image pull) with no termination",
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name:  "wrapper",
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
				}},
			}},
			want: []string{"container=wrapper", "waiting=ImagePullBackOff"},
		},
		{
			name: "bare pod failed falls back to pod reason/message",
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase:   corev1.PodFailed,
				Reason:  "Evicted",
				Message: "node out of memory",
			}},
			want: []string{"Evicted", "node out of memory"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := bootCrashDetail(&tc.pod)
			if len(tc.want) == 0 {
				if got != "" {
					t.Fatalf("bootCrashDetail = %q, want empty", got)
				}
				return
			}
			for _, sub := range tc.want {
				if !strings.Contains(got, sub) {
					t.Fatalf("bootCrashDetail = %q, want substring %q", got, sub)
				}
			}
		})
	}
}

func TestTruncateDetailCap(t *testing.T) {
	got := truncateDetail(strings.Repeat("x", bootCrashDetailCap+500))
	if !strings.HasSuffix(got, "...(truncated)") {
		t.Fatalf("want truncated suffix, got tail %q", got[len(got)-20:])
	}
	if want := bootCrashDetailCap + len("...(truncated)"); len(got) != want {
		t.Fatalf("truncated length = %d, want %d", len(got), want)
	}
	short := "container=wrapper exit=1"
	if got := truncateDetail(short); got != short {
		t.Fatalf("short detail must pass through unchanged, got %q", got)
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

// setPodContainerTerminated stamps a non-zero terminated wrapper container
// status (with a log tail in Terminated.Message, as the kubelet would under
// terminationMessagePolicy=FallbackToLogsOnError) onto an existing pod.
func setPodContainerTerminated(t *testing.T, podName string, exit int32, reason, msg string) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: podName}, pod); err != nil {
		t.Fatalf("get pod %s: %v", podName, err)
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "wrapper",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: exit, Reason: reason, Message: msg,
		}},
	}}
	if err := k8sClient.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("set pod container terminated %s: %v", podName, err)
	}
}

// TestHandleBootCrashCapturesDiagnostics asserts the crash cause is captured
// from pod.Status into the annotation BEFORE resetAgentRun deletes the pod, so
// it survives the respawn.
func TestHandleBootCrashCapturesDiagnostics(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, task := seedBootCrashTask(t, "bootcrash-capture", corev1.PodRunning)
	setPodContainerTerminated(t, agent.PodName(task), 1, "Error", "panic: nil map deref")
	task = getTask(t, "bootcrash-capture")

	_, err, handled := r.handleBootCrash(ctx, task)
	if err != nil {
		t.Fatalf("handleBootCrash: %v", err)
	}
	if !handled {
		t.Fatal("a ContainerExited pod must be handled")
	}

	got := getTask(t, "bootcrash-capture")
	// The respawn ran: phase reset, attempts bumped, pod deleted.
	if got.Status.Phase != "" {
		t.Fatalf("phase = %q, want reset to empty", got.Status.Phase)
	}
	if n := got.Annotations[annBootCrashAttempts]; n != "1" {
		t.Fatalf("boot-crash attempts = %q, want 1", n)
	}
	// Diagnostics survived resetAgentRun (which deletes the pod) - the whole point.
	diag := got.Annotations[annBootCrashDiagnostics]
	if !strings.Contains(diag, "exit=1") || !strings.Contains(diag, "log=panic: nil map deref") {
		t.Fatalf("diagnostics annotation = %q, want exit=1 + log tail", diag)
	}
}

// TestHandleBootCrashExhaustedSurfacesDiagnostics asserts that at budget
// exhaustion the captured cause is folded into the terminal BootCrashLoop
// condition message AND posted once to the linked issue.
func TestHandleBootCrashExhaustedSurfacesDiagnostics(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	fw := &fakeWriter{}
	reg := prometheus.NewRegistry()
	r := newTaskReconciler(newFakeSession())
	r.Metrics = obs.NewOperatorMetrics(reg)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "bcd-scm", Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat"), "webhookSecret": []byte("w")},
	}
	require.NoError(t, k8sClient.Create(ctx, sec))
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "bcd-proj", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "bcd-scm", Scm: &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "bot"}},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))
	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "bcd-repo", Namespace: testNS},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: "bcd-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"},
	}
	require.NoError(t, k8sClient.Create(ctx, repo))

	task := &tatarav1alpha1.Task{}
	task.Name = "bootcrash-diag-exhausted"
	task.Namespace = testNS
	task.Spec.ProjectRef = "bcd-proj"
	task.Spec.RepositoryRef = "bcd-repo"
	task.Spec.Goal = "g"
	task.Spec.Kind = "issueLifecycle"
	task.Spec.Source = &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#82", Number: 82}
	// Pre-spend the budget so the next detection terminates.
	task.Annotations = map[string]string{annBootCrashAttempts: strconv.Itoa(maxPodRecreations)}
	require.NoError(t, k8sClient.Create(ctx, task))
	task.Status.Phase = "Planning"
	task.Status.PodName = agent.PodName(task)
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: agent.PodName(task), Namespace: testNS},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "wrapper", Image: "wrapper:1"}}},
	}
	require.NoError(t, k8sClient.Create(ctx, pod))
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "wrapper",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 1, Reason: "Error", Message: "panic: boom",
		}},
	}}
	require.NoError(t, k8sClient.Status().Update(ctx, pod))

	_, err, handled := r.handleBootCrash(ctx, getTask(t, "bootcrash-diag-exhausted"))
	require.NoError(t, err)
	require.True(t, handled)

	got := getTask(t, "bootcrash-diag-exhausted")
	require.Equal(t, "Failed", got.Status.Phase)
	cond := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
	require.NotNil(t, cond)
	require.Equal(t, "BootCrashLoop", cond.Reason)
	require.Contains(t, cond.Message, "ContainerExited")
	require.Contains(t, cond.Message, "exit=1")
	require.Contains(t, cond.Message, "panic: boom")

	// The cause is posted exactly once to the linked issue (durable past CRD GC).
	require.Len(t, fw.commentArgs, 1)
	require.Contains(t, fw.commentArgs[0], "o/r#82|")
	require.Contains(t, fw.commentArgs[0], "panic: boom")

	require.Equal(t, float64(1), counterValue(t, reg, "operator_agent_boot_crash_total",
		map[string]string{"reason": "ContainerExited", "outcome": "failed"}))
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
