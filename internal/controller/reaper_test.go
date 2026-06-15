package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// mkWrapperPodSvc creates a labelled wrapper Pod + Service named after the pod,
// correlated to taskName/taskUID via the reaper's labels.
func mkWrapperPodSvc(t *testing.T, name, taskName, taskUID string) {
	t.Helper()
	labels := map[string]string{
		agent.LabelManagedBy: agent.ManagedByValue,
		agent.LabelComponent: agent.ComponentAgent,
		agent.LabelTask:      taskName,
	}
	if taskUID != "" {
		labels[agent.LabelTaskUID] = taskUID
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: labels},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: "wrapper", Image: "wrapper:1"}},
		},
	}
	if err := k8sClient.Create(context.Background(), pod); err != nil {
		t.Fatalf("create pod %s: %v", name, err)
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: labels},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080}}},
	}
	if err := k8sClient.Create(context.Background(), svc); err != nil {
		t.Fatalf("create service %s: %v", name, err)
	}
}

func podExists(t *testing.T, name string) bool {
	t.Helper()
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &corev1.Pod{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get pod %s: %v", name, err)
	}
	return err == nil
}

func TestReapOrphans_TaskAbsent(t *testing.T) {
	mkWrapperPodSvc(t, "reap-absent", "no-such-task", "uid-x")
	newCallbackServer().ReapOrphans(context.Background())
	if podExists(t, "reap-absent") {
		t.Error("expected pod for absent task to be reaped")
	}
}

func TestReapOrphans_TerminalPhase(t *testing.T) {
	mkTaskProject(t, "p-reap-ph", 3)
	mkTaskRepository(t, "r-reap-ph", "p-reap-ph")
	mkTask(t, "t-reap-ph", "p-reap-ph", "r-reap-ph")
	setTaskPhase(t, "t-reap-ph", "Succeeded")
	mkWrapperPodSvc(t, "reap-phase", "t-reap-ph", string(getTask(t, "t-reap-ph").UID))

	newCallbackServer().ReapOrphans(context.Background())
	if podExists(t, "reap-phase") {
		t.Error("expected pod for Succeeded task to be reaped")
	}
}

func TestReapOrphans_TerminalLifecycle(t *testing.T) {
	mkTaskProject(t, "p-reap-lc", 3)
	mkTaskRepository(t, "r-reap-lc", "p-reap-lc")
	mkTask(t, "t-reap-lc", "p-reap-lc", "r-reap-lc")
	tk := getTask(t, "t-reap-lc")
	tk.Status.LifecycleState = "Done"
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("set lifecycle: %v", err)
	}
	mkWrapperPodSvc(t, "reap-lc", "t-reap-lc", string(tk.UID))

	newCallbackServer().ReapOrphans(context.Background())
	if podExists(t, "reap-lc") {
		t.Error("expected pod for Done lifecycle task to be reaped")
	}
}

func TestReapOrphans_StaleUID(t *testing.T) {
	mkTaskProject(t, "p-reap-uid", 3)
	mkTaskRepository(t, "r-reap-uid", "p-reap-uid")
	mkTask(t, "t-reap-uid", "p-reap-uid", "r-reap-uid")
	// Pod carries a UID from a prior incarnation that reused the task name.
	mkWrapperPodSvc(t, "reap-uid", "t-reap-uid", "stale-uid-from-old-task")

	newCallbackServer().ReapOrphans(context.Background())
	if podExists(t, "reap-uid") {
		t.Error("expected pod with stale task-uid to be reaped")
	}
}

func TestReapOrphans_LiveTaskKept(t *testing.T) {
	mkTaskProject(t, "p-reap-live", 3)
	mkTaskRepository(t, "r-reap-live", "p-reap-live")
	mkTask(t, "t-reap-live", "p-reap-live", "r-reap-live")
	setTaskPhase(t, "t-reap-live", "Running")
	mkWrapperPodSvc(t, "reap-live", "t-reap-live", string(getTask(t, "t-reap-live").UID))

	newCallbackServer().ReapOrphans(context.Background())
	if !podExists(t, "reap-live") {
		t.Error("expected pod for live non-terminal task to be kept")
	}
}
