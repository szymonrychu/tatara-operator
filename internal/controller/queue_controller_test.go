package controller

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func qe(name, class, state, taskRef string) tatarav1alpha1.QueuedEvent {
	return tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec:       tatarav1alpha1.QueuedEventSpec{Class: class, ProjectRef: "p"},
		Status:     tatarav1alpha1.QueuedEventStatus{State: state, TaskRef: taskRef},
	}
}

func tk(name, phase, lifecycle, queuedEvent string) tatarav1alpha1.Task {
	return tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara", Labels: map[string]string{LabelQueuedEvent: queuedEvent}},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "p"},
		Status:     tatarav1alpha1.TaskStatus{Phase: phase, LifecycleState: lifecycle},
	}
}

func TestPoolInflight_CountsAdmittedNonTerminal(t *testing.T) {
	r := &DispatcherReconciler{}
	qes := []tatarav1alpha1.QueuedEvent{
		qe("a", tatarav1alpha1.QueueClassNormal, tatarav1alpha1.QueueStateAdmitted, "t-a"), // running -> counts
		qe("b", tatarav1alpha1.QueueClassNormal, tatarav1alpha1.QueueStateAdmitted, "t-b"), // terminal -> not
		qe("c", tatarav1alpha1.QueueClassAlert, tatarav1alpha1.QueueStateAdmitted, "t-c"),  // alert running
		qe("d", tatarav1alpha1.QueueClassNormal, tatarav1alpha1.QueueStateQueued, ""),      // queued -> not
	}
	tasks := []tatarav1alpha1.Task{
		tk("t-a", "Running", "", "a"),
		tk("t-b", "Succeeded", "", "b"),
		tk("t-c", "Running", "", "c"),
	}
	if got := r.poolInflight(qes, tasks, tatarav1alpha1.QueueClassNormal); got != 1 {
		t.Fatalf("normal inflight = %d, want 1", got)
	}
	if got := r.poolInflight(qes, tasks, tatarav1alpha1.QueueClassAlert); got != 1 {
		t.Fatalf("alert inflight = %d, want 1", got)
	}
}
