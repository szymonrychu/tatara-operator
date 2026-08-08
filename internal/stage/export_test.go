package stage_test

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// now is the single fixed clock every pure stage test measures against.
var now = time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

// task returns a Task in state, spec.kind=implement, with StateEnteredAt set.
// It is an ordinary constructor, not an abstraction: internal/stage is pure, so
// a fixture is a struct literal and nothing more.
func task(state string) *v1alpha1.Task {
	stamp := metav1.NewTime(now)
	return &v1alpha1.Task{
		Spec: v1alpha1.TaskSpec{ProjectRef: "proj", Kind: "implement", Goal: "g"},
		Status: v1alpha1.TaskStatus{
			State:          state,
			StateEnteredAt: &stamp,
		},
	}
}

func taskOfKind(state, kind string) *v1alpha1.Task {
	t := task(state)
	t.Spec.Kind = kind
	return t
}

func ptrTime(at time.Time) *metav1.Time {
	s := metav1.NewTime(at)
	return &s
}
