package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func mkPRTask(repo string, pr int, lc string) tatarav1alpha1.Task {
	return tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labelSourceRepo: sanitizeRepoLabel(repo)}},
		Spec: tatarav1alpha1.TaskSpec{
			Kind:   "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{Number: pr, IsPR: true},
		},
		Status: tatarav1alpha1.TaskStatus{LifecycleState: lc},
	}
}

func TestPriorTerminalAttempts_CountsTerminalPRTasks(t *testing.T) {
	existing := []tatarav1alpha1.Task{
		mkPRTask("o/r", 50, "Parked"),
		mkPRTask("o/r", 50, "Done"),
		mkPRTask("o/r", 50, "Implement"), // non-terminal: not counted
		mkPRTask("o/r", 51, "Parked"),    // different PR: not counted
		mkPRTask("o/x", 50, "Parked"),    // different repo: not counted
	}
	require.Equal(t, 2, priorTerminalAttempts(existing, "o/r", 50))
	require.Equal(t, 0, priorTerminalAttempts(existing, "o/r", 99))
}

func TestRecoveryBoundThreshold(t *testing.T) {
	require.Equal(t, 3, maxRecoveryAttempts)
}
