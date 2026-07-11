package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// Phase 6 sub-step 1: the clarify->implement handoff producer. An issue carrying
// tatara-implementation with no live Task and no prior implement Task needs a
// fresh implement Task (clarify terminated its own Task after flipping the label,
// leaving the issue dead-ended).

func mkProducerTask(name, kind, issueRef string, number int, lifecycleState string) tatarav1alpha1.Task {
	return tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{tatarav1alpha1.LabelSourceKind: kind},
		},
		Spec: tatarav1alpha1.TaskSpec{
			Kind:   kind,
			Source: &tatarav1alpha1.TaskSource{IssueRef: issueRef, Number: number},
		},
		Status: tatarav1alpha1.TaskStatus{DeployState: lifecycleState},
	}
}

func TestNeedsImplementProducer(t *testing.T) {
	impl := "tatara-implementation"
	issue := candidate{repo: "o/r", number: 5, labels: []string{impl}}

	t.Run("handed-off issue with only a terminal clarify Task produces", func(t *testing.T) {
		existing := []tatarav1alpha1.Task{mkProducerTask("clarify-1", "clarify", "o/r#5", 5, "Done")}
		require.True(t, needsImplementProducer(issue, existing, impl))
	})

	t.Run("issue with no Task at all produces", func(t *testing.T) {
		require.True(t, needsImplementProducer(issue, nil, impl))
	})

	t.Run("issue without the implementation label does not produce", func(t *testing.T) {
		plain := candidate{repo: "o/r", number: 5, labels: []string{"tatara-brainstorming"}}
		require.False(t, needsImplementProducer(plain, nil, impl))
	})

	t.Run("a PR candidate never produces", func(t *testing.T) {
		pr := candidate{repo: "o/r", number: 5, labels: []string{impl}, isPR: true}
		require.False(t, needsImplementProducer(pr, nil, impl))
	})

	t.Run("a live Task already owning the issue blocks production", func(t *testing.T) {
		existing := []tatarav1alpha1.Task{mkProducerTask("impl-live", "implement", "o/r#5", 5, "")}
		require.False(t, needsImplementProducer(issue, existing, impl))
	})

	t.Run("a live draining issueLifecycle Task blocks production", func(t *testing.T) {
		existing := []tatarav1alpha1.Task{mkProducerTask("bridge", "issueLifecycle", "o/r#5", 5, "Implement")}
		require.False(t, needsImplementProducer(issue, existing, impl))
	})

	t.Run("a terminal issueLifecycle Task blocks production (backstop drain owns it)", func(t *testing.T) {
		existing := []tatarav1alpha1.Task{mkProducerTask("bridge-done", "issueLifecycle", "o/r#5", 5, "Done")}
		require.False(t, needsImplementProducer(issue, existing, impl))
	})

	t.Run("a prior terminal implement Task blocks re-production (no re-fire loop)", func(t *testing.T) {
		existing := []tatarav1alpha1.Task{mkProducerTask("impl-done", "implement", "o/r#5", 5, "Done")}
		require.False(t, needsImplementProducer(issue, existing, impl))
	})
}

func TestHasKindTaskForIssue(t *testing.T) {
	existing := []tatarav1alpha1.Task{
		mkProducerTask("clar", "clarify", "o/r#7", 7, "Done"),
		mkProducerTask("impl", "implement", "o/r#7", 7, "Done"),
	}
	require.True(t, hasKindTaskForIssue(existing, "o/r", 7, "implement"))
	require.True(t, hasKindTaskForIssue(existing, "o/r", 7, "clarify"))
	require.False(t, hasKindTaskForIssue(existing, "o/r", 7, "review"))
	require.False(t, hasKindTaskForIssue(existing, "o/r", 8, "implement"))
}
