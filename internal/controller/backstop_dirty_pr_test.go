package controller

// Task 11: backstop sweep creates conflict-fix task for stranded DIRTY bot PRs.

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// dirtyPRFakeWriter implements scm.SCMWriter with configurable GetMergeState.
// Other methods fall back to the embedded interface (nil, safe when not called).
type dirtyPRFakeWriter struct {
	scm.SCMWriter
	mergeState scm.MergeState
	closeCalls int
}

func (f *dirtyPRFakeWriter) GetMergeState(_ context.Context, _, _ string, _ int) (scm.MergeState, error) {
	return f.mergeState, nil
}

func (f *dirtyPRFakeWriter) ClosePR(_ context.Context, _, _ string, _ int, _ string) error {
	f.closeCalls++
	return nil
}

func (f *dirtyPRFakeWriter) EnsureLabel(_ context.Context, _, _, _, _ string) error { return nil }
func (f *dirtyPRFakeWriter) AddLabel(_ context.Context, _, _, _ string) error       { return nil }
func (f *dirtyPRFakeWriter) Comment(_ context.Context, _, _, _ string) error        { return nil }

// seedLiveDirtyPRTask creates a non-terminal (live) issueLifecycle Task matching
// (repo slug "o/r", number=issueNum) so hasLiveLifecycleTaskForIssue returns true.
func seedLiveDirtyPRTask(t *testing.T, projName, repoName, issueRef string, issueNum int) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()
	task := &tatarav1alpha1.Task{}
	task.GenerateName = "live-"
	task.Namespace = testNS
	task.Labels = map[string]string{
		labelSourceKind: "issueLifecycle",
		labelActivity:   "mrScan",
	}
	task.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    projName,
		RepositoryRef: repoName,
		Goal:          "live task for " + issueRef,
		Kind:          "issueLifecycle",
		Source: &tatarav1alpha1.TaskSource{
			Provider: "github",
			IssueRef: issueRef,
			Number:   issueNum,
			IsPR:     false,
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create live task: %v", err)
	}
	task.Status.LifecycleState = "Implement"
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("update live task status: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), task) })
	return task
}

// seedLivePodForTask creates a Pod in k8s that makes podIsLive return true for task.
func seedLivePodForTask(t *testing.T, task *tatarav1alpha1.Task) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      agent.PodName(task),
			Namespace: task.Namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "wrapper", Image: "wrapper:1"}},
		},
	}
	if err := k8sClient.Create(context.Background(), pod); err != nil {
		t.Fatalf("create pod for task %s: %v", task.Name, err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), pod) })
}

// TestBackstopSweep_DirtyBotPR_CreatesConflictFixTask verifies that a stranded
// DIRTY bot PR with no live pod and no live lifecycle task gets a conflict-fix
// issueLifecycle task. The four cases confirm the guards:
//   - dirty-no-pod-no-task: creates a conflict-fix QE (bsActionDirtyPR)
//   - clean-no-task: no conflict-fix QE (CLEAN merge state; normal reactivation
//     QE still fires via bsActionReactivate but that is pre-existing behavior)
//   - dirty-pod-live: no QE at all (bsActionNone; pod active)
//   - dirty-live-task: no conflict-fix QE (dedup guard fires)
func TestBackstopSweep_DirtyBotPR_CreatesConflictFixTask(t *testing.T) {
	cases := []struct {
		name            string
		mergeState      scm.MergeState
		podLive         bool
		liveTask        bool
		wantConflictFix bool // expect a conflict-fix QE (goal contains "conflict self-heal")
	}{
		{"dirty-no-pod-no-task", scm.MergeStateDirty, false, false, true},
		// CLEAN: no conflict-fix QE; normal reactivation QE fires (pre-existing behavior).
		{"clean-no-task", scm.MergeStateClean, false, false, false},
		// Pod live: bsActionNone, DIRTY probe skipped.
		{"dirty-pod-live", scm.MergeStateDirty, true, false, false},
		// Dedup guard: live lifecycle task for linked issue already in-flight.
		{"dirty-live-task", scm.MergeStateDirty, false, true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			projName := "dirty-pr-" + tc.name
			const prNumber = 267
			const issueNumber = 7

			proj, repo := seedBackstopSweepProject(t, projName)

			// Stranded task: open PR #267, open source issue #7.
			strandedTask := makeStrandedTask(t, projName, repo.Name, prNumber, issueNumber)

			if tc.liveTask {
				// Seed a non-terminal lifecycle task matching source issue #7.
				seedLiveDirtyPRTask(t, projName, repo.Name, "o/r#7", issueNumber)
			}

			if tc.podLive {
				// Create actual pod so podIsLive returns true.
				seedLivePodForTask(t, strandedTask)
			}

			fw := &dirtyPRFakeWriter{mergeState: tc.mergeState}

			reader := &backstopFakeReader{
				openIssues: map[string][]scm.IssueRef{"o/r": {{Repo: "o/r", Number: issueNumber}}},
				openPRs:    map[string][]scm.PRRef{"o/r": {{Repo: "o/r", Number: prNumber, HeadSHA: "sha1"}}},
			}

			r := newScanReconciler(reader)
			r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
			r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

			repos := []tatarav1alpha1.Repository{repo}
			r.backstopSweep(context.Background(), proj, reader, repos)

			// Filter QEs to only conflict-fix ones (bsActionDirtyPR creates these).
			// Regular reactivation QEs (bsActionReactivate, pre-existing) are excluded.
			var conflictFixQEs []tatarav1alpha1.QueuedEvent
			for _, qe := range listScanQEs(t, projName) {
				if strings.Contains(qe.Spec.Payload.Goal, "conflict self-heal") {
					conflictFixQEs = append(conflictFixQEs, qe)
				}
			}

			if tc.wantConflictFix {
				assert.Len(t, conflictFixQEs, 1, "must create one conflict-fix QE for %s", tc.name)
				if len(conflictFixQEs) > 0 {
					ann := conflictFixQEs[0].Spec.Payload.Annotations[tatarav1alpha1.LifecycleEntryAnnotation]
					assert.Equal(t, "MRCI", ann, "conflict-fix QE must enter at MRCI")
				}
			} else {
				assert.Empty(t, conflictFixQEs, "must NOT create a conflict-fix QE for %s", tc.name)
			}

			_ = client.ObjectKeyFromObject(strandedTask) // reference used
		})
	}
}
