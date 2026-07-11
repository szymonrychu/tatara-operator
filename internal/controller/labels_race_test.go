package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
)

// sequencedLabelReader returns a DIFFERENT label set on each successive
// ListOpenIssues call, simulating a concurrent controller mutating the issue's
// labels between setLifecycleLabel's pre-add read and its pre-remove re-read.
type sequencedLabelReader struct {
	fakeProposalReader
	calls [][]string
	n     int
}

func (r *sequencedLabelReader) ListOpenIssues(_ context.Context, _, _ string) ([]scm.IssueRef, error) {
	labels := r.calls[len(r.calls)-1]
	if r.n < len(r.calls) {
		labels = r.calls[r.n]
	}
	r.n++
	return []scm.IssueRef{{Repo: "o/r", Number: 7, Labels: labels}}, nil
}

func newSeqReaderReconciler(w scm.SCMWriter, rdr scm.SCMReader) *TaskReconciler {
	return &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return w, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return rdr, nil }}
}

// TestSetLifecycleLabel_RefusesBrainstormingOverImplementation is the finding-3
// second-half guard: a stale front-half (spurious clarify re-entering at Triage,
// or a triage revert racing a handoff) must NOT re-stamp brainstorming on an issue
// that already carries the implementation label - that would drag an
// actively-implementing issue back a phase. setLifecycleLabel refuses the write.
func TestSetLifecycleLabel_RefusesBrainstormingOverImplementation(t *testing.T) {
	r, task, w := seedLabelTask(t, "impl-guard", []string{"tatara-implementation"})
	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))
	require.NoError(t, r.setLifecycleLabel(context.Background(), &proj, task, "tatara-brainstorming"))
	require.Empty(t, w.added, "must not add brainstorming over an implementing issue")
	require.Empty(t, w.removed, "must not strip the implementation label")
}

// TestSetLifecycleLabel_ReAddsDesiredStrippedConcurrently is finding-5 "add-then-
// verify": the desired label was present at the pre-add read (so the add was
// skipped), but a racing controller stripped it before our removes. The pre-remove
// re-list observes the loss and re-adds it, so the desired label is never lost.
func TestSetLifecycleLabel_ReAddsDesiredStrippedConcurrently(t *testing.T) {
	_, task, w := seedLabelTask(t, "readd", nil)
	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))
	// Pre-add read shows tatara-approved present -> add skipped; re-list shows it
	// stripped (concurrent removal) -> re-add.
	rdr := &sequencedLabelReader{calls: [][]string{{"tatara-approved"}, {}}}
	r := newSeqReaderReconciler(w, rdr)
	require.NoError(t, r.setLifecycleLabel(context.Background(), &proj, task, "tatara-approved"))
	require.Contains(t, w.added, "tatara-approved", "a concurrently-stripped desired label must be re-added")
}

// TestSetLifecycleLabel_ReListNarrowsRemovals is finding-5 "re-list before remove":
// a managed label present at the pre-add read but already gone at the pre-remove
// re-list must NOT be removed - removing off the stale read could strip a label a
// racing controller just set. The fresh read is authoritative for the removes.
func TestSetLifecycleLabel_ReListNarrowsRemovals(t *testing.T) {
	_, task, w := seedLabelTask(t, "narrow", nil)
	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))
	// Pre-add: [tatara-idea] (desired approved absent -> add). Re-list: [tatara-approved]
	// only (idea gone concurrently), so idea must NOT be removed off the stale read.
	rdr := &sequencedLabelReader{calls: [][]string{{"tatara-idea"}, {"tatara-approved"}}}
	r := newSeqReaderReconciler(w, rdr)
	require.NoError(t, r.setLifecycleLabel(context.Background(), &proj, task, "tatara-approved"))
	require.Contains(t, w.added, "tatara-approved")
	require.NotContains(t, w.removed, "tatara-idea", "must not remove a label the fresh read shows already gone")
}
