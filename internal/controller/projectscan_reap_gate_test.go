package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// staleReapableIssue is a bot-authored, brainstorming-labelled, long-stale issue
// the reaper would close. issueScan and recoverOrphans must NOT create a fresh
// lifecycle task for it: such a task races the same-cycle reaper, which would
// then close an issue that owns a live (non-terminal) task.
func staleReapableIssue() scm.IssueRef {
	return scm.IssueRef{Repo: "o/r", Number: 5, Author: "tatara-bot",
		Labels: []string{"tatara-brainstorming"}, UpdatedAt: time.Now().Add(-30 * 24 * time.Hour)}
}

func enableReaper(proj *tatarav1alpha1.Project, days int) {
	proj.Spec.Scm.Cron.Brainstorm.StaleProposalDays = days
}

func TestIssueScan_SkipsReapEligibleProposal(t *testing.T) {
	proj, repo := seedBackstopProject(t, "reapgate-issuescan")
	enableReaper(proj, 14)
	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{"o/r": {staleReapableIssue()}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil, proj.Spec.Scm.Cron.IssueScan)

	qes := listScanQEs(t, "reapgate-issuescan")
	require.Empty(t, qes, "issueScan must not create a lifecycle task for a reap-eligible proposal")
}

func TestIssueScan_TriagesWhenReaperDisabled(t *testing.T) {
	proj, repo := seedBackstopProject(t, "reapgate-issuescan-off")
	enableReaper(proj, -1) // negative -> reaper explicitly disabled (finding #8: 0 now defaults ON)
	// Reaper disabled -> normal triage (guard against the gate firing when the
	// feature is off). A human comment is seeded so the (unrelated)
	// bot-brainstorm-no-human-activity gate does not also suppress it.
	reader := &perRepoFakeReader{
		fakeReader:   fakeReader{comments: []scm.IssueComment{{Author: "szymonrychu", Body: "please build this", CreatedAt: time.Now()}}},
		issuesByRepo: map[string][]scm.IssueRef{"o/r": {staleReapableIssue()}},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil, proj.Spec.Scm.Cron.IssueScan)

	qes := listScanQEs(t, "reapgate-issuescan-off")
	require.Len(t, qes, 1, "with the reaper disabled issueScan triages the proposal as before")
}

func TestRecoverOrphans_SkipsReapEligibleProposal(t *testing.T) {
	proj, repo := seedBackstopProject(t, "reapgate-orphans")
	enableReaper(proj, 14)
	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{"o/r": {staleReapableIssue()}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.recoverOrphans(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo}, nil)

	qes := listScanQEs(t, "reapgate-orphans")
	require.Empty(t, qes, "recoverOrphans must not recover a reap-eligible proposal")
}

func TestReapStaleProposals_RelistsLiveTaskCreatedThisCycle(t *testing.T) {
	reader := &reapFakeReader{} // no human comments -> would otherwise close
	fw := &reapFakeWriter{}
	r, proj := newReapReconciler(t, "reap-relist", reader, fw)
	enableReaper(proj, 14)

	// A live (non-terminal) lifecycle task for o/r#5 exists in the cluster but is
	// NOT in the (stale) `existing` slice passed in - simulating a task created by
	// issueScan/recoverOrphans earlier this same reconcile.
	task := &tatarav1alpha1.Task{}
	task.GenerateName = "reap-relist-"
	task.Namespace = testNS
	task.Labels = map[string]string{labelSourceKind: "issueLifecycle", labelActivity: "issueScan"}
	task.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef: "reap-relist", RepositoryRef: "r", Goal: "Triage", Kind: "issueLifecycle",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
	}
	require.NoError(t, k8sClient.Create(context.Background(), task))
	task.Status.DeployState = "Triage" // non-terminal
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), task) })

	r.reapStaleProposals(context.Background(), proj, reader, staleIssueCache(), nil, proj.Spec.Scm.Cron.Brainstorm)

	require.Empty(t, fw.closeIssueCalls,
		"reaper must re-list tasks and skip an issue that owns a live task created this cycle")
}
