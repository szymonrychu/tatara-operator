package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

func anchorlessIssue(name, author, body string, state string, anchor string) tatarav1alpha1.Issue {
	iss := tatarav1alpha1.Issue{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS}}
	iss.Spec.ProposalBodyHash = anchor
	iss.Status.Author = author
	iss.Status.Body = body
	iss.Status.State = state
	return iss
}

// THE SILENT CLASS. A bot-authored issue whose body carries a valid
// tatara-proposed-by marker but whose Spec holds NO anchor is a proposal the
// platform can never act on autonomously: autoApproveApplies fails closed on an
// empty anchor, and effectiveProposalKind refuses it too, so it does not even
// count against the backlog target it occupies. It is not repairable - nothing
// may derive an anchor from the mirrored body - so the only thing left is to
// make it VISIBLE instead of letting it park forever unexplained.
func TestAnchorlessProposals(t *testing.T) {
	marked := tatarav1alpha1.StampProposalMarker("an idea", tatarav1alpha1.ProposalKindBrainstorm)
	anchored := tatarav1alpha1.ComputeProposalContentHash(marked)

	for _, tc := range []struct {
		name string
		iss  tatarav1alpha1.Issue
		want bool
	}{
		{"bot, marked, no anchor, open", anchorlessIssue("a", "tatara-bot", marked, "open", ""), true},
		{"anchored", anchorlessIssue("b", "tatara-bot", marked, "open", anchored), false},
		{"human-authored", anchorlessIssue("c", "a-human", marked, "open", ""), false},
		{"no marker", anchorlessIssue("d", "tatara-bot", "an idea", "open", ""), false},
		{"closed is moot", anchorlessIssue("e", "tatara-bot", marked, "closed", ""), false},
		{"no author", anchorlessIssue("f", "", marked, "open", ""), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := anchorlessProposals([]tatarav1alpha1.Issue{tc.iss}, "tatara-bot")
			if tc.want {
				require.Equal(t, []string{tc.iss.Name}, got)
				return
			}
			require.Empty(t, got)
		})
	}
}

// An unconfigured botLogin must never make every open marked issue look
// anchorless: the authorship half fails closed, exactly as issueAuthoredByBot does.
func TestAnchorlessProposals_EmptyBotLoginFailsClosed(t *testing.T) {
	marked := tatarav1alpha1.StampProposalMarker("an idea", tatarav1alpha1.ProposalKindBrainstorm)
	require.Empty(t, anchorlessProposals(
		[]tatarav1alpha1.Issue{anchorlessIssue("a", "tatara-bot", marked, "open", "")}, ""))
}

func TestAnchorlessProposals_ReportsEveryOne(t *testing.T) {
	marked := tatarav1alpha1.StampProposalMarker("an idea", tatarav1alpha1.ProposalKindIncident)
	got := anchorlessProposals([]tatarav1alpha1.Issue{
		anchorlessIssue("iss-terraform-10", "tatara-bot", marked, "open", ""),
		anchorlessIssue("iss-ansible-9", "tatara-bot", marked, "open", ""),
	}, "tatara-bot")
	require.Equal(t, []string{"iss-terraform-10", "iss-ansible-9"}, got)
}

// The refill pass is the ONE place that already holds every proposal Issue in
// the project, so it is where the stuck set gets published. The gauge must be
// written every pass INCLUDING the zero: a level that latches its last nonzero
// value is indistinguishable from a class that was never cleared.
func TestBrainstormPublishesTheAnchorlessProposalGauge(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-anchorless-gauge", []string{"o/r1"}, ptrInt(3))
	r := newScanReconciler(emptyReader("o/r1"))
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	body := tatarav1alpha1.StampProposalMarker("an idea", tatarav1alpha1.ProposalKindBrainstorm)
	seedAnchorlessProposal(t, r, proj, repos[0].Name, 41, body)

	r.brainstorm(ctx, proj, emptyReader("o/r1"), repos, nil, proj.Spec.Scm.Cron.Brainstorm)

	got, ok := projectGaugeSeries(t, ctrlmetrics.Registry.(*prometheus.Registry),
		"operator_anchorless_proposals", "project", proj.Name)
	require.True(t, ok, "the gauge must be published, not absent")
	require.Equal(t, float64(1), got)
}

func TestBrainstormPublishesAnExplicitZeroAnchorlessProposals(t *testing.T) {
	ctx := context.Background()
	proj, repos := seedBrainstormProject(t, "bs-anchorless-zero", []string{"o/r1"}, ptrInt(0))
	r := newScanReconciler(emptyReader("o/r1"))
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.brainstorm(ctx, proj, emptyReader("o/r1"), repos, nil, proj.Spec.Scm.Cron.Brainstorm)

	got, ok := projectGaugeSeries(t, ctrlmetrics.Registry.(*prometheus.Registry),
		"operator_anchorless_proposals", "project", proj.Name)
	require.True(t, ok, "a clean project reads an explicit 0, never absent")
	require.Equal(t, float64(0), got)
}

// seedAnchorlessProposal writes the LIVE SHAPE of the sixteen stuck issues: the
// mirror's Spec (no ProposalKind, no ProposalBodyHash) over a bot-authored,
// marker-carrying body.
func seedAnchorlessProposal(t *testing.T, r *ProjectReconciler, proj *tatarav1alpha1.Project,
	repoRef string, number int, body string) {
	t.Helper()
	ctx := context.Background()
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.IssueName(repoRef, number), Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repoRef, Number: number, ProjectRef: proj.Name,
			URL: "https://github.com/o/r1/issues/1",
		},
	}
	require.NoError(t, r.Create(ctx, iss))
	iss.Status.Author, iss.Status.Body, iss.Status.State = "tatara-bot", body, "open"
	require.NoError(t, r.Status().Update(ctx, iss))
}
