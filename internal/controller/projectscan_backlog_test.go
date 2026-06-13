package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

type backlogReader struct {
	fakeProposalReader
	issues []scm.IssueRef
}

func (r *backlogReader) ListOpenIssues(_ context.Context, _, _ string) ([]scm.IssueRef, error) {
	return r.issues, nil
}

func TestProposalBacklog_CountsIdeaLabel(t *testing.T) {
	repo := &tatarav1alpha1.Repository{Spec: tatarav1alpha1.RepositorySpec{URL: "https://github.com/o/r.git"}}
	rdr := &backlogReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 1, Labels: []string{"tatara-idea"}},
		{Repo: "o/r", Number: 2, Labels: []string{"tatara-approved"}},
		{Repo: "o/r", Number: 3, Labels: []string{"tatara-idea"}, IsPR: true},
		{Repo: "o/r", Number: 4, Labels: []string{"tatara-idea"}},
	}}
	r := &ProjectReconciler{Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	n, err := r.proposalBacklog(context.Background(), rdr, repo, "tatara-idea", nil, nil)
	require.NoError(t, err)
	require.Equal(t, 2, n)
}
