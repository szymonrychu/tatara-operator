package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// Phase 6 sub-step 3: the deploy supervisor is the ONLY writer.Merge caller,
// gated on tatara-approved + green + mergeable + !merged.

type mergeGateWriter struct {
	scm.SCMWriter
	prState      scm.PRState
	mergeState   scm.MergeState
	mergeCalled  bool
	mergeNumber  int
	ensureLabels []string
	addLabels    []string
}

func (w *mergeGateWriter) GetPRState(context.Context, string, string, int) (scm.PRState, error) {
	return w.prState, nil
}
func (w *mergeGateWriter) GetIssueState(context.Context, string, string, int) (scm.IssueState, error) {
	return scm.IssueState{}, nil
}
func (w *mergeGateWriter) GetMergeState(context.Context, string, string, int) (scm.MergeState, error) {
	if w.mergeState == "" {
		return scm.MergeStateClean, nil
	}
	return w.mergeState, nil
}
func (w *mergeGateWriter) Merge(_ context.Context, _, _ string, number int, _ string) (string, error) {
	w.mergeCalled = true
	w.mergeNumber = number
	return "mergedsha", nil
}
func (w *mergeGateWriter) EnsureLabel(_ context.Context, _, _, name, _ string) error {
	w.ensureLabels = append(w.ensureLabels, name)
	return nil
}
func (w *mergeGateWriter) AddLabel(_ context.Context, _, _, label string) error {
	w.addLabels = append(w.addLabels, label)
	return nil
}

type mergeGateReader struct {
	scm.SCMReader
	prs []scm.PRRef
}

func (r *mergeGateReader) ListOpenPRs(context.Context, string, string) ([]scm.PRRef, error) {
	return r.prs, nil
}

func seedMergeGateScene(t *testing.T, suffix string) *tatarav1alpha1.Project {
	t.Helper()
	ctx := context.Background()
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mg-scm-" + suffix, Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat"), "webhookSecret": []byte("w")},
	}
	require.NoError(t, k8sClient.Create(ctx, sec))
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "mg-proj-" + suffix, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: sec.Name,
			TriggerLabel: "tatara",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: "tatara-bot"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))
	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "mg-repo-" + suffix, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: proj.Name, URL: "https://github.com/o/r.git",
			DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	require.NoError(t, k8sClient.Create(ctx, repo))
	return proj
}

func newMergeGateReconciler(fw scm.SCMWriter, rd scm.SCMReader) *ProjectReconciler {
	return &ProjectReconciler{
		Client:    k8sClient,
		Scheme:    k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return fw, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return rd, nil },
	}
}

func TestSuperviseApprovedPRs_MergesApprovedGreenBotPR(t *testing.T) {
	proj := seedMergeGateScene(t, "ok")
	var repos tatarav1alpha1.RepositoryList
	require.NoError(t, k8sClient.List(context.Background(), &repos))
	fw := &mergeGateWriter{prState: scm.PRState{CIStatus: "success", Merged: false}}
	rd := &mergeGateReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 42, Author: "tatara-bot", Labels: []string{"tatara-approved"}},
	}}
	r := newMergeGateReconciler(fw, rd)

	r.superviseApprovedPRs(context.Background(), proj, rd, filterRepos(repos.Items, proj.Name))

	require.True(t, fw.mergeCalled, "approved + green + mergeable bot PR must be merged")
	require.Equal(t, 42, fw.mergeNumber)
	// landmine (b): a semver:* label must be stamped before merge or push-CD's tag
	// step fails closed.
	require.True(t, containsPrefix(fw.ensureLabels, "semver:") || containsPrefix(fw.addLabels, "semver:"),
		"a semver:* label must be ensured before the operator merge")
}

func TestSuperviseApprovedPRs_SkipsUnapprovedPR(t *testing.T) {
	proj := seedMergeGateScene(t, "noapprove")
	var repos tatarav1alpha1.RepositoryList
	require.NoError(t, k8sClient.List(context.Background(), &repos))
	fw := &mergeGateWriter{prState: scm.PRState{CIStatus: "success"}}
	rd := &mergeGateReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 7, Author: "tatara-bot", Labels: []string{"tatara-implementation"}},
	}}
	r := newMergeGateReconciler(fw, rd)

	r.superviseApprovedPRs(context.Background(), proj, rd, filterRepos(repos.Items, proj.Name))

	require.False(t, fw.mergeCalled, "a PR without tatara-approved must never be merged")
}

func TestSuperviseApprovedPRs_SkipsNonGreenPR(t *testing.T) {
	proj := seedMergeGateScene(t, "notgreen")
	var repos tatarav1alpha1.RepositoryList
	require.NoError(t, k8sClient.List(context.Background(), &repos))
	fw := &mergeGateWriter{prState: scm.PRState{CIStatus: "pending"}}
	rd := &mergeGateReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 8, Author: "tatara-bot", Labels: []string{"tatara-approved"}},
	}}
	r := newMergeGateReconciler(fw, rd)

	r.superviseApprovedPRs(context.Background(), proj, rd, filterRepos(repos.Items, proj.Name))

	require.False(t, fw.mergeCalled, "an approved but non-green PR must not be merged")
}

func TestSuperviseApprovedPRs_SkipsAlreadyMerged(t *testing.T) {
	proj := seedMergeGateScene(t, "merged")
	var repos tatarav1alpha1.RepositoryList
	require.NoError(t, k8sClient.List(context.Background(), &repos))
	fw := &mergeGateWriter{prState: scm.PRState{CIStatus: "success", Merged: true}}
	rd := &mergeGateReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 9, Author: "tatara-bot", Labels: []string{"tatara-approved"}},
	}}
	r := newMergeGateReconciler(fw, rd)

	r.superviseApprovedPRs(context.Background(), proj, rd, filterRepos(repos.Items, proj.Name))

	require.False(t, fw.mergeCalled, "a PR already merged (native auto-merge) must not be re-merged")
}

func TestSuperviseApprovedPRs_SkipsNonBotPR(t *testing.T) {
	proj := seedMergeGateScene(t, "nonbot")
	var repos tatarav1alpha1.RepositoryList
	require.NoError(t, k8sClient.List(context.Background(), &repos))
	fw := &mergeGateWriter{prState: scm.PRState{CIStatus: "success"}}
	rd := &mergeGateReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 10, Author: "some-human", Labels: []string{"tatara-approved"}},
	}}
	r := newMergeGateReconciler(fw, rd)

	r.superviseApprovedPRs(context.Background(), proj, rd, filterRepos(repos.Items, proj.Name))

	require.False(t, fw.mergeCalled, "a non-bot PR must never be operator-merged (agents never merge)")
}

func filterRepos(all []tatarav1alpha1.Repository, project string) []tatarav1alpha1.Repository {
	var out []tatarav1alpha1.Repository
	for i := range all {
		if all[i].Spec.ProjectRef == project {
			out = append(out, all[i])
		}
	}
	return out
}

func containsPrefix(ss []string, prefix string) bool {
	for _, s := range ss {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}
