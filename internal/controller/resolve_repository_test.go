package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestResolveRepository_NameAndSlug(t *testing.T) {
	ctx := context.Background()
	proj := "rr-proj"
	mkRepo := func(name, url string) {
		require.NoError(t, k8sClient.Create(ctx, &tatarav1alpha1.Repository{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
			Spec: tatarav1alpha1.RepositorySpec{
				ProjectRef: proj, URL: url, DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
			},
		}))
	}
	mkRepo("rr-repo", "https://github.com/o/rr-repo.git")
	mkRepo("rr-other", "https://github.com/o/rr-other.git")

	r := &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

	// Direct CR name resolves.
	got, err := r.resolveRepository(ctx, testNS, proj, "rr-repo")
	require.NoError(t, err)
	require.Equal(t, "rr-repo", got.Name)

	// owner/repo slug resolves to the CR by URL (this is what the brainstorm agent sends).
	got, err = r.resolveRepository(ctx, testNS, proj, "o/rr-repo")
	require.NoError(t, err)
	require.Equal(t, "rr-repo", got.Name)

	// Unknown ref errors.
	_, err = r.resolveRepository(ctx, testNS, proj, "o/does-not-exist")
	require.Error(t, err)
}
