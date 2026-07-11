package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func crdProject(name string, scm tatarav1alpha1.ScmSpec) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "crd-scm-" + name, Scm: &scm},
	}
}

func TestProjectCRDValidation_BotLogin(t *testing.T) {
	ctx := context.Background()
	base := tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"}

	// Accept-case first: a valid Project (bot set, not in any list) must be admitted.
	valid := crdProject("crd-ok", base)
	valid.Spec.Scm.MaintainerLogins = []string{"szymon"}
	valid.Spec.Scm.ReporterLogins = []string{"szymon"}
	require.NoError(t, k8sClient.Create(ctx, valid), "valid Project must be admitted")

	empty := base
	empty.BotLogin = ""
	require.Error(t, k8sClient.Create(ctx, crdProject("crd-empty", empty)),
		"empty botLogin must be rejected (MinLength=1)")

	botMaint := base
	botMaint.MaintainerLogins = []string{"szymon", "tatara-bot"}
	require.Error(t, k8sClient.Create(ctx, crdProject("crd-botmaint", botMaint)),
		"botLogin in maintainerLogins must be rejected (CEL)")

	botRep := base
	botRep.ReporterLogins = []string{"tatara-bot"}
	require.Error(t, k8sClient.Create(ctx, crdProject("crd-botrep", botRep)),
		"botLogin in reporterLogins must be rejected (CEL)")
}
