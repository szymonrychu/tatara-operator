package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// upgradeProject is the minimum a Project needs to be admitted, so each test
// below fails on exactly the field it is about and nothing else.
func upgradeProject(t *testing.T, name string) *tataradevv1alpha1.Project {
	t.Helper()
	mkSecret(t, name+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	p := &tataradevv1alpha1.Project{}
	p.Name = name
	p.Namespace = testNS
	p.Spec.ScmSecretRef = name + "-scm"
	p.Spec.Scm = &tataradevv1alpha1.ScmSpec{
		Provider: "github", Owner: "o", BotLogin: "bot",
		Cron: &tataradevv1alpha1.ScmCron{},
	}
	return p
}

// The CEL key allowlist is CLOSED: an unlisted key is rejected at admission, so
// a project that tries to tier upgrade separately fails its helm apply with a
// message naming the allowed keys.
func TestProjectCRD_AcceptsUpgradeInModelAndEffortByKind(t *testing.T) {
	ctx := context.Background()
	proj := upgradeProject(t, "p-upgrade-keys")
	proj.Spec.Agent.ModelByKind = map[string]string{"upgrade": "claude-opus-5"}
	proj.Spec.Agent.EffortByKind = map[string]string{"upgrade": "high"}
	proj.Spec.TokenBudget = &tataradevv1alpha1.TokenBudgetSpec{
		SpawnCeilingByKind: map[string]int32{"upgrade": 80},
	}
	require.NoError(t, k8sClient.Create(ctx, proj), "the upgrade key must be admitted by every byKind allowlist")
}

func TestProjectCRD_AcceptsTheUpgradeCronAndPolicy(t *testing.T) {
	ctx := context.Background()
	proj := upgradeProject(t, "p-upgrade-cron")
	proj.Spec.Scm.Cron.Upgrade = tataradevv1alpha1.UpgradeActivity{Schedule: "0 */4 * * *", MaxOpenUpgrades: 2}
	proj.Spec.UpgradePolicy = &tataradevv1alpha1.UpgradePolicySpec{
		Engine:            "renovate",
		MajorStrategy:     "nextHopOnly",
		MinimumReleaseAge: &tataradevv1alpha1.ReleaseAgeSpec{Major: 0, Minor: 0, Patch: 0},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))

	var got tataradevv1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(proj), &got))
	require.Equal(t, "0 */4 * * *", got.Spec.Scm.Cron.Upgrade.Schedule)
	require.Equal(t, 2, got.Spec.Scm.Cron.Upgrade.MaxOpenUpgrades)
	require.Equal(t, "renovate", got.Spec.UpgradePolicy.Engine)
	require.Equal(t, "nextHopOnly", got.Spec.UpgradePolicy.MajorStrategy)
	require.NotNil(t, got.Spec.UpgradePolicy.MinimumReleaseAge)

	// The status stamp the cron advances the schedule with round-trips too.
	now := metav1.Now()
	got.Status.LastUpgrade = &now
	require.NoError(t, k8sClient.Status().Update(ctx, &got))
	var back tataradevv1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(proj), &back))
	require.NotNil(t, back.Status.LastUpgrade)
}

func TestProjectCRD_RejectsABadUpgradeCronAndABadEngine(t *testing.T) {
	ctx := context.Background()
	proj := upgradeProject(t, "p-upgrade-badcron")
	proj.Spec.Scm.Cron.Upgrade = tataradevv1alpha1.UpgradeActivity{Schedule: "not a cron"}
	require.Error(t, k8sClient.Create(ctx, proj), "a non-5-field schedule must be rejected by the pattern")

	p2 := upgradeProject(t, "p-upgrade-badengine")
	p2.Spec.UpgradePolicy = &tataradevv1alpha1.UpgradePolicySpec{Engine: "dependabot"}
	require.Error(t, k8sClient.Create(ctx, p2), "an unknown engine must be rejected by the enum")

	p3 := upgradeProject(t, "p-upgrade-badstrategy")
	p3.Spec.UpgradePolicy = &tataradevv1alpha1.UpgradePolicySpec{MajorStrategy: "yolo"}
	require.Error(t, k8sClient.Create(ctx, p3), "an unknown majorStrategy must be rejected by the enum")
}

// THE RECORDED LANDMINE: a kubebuilder default applies at WRITE and never
// retroactively, so an EXISTING Project never picks up a raised default. The
// enrollment values therefore set maxOpenUpgrades explicitly; this test only
// pins that an omitted value defaults to 1 for a FRESH Project.
func TestProjectCRD_MaxOpenUpgradesDefaultsToOneOnCreate(t *testing.T) {
	ctx := context.Background()
	proj := upgradeProject(t, "p-upgrade-default")
	proj.Spec.Scm.Cron.Upgrade = tataradevv1alpha1.UpgradeActivity{Schedule: "0 6 * * *"}
	require.NoError(t, k8sClient.Create(ctx, proj))

	var got tataradevv1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(proj), &got))
	require.Equal(t, 1, got.Spec.Scm.Cron.Upgrade.MaxOpenUpgrades)
	require.Nil(t, got.Spec.UpgradePolicy,
		"an omitted upgradePolicy stays absent: a default inside a nil struct pointer is never applied, "+
			"which is why the goal renderer resolves the default-off shape itself")
}
