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

// Adoption is OFF by default. The CRD default is deliberately EMPTY, not
// "renovate/": a structural-schema default is applied on EVERY write, so a
// non-empty default would arm adoption on whichever unrelated helm apply
// happens next, at a moment nobody chose. It is armed explicitly in the
// enrollment values instead, exactly like maxOpenUpgrades.
func TestProjectCRD_AdoptBranchPrefixDefaultsToEmpty(t *testing.T) {
	ctx := context.Background()
	proj := upgradeProject(t, "p-adopt-default")
	proj.Spec.UpgradePolicy = &tataradevv1alpha1.UpgradePolicySpec{Engine: "renovate"}
	require.NoError(t, k8sClient.Create(ctx, proj))

	var got tataradevv1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(proj), &got))
	require.Empty(t, got.Spec.UpgradePolicy.AdoptBranchPrefix,
		"adoptBranchPrefix must default to empty: adoption is off until an enrollment value arms it")
}

func TestProjectCRD_AcceptsAdoptBranchPrefix(t *testing.T) {
	ctx := context.Background()
	proj := upgradeProject(t, "p-adopt-set")
	proj.Spec.UpgradePolicy = &tataradevv1alpha1.UpgradePolicySpec{
		Engine: "renovate", AdoptBranchPrefix: "renovate/",
	}
	require.NoError(t, k8sClient.Create(ctx, proj))

	var got tataradevv1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(proj), &got))
	require.Equal(t, "renovate/", got.Spec.UpgradePolicy.AdoptBranchPrefix)
}

// The trailing slash is REQUIRED. Without it a prefix of "renovate" would also
// match a human branch called renovate-experiment, and adoption would take a
// merge request nobody meant it to have.
func TestProjectCRD_RejectsAPrefixWithNoTrailingSlash(t *testing.T) {
	ctx := context.Background()
	proj := upgradeProject(t, "p-adopt-noslash")
	proj.Spec.UpgradePolicy = &tataradevv1alpha1.UpgradePolicySpec{AdoptBranchPrefix: "renovate"}
	require.Error(t, k8sClient.Create(ctx, proj),
		"a prefix with no trailing slash must be rejected by the pattern")
}

// The allowlist is OPTIONAL and defaults EMPTY. On project-infrastructure it
// STAYS empty: Renovate runs with the platform bot's own token, so its merge
// requests and its pushes are already botLogin and need no allowlisting. The
// field exists for the deployment shape where the engine has its own account.
func TestProjectCRD_UpgradeEngineLoginsDefaultsToEmpty(t *testing.T) {
	ctx := context.Background()
	proj := upgradeProject(t, "p-engine-default")
	proj.Spec.UpgradePolicy = &tataradevv1alpha1.UpgradePolicySpec{
		Engine: "renovate", AdoptBranchPrefix: "renovate/",
	}
	require.NoError(t, k8sClient.Create(ctx, proj))

	var got tataradevv1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(proj), &got))
	require.Empty(t, got.Spec.UpgradePolicy.UpgradeEngineLogins)
}

func TestProjectCRD_AcceptsUpgradeEngineLogins(t *testing.T) {
	ctx := context.Background()
	proj := upgradeProject(t, "p-engine-set")
	proj.Spec.UpgradePolicy = &tataradevv1alpha1.UpgradePolicySpec{
		AdoptBranchPrefix:   "renovate/",
		UpgradeEngineLogins: []string{"renovate-bot"},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))

	var got tataradevv1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(proj), &got))
	require.Equal(t, []string{"renovate-bot"}, got.Spec.UpgradePolicy.UpgradeEngineLogins)
}

// THE TWO AUTHOR ENUMS THE PLAN AND THE DESIGN BOTH MISSED, and they are
// fail-closed at the API SERVER rather than in Go, so nothing in the operator
// would have caught them.
//
//   - Task.status.notes[].agent is written verbatim from the outcome's kind
//     (restapi/outcome.go agentNote). The FIRST upgrade outcome would have had
//     its status write rejected - and, because appending a note revalidates its
//     siblings, the Task would then error on every subsequent write.
//   - Issue/MergeRequest .status.comments[].agentKind is written from the owning
//     Task's status.agentKind (controller/reviewpost.go pendingCommentAgentKind),
//     so an upgrade Task draining a pending comment would be rejected the same
//     way.
//
// A new agent kind must reach BOTH, not just the six maps the plan listed.
func TestUpgradeIsAValidNoteAuthorAndCommentAgentKind(t *testing.T) {
	ctx := context.Background()

	tk := &tataradevv1alpha1.Task{}
	tk.Name = "upgrade-note-author"
	tk.Namespace = testNS
	tk.Spec = tataradevv1alpha1.TaskSpec{ProjectRef: "p", Kind: "upgrade", Goal: "g"}
	require.NoError(t, k8sClient.Create(ctx, tk))
	tk.Status.AgentKind = "upgrade"
	tk.Status.Notes = []tataradevv1alpha1.Note{{
		At: metav1.Now(), Agent: "upgrade", Kind: "note", Body: "submitted: cilium 1.16 -> 1.17",
	}}
	require.NoError(t, k8sClient.Status().Update(ctx, tk),
		"status.notes[].agent=upgrade must be admitted or the first upgrade outcome wedges the Task")

	mr := &tataradevv1alpha1.MergeRequest{}
	mr.Name = "upgrade-comment-author"
	mr.Namespace = testNS
	mr.Spec = tataradevv1alpha1.MergeRequestSpec{
		ProjectRef: "p", RepositoryRef: "charts", Number: 41,
	}
	require.NoError(t, k8sClient.Create(ctx, mr))
	mr.Status.Comments = []tataradevv1alpha1.Comment{{
		ExternalID: "c1", Author: "tatara-bot", AgentKind: "upgrade", Body: "b",
		CreatedAt: metav1.Now(),
	}}
	require.NoError(t, k8sClient.Status().Update(ctx, mr),
		"status.comments[].agentKind=upgrade must be admitted or the pending-comment drain is rejected")
}

// upgradeEngineLogins WIDENS MERGE PERMISSION, and it must not be armable on its
// own. Setting it makes ownershipForAuthor classify that login's merge requests
// `tatara` (which the merge corridor merges) and makes isUpgradeEngineActor
// re-anchor that login's pushes instead of standing the merge request down -
// both live the moment the field is written, and both pointless without
// adoption. The CEL rule ties them together at admission.
func TestUpgradePolicy_EngineLoginsRequireTheAdoptionPrefix(t *testing.T) {
	ctx := context.Background()
	proj, _ := seedProjectRepo(t, ctx)

	proj.Spec.UpgradePolicy = &tataradevv1alpha1.UpgradePolicySpec{
		UpgradeEngineLogins: []string{"renovate-bot"},
	}
	if err := k8sClient.Update(ctx, proj); err == nil {
		t.Fatal("the API server accepted upgradeEngineLogins with no adoptBranchPrefix: " +
			"merge ownership is widened for an account with adoption switched off")
	}

	proj.Spec.UpgradePolicy.AdoptBranchPrefix = "renovate/"
	if err := k8sClient.Update(ctx, proj); err != nil {
		t.Fatalf("the pair must be accepted together: %v", err)
	}
	// And the field stays optional: adoption armed with the bot's own token
	// lists nobody.
	proj.Spec.UpgradePolicy.UpgradeEngineLogins = nil
	if err := k8sClient.Update(ctx, proj); err != nil {
		t.Fatalf("adoptBranchPrefix alone must stay legal: %v", err)
	}
}
