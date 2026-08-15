package webhook

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// engineProject is peProject armed with an upgrade engine running under its OWN
// forge account, which is the deployment shape this predicate exists for.
func engineProject(engineLogins ...string) *tatarav1.Project {
	p := peProject("tatara-bot", "maintainer")
	p.Spec.UpgradePolicy = &tatarav1.UpgradePolicySpec{
		Engine: "renovate", AdoptBranchPrefix: "renovate/", UpgradeEngineLogins: engineLogins,
	}
	return p
}

// syncFrom drives one mr/synchronize delivery from actor and returns the mirror.
func syncFrom(t *testing.T, proj *tatarav1.Project, number int, actor, headSHA string) *tatarav1.MergeRequest {
	t.Helper()
	task := peTask("t-adopted", tatarav1.StateAwaitingReview, "", tatarav1.IssueName("pe-repo", 7))
	task.Status.MRRefs = []string{tatarav1.MergeRequestName("pe-repo", number)}
	mr := peMR(number, task, tatarav1.MergeRequestStatus{
		State: "open", HeadSHA: "oldhead", LastBotHeadSHA: "old-bot-head", HeadBranch: "renovate/cilium"})
	c := peClient(t, proj, peRepo(), task, mr)
	s := peServer(c, &stubSpiller{}, nil)

	ev := scm.WebhookEvent{Kind: "mr", IsPR: true, Action: "synchronize", Number: number,
		Repo: peRepoURL, HeadSHA: headSHA, ActorLogin: actor}
	s.handleMRSynchronize(context.Background(), httptest.NewRecorder(), "github", *proj, ev)

	var got tatarav1.MergeRequest
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: peNS, Name: mr.Name}, &got))
	return &got
}

// The engine pushed. Its own rebase is not a human taking the branch back, so
// the baseline advances and the drift check never fires.
func TestHandleMRSynchronize_AnAllowlistedEngineActorAdvancesTheBotHead(t *testing.T) {
	got := syncFrom(t, engineProject("renovate-bot"), 40, "renovate-bot", "engine-head-abc")
	require.Equal(t, "engine-head-abc", got.Status.HeadSHA)
	require.Equal(t, "engine-head-abc", got.Status.LastBotHeadSHA,
		"an allowlisted upgrade engine's own push must re-anchor the baseline, not stand the merge request down")
}

// THE SAFETY TEST, AND IT IS THE POINT OF THE WHOLE TASK. A human push must
// still leave LastBotHeadSHA stale, so ReconcileOwnership sees the drift and
// stands the merge request down. This is what hands a merge request back to a
// human and it must not be weakened.
func TestHandleMRSynchronize_AHumanActorStillLeavesTheBotHeadStale(t *testing.T) {
	proj := engineProject("renovate-bot")
	for _, tc := range []struct {
		name  string
		actor string
	}{
		// The MAINTAINER is the hardest case: he is trusted for every OTHER
		// purpose in this project.
		{"the project maintainer", "maintainer"},
		{"an unknown login", "some-contributor"},
		{"an EMPTY actor - unattributable is not attributable", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := syncFrom(t, proj, 41, tc.actor, "human-head-xyz")
			require.Equal(t, "human-head-xyz", got.Status.HeadSHA, "the HeadSHA mirror still advances")
			require.Equal(t, "old-bot-head", got.Status.LastBotHeadSHA,
				"%s must leave the baseline stale so ReconcileOwnership flips", tc.name)
		})
	}
}

// The bot itself is unchanged. This passes BEFORE the change and exists to keep
// doing so.
func TestHandleMRSynchronize_TheBotStillAdvancesTheBotHead(t *testing.T) {
	got := syncFrom(t, engineProject(), 42, "tatara-bot", "bot-head-abc")
	require.Equal(t, "bot-head-abc", got.Status.LastBotHeadSHA)
}

// The seven ignore-callers must not move. isBotActor is the loop-breaker that
// stops tatara reacting to its own comments, labels and issues; an upgrade
// engine's comments and labels are things tatara should still SEE.
func TestIsBotActor_IsNotWidenedByTheEngineAllowlist(t *testing.T) {
	proj := engineProject("renovate-bot")
	require.False(t, isBotActor(proj, "renovate-bot"),
		"widening isBotActor would make tatara deaf to the engine's comments everywhere")
	require.True(t, isUpgradeEngineActor(proj, "renovate-bot"))
	require.True(t, isUpgradeEngineActor(proj, "tatara-bot"))
	require.False(t, isUpgradeEngineActor(proj, ""))
	require.False(t, isUpgradeEngineActor(proj, "maintainer"))
	// No policy at all reduces the predicate exactly to isBotActor.
	bare := peProject("tatara-bot", "maintainer")
	require.True(t, isUpgradeEngineActor(bare, "tatara-bot"))
	require.False(t, isUpgradeEngineActor(bare, "renovate-bot"))
}
