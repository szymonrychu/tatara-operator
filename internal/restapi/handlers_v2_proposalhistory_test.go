package restapi_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// brainstormHistoryProject is projectV2 plus a brainstorm cron block, which is
// what ProposalHistoryFor reads the history window off. projectV2 leaves
// Spec.Scm.Cron nil, which is exactly why every other v2 test still renders a
// bundle with no <proposal_history> element.
func brainstormHistoryProject(window int) *tatarav1alpha1.Project {
	p := projectV2("tatara")
	p.Spec.Scm.Cron = &tatarav1alpha1.ScmCron{
		Brainstorm: tatarav1alpha1.BrainstormActivity{Enabled: true, HistoryWindow: &window},
	}
	return p
}

// declinedProposalV2 is a killed proposal: its forge issue is CLOSED, so an
// open-issue scan cannot see it. It is UNOWNED, so it is not part of the Task's
// own <issue> set either - the history block is the only path to it.
func declinedProposalV2(number int) *tatarav1alpha1.Issue {
	at := metav1.NewTime(time.Now().Add(-24 * time.Hour))
	return &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.IssueName("tatara-operator", number), Namespace: ns,
		},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: "tatara-operator", Number: number, ProjectRef: "tatara",
			ProposalKind: tatarav1alpha1.ProposalKindBrainstorm,
		},
		Status: tatarav1alpha1.IssueStatus{
			Title: "rewrite the scheduler", Body: "a killed idea", Author: "tatara-bot",
			State: "closed", Status: "new", CreatedAt: &at,
			Comments: []tatarav1alpha1.Comment{{
				ExternalID: "c1", Author: "maintainer", Body: "we tried this in 2024",
				CreatedAt: at,
			}},
		},
	}
}

// TestTaskContext_BrainstormReReadCarriesTheProposalHistory is the REST half of
// the O8 contract. tatara-brainstorm-guardrails tells the brainstorm agent it
// may re-read its own bundle with task_context(task=<name>) mid-turn; if that
// re-read dropped the block, the project's declined proposals would silently
// vanish while the bundle's standing trailer still named the element - the exact
// re-propose-a-killed-idea failure the block exists to close.
func TestTaskContext_BrainstormReReadCarriesTheProposalHistory(t *testing.T) {
	e := buildV2(t, v2Opts{},
		brainstormHistoryProject(20), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm"),
		declinedProposalV2(291),
	)
	w := e.do(t, http.MethodGet, "/tasks/t1/context", "")
	require.Equal(t, http.StatusOK, w.Code)
	var out struct {
		Task, Bundle string
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Contains(t, out.Bundle, `<proposal_history count="1" total="1">`)
	require.Contains(t, out.Bundle, `<proposal repo="tatara-operator" number="291" status="declined"`)
	require.Contains(t, out.Bundle, `bot="false">we tried this in 2024</comment>`,
		"the maintainer's verdict is the load-bearing part of the block")
}

// TestTaskContext_NonBrainstormReReadOmitsTheProposalHistory keeps the block off
// every other agent's re-read, where it is pure byte cost.
func TestTaskContext_NonBrainstormReReadOmitsTheProposalHistory(t *testing.T) {
	e := buildV2(t, v2Opts{},
		brainstormHistoryProject(20), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"),
		declinedProposalV2(291),
	)
	w := e.do(t, http.MethodGet, "/tasks/t1/context", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.NotContains(t, w.Body.String(), "<proposal_history ")
}
