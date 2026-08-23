package restapi_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// #639's SECOND hole: mrOpen and submit_outcome(action=submitted) never read
// Issue.status.approval, so an implement agent that skipped the gate entirely
// still shipped. These are the two reads.

// unapprovedIssue is issueV2's default shape spelled out: live, no evidence.
func unapprovedIssue(repo string, number int, owner string) *tatarav1alpha1.Issue {
	return issueV2(repo, number, owner)
}

func maintainerSpoke(i *tatarav1alpha1.Issue) {
	i.Status.Comments = append(i.Status.Comments, tatarav1alpha1.Comment{
		ExternalID: "c1", Author: "szymonrychu", Body: "go ahead",
		CreatedAt: metav1.NewTime(frozenNow),
	})
}

func maintainerProjectV2(name string) *tatarav1alpha1.Project {
	p := projectV2(name)
	p.Spec.Scm.MaintainerLogins = []string{"szymonrychu"}
	return p
}

func decodeApprovalRefusal(t *testing.T, body []byte) struct {
	Error   string `json:"error"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
	Blocked []struct {
		Repo     string `json:"repo"`
		Number   int    `json:"number"`
		Detail   string `json:"detail"`
		Guidance string `json:"guidance"`
	} `json:"blocked"`
} {
	t.Helper()
	var got struct {
		Error   string `json:"error"`
		Reason  string `json:"reason"`
		Message string `json:"message"`
		Blocked []struct {
			Repo     string `json:"repo"`
			Number   int    `json:"number"`
			Detail   string `json:"detail"`
			Guidance string `json:"guidance"`
		} `json:"blocked"`
	}
	require.NoError(t, json.Unmarshal(body, &got))
	return got
}

// TestMROpen_RefusedWhileTheIssueIsUnapproved is the headline: no PR can carry
// work the gate has not released, which is what makes "work before the gate is
// LOST" true rather than an instruction.
func TestMROpen_RefusedWhileTheIssueIsUnapproved(t *testing.T) {
	before := testutil.ToFloat64(obs.RestOwnershipRefusedTotal.WithLabelValues("approval-required"))
	e := buildV2(t, v2Opts{writer: panicForge{}}, maintainerProjectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		unapprovedIssue("tatara-cli", 32, "t1"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-cli","title":"T","body":"B"}`)

	require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
	got := decodeApprovalRefusal(t, w.Body.Bytes())
	require.Equal(t, "approval-required", got.Reason)
	require.NotEmpty(t, got.Error, "the generic error key must be present for a client that does not know this reason")
	require.Len(t, got.Blocked, 1)
	require.Equal(t, "tatara-cli", got.Blocked[0].Repo)
	require.Equal(t, 32, got.Blocked[0].Number)
	require.Equal(t, controller.ShipBlockedNeedsMaintainerComment, got.Blocked[0].Detail)
	require.NotEmpty(t, got.Blocked[0].Guidance, "#639: every refusal must guide the agent to the next step")
	require.Equal(t, before+1,
		testutil.ToFloat64(obs.RestOwnershipRefusedTotal.WithLabelValues("approval-required")))
}

// The two no-evidence details differ, because the remedies differ.
func TestMROpen_RefusalNamesTheApprovalToolOnceAMaintainerHasSpoken(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, maintainerProjectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		issueV2("tatara-cli", 32, "t1", maintainerSpoke))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-cli","title":"T","body":"B"}`)

	require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
	got := decodeApprovalRefusal(t, w.Body.Bytes())
	require.Equal(t, controller.ShipBlockedNeedsApprovalTool, got.Blocked[0].Detail)
	require.Contains(t, got.Blocked[0].Guidance, "action=approved")
}

// An APPROVED issue opens exactly as before. This is the no-behaviour-change pin
// for the ordinary path.
func TestMROpen_ApprovedIssueOpensUnchanged(t *testing.T) {
	forge := newRecordingForge()
	e := buildV2(t, v2Opts{writer: forge}, maintainerProjectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		approvedIssueV2("tatara-cli", 32, "t1"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-cli","title":"T","body":"B"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, forge.openedURLs, 1)
}

// A takeover Task owns no Issue and was authorised at the takeover endpoint; an
// adopted upgrade Task owns an MR and no Issue. Neither may be gated here.
func TestMROpen_TaskOwningNoIssueStaysUngated(t *testing.T) {
	forge := newRecordingForge()
	e := buildV2(t, v2Opts{writer: forge}, maintainerProjectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-cli","title":"T","body":"B"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, forge.openedURLs, 1)
}

// The IDEMPOTENT answer is a READ of an MR that already exists, not a new open,
// and it must survive the gate: a TTL-stopped pod resuming needs the number of
// the MR it already has, and refusing it strands the Task with no way to learn
// it. Nothing new reaches the forge on that path.
func TestMROpen_IdempotentAnswerSurvivesTheApprovalGate(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, maintainerProjectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-cli", 80, "t1"),
		unapprovedIssue("tatara-cli", 32, "t1"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-cli","title":"T","body":"B"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"existing":true`)
}

// Only the implement agent meets this gate. A review or documentation pod
// reaching mr_write has a different mandate and no approval of its own to carry.
func TestMROpen_OnlyTheImplementAgentIsGated(t *testing.T) {
	forge := newRecordingForge()
	e := buildV2(t, v2Opts{writer: forge}, maintainerProjectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "documentation", tatarav1alpha1.StateUnderImplementation, "documentation"),
		unapprovedIssue("tatara-cli", 32, "t1"))

	w := e.do(t, http.MethodPost, "/projects/tatara/scm/mr-write",
		`{"task":"t1","action":"open","repo":"tatara-cli","title":"T","body":"B"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, forge.openedURLs, 1)
}

// TestOutcome_Submitted_RefusedWhileTheIssueIsUnapproved closes the other half:
// an agent whose MR predates this change, or that opened one through some other
// path, must not be able to hand it to a reviewer either.
func TestOutcome_Submitted_RefusedWhileTheIssueIsUnapproved(t *testing.T) {
	e := buildV2(t, v2Opts{}, maintainerProjectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-cli", 80, "t1"),
		unapprovedIssue("tatara-cli", 32, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"patch"}}`)

	require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
	got := decodeApprovalRefusal(t, w.Body.Bytes())
	require.Equal(t, "approval-required", got.Reason)
	require.Len(t, got.Blocked, 1)
	require.Equal(t, controller.ShipBlockedNeedsMaintainerComment, got.Blocked[0].Detail)

	// The Task did NOT transition: a refused submit leaves the work exactly
	// where it was, with the pod alive.
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, e.task(t, "t1").Status.State)
}

// THE CEILING BITES HERE AND NOWHERE ELSE. An auto-approved issue's grant is
// provisional; the declared change_significance is the value it is settled
// against, and it does not exist until this call.
func TestOutcome_Submitted_AutoApprovedIsCappedByTheProjectCeiling(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ceiling string
		sig     string
		refused bool
	}{
		{"minor ceiling admits a patch", "minor", "patch", false},
		{"minor ceiling admits a minor", "minor", "minor", false},
		{"minor ceiling refuses a major", "minor", "major", true},
		{"major ceiling admits a major", "major", "major", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proj := maintainerProjectV2("tatara")
			proj.Spec.AutoApproveMaxSignificance = tc.ceiling
			e := buildV2(t, v2Opts{}, proj, scmSecretV2(),
				repoV2("tatara-cli", "tatara"),
				taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
				mrV2("tatara-cli", 80, "t1"),
				autoApprovedIssueV2("tatara-cli", 32, "t1"))

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"`+tc.sig+`"}}`)

			if !tc.refused {
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				return
			}
			require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
			got := decodeApprovalRefusal(t, w.Body.Bytes())
			require.Equal(t, controller.ShipBlockedOverCeiling, got.Blocked[0].Detail)
			require.Contains(t, got.Blocked[0].Guidance, "autoApproveMaxSignificance")
		})
	}
}

// A HUMAN-CITED approval is never capped. The ceiling bounds what tatara
// approves for itself; a maintainer who read the plan already made that call.
func TestOutcome_Submitted_HumanCitedApprovalIsNeverCapped(t *testing.T) {
	proj := maintainerProjectV2("tatara")
	proj.Spec.AutoApproveMaxSignificance = tatarav1alpha1.AutoApproveOff
	e := buildV2(t, v2Opts{}, proj, scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-cli", 80, "t1"),
		approvedIssueV2("tatara-cli", 32, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"major"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
}

// action=declined must NOT be gated. An agent that cannot get approval has to be
// able to say so and terminate; refusing the decline wedges the Task at
// no-outcome instead.
func TestOutcome_Declined_IsNotGatedOnApproval(t *testing.T) {
	e := buildV2(t, v2Opts{}, maintainerProjectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		unapprovedIssue("tatara-cli", 32, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"declined","reason":"no approval and none coming"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
}

// TestGate_RefusalCarriesGuidance and its grant twin are #639's "all tool
// outputs must guide the agent" clause, at the gate rather than the ship. The
// reason constant is a Prometheus label; it names the fault and says nothing
// about the remedy, and the remedies genuinely differ per reason.
func TestGate_RefusalCarriesGuidance(t *testing.T) {
	e := buildV2(t, v2Opts{approval: &fakeApproval{}}, maintainerProjectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined),
		unapprovedIssue("tatara-cli", 32, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"r","planNoteId":"`+gatePlanNoteID+`"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var got gateResponseDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.False(t, got.Granted)
	require.Equal(t, controller.ApprovalRefusedNoMaintainer, got.Reason)
	require.Equal(t, controller.ApprovalRefusalGuidance(got.Reason), got.Guidance)
	require.NotEmpty(t, got.Guidance)
}

// The GRANT is the answer that says "now go", and until #639 it said nothing at
// all - no `granted` key, so an agent following the documented contract read a
// missing field as false.
func TestGate_GrantCarriesGrantedAndGuidance(t *testing.T) {
	iss := issueV2("tatara-cli", 32, "t1", maintainerSpoke)
	e := buildV2(t, v2Opts{approval: &fakeApproval{grant: map[string]bool{iss.Name: true}}},
		maintainerProjectV2("tatara"), scmSecretV2(),
		repoV2("tatara-cli", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined), iss)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"r","approvingMaintainer":"szymonrychu",`+
			`"planNoteId":"`+gatePlanNoteID+`","approvalCitations":[{"id":"c1","quote":"go ahead"}]}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var got struct {
		Granted  bool   `json:"granted"`
		Guidance string `json:"guidance"`
		Task     struct {
			Name string `json:"name"`
		} `json:"task"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.True(t, got.Granted, "the grant must say so in the field the skills tell the agent to read")
	require.Equal(t, controller.ApprovalGrantGuidance(), got.Guidance)
	require.Equal(t, "t1", got.Task.Name, "the Task DTO is preserved verbatim under `task`")
}
