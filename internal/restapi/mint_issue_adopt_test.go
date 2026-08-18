package restapi_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/queue"
)

// mirrorStubIssue is EXACTLY what controller.ensureIssueCR writes: the natural
// key, no owner, no Status at all, and - the whole point - no proposal anchor.
// Every non-filing minter in the operator produces this shape.
func mirrorStubIssue(repo string, number int) *tatarav1alpha1.Issue {
	return &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.IssueName(repo, number), Namespace: ns},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo, Number: number, ProjectRef: "tatara",
			URL: "https://example.invalid/i/" + strconv.Itoa(number),
		},
	}
}

// THE BLEED. mintIssueCR is the ONLY writer of Spec.ProposalBodyHash, and it
// wrote it only on a Create that WON. The forge issue is posted first and the CR
// is written second, so anything that mints the mirror stub for that number in
// between - the gate Task's own triage (task_stage.go mintIssueCRs, which fires
// the moment mintGateTask creates the Task, one statement before this call), a
// sweep pass, a repairIssueBinding - took the natural key, and the Create came
// back AlreadyExists. The filing then FAILED, and the stub it collided with
// stayed anchorless FOREVER: nothing else ever writes the anchor, and nothing
// may back-fill it from the mirrored body, because a body anchored to itself is
// no tamper guard at all. autoApproveApplies fails closed on an empty anchor, so
// that proposal can never be auto-approved - it parks awaiting a human until
// someone notices.
//
// The repair has to happen HERE, at filing time, because this is the only moment
// the operator still holds the body it POSTED (not the body the forge reports
// back). Adopting the stub with that body keeps the guard exactly as strong: a
// later forge-side edit still diverges from it.
func TestMintIssueCR_IncidentAdoptsAMirrorStubAndStillAnchors(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"),
		mirrorStubIssue("tatara-operator", 101))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"real outage",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace"}}}`)
	require.Equal(t, http.StatusOK, w.Code,
		"a mirror stub that beat the filing must not fail the filing")

	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	require.Equal(t, tatarav1alpha1.ProposalKindIncident, iss.Spec.ProposalKind,
		"the adopted stub must carry the durable provenance")
	require.NotEmpty(t, iss.Spec.ProposalBodyHash, "the adopted stub must carry the anchor")
	require.True(t, tatarav1alpha1.ProposalBodyMatchesAnchor(iss.Status.Body, iss.Spec.ProposalBodyHash),
		"the anchor must be the body the operator POSTED, so autoApproveApplies can grant")
	require.Equal(t, "t1", iss.OwnerReferences[0].Name)
	require.True(t, *iss.OwnerReferences[0].Controller,
		"an unowned stub is adopted by the filing Task, exactly as a fresh mint would be")
	require.Contains(t, e.task(t, "t1").Status.IssueRefs, iss.Name)
}

func TestMintIssueCR_BrainstormAdoptsAMirrorStubAndStillAnchors(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm"),
		mirrorStubIssue("tatara-operator", 101))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"brainstorm","payload":{
	  "action":"propose","proposals":[
	    {"repo":"tatara-operator","title":"one","body":"do the thing","kind":"bug"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	require.Equal(t, tatarav1alpha1.ProposalKindBrainstorm, iss.Spec.ProposalKind)
	require.True(t, tatarav1alpha1.ProposalBodyMatchesAnchor(iss.Status.Body, iss.Spec.ProposalBodyHash),
		"the proposal a maintainer must approve is auto-approvable, not permanently stuck")
	require.Equal(t, "new", iss.Status.Status,
		"an adopted stub is seeded exactly like a fresh mint: untriaged")
}

// The incident tracker's rule-key label (O4/O5) is what the suppression lookup
// and the human-visible forge recovery index both read. A mirror stub never
// carries it, so adopting one has to stamp it or the adopted tracker is invisible
// to dedup - a silent duplicate-incident source.
func TestMintIssueCR_AdoptStampsTheIncidentRuleKeyLabel(t *testing.T) {
	task := taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident")
	task.Spec.DedupKey = "abc123def4567890" //gitleaks:allow // test fixture, not a secret
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		task, mirrorStubIssue("tatara-operator", 101))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"real outage",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace"}}}`)
	require.Equal(t, http.StatusOK, w.Code)

	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	require.Equal(t, "abc123def4567890", iss.Labels[queue.LabelAlertRuleKey])
}

// THE ADVERSARIAL HALF. The adopt branch must widen NOTHING.
//
// The generic issue_write action=create path declares no proposal kind on
// purpose (its body is agent-written, a prompt-injection surface). Colliding
// with a mirror stub must not become the back door that stamps an anchor the
// direct path refuses.
func TestMintIssueCR_AdoptNeverStampsProvenanceTheCallerDidNotDeclare(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
		mirrorStubIssue("tatara-operator", 101))

	body := tatarav1alpha1.StampProposalMarker("B", tatarav1alpha1.ProposalKindBrainstorm)
	w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
		`{"task":"t1","action":"create","repo":"tatara-operator","title":"T","body":`+
			strconv.Quote(body)+`}`)
	require.Equal(t, http.StatusOK, w.Code)

	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	require.Empty(t, iss.Spec.ProposalKind,
		"an agent-supplied body must never stamp provenance, adopt path included")
	require.Empty(t, iss.Spec.ProposalBodyHash,
		"and must never mint the auto-approve anchor either")
}

// An anchor already on the CR is the record of an EARLIER filing. The adopt
// branch must never overwrite it: re-anchoring to a body posted later is exactly
// the "anchor the body to itself" move that deletes the tamper guard.
func TestMintIssueCR_AdoptNeverOverwritesAnExistingAnchor(t *testing.T) {
	stub := mirrorStubIssue("tatara-operator", 101)
	stub.Spec.ProposalKind = tatarav1alpha1.ProposalKindBrainstorm
	stub.Spec.ProposalBodyHash = tatarav1alpha1.ComputeProposalContentHash("the body that was actually filed")

	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"), stub)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"real outage",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace"}}}`)
	require.Equal(t, http.StatusOK, w.Code)

	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	require.Equal(t, tatarav1alpha1.ComputeProposalContentHash("the body that was actually filed"),
		iss.Spec.ProposalBodyHash, "the incumbent anchor is the integrity record and is immutable")
	require.Equal(t, tatarav1alpha1.ProposalKindBrainstorm, iss.Spec.ProposalKind)
}

// A stub some OTHER Task already controls is live work. The filing stamps the
// anchor (it is the truth about the body it just posted, whoever owns the
// mirror) and then REFUSES, exactly as the pre-adopt code refused - it must
// never steal a controller ref.
func TestMintIssueCR_AdoptNeverStealsAForeignControllerRef(t *testing.T) {
	stub := mirrorStubIssue("tatara-operator", 101)
	yes := true
	stub.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: tatarav1alpha1.GroupVersion.String(), Kind: "Task",
		Name: "someone-else", UID: "uid-other", Controller: &yes,
	}}

	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"), stub)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"real outage",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace"}}}`)
	require.NotEqual(t, http.StatusOK, w.Code, "a foreign-owned mirror is not ours to take")

	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	ctrl := 0
	for _, r := range iss.OwnerReferences {
		if r.Controller != nil && *r.Controller {
			ctrl++
			require.Equal(t, "someone-else", r.Name)
		}
	}
	require.Equal(t, 1, ctrl, "exactly one controller ref, and not ours")
}
