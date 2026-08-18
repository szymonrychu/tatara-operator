package restapi_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// THE EVIDENCE HAS TO BE CAPTURED AT DECLINE TIME. A decline is either a verdict
// on the CHANGE (permanent) or a verdict on the INFRASTRUCTURE (transient), and
// nothing about parked(implement-declined) tells the two apart afterwards. The
// only honest discriminator is what CI read at the moment the agent gave up, so
// the decline records it.
func TestOutcome_Implement_DeclineStampsTheCIEvidence(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1", func(mr *tatarav1alpha1.MergeRequest) {
			mr.Status.Ownership = tatarav1alpha1.OwnershipTatara
			mr.Status.CIStatus = "red"
			mr.Status.HeadSHA = "4c11cad2"
		}))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"declined","reason":"cannot submit: ci is red at head 4c11cad2"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.CIEvidenceRed, got.Annotations[tatarav1alpha1.AnnDeclineCI])
	require.Equal(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)+"@4c11cad2",
		got.Annotations[tatarav1alpha1.AnnDeclineHeads])
}

// A decline made while CI was GREEN records exactly that, and it is what keeps
// helmfile!1407 - declined because main had moved past it and the merge request
// carried zero net change - permanent forever.
func TestOutcome_Implement_DeclineOnTheMeritsRecordsGreen(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1", func(mr *tatarav1alpha1.MergeRequest) {
			mr.Status.Ownership = tatarav1alpha1.OwnershipTatara
			mr.Status.CIStatus = "green"
			mr.Status.HeadSHA = "4c11cad2"
		}))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"declined","reason":"main already carries this change"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.CIEvidenceGreen, got.Annotations[tatarav1alpha1.AnnDeclineCI])
}

// A STALE red must never outlive the world that produced it. The same Task can
// decline twice; if the second decline is on the merits the first decline's red
// would otherwise still be sitting there, and the driver would read it as a
// licence to re-drive a settled decision.
func TestOutcome_Implement_ASecondDeclineOverwritesTheEvidence(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1", func(mr *tatarav1alpha1.MergeRequest) {
			mr.Status.Ownership = tatarav1alpha1.OwnershipExternal
			mr.Status.CIStatus = "green"
			mr.Status.HeadSHA = "4c11cad2"
		}))
	// Seed the annotations as an earlier, CI-blocked decline would have left them.
	tk := e.task(t, "t1")
	tk.Annotations = map[string]string{
		tatarav1alpha1.AnnDeclineCI:    tatarav1alpha1.CIEvidenceRed,
		tatarav1alpha1.AnnDeclineHeads: tatarav1alpha1.MergeRequestName("tatara-operator", 295) + "@4c11cad2",
	}
	require.NoError(t, e.c.Update(context.Background(), tk))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"declined","reason":"a maintainer took this branch back"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	got := e.task(t, "t1")
	require.NotContains(t, got.Annotations, tatarav1alpha1.AnnDeclineCI,
		"no merge request qualifies any more, so the stale evidence is REMOVED, not left to rot")
	require.NotContains(t, got.Annotations, tatarav1alpha1.AnnDeclineHeads)
}

// DISCUSS IS THE SAME AMBIGUITY WEARING A DIFFERENT VERDICT - but only when the
// agent actually hit the wall. discuss parks awaiting-human, which the CI
// recovery driver accepts, so stamping it off the MIRROR alone made every
// genuine question asked while an unrelated flake happened to be red into a
// licence to un-park a Task with the human's answer still outstanding. Asking a
// question is NOT giving up on a red pipeline, and nothing in the mirror can
// tell the two apart.
func TestDiscussAloneNeverStampsTheCIEvidence(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1", func(mr *tatarav1alpha1.MergeRequest) {
			mr.Status.Ownership = tatarav1alpha1.OwnershipTatara
			mr.Status.CIStatus = "red"
			mr.Status.HeadSHA = "4c11cad2"
		}))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"discuss","reason":"which of the two migrations do you want"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	got := e.task(t, "t1")
	require.NotContains(t, got.Annotations, tatarav1alpha1.AnnDeclineCI,
		"a question a human owes an answer to must never be auto-un-parked by the ci recovery")
	require.NotContains(t, got.Annotations, tatarav1alpha1.AnnDeclineHeads)
}

// WHAT DOES record it is the refusal itself: the operator's own readiness gate
// answering 409 pr-not-ready on the ci-red axis IS the "I could not hand this
// on because the infrastructure said no" fact the recovery is keyed to. A
// discuss (or a decline) that follows it inherits the evidence unchanged.
func TestACIRedRefusalRecordsTheEvidenceAndADiscussInheritsIt(t *testing.T) {
	forge := newRecordingForge()
	forge.heads["295"] = "4c11cad2"
	forge.ciStatuses[295] = "failure"
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1", func(mr *tatarav1alpha1.MergeRequest) {
			mr.Status.Ownership = tatarav1alpha1.OwnershipTatara
			mr.Status.CIStatus = "red"
			mr.Status.HeadSHA = "4c11cad2"
		}))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", submitImplement)
	require.Equal(t, http.StatusConflict, w.Code, w.Body.String())

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.CIEvidenceRed, got.Annotations[tatarav1alpha1.AnnDeclineCI])
	require.Equal(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)+"@4c11cad2",
		got.Annotations[tatarav1alpha1.AnnDeclineHeads])

	w = e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"discuss","reason":"ci is red at 4c11cad2 and it is not my change"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	got = e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.CIEvidenceRed, got.Annotations[tatarav1alpha1.AnnDeclineCI],
		"the refusal is the writer; the give-up that follows it neither re-derives nor drops it")
}

// AND A CLEAN READINESS PASS VOIDS IT. Evidence that outlives the refusal it
// describes is a licence an unrelated later park inherits - the shape the whole
// narrowing exists to prevent.
func TestACleanSubmissionClearsStaleCIRefusalEvidence(t *testing.T) {
	forge := newRecordingForge()
	forge.heads["295"] = "4c11cad2"
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1", func(mr *tatarav1alpha1.MergeRequest) {
			mr.Status.Ownership = tatarav1alpha1.OwnershipTatara
			mr.Status.HeadSHA = "4c11cad2"
		}))
	tk := e.task(t, "t1")
	tk.Annotations = map[string]string{
		tatarav1alpha1.AnnDeclineCI:    tatarav1alpha1.CIEvidenceRed,
		tatarav1alpha1.AnnDeclineHeads: tatarav1alpha1.MergeRequestName("tatara-operator", 295) + "@4c11cad2",
	}
	require.NoError(t, e.c.Update(context.Background(), tk))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", submitImplement)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	got := e.task(t, "t1")
	require.NotContains(t, got.Annotations, tatarav1alpha1.AnnDeclineCI)
	require.NotContains(t, got.Annotations, tatarav1alpha1.AnnDeclineHeads)
}

// A Task that owns no merge request at all - the ordinary "I looked and there is
// nothing to do" decline - records nothing and is therefore never re-driven.
func TestOutcome_Implement_DeclineWithNoOwnedMRStampsNothing(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"declined","reason":"the issue is already fixed"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	got := e.task(t, "t1")
	require.NotContains(t, got.Annotations, tatarav1alpha1.AnnDeclineCI)
	require.NotContains(t, got.Annotations, tatarav1alpha1.AnnDeclineHeads)
}
