package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

func TestTataraProposedByKind_ParsesMarker(t *testing.T) {
	body := "some issue body\n\n<!-- tatara-proposed-by:incident -->"
	if got := tataraProposedByKind(body); got != "incident" {
		t.Fatalf("tataraProposedByKind() = %q, want %q", got, "incident")
	}
}

func TestTataraProposedByKind_NoMarker_ReturnsEmpty(t *testing.T) {
	if got := tataraProposedByKind("plain body, no marker"); got != "" {
		t.Fatalf("tataraProposedByKind() = %q, want empty", got)
	}
}

func TestIsTataraAuthored_StillMatchesKindBearingMarker(t *testing.T) {
	body := "some issue body\n\n" + tataraAuthoredMarker + "\n" + tataraProposedByMarker("brainstorm")
	if !strings.Contains(body, tataraAuthoredMarker) {
		t.Fatal("kind-bearing marker must not break the plain tataraAuthoredMarker substring check")
	}
	if tataraProposedByKind(body) != "brainstorm" {
		t.Fatal("both markers must coexist and each parse correctly")
	}
}

func TestCreateProposal_WritesKindBearingMarker(t *testing.T) {
	fw := &fakeProposalWriter{}
	r := newProposalReconciler(t, fw, nil)

	task := seedProposalTask(t, "prop-kind-bs", "prop-kind-bs-proj", "prop-kind-bs-repo", "prop-kind-bs-scm", "A brainstorm proposal")

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(context.Background(), &proj, task)
	require.NoError(t, err)

	require.Contains(t, fw.lastReq.Body, tataraAuthoredMarker)
	require.Contains(t, fw.lastReq.Body, tataraProposedByMarker("brainstorm"))
}

func TestCreateProposal_IncidentKind_WritesIncidentMarker(t *testing.T) {
	fw := &fakeProposalWriter{}
	r := newProposalReconciler(t, fw, nil)

	task := seedProposalTask(t, "prop-kind-inc", "prop-kind-inc-proj", "prop-kind-inc-repo", "prop-kind-inc-scm", "An incident proposal")
	task = markIncidentProposal(t, task, "deadbeefcafe1234")

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(context.Background(), &proj, task)
	require.NoError(t, err)

	require.Contains(t, fw.lastReq.Body, tataraProposedByMarker("incident"))
}
