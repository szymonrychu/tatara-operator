package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// TestMaybeOpenFollowupIssue_SyncsSiblingLinks is FIX-5: appending the
// follow-up issue to DiscoveredIssues must also cross-link it against any
// existing sibling (syncSiblingLinks), mirroring completeProposal - not just
// the brainstorm path.
func TestMaybeOpenFollowupIssue_SyncsSiblingLinks(t *testing.T) {
	ctx := context.Background()
	name := "lc-followup-links"
	proj := "lc-followup-links-p"
	repo := "lc-followup-links-r"
	sec := "lc-followup-links-s"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#90",
		URL: "https://github.com/o/r/issues/90", Number: 90,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.PrURL = "https://github.com/o/r/pull/91"
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{RemainingScope: "logout endpoint"}
	// Seed one prior discovered issue so the follow-up create yields 2 siblings.
	task.Status.DiscoveredIssues = []string{"https://github.com/o/r/issues/7"}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &fakeProposalWriter{}
	reader := &fakeProposalReader{bodies: map[string]string{
		"o/r#7":  "first sibling body",
		"o/r#99": "second sibling body", // fakeProposalWriter.CreateIssue always lands the new issue at o/r#99
	}}
	r := &TaskReconciler{
		Client:    k8sClient,
		Scheme:    k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return fw, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return reader, nil },
	}

	if err := r.maybeOpenFollowupIssue(ctx, task); err != nil {
		t.Fatalf("maybeOpenFollowupIssue: %v", err)
	}

	edits := fw.editCallsSnapshot()
	if len(edits) != 2 {
		t.Fatalf("want 2 sibling-link edits (both issues cross-linked), got %d: %+v", len(edits), edits)
	}
	found7, found99 := false, false
	for _, e := range edits {
		if e.number == 7 && strings.Contains(e.body, "https://github.com/o/r/issues/99") {
			found7 = true
		}
		if e.number == 99 && strings.Contains(e.body, "https://github.com/o/r/issues/7") {
			found99 = true
		}
	}
	if !found7 || !found99 {
		t.Errorf("sibling cross-links incomplete: %+v", edits)
	}
}

// TestMaybeOpenFollowupIssue_StampsProposedByMarker is FIX-7: the follow-up
// issue body must carry tataraProposedByMarker("followup") alongside
// tataraAuthoredMarker for provenance-consistent spec.
func TestMaybeOpenFollowupIssue_StampsProposedByMarker(t *testing.T) {
	ctx := context.Background()
	name := "lc-followup-marker"
	proj := "lc-followup-marker-p"
	repo := "lc-followup-marker-r"
	sec := "lc-followup-marker-s"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#90",
		URL: "https://github.com/o/r/issues/90", Number: 90,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.PrURL = "https://github.com/o/r/pull/91"
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{RemainingScope: "logout endpoint"}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &fakeProposalWriter{}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:  func(string) (scm.SCMWriter, error) { return fw, nil },
	}

	if err := r.maybeOpenFollowupIssue(ctx, task); err != nil {
		t.Fatalf("maybeOpenFollowupIssue: %v", err)
	}

	if !strings.Contains(fw.lastReq.Body, tataraAuthoredMarker) {
		t.Errorf("follow-up issue body = %q, want tataraAuthoredMarker", fw.lastReq.Body)
	}
	if !strings.Contains(fw.lastReq.Body, tataraProposedByMarker("followup")) {
		t.Errorf("follow-up issue body = %q, want tataraProposedByMarker(followup)", fw.lastReq.Body)
	}
}
