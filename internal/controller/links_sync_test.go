// Copyright 2026 tatara authors.

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

// TestSyncAllSiblingLinksIfNeeded_PostsToEveryMember verifies item Request
// C/b: any Task whose issue-sibling union holds 2+ members gets the
// tatara-links comment posted/refreshed on every member, not only from the
// two legacy triggers (proposal completion, the removed implement follow-up).
func TestSyncAllSiblingLinksIfNeeded_PostsToEveryMember(t *testing.T) {
	ctx := context.Background()
	name := "links-multi"
	proj := "links-multi-p"
	repo := "links-multi-r"
	sec := "links-multi-s"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#5",
		URL: "https://github.com/o/r/issues/5", Number: 5,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 5, Kind: tatarav1alpha1.WorkItemIssue},
		{Provider: "github", Repo: "o/r", Number: 6, Kind: tatarav1alpha1.WorkItemIssue},
	}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &fakeProposalWriter{}
	reader := &fakeProposalReader{bodies: map[string]string{
		"o/r#5": "issue 5 body",
		"o/r#6": "issue 6 body",
	}}
	r := &TaskReconciler{
		Client:    k8sClient,
		Scheme:    k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return fw, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return reader, nil },
	}

	r.syncAllSiblingLinksIfNeeded(ctx, task)

	edits := fw.editCallsSnapshot()
	if len(edits) != 2 {
		t.Fatalf("want 2 sibling-link edits, got %d: %+v", len(edits), edits)
	}
	found5, found6 := false, false
	for _, e := range edits {
		if e.number == 5 && strings.Contains(e.body, "https://github.com/o/r/issues/6") {
			found5 = true
		}
		if e.number == 6 && strings.Contains(e.body, "https://github.com/o/r/issues/5") {
			found6 = true
		}
	}
	if !found5 || !found6 {
		t.Errorf("sibling cross-links incomplete: %+v", edits)
	}
}

// TestSyncAllSiblingLinksIfNeeded_SkipsSingleIssue verifies a lone-issue Task
// (no siblings) never triggers an SCM edit.
func TestSyncAllSiblingLinksIfNeeded_SkipsSingleIssue(t *testing.T) {
	ctx := context.Background()
	name := "links-single"
	proj := "links-single-p"
	repo := "links-single-r"
	sec := "links-single-s"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#9",
		URL: "https://github.com/o/r/issues/9", Number: 9,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	fw := &fakeProposalWriter{}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:  func(string) (scm.SCMWriter, error) { return fw, nil },
	}

	r.syncAllSiblingLinksIfNeeded(ctx, task)

	if edits := fw.editCallsSnapshot(); len(edits) != 0 {
		t.Errorf("want 0 edits for a lone issue, got %d: %+v", len(edits), edits)
	}
}
