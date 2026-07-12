// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
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

// TestSyncAllSiblingLinksIfNeeded_SkipsResyncWhenSiblingSetUnchanged verifies
// F5: a second reconcile with the SAME sibling set must NOT re-sweep the SCM
// (one GetIssue+EditIssue per sibling, every reconcile, forever) - only the
// FIRST sync (or a sibling-set CHANGE) may trigger the SCM read/write sweep.
// fakeProposalReader's bodies map is static (unlike a real SCM, it never
// reflects the prior EditIssue), so an unbounded resync would double the edit
// count on the second call; the gate must keep it at 2.
func TestSyncAllSiblingLinksIfNeeded_SkipsResyncWhenSiblingSetUnchanged(t *testing.T) {
	ctx := context.Background()
	name := "links-steady"
	proj := "links-steady-p"
	repo := "links-steady-r"
	sec := "links-steady-s"
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
	if got := len(fw.editCallsSnapshot()); got != 2 {
		t.Fatalf("after first sync: want 2 edits, got %d", got)
	}

	// Re-Get the task so the second call sees the persisted LinksSyncedURLs
	// (same pattern a fresh reconcile would use).
	fresh := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, fresh); err != nil {
		t.Fatalf("re-get task: %v", err)
	}
	if len(fresh.Status.LinksSyncedURLs) != 2 {
		t.Fatalf("LinksSyncedURLs not persisted after first sync: %v", fresh.Status.LinksSyncedURLs)
	}

	r.syncAllSiblingLinksIfNeeded(ctx, fresh)
	if got := len(fw.editCallsSnapshot()); got != 2 {
		t.Fatalf("after second sync (unchanged sibling set): want still 2 edits (no resweep), got %d", got)
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

// TestSyncAllSiblingLinksIfNeeded_TransientFailureDoesNotStamp is the M5
// regression: syncSiblingLinks used to swallow every GetIssue/EditIssue error
// (best-effort) while syncAllSiblingLinksIfNeeded stamped LinksSyncedURLs
// UNCONDITIONALLY afterwards - so under SCM rate-limiting (a transient error)
// a sibling whose read failed was never retried until the sibling SET
// changed. A transient failure on one sibling must leave LinksSyncedURLs
// unstamped so the NEXT reconcile retries the whole sweep.
func TestSyncAllSiblingLinksIfNeeded_TransientFailureDoesNotStamp(t *testing.T) {
	ctx := context.Background()
	name := "links-transient-fail"
	proj := "links-transient-fail-p"
	repo := "links-transient-fail-r"
	sec := "links-transient-fail-s"
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
	reader := &fakeProposalReader{
		bodies: map[string]string{"o/r#5": "issue 5 body", "o/r#6": "issue 6 body"},
		// #6's GetIssue fails transiently (e.g. rate-limited): NOT a 404/410.
		getIssueErrs: map[string]error{"o/r#6": errors.New("rate limited")},
	}
	r := &TaskReconciler{
		Client:    k8sClient,
		Scheme:    k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return fw, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return reader, nil },
	}

	r.syncAllSiblingLinksIfNeeded(ctx, task)

	fresh := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, fresh); err != nil {
		t.Fatalf("re-get task: %v", err)
	}
	if len(fresh.Status.LinksSyncedURLs) != 0 {
		t.Fatalf("LinksSyncedURLs must NOT be stamped after a transient sibling failure, got %v", fresh.Status.LinksSyncedURLs)
	}

	// A second reconcile must retry the sweep (not gated by a wrongly-stamped
	// URL set): the failing sibling's edit is attempted again.
	editsBefore := len(fw.editCallsSnapshot())
	r.syncAllSiblingLinksIfNeeded(ctx, fresh)
	if got := len(fw.editCallsSnapshot()); got <= editsBefore {
		t.Fatalf("second reconcile after a transient failure must retry the sweep; edits before=%d after=%d", editsBefore, got)
	}
}

// TestSyncAllSiblingLinksIfNeeded_PermanentGoneStillStamps: a 404/410 on a
// sibling is terminal (the issue can never be read again), so it must count
// as "done" for that sibling and the sweep may still stamp LinksSyncedURLs -
// unlike a transient error, it must not retry forever.
func TestSyncAllSiblingLinksIfNeeded_PermanentGoneStillStamps(t *testing.T) {
	ctx := context.Background()
	name := "links-gone"
	proj := "links-gone-p"
	repo := "links-gone-r"
	sec := "links-gone-s"
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
	reader := &fakeProposalReader{
		bodies:       map[string]string{"o/r#5": "issue 5 body"},
		getIssueErrs: map[string]error{"o/r#6": &scm.HTTPError{Status: 404, Path: "/repos/o/r/issues/6"}},
	}
	r := &TaskReconciler{
		Client:    k8sClient,
		Scheme:    k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return fw, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return reader, nil },
	}

	r.syncAllSiblingLinksIfNeeded(ctx, task)

	fresh := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, fresh); err != nil {
		t.Fatalf("re-get task: %v", err)
	}
	if len(fresh.Status.LinksSyncedURLs) != 2 {
		t.Fatalf("a permanently-gone sibling must still count as a clean sweep, got LinksSyncedURLs=%v", fresh.Status.LinksSyncedURLs)
	}
}

// TestSyncAllSiblingLinksIfNeeded_PermanentNon404FailureStopsAtCap is the D2
// regression: syncSiblingLinks reports only clean/unclean, and
// isPermanentTargetGone classifies ONLY 404/410 as terminal. Any OTHER
// permanent failure - 403 conversation-locked, a bot without issues:write, a
// 422 body over GitHub's 65536-char limit - kept clean=false forever, so
// LinksSyncedURLs was never stamped and syncAllSiblingLinksIfNeeded re-read
// EVERY sibling on EVERY reconcile, for good: exactly the read amplification
// the F5 gate exists to bound. The sweep must give up after a bounded number
// of attempts (linksSyncFailureCap, mirroring writebackSkip4xxCap) and stop
// re-reading.
func TestSyncAllSiblingLinksIfNeeded_PermanentNon404FailureStopsAtCap(t *testing.T) {
	ctx := context.Background()
	name := "links-perm-403"
	proj := "links-perm-403-p"
	repo := "links-perm-403-r"
	sec := "links-perm-403-s"
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
	reader := &fakeProposalReader{
		bodies: map[string]string{"o/r#5": "issue 5 body"},
		// #6 is permanently unreadable, but NOT with a 404/410: the conversation
		// is locked / the bot lacks issues:write. This never resolves.
		getIssueErrs: map[string]error{"o/r#6": &scm.HTTPError{Status: 403, Path: "/repos/o/r/issues/6"}},
	}
	r := &TaskReconciler{
		Client:    k8sClient,
		Scheme:    k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return fw, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return reader, nil },
	}

	// Reconcile far past the cap. The reads must plateau, not grow linearly.
	var atCap int
	for i := 0; i < linksSyncFailureCap+4; i++ {
		fresh := &tatarav1alpha1.Task{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, fresh); err != nil {
			t.Fatalf("re-get task: %v", err)
		}
		r.syncAllSiblingLinksIfNeeded(ctx, fresh)
		if i == linksSyncFailureCap-1 {
			atCap = reader.getIssueCalls
		}
	}

	if atCap == 0 {
		t.Fatalf("no GetIssue calls at all; the sweep never ran")
	}
	if reader.getIssueCalls != atCap {
		t.Fatalf("GetIssue calls kept growing after the cap: %d at cap, %d after %d more reconciles - a permanently-failing sweep must stop re-reading",
			atCap, reader.getIssueCalls, 4)
	}
}
