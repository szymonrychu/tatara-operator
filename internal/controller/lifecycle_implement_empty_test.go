// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"strings"
	"sync"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// noChangeRecordingSCMWriter returns 422 for OpenChange (simulating no commit /
// branch absent) and records any Comment calls so tests can assert on them.
type noChangeRecordingSCMWriter struct {
	scm.SCMWriter
	mu           sync.Mutex
	commentCalls []struct{ issueRef, body string }
}

func (n *noChangeRecordingSCMWriter) OpenChange(_ context.Context, _, _, _, _, _, _ string) (string, error) {
	return "", &scm.HTTPError{Status: 422, Body: "no diff", Path: "/pulls"}
}

func (n *noChangeRecordingSCMWriter) Comment(_ context.Context, _, issueRef, body string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.commentCalls = append(n.commentCalls, struct{ issueRef, body string }{issueRef, body})
	return nil
}

func (n *noChangeRecordingSCMWriter) AddLabel(_ context.Context, _, _, _ string) error    { return nil }
func (n *noChangeRecordingSCMWriter) RemoveLabel(_ context.Context, _, _, _ string) error { return nil }

// commentBodies returns all comment bodies posted to issueRef.
func (n *noChangeRecordingSCMWriter) commentBodies(issueRef string) []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []string
	for _, c := range n.commentCalls {
		if c.issueRef == issueRef {
			out = append(out, c.body)
		}
	}
	return out
}

// seedEmptyImplementTask creates a lifecycle task in Implement/Succeeded state with
// ImplementEmptyRetries pre-set to retries. The fake SCM writer will return 422.
func seedEmptyImplementTask(t *testing.T, name, proj, repo, sec string, retries int) (*tatarav1alpha1.Task, *noChangeRecordingSCMWriter) {
	t.Helper()
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#200",
		URL: "https://github.com/o/r/issues/200", Number: 200,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.ImplementEmptyRetries = retries
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	fw := &noChangeRecordingSCMWriter{}
	return task, fw
}

// reconcileEmptyImplementTask reconciles the named task with the given SCM writer.
func reconcileEmptyImplementTask(t *testing.T, name string, fw *noChangeRecordingSCMWriter) *tatarav1alpha1.Task {
	t.Helper()
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r := newLifecycleReconciler(t, nil)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	return got
}

// TestFinishImplement_EmptyRun_FirstRetry: counter 0 -> retry (counter==1,
// ImplementContext set, LifecycleState still Implement, Phase=="").
func TestFinishImplement_EmptyRun_FirstRetry(t *testing.T) {
	name := "lc-empty-retry-first"
	_, fw := seedEmptyImplementTask(t, name, "lc-erp-first", "lc-err-first", "lc-ers-first", 0)

	got := reconcileEmptyImplementTask(t, name, fw)

	if got.Status.LifecycleState != "Implement" {
		t.Errorf("LifecycleState = %q, want Implement (should still be in Implement, retrying)", got.Status.LifecycleState)
	}
	if got.Status.Phase != "" {
		t.Errorf("Phase = %q, want empty (resetAgentRun clears phase for re-spawn)", got.Status.Phase)
	}
	if got.Status.ImplementEmptyRetries != 1 {
		t.Errorf("ImplementEmptyRetries = %d, want 1", got.Status.ImplementEmptyRetries)
	}
	if got.Status.ImplementContext == "" {
		t.Errorf("ImplementContext is empty, want re-entry nudge prompt to be set")
	}
	if !strings.Contains(got.Status.ImplementContext, "previous attempt") {
		t.Errorf("ImplementContext = %q, want it to mention 'previous attempt'", got.Status.ImplementContext)
	}

	// No comment posted on first retry.
	bodies := fw.commentBodies("o/r#200")
	if len(bodies) != 0 {
		t.Errorf("comment posted on first retry; want none: %v", bodies)
	}
}

// TestFinishImplement_EmptyRun_ParksAtCap: counter pre-set to 2 (==cap) ->
// LifecycleState==Parked, comment posted containing "no change".
func TestFinishImplement_EmptyRun_ParksAtCap(t *testing.T) {
	name := "lc-empty-retry-cap"
	_, fw := seedEmptyImplementTask(t, name, "lc-erp-cap", "lc-err-cap", "lc-ers-cap", 2)

	got := reconcileEmptyImplementTask(t, name, fw)

	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked (at cap)", got.Status.LifecycleState)
	}

	bodies := fw.commentBodies("o/r#200")
	if len(bodies) == 0 {
		t.Error("expected a comment to be posted at cap; got none")
	} else if !strings.Contains(strings.Join(bodies, " "), "no change") {
		t.Errorf("comment body %q does not mention 'no change'", bodies)
	}
}

// TestFinishImplement_PROpened_ResetsCounter: counter pre-set to 1, fake returns
// a real PR URL -> PrURL set, ImplementEmptyRetries==0.
func TestFinishImplement_PROpened_ResetsCounter(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-empty-reset"
	proj := "lc-erp-reset"
	repo := "lc-err-reset"
	sec := "lc-ers-reset"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#201",
		URL: "https://github.com/o/r/issues/201", Number: 201,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.ImplementEmptyRetries = 1
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	// Use a writer that returns a real PR URL.
	fw := &lifecycleFakeSCMWriter{openPRURL: "https://github.com/o/r/pull/202"}
	r := newLifecycleReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}

	if got.Status.PrURL == "" {
		t.Error("PrURL is empty; expected PR URL to be set on success")
	}
	if got.Status.ImplementEmptyRetries != 0 {
		t.Errorf("ImplementEmptyRetries = %d, want 0 (reset on PR opened)", got.Status.ImplementEmptyRetries)
	}
}
