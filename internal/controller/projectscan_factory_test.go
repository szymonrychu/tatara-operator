package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

func newScanReconciler(reader scm.SCMReader) *ProjectReconciler {
	r := newProjectReconciler()
	r.Seq = &queue.SeqSource{Client: k8sClient, Namespace: testNS}
	r.ReaderFor = func(string, string) (scm.SCMReader, error) { return reader, nil }
	return r
}

func TestCreateScanTask(t *testing.T) {
	ctx := context.Background()
	mkSecret(t, "factory-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{}
	proj.Name = "factory-proj"
	proj.Namespace = testNS
	proj.Spec.ScmSecretRef = "factory-scm"
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "bot"}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	repo := &tatarav1alpha1.Repository{}
	repo.Name = "factory-repo"
	repo.Namespace = testNS
	repo.Spec = tatarav1alpha1.RepositorySpec{ProjectRef: "factory-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"}
	if err := k8sClient.Create(ctx, repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}

	r := newScanReconciler(nil)
	c := candidate{repo: "o/r", number: 5, headSHA: "abc", isPR: true}
	ok, err := r.createScanTask(ctx, proj, repo, c, c, "mrScan", "review", "review PR o/r#5", nil, nil)
	if err != nil {
		t.Fatalf("createScanTask: %v", err)
	}
	if !ok {
		t.Fatalf("createScanTask: want created=true")
	}

	var qel tatarav1alpha1.QueuedEventList
	if err := k8sClient.List(ctx, &qel); err != nil {
		t.Fatalf("list QEs: %v", err)
	}
	var got *tatarav1alpha1.QueuedEvent
	for i := range qel.Items {
		if qel.Items[i].Spec.ProjectRef == "factory-proj" {
			got = &qel.Items[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("no QueuedEvent created for factory-proj")
	}
	if got.Spec.Kind != "review" || got.Spec.RepositoryRef != "factory-repo" {
		t.Fatalf("QE spec = %+v", got.Spec)
	}
	// Phase 1: source-repo, source-number, head-sha labels no longer written.
	for _, key := range []string{labelSourceRepo, labelSourceNumber, labelHeadSHA} {
		if v := got.Spec.Payload.Labels[key]; v != "" {
			t.Fatalf("QE payload label %q must not be written (Phase 1), got %q", key, v)
		}
	}
	if got.Spec.Payload.Labels[labelActivity] != "mrScan" {
		t.Fatalf("QE payload labels = %+v", got.Spec.Payload.Labels)
	}
	src := got.Spec.Payload.Source
	if src == nil || src.Number != 5 || !src.IsPR || src.Provider != "github" {
		t.Fatalf("QE source = %+v", src)
	}
	if src.IssueRef != "o/r#5" {
		t.Fatalf("github PR ref = %q, want o/r#5", src.IssueRef)
	}
}

func TestCreateScanTaskGitLabMR(t *testing.T) {
	ctx := context.Background()
	mkSecret(t, "factory-gl-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{}
	proj.Name = "factory-gl-proj"
	proj.Namespace = testNS
	proj.Spec.ScmSecretRef = "factory-gl-scm"
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "gitlab", Owner: "g", BotLogin: "bot"}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	repo := &tatarav1alpha1.Repository{}
	repo.Name = "factory-gl-repo"
	repo.Namespace = testNS
	repo.Spec = tatarav1alpha1.RepositorySpec{ProjectRef: "factory-gl-proj", URL: "https://gitlab.com/g/p.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"}
	if err := k8sClient.Create(ctx, repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}

	r := newScanReconciler(nil)

	// MR candidate: ref must use '!' so write-back lands on the MR.
	mr := candidate{repo: "g/p", number: 42, isPR: true}
	ok, err := r.createScanTask(ctx, proj, repo, mr, mr, "mrScan", "review", "review MR g/p!42", nil, nil)
	if err != nil {
		t.Fatalf("createScanTask MR: %v", err)
	}
	if !ok {
		t.Fatalf("createScanTask MR: want created=true")
	}

	var qel tatarav1alpha1.QueuedEventList
	if err := k8sClient.List(ctx, &qel); err != nil {
		t.Fatalf("list QEs: %v", err)
	}
	var mrQE *tatarav1alpha1.QueuedEvent
	for i := range qel.Items {
		if qel.Items[i].Spec.ProjectRef == "factory-gl-proj" && qel.Items[i].Spec.Payload.Source != nil && qel.Items[i].Spec.Payload.Source.IsPR {
			mrQE = &qel.Items[i]
			break
		}
	}
	if mrQE == nil {
		t.Fatalf("no MR QueuedEvent found for factory-gl-proj")
	}
	src := mrQE.Spec.Payload.Source
	if src == nil || !src.IsPR || src.IssueRef != "g/p!42" {
		t.Fatalf("gitlab MR ref = %q, want g/p!42 (source=%+v)", src.IssueRef, src)
	}

	// Issue candidate on GitLab keeps '#'.
	iss := candidate{repo: "g/p", number: 7, isPR: false}
	ok2, err := r.createScanTask(ctx, proj, repo, iss, iss, "issueScan", "issueLifecycle", "triage issue g/p#7", nil, nil)
	if err != nil {
		t.Fatalf("createScanTask issue: %v", err)
	}
	if !ok2 {
		t.Fatalf("createScanTask issue: want created=true")
	}

	if err := k8sClient.List(ctx, &qel); err != nil {
		t.Fatalf("list QEs 2: %v", err)
	}
	var issQE *tatarav1alpha1.QueuedEvent
	for i := range qel.Items {
		if qel.Items[i].Spec.ProjectRef == "factory-gl-proj" && qel.Items[i].Spec.Payload.Source != nil && !qel.Items[i].Spec.Payload.Source.IsPR {
			issQE = &qel.Items[i]
			break
		}
	}
	if issQE == nil {
		t.Fatalf("no issue QueuedEvent found for factory-gl-proj")
	}
	src2 := issQE.Spec.Payload.Source
	if src2 == nil || src2.IsPR || src2.IssueRef != "g/p#7" {
		t.Fatalf("gitlab issue ref = %q, want g/p#7 (source=%+v)", src2.IssueRef, src2)
	}
}
