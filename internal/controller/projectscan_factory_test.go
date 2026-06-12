package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
)

func newScanReconciler(reader scm.SCMReader) *ProjectReconciler {
	r := newProjectReconciler()
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
	created, err := r.createScanTask(ctx, proj, repo, c, c, "mrScan", "review", "review PR o/r#5", nil)
	if err != nil {
		t.Fatalf("createScanTask: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: created.Name}, got); err != nil {
		t.Fatalf("get created task: %v", err)
	}
	if got.Spec.Kind != "review" || got.Spec.ProjectRef != "factory-proj" || got.Spec.RepositoryRef != "factory-repo" {
		t.Fatalf("task spec = %+v", got.Spec)
	}
	if got.Labels[labelSourceRepo] != "o.r" || got.Labels[labelSourceNumber] != "5" || got.Labels[labelHeadSHA] != "abc" || got.Labels[labelActivity] != "mrScan" {
		t.Fatalf("task labels = %+v", got.Labels)
	}
	if got.Spec.Source == nil || got.Spec.Source.Number != 5 || !got.Spec.Source.IsPR || got.Spec.Source.Provider != "github" {
		t.Fatalf("task source = %+v", got.Spec.Source)
	}
}
