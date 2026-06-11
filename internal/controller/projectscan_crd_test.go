package controller

import (
	"context"
	"testing"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestProjectCronFieldsRoundTrip(t *testing.T) {
	ctx := context.Background()
	mkSecret(t, "cron-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	p := &tataradevv1alpha1.Project{}
	p.Name = "cron-proj"
	p.Namespace = testNS
	p.Spec.ScmSecretRef = "cron-scm"
	p.Spec.Scm = &tataradevv1alpha1.ScmSpec{
		Provider: "github", Owner: "o", BotLogin: "bot",
		PriorityLabel: "tatara/priority",
		Cron: &tataradevv1alpha1.ScmCron{
			MRScan:    tataradevv1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerCycle: 2},
			IssueScan: tataradevv1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerCycle: 1},
			Brainstorm: tataradevv1alpha1.BrainstormActivity{
				Enabled: true, Schedule: "0 6 * * *", MaxPerCycle: 1,
				Sources: []string{"docs", "memory", "internet"},
			},
		},
	}
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create project: %v", err)
	}
	got := &tataradevv1alpha1.Project{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "cron-proj"}, got); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if got.Spec.Scm.PriorityLabel != "tatara/priority" || got.Spec.Scm.Cron.MRScan.MaxPerCycle != 2 {
		t.Fatalf("cron fields not persisted: %+v", got.Spec.Scm)
	}
	if !got.Spec.Scm.Cron.Brainstorm.Enabled || got.Spec.Scm.Cron.Brainstorm.Sources[2] != "internet" {
		t.Fatalf("brainstorm fields not persisted: %+v", got.Spec.Scm.Cron.Brainstorm)
	}
	now := metav1.Now()
	got.Status.LastMRScan = &now
	got.Status.LastIssueScan = &now
	got.Status.LastBrainstorm = &now
	if err := k8sClient.Status().Update(ctx, got); err != nil {
		t.Fatalf("status update: %v", err)
	}
}

func TestTaskKindEnumAndIssueOutcome(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		kind string
	}{
		{"enum-triage-issue", "triageIssue"},
		{"enum-brainstorm", "brainstorm"},
	}
	for _, tc := range cases {
		tk := &tataradevv1alpha1.Task{}
		tk.Name = tc.name
		tk.Namespace = testNS
		tk.Spec = tataradevv1alpha1.TaskSpec{
			ProjectRef: "p", RepositoryRef: "r", Goal: "g", Kind: tc.kind,
		}
		if err := k8sClient.Create(ctx, tk); err != nil {
			t.Fatalf("create task kind=%s: %v", tc.kind, err)
		}
	}
	tk := &tataradevv1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "enum-triage-issue"}, tk); err != nil {
		t.Fatalf("get task: %v", err)
	}
	tk.Status.IssueOutcome = &tataradevv1alpha1.IssueOutcome{Action: "close", Comment: "out of scope"}
	if err := k8sClient.Status().Update(ctx, tk); err != nil {
		t.Fatalf("status update issueOutcome: %v", err)
	}
}
