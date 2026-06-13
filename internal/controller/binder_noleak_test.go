package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestBindersNeverCreateTriageIssueOrSelfImprove asserts that neither issueScan
// nor mrScan creates Tasks with Kind=triageIssue or Kind=selfImprove. These kinds
// are kept as reachable writeback arms for in-flight migration safety, but no new
// binder creates them.
func TestBindersNeverCreateTriageIssueOrSelfImprove(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{
		IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 3},
		MRScan:    tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 3},
	}
	proj, _ := seedScanProject(t, "noleak-proj", cron)
	repos := []tatarav1alpha1.Repository{
		{ObjectMeta: metav1.ObjectMeta{Name: "noleak-proj-repo", Namespace: testNS},
			Spec: tatarav1alpha1.RepositorySpec{ProjectRef: "noleak-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main"}},
	}
	reader := &fakeReader{
		issues: []scm.IssueRef{
			{Repo: "o/r", Number: 1, UpdatedAt: time.Unix(100, 0)},
			{Repo: "o/r", Number: 2, UpdatedAt: time.Unix(200, 0)},
		},
		prs: []scm.PRRef{
			{Repo: "o/r", Number: 10, Author: "tatara-bot", HeadSHA: "a", UpdatedAt: time.Unix(100, 0)},
			{Repo: "o/r", Number: 11, Author: "human", HeadSHA: "b", UpdatedAt: time.Unix(200, 0)},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	b := 99
	r.issueScan(context.Background(), proj, reader, repos, nil, cron.IssueScan, &b)
	b = 99
	r.mrScan(context.Background(), proj, reader, repos, nil, cron.MRScan, &b)

	tasks := listScanTasks(t, "noleak-proj")
	for _, tk := range tasks {
		if tk.Spec.Kind == "triageIssue" {
			t.Errorf("binder created triageIssue task %s - binders must not create this kind", tk.Name)
		}
		if tk.Spec.Kind == "selfImprove" {
			t.Errorf("binder created selfImprove task %s - binders must not create this kind", tk.Name)
		}
	}
	// Verify the expected new kinds are present
	kinds := map[string]int{}
	for _, tk := range tasks {
		kinds[tk.Spec.Kind]++
	}
	if kinds["issueLifecycle"] == 0 {
		t.Errorf("expected issueLifecycle tasks from binders, got kinds: %v", kinds)
	}
	if kinds["review"] == 0 {
		t.Errorf("expected review task from human PR, got kinds: %v", kinds)
	}
}
