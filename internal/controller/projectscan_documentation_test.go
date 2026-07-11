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
	"k8s.io/apimachinery/pkg/types"
)

// docFakeReader returns per-slug default-branch heads and commit windows so the
// documentation cron can compute a diff-since-last-doc window.
type docFakeReader struct {
	fakeReader
	headBySlug    map[string]string
	commitsBySlug map[string][]scm.CommitRef
}

func (f *docFakeReader) GetDefaultBranchHeadSHA(_ context.Context, owner, repo string) (string, error) {
	return f.headBySlug[owner+"/"+repo], nil
}

func (f *docFakeReader) ListCommits(_ context.Context, owner, repo string, _ time.Time) ([]scm.CommitRef, error) {
	return f.commitsBySlug[owner+"/"+repo], nil
}

// seedDocumentationProject creates a Project with a documentation cron, the
// Documentation spec pointed at the docs repo, plus the source repo and the
// docs repo enrolled as Repository CRs.
func seedDocumentationProject(t *testing.T, name, docsURL string, sourceSlugs []string) (*tatarav1alpha1.Project, []tatarav1alpha1.Repository) {
	t.Helper()
	ctx := context.Background()
	mkSecret(t, name+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	cron := &tatarav1alpha1.ScmCron{
		Documentation: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 1},
	}
	proj := &tatarav1alpha1.Project{}
	proj.Name = name
	proj.Namespace = testNS
	proj.Spec.ScmSecretRef = name + "-scm"
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot", Cron: cron}
	proj.Spec.Documentation = &tatarav1alpha1.DocumentationSpec{Enabled: true, Repo: docsURL}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	var repos []tatarav1alpha1.Repository
	mk := func(rname, url string) {
		rp := &tatarav1alpha1.Repository{}
		rp.Name = rname
		rp.Namespace = testNS
		rp.Spec = tatarav1alpha1.RepositorySpec{ProjectRef: name, URL: url, DefaultBranch: "main", ReingestSchedule: "0 6 * * *"}
		if err := k8sClient.Create(ctx, rp); err != nil {
			t.Fatalf("create repo %s: %v", rname, err)
		}
		repos = append(repos, *rp)
	}
	mk(name+"-docs", docsURL)
	for i, slug := range sourceSlugs {
		mk(name+"-src"+string(rune('a'+i)), "https://github.com/"+slug+".git")
	}
	return proj, repos
}

func listDocumentationQEs(t *testing.T, project string) []tatarav1alpha1.QueuedEvent {
	t.Helper()
	qes := listScanQEs(t, project)
	var out []tatarav1alpha1.QueuedEvent
	for _, qe := range qes {
		if qe.Spec.Payload.Labels[labelActivity] == "documentation" {
			out = append(out, qe)
		}
	}
	return out
}

// TestActivitySchedule_DocumentationCase: activityScheduleAndLast resolves the
// documentation schedule + LastDocumentation stamp.
func TestActivitySchedule_DocumentationCase(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Cron: &tatarav1alpha1.ScmCron{
		Documentation: tatarav1alpha1.CronActivity{Schedule: "0 3 * * *"},
	}}
	stamp := metav1.NewTime(time.Unix(1000, 0))
	proj.Status.LastDocumentation = &stamp

	sched, last := activityScheduleAndLast(proj, "documentation")
	if sched != "0 3 * * *" {
		t.Fatalf("schedule = %q, want '0 3 * * *'", sched)
	}
	if last == nil || !last.Equal(&stamp) {
		t.Fatalf("last = %v, want %v", last, stamp)
	}
}

// TestRunScans_DocumentationDueCreatesDocTask: a due documentation cron with a
// source repo that advanced since LastDocumentation enqueues one documentation
// Task repo-scoped to the docs repo, carrying the source diff annotations.
func TestRunScans_DocumentationDueCreatesDocTask(t *testing.T) {
	docsURL := "https://github.com/o/docs.git"
	proj, _ := seedDocumentationProject(t, "doc-due", docsURL, []string{"o/a"})
	past := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	proj.Status.LastDocumentation = &past
	if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
		t.Fatalf("seed last-documentation: %v", err)
	}

	reader := &docFakeReader{
		headBySlug: map[string]string{"o/a": "headsha"},
		commitsBySlug: map[string][]scm.CommitRef{
			"o/a": {
				{SHA: "c1", Date: time.Now().Add(-90 * time.Minute)},
				{SHA: "c2", Date: time.Now().Add(-30 * time.Minute)},
			},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}

	qes := listDocumentationQEs(t, "doc-due")
	if len(qes) != 1 {
		t.Fatalf("want 1 documentation QE, got %d", len(qes))
	}
	qe := qes[0]
	if qe.Spec.Kind != "documentation" {
		t.Fatalf("QE Kind = %q, want documentation", qe.Spec.Kind)
	}
	if qe.Spec.Payload.RepositoryRef != "doc-due-docs" {
		t.Fatalf("QE RepositoryRef = %q, want docs repo doc-due-docs", qe.Spec.Payload.RepositoryRef)
	}
	if qe.Spec.Payload.Annotations[tatarav1alpha1.AnnSourceHeadSHA] != "headsha" {
		t.Fatalf("head-sha annotation = %q, want headsha", qe.Spec.Payload.Annotations[tatarav1alpha1.AnnSourceHeadSHA])
	}
	if qe.Spec.Payload.Annotations[tatarav1alpha1.AnnSourceRepo] != "https://github.com/o/a.git" {
		t.Fatalf("source-repo annotation = %q", qe.Spec.Payload.Annotations[tatarav1alpha1.AnnSourceRepo])
	}

	var got tatarav1alpha1.Project
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "doc-due"}, &got); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if got.Status.LastDocumentation == nil || !got.Status.LastDocumentation.After(past.Time) {
		t.Fatalf("LastDocumentation not advanced: %+v", got.Status.LastDocumentation)
	}
}

// TestRunScans_DocumentationNoChangesNoTask: a due documentation cron with no
// commits since LastDocumentation creates no Task but still advances the stamp.
func TestRunScans_DocumentationNoChangesNoTask(t *testing.T) {
	docsURL := "https://github.com/o/docs2.git"
	proj, _ := seedDocumentationProject(t, "doc-nochange", docsURL, []string{"o/b"})
	past := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	proj.Status.LastDocumentation = &past
	if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
		t.Fatalf("seed last-documentation: %v", err)
	}

	reader := &docFakeReader{
		headBySlug:    map[string]string{"o/b": "headsha"},
		commitsBySlug: map[string][]scm.CommitRef{}, // no commits in window
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}

	if qes := listDocumentationQEs(t, "doc-nochange"); len(qes) != 0 {
		t.Fatalf("want 0 documentation QEs (no changes), got %d", len(qes))
	}
	var got tatarav1alpha1.Project
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "doc-nochange"}, &got); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if got.Status.LastDocumentation == nil || !got.Status.LastDocumentation.After(past.Time) {
		t.Fatalf("LastDocumentation not advanced on empty tick: %+v", got.Status.LastDocumentation)
	}
}

// TestHealthCheckCronDropped: the healthCheck cron dispatch is stripped from
// runScans - a due healthCheck activity fires no Task and never stamps
// LastHealthCheck (the kind is absorbed into brainstorm; the type is kept inert
// for stored-CR back-compat).
func TestHealthCheckCronDropped(t *testing.T) {
	proj, _ := seedHealthCheckProject(t, "hc-dropped", []string{"o/h"}, 3)
	past := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	proj.Status.LastHealthCheck = &past
	if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
		t.Fatalf("seed last-healthcheck: %v", err)
	}

	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{"o/h": {}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}

	if qes := listHealthCheckQEs(t, "hc-dropped"); len(qes) != 0 {
		t.Fatalf("want 0 healthCheck QEs from runScans (dispatch dropped), got %d", len(qes))
	}
	var got tatarav1alpha1.Project
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "hc-dropped"}, &got); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if got.Status.LastHealthCheck == nil || got.Status.LastHealthCheck.After(past.Time) {
		t.Fatalf("LastHealthCheck must not advance (dispatch dropped): %+v", got.Status.LastHealthCheck)
	}
}
