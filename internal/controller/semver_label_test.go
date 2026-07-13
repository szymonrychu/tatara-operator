package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// mdGitLabRepo is mdRepo on a GitLab remote. The platform runs BOTH providers,
// and the two label routes are NOT the same: GitHub labels a PR through its
// issue route (owner/repo#N), GitLab needs the '!' form or the write lands on
// the ISSUE that happens to share the iid.
func mdGitLabRepo(name string) *tatarav1alpha1.Repository {
	repo := mdRepo(name)
	repo.Spec.URL = "https://gitlab.com/szymonrychu/" + name
	return repo
}

func mdGitLabProject() *tatarav1alpha1.Project {
	proj := mdProject()
	proj.Spec.Scm.Provider = "gitlab"
	return proj
}

// TestSemverLabelProjection is contract H.4: CI cuts the release tag FROM THE
// LABEL. status.significance (IMPLEMENT-OWNED, escalate-only) is projected onto
// exactly ONE semver:<level> label on the PR.
func TestSemverLabelProjection(t *testing.T) {
	tests := []struct {
		name         string
		significance string
		annotation   string // what a previous projection recorded
		wantAdded    []string
		wantRemoved  []string
		wantEnsured  []string
		wantAnn      string
	}{
		{
			name: "a declared minor projects semver:minor", significance: "minor",
			wantAdded:   []string{"szymonrychu/tatara-operator#7|semver:minor"},
			wantRemoved: []string{"szymonrychu/tatara-operator#7|semver:major", "szymonrychu/tatara-operator#7|semver:patch"},
			wantEnsured: []string{"semver:minor|d93f0b"},
			wantAnn:     "semver:minor",
		},
		{
			// IDEMPOTENT: re-running must not duplicate the label or 422 the PR.
			name: "the label already projected writes nothing", significance: "minor",
			annotation: "semver:minor", wantAnn: "semver:minor",
		},
		{
			// A RAISE REPLACES. A PR carrying semver:patch AND semver:minor lets CI
			// key the tag off either one.
			name: "a RAISE replaces the superseded label", significance: "minor",
			annotation:  "semver:patch",
			wantAdded:   []string{"szymonrychu/tatara-operator#7|semver:minor"},
			wantRemoved: []string{"szymonrychu/tatara-operator#7|semver:major", "szymonrychu/tatara-operator#7|semver:patch"},
			wantEnsured: []string{"semver:minor|d93f0b"},
			wantAnn:     "semver:minor",
		},
		{
			// NO significance -> NO label. This is the human PR (a kind=review Task
			// mirrors a PR the operator did not author): H.4 says a HUMAN sets the
			// label there, and the operator never invents a significance.
			name: "no significance writes nothing", significance: "",
			annotation: "", wantAnn: "",
		},
		{
			name: "a declared major projects semver:major", significance: "major",
			wantAdded:   []string{"szymonrychu/tatara-operator#7|semver:major"},
			wantRemoved: []string{"szymonrychu/tatara-operator#7|semver:minor", "szymonrychu/tatara-operator#7|semver:patch"},
			wantEnsured: []string{"semver:major|b60205"},
			wantAnn:     "semver:major",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := mdTask("t1", "implement", tatarav1alpha1.StageReviewing)
			mr := mdMR(task, "tatara-operator", 7)
			mr.Status.Significance = tc.significance
			if tc.annotation != "" {
				mr.Annotations = map[string]string{AnnSemverLabel: tc.annotation}
			}
			c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

			f := newFakeForge(t)
			d := mdNewDriver(t, f, c)
			if err := d.ProjectSemverLabel(context.Background(), mdProject(), mdRepo("tatara-operator"), mr); err != nil {
				t.Fatalf("ProjectSemverLabel: %v", err)
			}

			requireLabels(t, "added", f.addedLabels, tc.wantAdded)
			requireLabels(t, "removed", f.removedLabels, tc.wantRemoved)
			requireLabels(t, "ensured", f.ensuredLabels, tc.wantEnsured)
			if got := mdGetMR(t, c, mr.Name).Annotations[AnnSemverLabel]; got != tc.wantAnn {
				t.Fatalf("%s = %q, want %q", AnnSemverLabel, got, tc.wantAnn)
			}
		})
	}
}

// A LOWER never reaches the label. status.significance is IMPLEMENT-OWNED and
// /outcome refuses a review's downgrade (TestOutcome_Review_ChangeSignificance
// EscalatesOnly), so what arrives here is the UNCHANGED higher level - and the
// projection then recognises its own annotation and writes NOTHING. The PR keeps
// semver:minor; a flaky reviewer cannot downgrade a minor release to a patch.
func TestSemverLabelNeverLowers(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageReviewing)
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.Significance = "minor" // a review asked for patch; /outcome kept minor
	mr.Annotations = map[string]string{AnnSemverLabel: "semver:minor"}
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	d := mdNewDriver(t, f, c)
	if err := d.ProjectSemverLabel(context.Background(), mdProject(), mdRepo("tatara-operator"), mr); err != nil {
		t.Fatalf("ProjectSemverLabel: %v", err)
	}
	if len(f.addedLabels) != 0 || len(f.removedLabels) != 0 {
		t.Fatalf("a downgraded review moved the semver label: added=%v removed=%v",
			f.addedLabels, f.removedLabels)
	}
	if got := mdGetMR(t, c, mr.Name).Annotations[AnnSemverLabel]; got != "semver:minor" {
		t.Fatalf("%s = %q, want semver:minor", AnnSemverLabel, got)
	}
}

// BOTH PROVIDERS. GitLab's label route is '!' for a merge request; the '#' form
// routes the write at the ISSUE with the same iid - a silent mislabel of an
// unrelated issue AND a PR that CI cuts no tag for.
func TestSemverLabelBothProviders(t *testing.T) {
	tests := []struct {
		name    string
		proj    *tatarav1alpha1.Project
		repo    *tatarav1alpha1.Repository
		wantRef string
	}{
		{"github", mdProject(), mdRepo("tatara-operator"), "szymonrychu/tatara-operator#7|semver:patch"},
		{"gitlab", mdGitLabProject(), mdGitLabRepo("tatara-operator"), "szymonrychu/tatara-operator!7|semver:patch"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := mdTask("t1", "implement", tatarav1alpha1.StageReviewing)
			mr := mdMR(task, "tatara-operator", 7)
			mr.Status.Significance = "patch"
			c := newMirrorClient(t, tc.proj, mdSecret(), tc.repo, task, mr)

			f := newFakeForge(t)
			d := mdNewDriver(t, f, c)
			if err := d.ProjectSemverLabel(context.Background(), tc.proj, tc.repo, mr); err != nil {
				t.Fatalf("ProjectSemverLabel: %v", err)
			}
			requireLabels(t, "added", f.addedLabels, []string{tc.wantRef})
		})
	}
}

// The label is projected BY THE MERGEREQUEST RECONCILER, the moment the
// implement outcome stamps status.significance - not by some sweep an hour
// later. This is the wiring test: without it the projection is dead code.
func TestMergeRequestReconcilerProjectsSemverLabel(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageReviewing)
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.Significance = "minor"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	r := &MergeRequestReconciler{
		Client: c,
		Driver: mdNewDriver(t, f, c),
		Now:    mdNow,
	}
	if _, err := r.Reconcile(context.Background(), reqFor(mr)); err != nil {
		t.Fatalf("reconcile mr: %v", err)
	}
	requireLabels(t, "added", f.addedLabels, []string{"szymonrychu/tatara-operator#7|semver:minor"})
}

// THE MERGE GATE. The label must be on the PR BEFORE it merges: CI reads it at
// the merge commit. A merge that lands first and a label that lands second is a
// release that never gets tagged.
func TestMergeProjectsSemverLabelBeforeMerging(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.ReviewedSHA = "sha-a"
	mr.Status.Status = "approved"
	mr.Status.Significance = "minor"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.head[7] = "sha-a"
	d := mdNewDriver(t, f, c)

	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	requireLabels(t, "added", f.addedLabels, []string{"szymonrychu/tatara-operator#7|semver:minor"})
	if f.mergeCalls != 1 {
		t.Fatalf("merge calls = %d, want 1", f.mergeCalls)
	}
	if len(f.mergesAtLabel) != 1 || f.mergesAtLabel[0] != 0 {
		t.Fatalf("the semver label was applied AFTER the merge (merges at label time = %v); CI cuts the tag from the label at the merge commit",
			f.mergesAtLabel)
	}
}

// An MR with NO declared significance about to MERGE is the exact wedge: CI cuts
// no tag, nothing publishes, no pin propagates, deployedAt is never stamped, and
// the Task sits in deploying until the budget parks it. /outcome makes it
// unreachable (changeSignificance is REQUIRED on action=submitted), so this is
// defence in depth - but it is LOUD, never silent.
func TestMergeWithoutSignificanceIsLoud(t *testing.T) {
	before := testutil.ToFloat64(obs.SemverLabelMissingTotal.WithLabelValues("tatara-operator"))

	task := mdTask("t1", "implement", tatarav1alpha1.StageMerging)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.ReviewedSHA = "sha-a"
	mr.Status.Status = "approved"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.head[7] = "sha-a"
	d := mdNewDriver(t, f, c)

	if _, err := d.ReconcileMerging(context.Background(), mdProject(), task); err != nil {
		t.Fatalf("ReconcileMerging: %v", err)
	}
	if len(f.addedLabels) != 0 {
		t.Fatalf("the operator INVENTED a significance: %v", f.addedLabels)
	}
	after := testutil.ToFloat64(obs.SemverLabelMissingTotal.WithLabelValues("tatara-operator"))
	if after-before != 1 {
		t.Fatalf("operator_semver_label_missing_total{repo=tatara-operator} moved by %v, want 1: an untaggable merge MUST be visible", after-before)
	}
}

func requireLabels(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s labels = %v, want %v", what, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s labels[%d] = %q, want %q (all: %v)", what, i, got[i], want[i], got)
		}
	}
}

func mdNow() time.Time { return time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC) }
