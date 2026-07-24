package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// TestReleaseGreenUsesFullProjectPathForGitLab is tatara-operator#426's
// PRIMARY fix: releaseGreen split repo.Spec.URL into owner/name (GitHub
// shape) and handed that straight to GetCommitCIStatus, so a GitLab commit
// status read hit "/projects/<owner>/repository/commits/..." with the repo
// component silently dropped -> 404 "Project Not Found". GitLab's commit
// status read wants the full project path as the sole "owner" argument.
func TestReleaseGreenUsesFullProjectPathForGitLab(t *testing.T) {
	ctx := context.Background()
	reader := &capturingCIReader{results: map[string]string{"deadbeef": "success"}}
	d := &StageDriver{
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return reader, nil },
		Now:       func() time.Time { return time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC) },
	}
	repo := &tatarav1alpha1.Repository{
		Spec: tatarav1alpha1.RepositorySpec{URL: "https://gitlab.example.com/szymonrychu/helmfile"},
	}

	green, err := d.releaseGreen(ctx, &tatarav1alpha1.Project{}, repo, "gitlab", "tok", "deadbeef")
	if err != nil {
		t.Fatalf("releaseGreen: %v", err)
	}
	if !green {
		t.Fatal("want green=true for a success status")
	}
	if len(reader.calls) != 1 {
		t.Fatalf("want exactly one GetCommitCIStatus call, got %d", len(reader.calls))
	}
	call := reader.calls[0]
	if call.owner != "szymonrychu/helmfile" || call.repo != "" {
		t.Fatalf("GetCommitCIStatus called with owner=%q repo=%q, want owner=%q repo=%q (full project path, empty repo)",
			call.owner, call.repo, "szymonrychu/helmfile", "")
	}
}
