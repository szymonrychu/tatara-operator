package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// TestGatedCommentCore_NilRepo_ClosedStateStillApplies is FIX-8: a call site
// with no Repository CR (repo==nil, e.g. deploy_supervision.go's umbrella-level
// park comments) must still honor the closed-state rule - repo is only needed
// for the repo-level login-override lookup (nil-safe fallback to project
// level), not for deriving owner/repo (which comes from ref itself).
func TestGatedCommentCore_NilRepo_ClosedStateStillApplies(t *testing.T) {
	fake := &gateFakeSCM{issueClosed: true}
	proj := &tatarav1alpha1.Project{Spec: tatarav1alpha1.ProjectSpec{Scm: &tatarav1alpha1.ScmSpec{BotLogin: "tatara-bot"}}}
	posted, err := gatedCommentCore(context.Background(), fake, fake, nil, proj, nil,
		"tok", "github", 5, false, "tatara-bot", "", "o/n#5", "hello")
	if err != nil {
		t.Fatalf("gatedCommentCore: %v", err)
	}
	if posted {
		t.Fatal("closed-state rule must suppress the comment even with repo==nil")
	}
}

// TestGatedCommentCore_NilRepo_DedupStillApplies: the content-dedup rule must
// also apply without a Repository CR.
func TestGatedCommentCore_NilRepo_DedupStillApplies(t *testing.T) {
	fake := &gateFakeSCM{comments: []scm.IssueComment{
		{Author: "tatara-bot", Body: "Deploy blocked: stuck.", CreatedAt: time.Unix(1_700_000_000, 0)},
		{Author: "human", Body: "ack", CreatedAt: time.Unix(1_700_000_100, 0)},
	}}
	proj := &tatarav1alpha1.Project{Spec: tatarav1alpha1.ProjectSpec{Scm: &tatarav1alpha1.ScmSpec{BotLogin: "tatara-bot"}}}
	posted, err := gatedCommentCore(context.Background(), fake, fake, nil, proj, nil,
		"tok", "github", 5, false, "tatara-bot", "", "o/n#5", "deploy blocked: stuck.")
	if err != nil {
		t.Fatalf("gatedCommentCore: %v", err)
	}
	if posted {
		t.Fatal("content-dedup rule must suppress the comment even with repo==nil")
	}
}

// TestGatedCommentCore_NilRepo_OpenStillPosts is the regression guard: an
// open, distinct-body comment must still post with repo==nil (the umbrella
// merge-timeout/deploy-stall park sites have no Repository CR to pass).
func TestGatedCommentCore_NilRepo_OpenStillPosts(t *testing.T) {
	fake := &gateFakeSCM{}
	proj := &tatarav1alpha1.Project{Spec: tatarav1alpha1.ProjectSpec{Scm: &tatarav1alpha1.ScmSpec{BotLogin: "tatara-bot"}}}
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	posted, err := gatedCommentCore(context.Background(), fake, fake, m, proj, nil,
		"tok", "github", 5, false, "tatara-bot", "", "o/n#5", "hello")
	if err != nil {
		t.Fatalf("gatedCommentCore: %v", err)
	}
	if !posted {
		t.Fatal("open, distinct comment must still post with repo==nil")
	}
	if !fake.commentPosted || fake.commentBody != "hello" {
		t.Fatalf("writer.Comment not called with expected body: posted=%v body=%q", fake.commentPosted, fake.commentBody)
	}
}
