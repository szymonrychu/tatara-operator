package controller

// TDD tests for the brainstorm dedup feature (PART A).
// These tests assert:
//   1. brainstormGoalProject embeds dedup-first instructions + issues context.
//   2. brainstorm() calls ListOpenIssues across all repos to build the context
//      and passes it to brainstormGoalProject.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// TestBrainstormGoalProject_ContainsDedupInstructions checks that the goal
// produced by brainstormGoalProject instructs the agent to survey existing open
// issues FIRST and decide between duplicate/comment/propose before calling
// propose_issue.
func TestBrainstormGoalProject_ContainsDedupInstructions(t *testing.T) {
	slugs := []string{"o/alpha", "o/beta"}
	ctx := "o/alpha#1 [] Fix login bug\no/beta#3 [enhancement] Add caching layer"
	g := brainstormGoalProject(slugs, ctx)

	// Must still name repos and skill.
	for _, slug := range slugs {
		if !strings.Contains(g, slug) {
			t.Fatalf("goal missing slug %q: %s", slug, g)
		}
	}
	if !strings.Contains(g, "tatara-deep-research") {
		t.Fatalf("goal does not reference tatara-deep-research: %s", g)
	}

	// Dedup-first: must instruct agent to check existing issues first.
	for _, kw := range []string{"duplicate", "comment_on_issue", "propose_issue"} {
		if !strings.Contains(g, kw) {
			t.Fatalf("goal missing dedup keyword %q: %s", kw, g)
		}
	}

	// Issues context block must appear verbatim in the goal.
	if !strings.Contains(g, ctx) {
		t.Fatalf("goal does not embed issues context:\n%s\n\ngoal:\n%s", ctx, g)
	}
}

// TestBrainstormGoalProject_NoContext_EmptyIssueSectionOmitted checks that when
// no open issues are found the goal degrades gracefully (no panic, no stale ctx).
func TestBrainstormGoalProject_NoContext_EmptyIssueSectionOmitted(t *testing.T) {
	slugs := []string{"o/solo"}
	g := brainstormGoalProject(slugs, "")
	if !strings.Contains(g, "o/solo") {
		t.Fatalf("goal missing slug: %s", g)
	}
	// Even with no context, the dedup guidance must remain.
	if !strings.Contains(g, "propose_issue") {
		t.Fatalf("goal missing propose_issue with no issues ctx: %s", g)
	}
}

// TestBrainstorm_ConsultsListOpenIssuesForContext verifies that when brainstorm()
// runs it calls ListOpenIssues for each repo (the dedup context step) and the
// resulting task goal embeds at least one issue title from the fake issues.
func TestBrainstorm_ConsultsListOpenIssuesForContext(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-dedup-ctx", []string{"o/ctx1", "o/ctx2"}, 5)

	capturedGoals := &goalCapturingReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/ctx1": {
				{Repo: "o/ctx1", Number: 7, Title: "improve caching layer", Labels: []string{"enhancement"}, UpdatedAt: time.Now()},
			},
			"o/ctx2": {
				{Repo: "o/ctx2", Number: 2, Title: "fix login redirect", Labels: []string{}, UpdatedAt: time.Now()},
			},
		},
	}

	r := newScanReconciler(capturedGoals)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 5}
	budget := 99
	r.brainstorm(context.Background(), proj, capturedGoals, repos, nil, act, &budget)

	tasks := listBrainstormTasks(t, "bs-dedup-ctx")
	if len(tasks) != 1 {
		t.Fatalf("want 1 brainstorm task, got %d", len(tasks))
	}
	goal := tasks[0].Spec.Goal

	// The issue titles must be embedded in the task goal.
	if !strings.Contains(goal, "improve caching layer") {
		t.Fatalf("task goal missing issue title 'improve caching layer':\n%s", goal)
	}
	if !strings.Contains(goal, "fix login redirect") {
		t.Fatalf("task goal missing issue title 'fix login redirect':\n%s", goal)
	}

	// ListOpenIssues must have been called for both repos (context building pass).
	if capturedGoals.queriedRepos["o/ctx1"] < 1 {
		t.Fatalf("ListOpenIssues not called for o/ctx1; queries: %v", capturedGoals.queriedRepos)
	}
	if capturedGoals.queriedRepos["o/ctx2"] < 1 {
		t.Fatalf("ListOpenIssues not called for o/ctx2; queries: %v", capturedGoals.queriedRepos)
	}
}

// TestBrainstorm_IssuesContextCappedAt60 verifies that if >60 issues are open
// across all repos the context is capped and a "(+N more omitted)" note appears
// in the task goal.
func TestBrainstorm_IssuesContextCappedAt60(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-dedup-cap60", []string{"o/big"}, 200)

	// 70 issues - 10 more than the cap of 60.
	issues := make([]scm.IssueRef, 70)
	for i := range issues {
		issues[i] = scm.IssueRef{
			Repo:      "o/big",
			Number:    i + 1,
			Title:     "issue title " + strings.Repeat("x", i),
			UpdatedAt: time.Now(),
		}
	}
	reader := &goalCapturingReader{
		issuesByRepo: map[string][]scm.IssueRef{"o/big": issues},
	}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 200}
	budget := 99
	r.brainstorm(context.Background(), proj, reader, repos, nil, act, &budget)

	tasks := listBrainstormTasks(t, "bs-dedup-cap60")
	if len(tasks) != 1 {
		t.Fatalf("want 1 brainstorm task, got %d", len(tasks))
	}
	goal := tasks[0].Spec.Goal
	if !strings.Contains(goal, "omitted") {
		t.Fatalf("task goal should contain 'omitted' truncation note:\n%s", goal)
	}
}

// goalCapturingReader records which repos were queried for issues and delegates
// to the issues map. It does NOT add issues to the backlog (IsPR=false means
// proposalBacklog counts them only if they carry the brainstorming label).
type goalCapturingReader struct {
	fakeReader
	issuesByRepo      map[string][]scm.IssueRef
	queriedRepos      map[string]int
	commentsBySlugNum map[string][]scm.IssueComment
}

func (g *goalCapturingReader) ListOpenIssues(_ context.Context, owner, repo string) ([]scm.IssueRef, error) {
	if g.queriedRepos == nil {
		g.queriedRepos = map[string]int{}
	}
	slug := owner + "/" + repo
	g.queriedRepos[slug]++
	return g.issuesByRepo[slug], nil
}

func (g *goalCapturingReader) ListIssueComments(_ context.Context, owner, repo string, number int) ([]scm.IssueComment, error) {
	key := fmt.Sprintf("%s/%s#%d", owner, repo, number)
	return g.commentsBySlugNum[key], nil
}

// TestGoalProjects_NoReCommentInstruction verifies both project goals tell the
// agent not to re-comment a [bot-engaged] issue and to prefer new improvements.
func TestGoalProjects_NoReCommentInstruction(t *testing.T) {
	slugs := []string{"o/alpha"}
	ctx := "o/alpha#1 [] Fix login bug [bot-engaged]"
	for _, g := range []string{
		brainstormGoalProject(slugs, ctx),
		healthCheckGoalProject(slugs, ctx),
	} {
		if !strings.Contains(g, "[bot-engaged]") {
			t.Fatalf("goal does not reference the bot-engaged marker:\n%s", g)
		}
		// Must instruct: do not comment again on a bot-engaged issue.
		if !strings.Contains(g, "do NOT comment again") {
			t.Fatalf("goal missing no-re-comment instruction:\n%s", g)
		}
		// Must still embed the context and keep the three action verbs.
		for _, kw := range []string{"comment_on_issue", "propose_issue", ctx} {
			if !strings.Contains(g, kw) {
				t.Fatalf("goal missing %q:\n%s", kw, g)
			}
		}
	}
}

// TestBrainstorm_BotEngagedIssueFlagged verifies that an issue the bot already
// commented on is marked [bot-engaged] in the goal, while an untouched issue is not.
func TestBrainstorm_BotEngagedIssueFlagged(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-botengaged", []string{"o/eng1", "o/eng2"}, 5)

	reader := &goalCapturingReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/eng1": {
				{Repo: "o/eng1", Number: 7, Title: "improve caching layer", UpdatedAt: time.Now()},
			},
			"o/eng2": {
				{Repo: "o/eng2", Number: 2, Title: "fix login redirect", UpdatedAt: time.Now()},
			},
		},
		commentsBySlugNum: map[string][]scm.IssueComment{
			// Bot already commented on o/eng1#7 (off-limits); o/eng2#2 untouched.
			"o/eng1#7": {{Author: "tatara-bot", Body: "looking into this"}},
		},
	}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 5}
	budget := 99
	r.brainstorm(context.Background(), proj, reader, repos, nil, act, &budget)

	tasks := listBrainstormTasks(t, "bs-botengaged")
	if len(tasks) != 1 {
		t.Fatalf("want 1 brainstorm task, got %d", len(tasks))
	}
	goal := tasks[0].Spec.Goal

	if !strings.Contains(goal, "o/eng1#7 [] improve caching layer [bot-engaged]") {
		t.Fatalf("bot-engaged issue not flagged:\n%s", goal)
	}
	if strings.Contains(goal, "fix login redirect [bot-engaged]") {
		t.Fatalf("untouched issue wrongly flagged:\n%s", goal)
	}
}
