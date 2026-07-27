package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// seedHistoryIssue creates one Issue CR with an explicit mirrored creation time
// (ageDays ago) so ordering is deterministic. The author is the fixture bot,
// which is what a genuine operator-filed proposal always looks like.
func seedHistoryIssue(t *testing.T, c client.Client, ns, proj, kind string,
	number, ageDays int, state, status string, comments ...tatarav1alpha1.Comment) {

	t.Helper()
	ctx := context.Background()
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.IssueName("r1", number), Namespace: ns},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: "r1", Number: number, ProjectRef: proj, ProposalKind: kind,
		},
	}
	if err := c.Create(ctx, iss); err != nil {
		t.Fatalf("create issue %d: %v", number, err)
	}
	at := metav1.NewTime(time.Now().Add(-time.Duration(ageDays) * 24 * time.Hour))
	iss.Status.CreatedAt = &at
	iss.Status.State, iss.Status.Status = state, status
	iss.Status.Author = testBotLogin
	iss.Status.Title = fmt.Sprintf("title-%d", number)
	iss.Status.Body = fmt.Sprintf("body-%d", number)
	iss.Status.Comments = comments
	if err := c.Status().Update(ctx, iss); err != nil {
		t.Fatalf("status update issue %d: %v", number, err)
	}
}

// historyProject builds a Project with the given history window. Spec.Scm.Cron
// is a POINTER, so it has to be addressed, not embedded by value.
func historyProject(window int) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "tatara"},
		Spec: tatarav1alpha1.ProjectSpec{
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github", Owner: "o", BotLogin: testBotLogin,
				Cron: &tatarav1alpha1.ScmCron{
					Brainstorm: tatarav1alpha1.BrainstormActivity{Enabled: true, HistoryWindow: &window},
				},
			},
		},
	}
}

func brainstormTaskFixture() *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "brainstorm-1", Namespace: "tatara"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "demo", Kind: "brainstorm", Goal: "g"},
	}
}

func TestProposalHistoryReadsIssueCRsNewestFirstWithinTheWindow(t *testing.T) {
	ctx := context.Background()
	c := newMirrorClient(t)
	// Newest to oldest: 5 (1d, open), 4 (2d, approved), 3 (3d, declined via
	// close), 2 (4d, declined via rejected), 1 (5d, open).
	seedHistoryIssue(t, c, "tatara", "demo", "brainstorm", 5, 1, "open", "new")
	seedHistoryIssue(t, c, "tatara", "demo", "brainstorm", 4, 2, "open", "approved")
	seedHistoryIssue(t, c, "tatara", "demo", "brainstorm", 3, 3, "closed", "new")
	seedHistoryIssue(t, c, "tatara", "demo", "brainstorm", 2, 4, "open", "rejected")
	seedHistoryIssue(t, c, "tatara", "demo", "brainstorm", 1, 5, "open", "new")
	// Neither of these may appear.
	seedHistoryIssue(t, c, "tatara", "demo", "incident", 9, 0, "open", "new")
	seedHistoryIssue(t, c, "tatara", "demo", "", 8, 0, "open", "new")

	r := &TaskReconciler{Client: c}
	got, err := r.proposalHistory(ctx, historyProject(3), brainstormTaskFixture(), stage.AgentBrainstorm)
	if err != nil {
		t.Fatalf("proposalHistory: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("proposalHistory returned %d entries, want 3 (the window caps the block): %+v", len(got), got)
	}
	for i, want := range []struct {
		number int
		status string
	}{{5, "open"}, {4, "approved"}, {3, "declined"}} {
		t.Run(fmt.Sprintf("entry-%d", i), func(t *testing.T) {
			if got[i].Number != want.number {
				t.Fatalf("entry %d is #%d, want #%d: newest first, so the most recent verdicts are the ones the agent always sees",
					i, got[i].Number, want.number)
			}
			if got[i].Status != want.status {
				t.Fatalf("entry %d (#%d) has status %q, want %q", i, got[i].Number, got[i].Status, want.status)
			}
		})
	}
	if got[0].Repo != "r1" || got[0].Title != "title-5" || got[0].Body != "body-5" {
		t.Fatalf("the newest entry lost its mirrored fields: %+v", got[0])
	}
}

func TestProposalHistoryDerivesDeclinedFromAPlainClose(t *testing.T) {
	ctx := context.Background()
	c := newMirrorClient(t)
	seedHistoryIssue(t, c, "tatara", "demo", "brainstorm", 1, 1, "closed", "new")

	r := &TaskReconciler{Client: c}
	got, err := r.proposalHistory(ctx, historyProject(20), brainstormTaskFixture(), stage.AgentBrainstorm)
	if err != nil {
		t.Fatalf("proposalHistory: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("proposalHistory returned %d entries, want 1", len(got))
	}
	if got[0].Status != "declined" {
		t.Fatalf("status = %q, want declined: a maintainer who just closes the issue has discarded the proposal", got[0].Status)
	}
}

// TestProposalHistoryCarriesTheCommentThread pins the whole point of the block:
// a status flag alone loses WHY the maintainer said no.
func TestProposalHistoryCarriesTheCommentThread(t *testing.T) {
	ctx := context.Background()
	c := newMirrorClient(t)
	seedHistoryIssue(t, c, "tatara", "demo", "brainstorm", 1, 1, "closed", "rejected",
		tatarav1alpha1.Comment{ExternalID: "c1", Author: testBotLogin, Body: "filed", IsBot: true,
			CreatedAt: metav1.NewTime(time.Now().Add(-2 * time.Hour))},
		tatarav1alpha1.Comment{ExternalID: "c2", Author: "maintainer", Body: "we tried this in 2024",
			CreatedAt: metav1.NewTime(time.Now().Add(-time.Hour))},
	)

	r := &TaskReconciler{Client: c}
	got, err := r.proposalHistory(ctx, historyProject(20), brainstormTaskFixture(), stage.AgentBrainstorm)
	if err != nil {
		t.Fatalf("proposalHistory: %v", err)
	}
	if len(got) != 1 || len(got[0].Comments) != 2 {
		t.Fatalf("want 1 entry with 2 comments, got %+v", got)
	}
	if got[0].Comments[1].Body != "we tried this in 2024" || got[0].Comments[1].IsBot {
		t.Fatalf("the maintainer's verdict comment did not survive: %+v", got[0].Comments[1])
	}
}

func TestProposalHistoryNilCases(t *testing.T) {
	tests := []struct {
		name      string
		proj      *tatarav1alpha1.Project
		agentKind string
	}{
		{"the block is brainstorm-only", historyProject(20), stage.AgentClarify},
		{"an explicit zero window disables the block", historyProject(0), stage.AgentBrainstorm},
		{"a project with no scm has no proposals", &tatarav1alpha1.Project{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "tatara"},
		}, stage.AgentBrainstorm},
		{"a project with no cron block does not panic", &tatarav1alpha1.Project{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "tatara"},
			Spec: tatarav1alpha1.ProjectSpec{Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github", Owner: "o", BotLogin: testBotLogin}},
		}, stage.AgentBrainstorm},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c := newMirrorClient(t)
			seedHistoryIssue(t, c, "tatara", "demo", "brainstorm", 1, 1, "open", "new")

			r := &TaskReconciler{Client: c}
			got, err := r.proposalHistory(ctx, tc.proj, brainstormTaskFixture(), tc.agentKind)
			if err != nil {
				t.Fatalf("proposalHistory: %v", err)
			}
			if got != nil {
				t.Fatalf("proposalHistory returned %+v, want nil", got)
			}
		})
	}
}

// TestProposalHistoryIgnoresOtherProjects is the multi-tenant guard: Issue CRs
// share a namespace, so the project filter is the only thing keeping one
// project's killed ideas out of another project's brainstorm.
func TestProposalHistoryIgnoresOtherProjects(t *testing.T) {
	ctx := context.Background()
	c := newMirrorClient(t)
	seedHistoryIssue(t, c, "tatara", "demo", "brainstorm", 1, 1, "open", "new")
	seedHistoryIssue(t, c, "tatara", "other", "brainstorm", 2, 1, "open", "new")

	r := &TaskReconciler{Client: c}
	got, err := r.proposalHistory(ctx, historyProject(20), brainstormTaskFixture(), stage.AgentBrainstorm)
	if err != nil {
		t.Fatalf("proposalHistory: %v", err)
	}
	if len(got) != 1 || got[0].Number != 1 {
		t.Fatalf("another project's proposals leaked into the block: %+v", got)
	}
}

// TestRenderBundleCarriesTheProposalHistory is the END-TO-END wiring check:
// renderBundle is contract E, the ENTIRE turn-0 text, so this is the only
// assertion that proves the block actually reaches an agent pod.
func TestRenderBundleCarriesTheProposalHistory(t *testing.T) {
	ctx := context.Background()
	c := newMirrorClient(t)
	seedHistoryIssue(t, c, "tatara", "demo", "brainstorm", 2, 1, "open", "new")
	seedHistoryIssue(t, c, "tatara", "demo", "brainstorm", 1, 5, "closed", "rejected",
		tatarav1alpha1.Comment{ExternalID: "c1", Author: "maintainer", Body: "we tried this in 2024",
			CreatedAt: metav1.NewTime(time.Now().Add(-time.Hour))},
	)

	r := &TaskReconciler{Client: c}
	task := brainstormTaskFixture()
	out, err := r.renderBundle(ctx, historyProject(20), task, stage.AgentBrainstorm)
	if err != nil {
		t.Fatalf("renderBundle: %v", err)
	}
	for _, want := range []string{
		`<proposal_history count="2" total="2">`,
		`<proposal repo="r1" number="2" status="open"`,
		`<proposal repo="r1" number="1" status="declined"`,
		`bot="false">we tried this in 2024</comment>`,
	} {
		t.Run(want, func(t *testing.T) {
			if !strings.Contains(out, want) {
				t.Fatalf("turn-0 bundle is missing %q:\n%s", want, out)
			}
		})
	}
}

// TestRenderBundleOmitsTheHistoryForOtherAgents keeps the block off every
// non-brainstorm turn-0 bundle, where it is pure byte cost.
func TestRenderBundleOmitsTheHistoryForOtherAgents(t *testing.T) {
	ctx := context.Background()
	c := newMirrorClient(t)
	seedHistoryIssue(t, c, "tatara", "demo", "brainstorm", 1, 1, "open", "new")

	r := &TaskReconciler{Client: c}
	out, err := r.renderBundle(ctx, historyProject(20), brainstormTaskFixture(), stage.AgentClarify)
	if err != nil {
		t.Fatalf("renderBundle: %v", err)
	}
	if strings.Contains(out, "<proposal_history ") {
		t.Fatalf("a clarify bundle carries the brainstorm history block:\n%s", out)
	}
}

// TestProposalHistoryReadsLegacyUnstampedProposals covers the O5 rollout path:
// a proposal minted before spec.proposalKind existed carries its provenance in
// the mirrored body plus the Spec anchor, and must still show up in the history.
func TestProposalHistoryReadsLegacyUnstampedProposals(t *testing.T) {
	ctx := context.Background()
	c := newMirrorClient(t)
	body := markedBody(tatarav1alpha1.ProposalKindBrainstorm)
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.IssueName("r1", 7), Namespace: "tatara"},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: "r1", Number: 7, ProjectRef: "demo",
			ProposalBodyHash: tatarav1alpha1.ComputeProposalContentHash(body),
		},
	}
	if err := c.Create(ctx, iss); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	at := metav1.NewTime(time.Now().Add(-time.Hour))
	iss.Status.CreatedAt = &at
	iss.Status.State, iss.Status.Status = "closed", "new"
	iss.Status.Author, iss.Status.Body = testBotLogin, body
	if err := c.Status().Update(ctx, iss); err != nil {
		t.Fatalf("status update: %v", err)
	}

	r := &TaskReconciler{Client: c}
	got, err := r.proposalHistory(ctx, historyProject(20), brainstormTaskFixture(), stage.AgentBrainstorm)
	if err != nil {
		t.Fatalf("proposalHistory: %v", err)
	}
	if len(got) != 1 || got[0].Status != "declined" {
		t.Fatalf("a legacy unstamped proposal is missing from the history: %+v", got)
	}
}
