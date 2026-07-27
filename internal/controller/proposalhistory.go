package controller

import (
	"context"
	"fmt"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/prompt"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ProposalHistoryFor is the <proposal_history> block for a brainstorm bundle:
// the most recent ResolveHistoryWindow() brainstorm-provenance Issue CRs in the
// project, newest first, each with its display status and its full comment
// thread. Non-brainstorm agent kinds get nil.
//
// It is the ONLY place a brainstorm agent can see a DECLINED proposal. A
// discarded proposal's forge issue is closed, so the agent's own dedup scan over
// OPEN issues cannot see it and would happily re-propose the idea the maintainer
// already killed. The comments come with it because they carry WHY, which a bare
// status flag loses.
//
// It is EXPORTED, and that is the point: there are TWO prompt.Render call sites
// (the operator's own turn-0 dispatch and internal/restapi's task_context
// handler), and tatara-brainstorm-guardrails actively tells the brainstorm agent
// it may re-read its own bundle with task_context(task=<name>) mid-turn. A block
// that silently vanished on that re-read - while the bundle's standing trailer
// still named <proposal_history> as present - would reopen the exact
// re-propose-a-declined-idea failure this block exists to close. Any future
// third Render call site must call this too.
//
// Everything is read from the Issue CR mirror in etcd, so the block costs no
// extra SCM API calls, and the byte budget is enforced downstream by
// prompt.Render (which evicts bot comments, then whole entries oldest-first).
//
// agentKind must be the string the bundle itself renders as agent="...", i.e.
// prompt.AgentKind(task), or the assignment text and the bundle can disagree
// about whether the block is there.
func ProposalHistoryFor(ctx context.Context, c client.Client, proj *tatarav1alpha1.Project,
	agentKind string) ([]prompt.ProposalHistoryEntry, error) {

	// Scm.Cron is a POINTER, so both hops need a guard: a brainstorm-disabled
	// project with no cron block at all would otherwise panic here.
	if agentKind != stage.AgentBrainstorm || proj.Spec.Scm == nil || proj.Spec.Scm.Cron == nil {
		return nil, nil
	}
	window := proj.Spec.Scm.Cron.Brainstorm.ResolveHistoryWindow()
	if window <= 0 {
		return nil, nil
	}
	// proj.Namespace, matching projectProposalIssues: the Project is the
	// authority for where its Issue CRs live, not whichever Task happens to be
	// asking.
	var list tatarav1alpha1.IssueList
	if err := c.List(ctx, &list, client.InNamespace(proj.Namespace)); err != nil {
		return nil, fmt.Errorf("proposal history for %s: list issues: %w", proj.Name, err)
	}
	botLogin := botLoginOf(proj)
	proposals := make([]*tatarav1alpha1.Issue, 0, len(list.Items))
	for i := range list.Items {
		it := &list.Items[i]
		if it.Spec.ProjectRef != proj.Name {
			continue
		}
		if effectiveProposalKind(it, botLogin) == tatarav1alpha1.ProposalKindBrainstorm {
			proposals = append(proposals, it)
		}
	}
	sort.SliceStable(proposals, func(i, j int) bool {
		a, b := proposalCreatedAt(proposals[i]), proposalCreatedAt(proposals[j])
		if !a.Equal(&b) {
			return a.After(b.Time) // newest first
		}
		return proposals[i].Name < proposals[j].Name
	})
	if len(proposals) > window {
		proposals = proposals[:window]
	}
	if len(proposals) == 0 {
		return nil, nil
	}
	out := make([]prompt.ProposalHistoryEntry, 0, len(proposals))
	for _, p := range proposals {
		out = append(out, prompt.ProposalHistoryEntry{
			Repo:     p.Spec.RepositoryRef,
			Number:   p.Spec.Number,
			Status:   proposalDisplayStatus(p),
			Title:    p.Status.Title,
			Body:     p.Status.Body,
			At:       proposalCreatedAt(p),
			Comments: p.Status.Comments,
		})
	}
	return out, nil
}

// proposalCreatedAt prefers the mirrored forge creation time and falls back to
// the CR's own creation timestamp when the mirror has not synced yet.
func proposalCreatedAt(iss *tatarav1alpha1.Issue) metav1.Time {
	if iss.Status.CreatedAt != nil {
		return *iss.Status.CreatedAt
	}
	return iss.CreationTimestamp
}
