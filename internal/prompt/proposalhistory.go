package prompt

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// ProposalHistoryEntry is one prior brainstorm proposal fed into a brainstorm
// turn-0 bundle. Everything in it is read from the Issue CR mirror, so the block
// costs no extra SCM API calls.
//
// Comments are LOAD-BEARING, not decoration: they carry WHY a maintainer
// declined a proposal, which a bare status flag loses. This is what closes the
// "immortal proposal" failure mode - a discarded proposal's forge issue is
// CLOSED, so the agent's own dedup scan over OPEN issues cannot see it and would
// happily re-propose the idea the maintainer already killed.
type ProposalHistoryEntry struct {
	Repo   string
	Number int
	// Status is the DISPLAY status: open | approved | declined.
	Status   string
	Title    string
	Body     string
	At       metav1.Time
	Comments []v1alpha1.Comment
}

type proposalCommentView struct {
	Author, At string
	Bot        bool
	Body       string
}

type proposalView struct {
	Repo, Status, Title, Body, At string
	Number                        int
	Comments                      []proposalCommentView
}

type proposalHistoryView struct {
	Total, Rendered int
	Items           []proposalView
}

// buildProposalHistory renders the first `keep` entries (the input is already
// newest-first). withBots=false drops every bot comment: bot comments render
// last and are evicted first, so a byte-starved bundle keeps the maintainer's
// words and drops the agent's own.
//
// It returns nil - which omits the element entirely - when nothing is kept. That
// is what makes the eviction ladder end in FEWER WHOLE proposals rather than a
// truncated mess: an entry is either rendered complete or not at all.
func buildProposalHistory(entries []ProposalHistoryEntry, keep int, withBots bool) *proposalHistoryView {
	if len(entries) == 0 || keep <= 0 {
		return nil
	}
	kept := entries[:min(keep, len(entries))]
	v := &proposalHistoryView{Total: len(entries), Rendered: len(kept)}
	for _, e := range kept {
		pv := proposalView{
			Repo: e.Repo, Number: e.Number, Status: e.Status,
			Title: e.Title, Body: e.Body, At: stamp(e.At),
		}
		// Humans first, bots last: the render order IS the eviction order.
		for _, pass := range []bool{false, true} {
			if pass && !withBots {
				break
			}
			for _, c := range e.Comments {
				if c.IsBot != pass {
					continue
				}
				pv.Comments = append(pv.Comments, proposalCommentView{
					Author: c.Author, At: stamp(c.CreatedAt), Bot: c.IsBot, Body: c.Body,
				})
			}
		}
		v.Items = append(v.Items, pv)
	}
	return v
}
