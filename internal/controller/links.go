package controller

import (
	"context"
	"regexp"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

const (
	linksBlockStart = "<!-- tatara-links:start -->"
	linksBlockEnd   = "<!-- tatara-links:end -->"
)

var linksBlockRE = regexp.MustCompile(`(?s)` + regexp.QuoteMeta(linksBlockStart) + `.*?` + regexp.QuoteMeta(linksBlockEnd))

// RenderLinksBlock builds the marker-delimited managed block cross-linking
// sibling issues opened for the same Task (item 5). urls is the OTHER
// siblings only (not the issue the block is being written into).
func RenderLinksBlock(urls []string) string {
	return linksBlockStart + "\nRelated issues (same task): " + strings.Join(urls, ", ") + "\n" + linksBlockEnd
}

// SpliceLinksBlock idempotently rewrites the managed block in body: replaces
// an existing block in place, or appends a new one when absent. The rest of
// body is preserved verbatim.
func SpliceLinksBlock(body, block string) string {
	if linksBlockRE.MatchString(body) {
		return linksBlockRE.ReplaceAllString(body, block)
	}
	if body == "" {
		return block
	}
	return body + "\n\n" + block
}

// syncSiblingLinks rewrites the managed tatara-links block in every sibling
// issue so each one lists the OTHERS. Called whenever a Task's issue-kind
// ledger gains a member (item 5). No-op when fewer than 2 issue siblings
// exist (a lone issue has nothing to cross-link). Best-effort per sibling: one
// failed edit does not block the rest.
func (r *TaskReconciler) syncSiblingLinks(ctx context.Context, provider, token string, siblingURLs []string) {
	if len(siblingURLs) < 2 || r.ReaderFor == nil {
		return
	}
	reader, err := r.ReaderFor(provider, token)
	if err != nil {
		return
	}
	writer, err := r.SCMFor(provider)
	if err != nil {
		return
	}
	for _, self := range siblingURLs {
		repoSlug, number, ok := parseIssueURL(self)
		if !ok {
			continue
		}
		others := make([]string, 0, len(siblingURLs)-1)
		for _, u := range siblingURLs {
			if u != self {
				others = append(others, u)
			}
		}
		owner, name, _ := strings.Cut(repoSlug, "/")
		content, gerr := reader.GetIssue(ctx, owner, name, number)
		if gerr != nil {
			continue
		}
		newBody := SpliceLinksBlock(content.Body, RenderLinksBlock(others))
		if newBody == content.Body {
			continue // idempotent: nothing changed, skip the write
		}
		if eerr := writer.EditIssue(ctx, token, repoSlug, number, scm.EditIssueReq{Body: &newBody}); eerr != nil {
			log.FromContext(ctx).Error(eerr, "syncSiblingLinks: edit issue (non-fatal, best-effort)", "issue", self)
		}
	}
}

// discoveredIssueSiblings returns Status.DiscoveredIssues when it holds 2+
// entries (the sibling-link threshold), else nil.
func discoveredIssueSiblings(task *tatarav1alpha1.Task) []string {
	if len(task.Status.DiscoveredIssues) < 2 {
		return nil
	}
	return task.Status.DiscoveredIssues
}
