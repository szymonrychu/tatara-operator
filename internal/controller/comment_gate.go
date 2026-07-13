package controller

import (
	"regexp"
	"slices"
	"strings"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// volatileTokenRE strips turn counters and RFC3339 timestamps so two bot
// comments differing only in those render identically for dedup purposes.
var volatileTokenRE = regexp.MustCompile(`(?i)\bturn\s+\d+\b|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})?`)

// normalizeCommentBody collapses whitespace, lowercases, and strips volatile
// tokens so two comments are compared on stable content only.
func normalizeCommentBody(body string) string {
	stripped := volatileTokenRE.ReplaceAllString(body, "")
	return strings.ToLower(strings.Join(strings.Fields(stripped), " "))
}

// NormalizeCommentBody is the exported form for restapi reuse (rule 3 parity).
func NormalizeCommentBody(body string) string { return normalizeCommentBody(body) }

// duplicateRecentBotComment reports whether body normalizes identically to a
// bot comment already present in the thread window.
func duplicateRecentBotComment(comments []scm.IssueComment, botLogin, body string) bool {
	want := normalizeCommentBody(body)
	if want == "" {
		return false
	}
	for _, c := range comments {
		if c.Author == botLogin && normalizeCommentBody(c.Body) == want {
			return true
		}
	}
	return false
}

// DuplicateRecentBotComment is the exported form for restapi reuse.
func DuplicateRecentBotComment(comments []scm.IssueComment, botLogin, body string) bool {
	return duplicateRecentBotComment(comments, botLogin, body)
}

// CommentSilenceBreakers returns the deduped set of logins whose comment breaks
// the bot's silence: the reporter intake allowlist unioned with the
// maintainer/approver allowlist for this repo. An empty result means no lists are
// configured, in which case any non-bot author breaks silence. Exported so the
// MCP/REST comment boundary (restapi) can feed the same breaker set into
// PermitComment as the reconciler-side decideCommentGate does.
func CommentSilenceBreakers(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository) []string {
	seen := map[string]bool{}
	var out []string
	add := func(list []string) {
		for _, l := range list {
			if l != "" && !seen[l] {
				seen[l] = true
				out = append(out, l)
			}
		}
	}
	add(tatarav1alpha1.EffectiveReporterLogins(proj, repo))
	add(tatarav1alpha1.EffectiveMaintainerLogins(proj, repo))
	return out
}

// botHasLastWordAmong reports whether the bot must stay silent: it has a comment
// and no silence-breaker has commented at-or-after it. A comment breaks silence
// iff its author is non-empty, not the bot, and (breakers empty OR author in
// breakers). Order is by CreatedAt, robust to SCM list ordering. Zero-timestamp
// comments are skipped (a parse failure must not silently swing the decision).
// Ties favour posting: a breaker whose comment shares the bot's timestamp (SCM
// created_at is second-granularity) counts as "after", so a same-second human
// reply is never suppressed.
func botHasLastWordAmong(comments []scm.IssueComment, botLogin string, breakers []string) bool {
	var tBot, tBreak time.Time
	var haveBot bool
	for _, c := range comments {
		if c.CreatedAt.IsZero() {
			continue
		}
		switch {
		case c.Author == botLogin:
			if !haveBot || c.CreatedAt.After(tBot) {
				tBot = c.CreatedAt
				haveBot = true
			}
		case c.Author != "" && (len(breakers) == 0 || slices.Contains(breakers, c.Author)):
			if c.CreatedAt.After(tBreak) {
				tBreak = c.CreatedAt
			}
		}
	}
	if !haveBot {
		return false
	}
	return tBreak.IsZero() || tBreak.Before(tBot)
}

// PermitComment is the permission-layer self-comment guard enforced at the MCP/
// REST comment boundary (CROSS-REPO-CONTRACT "Self-comment guard is
// PERMISSION-LAYER"). It REFUSES an agent's comment when the last comment on the
// thread is tatara(bot)-authored, so the bot never answers its own comment in a
// loop (the Replit-postmortem guardrail-in-the-permission-layer, not the prompt).
// It consolidates the comment-time bot-last-word predicates (botHasLastWordAmong,
// the triage isTataraAuthored/botHasLastWord family) behind one call site so the
// refusal is uniform across kinds.
//
// SOLE exceptions: kind=="refine" is always permitted - the backlog refiner is the
// one kind allowed to answer tatara's own prior comment (e.g. a sharper-scope note
// on a gave-up issue, or a "scope already delivered" reply). kind=="incident" is
// also always permitted - an incident agent posts sequential evidence/status
// updates on its own tracker issue as investigation progresses, which is expected
// self-follow-up, not a runaway loop. Both carve-outs are kept deliberately narrow:
// only the exact strings "refine" and "incident".
//
// Fail-open matches the rest of the family: an empty botLogin (the guard cannot be
// evaluated) permits, and callers permit on a comment-list read error, so a lost
// webhook is still recoverable by a later scan. The returned reason is
// machine-readable ("bot_last_word") so the pod skill can react to a refusal.
func PermitComment(kind string, comments []scm.IssueComment, botLogin string, breakers []string) (bool, string) {
	if kind == "refine" || kind == "incident" {
		return true, ""
	}
	if botLogin == "" {
		return true, ""
	}
	if botHasLastWordAmong(comments, botLogin, breakers) {
		return false, "bot_last_word"
	}
	return true, ""
}
