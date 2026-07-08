package controller

import (
	"context"
	"slices"

	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// gateReason is why a bot comment was withheld. gateOpen means post.
type gateReason string

const (
	gateOpen     gateReason = ""
	gateBotMR    gateReason = "bot_mr"    // rule 2: never comment on the bot's own MR
	gateLastWord gateReason = "last_word" // rule 1: bot already had the last word
)

// commentSilenceBreakers returns the deduped set of logins whose comment breaks
// the bot's silence: the reporter intake allowlist unioned with the
// maintainer/approver allowlist for this repo. An empty result means no lists are
// configured, in which case any non-bot author breaks silence.
func commentSilenceBreakers(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository) []string {
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
// and no silence-breaker has commented since. A comment breaks silence iff its
// author is non-empty, not the bot, and (breakers empty OR author in breakers).
// Order is by CreatedAt, robust to SCM list ordering.
func botHasLastWordAmong(comments []scm.IssueComment, botLogin string, breakers []string) bool {
	var tBot, tBreak int64 = -1, -1
	for _, c := range comments {
		ts := c.CreatedAt.UnixNano()
		switch {
		case c.Author == botLogin:
			if ts > tBot {
				tBot = ts
			}
		case c.Author != "" && (len(breakers) == 0 || slices.Contains(breakers, c.Author)):
			if ts > tBreak {
				tBreak = ts
			}
		}
	}
	if tBot < 0 {
		return false
	}
	return tBreak <= tBot
}

// resolveBotMR reports whether the PR/MR at number is authored by the bot.
// Prefers the pre-known hint (TaskSource.AuthorLogin); else reads GetPRState.
// A read error resolves to false (fall through to the rule-1 turn-taking gate).
func resolveBotMR(ctx context.Context, writer scm.SCMWriter, repoURL, token string, number int, botLogin, hint string) bool {
	if hint != "" {
		return hint == botLogin
	}
	if writer == nil {
		return false
	}
	st, err := writer.GetPRState(ctx, repoURL, token, number)
	if err != nil {
		return false
	}
	return st.Author == botLogin
}

// decideCommentGate reports whether a bot comment must be withheld and why.
// Rule 2 (bot MR) short-circuits before any comment listing. Rule 1 lists the
// conversation and applies botHasLastWordAmong. Fail-open (gateOpen) on missing
// inputs or read errors, matching botHadLastWord / humanCommentAfter so a lost
// webhook can still be recovered by a later scan.
func decideCommentGate(ctx context.Context, reader scm.SCMReader, writer scm.SCMWriter, owner, name, repoURL, token string, number int, isPR bool, botLogin, authorHint string, breakers []string) gateReason {
	if botLogin == "" || reader == nil || owner == "" {
		return gateOpen
	}
	if isPR && resolveBotMR(ctx, writer, repoURL, token, number, botLogin, authorHint) {
		return gateBotMR
	}
	var (
		comments []scm.IssueComment
		err      error
	)
	if isPR {
		if pl, ok := reader.(scm.PRCommentLister); ok {
			comments, err = pl.ListPRComments(ctx, owner, name, number)
		} else {
			comments, err = reader.ListIssueComments(ctx, owner, name, number)
		}
	} else {
		comments, err = reader.ListIssueComments(ctx, owner, name, number)
	}
	if err != nil {
		return gateOpen
	}
	if botHasLastWordAmong(comments, botLogin, breakers) {
		return gateLastWord
	}
	return gateOpen
}

// commentGateReason resolves the reader + silence-breakers for the task's
// project/repo and returns the gate decision for a bot comment on (number,isPR).
// Fail-open (gateOpen) when ReaderFor is nil, the reader cannot be built, or the
// repo URL is unsplittable.
func (r *TaskReconciler) commentGateReason(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, writer scm.SCMWriter, token, provider string, number int, isPR bool, authorHint string) gateReason {
	botLogin := ""
	if proj != nil && proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	if botLogin == "" || r.ReaderFor == nil || repo == nil {
		return gateOpen
	}
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return gateOpen
	}
	reader, err := r.ReaderFor(provider, token)
	if err != nil {
		return gateOpen
	}
	breakers := commentSilenceBreakers(proj, repo)
	return decideCommentGate(ctx, reader, writer, owner, name, repo.Spec.URL, token, number, isPR, botLogin, authorHint, breakers)
}

// gatedComment posts body to ref via writer.Comment unless commentGateReason
// withholds it. A withheld post is (false, nil) and records
// SCMWrite(provider,"comment","suppressed_<reason>"). ref is the SCM ref the
// caller already builds so provider sigils stay identical.
func (r *TaskReconciler) gatedComment(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, writer scm.SCMWriter, token, provider string, number int, isPR bool, authorHint, ref, body string) (bool, error) {
	reason := r.commentGateReason(ctx, proj, repo, writer, token, provider, number, isPR, authorHint)
	if reason != gateOpen {
		if r.Metrics != nil {
			r.Metrics.SCMWrite(provider, "comment", "suppressed_"+string(reason))
		}
		log.FromContext(ctx).Info("scm comment suppressed",
			"action", "scm_comment_suppressed", "reason", string(reason), "ref", ref)
		return false, nil
	}
	err := writer.Comment(ctx, token, ref, body)
	r.recordSCM(provider, "comment", err)
	return err == nil, err
}
