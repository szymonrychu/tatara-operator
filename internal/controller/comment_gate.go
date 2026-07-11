package controller

import (
	"context"
	"slices"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
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
// SOLE exception: kind=="refine" is always permitted - the backlog refiner is the
// one kind allowed to answer tatara's own prior comment (e.g. a sharper-scope note
// on a gave-up issue, or a "scope already delivered" reply). The carve-out is kept
// deliberately narrow: only the exact string "refine".
//
// Fail-open matches the rest of the family: an empty botLogin (the guard cannot be
// evaluated) permits, and callers permit on a comment-list read error, so a lost
// webhook is still recoverable by a later scan. The returned reason is
// machine-readable ("bot_last_word") so the pod skill can react to a refusal.
func PermitComment(kind string, comments []scm.IssueComment, botLogin string, breakers []string) (bool, string) {
	if kind == "refine" {
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

// resolveBotMR reports whether the PR/MR at number is authored by the bot. The
// pre-known hint (TaskSource.AuthorLogin) is trusted ONLY for provider=="github",
// where AuthorLogin is the real author. On GitLab AuthorLogin is the webhook
// actor, not the resource author (see internal/webhook/server.go), so the hint is
// ignored and the authoritative GetPRState.Author is read instead. A read error
// resolves to false (fall through to the rule-1 turn-taking gate).
func resolveBotMR(ctx context.Context, writer scm.SCMWriter, repoURL, token string, number int, botLogin, hint, provider string) bool {
	if hint != "" && provider == "github" {
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
func decideCommentGate(ctx context.Context, reader scm.SCMReader, writer scm.SCMWriter, owner, name, repoURL, token, provider string, number int, isPR bool, botLogin, authorHint string, breakers []string) gateReason {
	if botLogin == "" || reader == nil || owner == "" {
		return gateOpen
	}
	if isPR && resolveBotMR(ctx, writer, repoURL, token, number, botLogin, authorHint, provider) {
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
	breakers := CommentSilenceBreakers(proj, repo)
	return decideCommentGate(ctx, reader, writer, owner, name, repo.Spec.URL, token, provider, number, isPR, botLogin, authorHint, breakers)
}

// parkIsBotMRByHint reports whether a park target is the bot's own PR/MR using
// only the cheap author hint (reliable on GitHub, where AuthorLogin is the real
// author). Used in the parkWithComment fail-open path where scmContext, and thus
// the full gate, is unavailable. GitLab (hint is the actor) falls through to a
// post - best effort, matching resolveBotMR's provider guard.
func (r *TaskReconciler) parkIsBotMRByHint(ctx context.Context, task *tatarav1alpha1.Task) bool {
	if task.Spec.Source == nil || !task.Spec.Source.IsPR ||
		task.Spec.Source.AuthorLogin == "" || task.Spec.Source.Provider != "github" {
		return false
	}
	var proj tatarav1alpha1.Project
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &proj); err != nil || proj.Spec.Scm == nil {
		return false
	}
	return task.Spec.Source.AuthorLogin == proj.Spec.Scm.BotLogin
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
