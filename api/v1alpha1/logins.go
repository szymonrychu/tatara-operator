package v1alpha1

// Trusted-login resolution (issue #102). The reporter and maintainer/approver
// allowlists live on the Project's ScmSpec and may be overridden per-Repository:
// a nil override pointer inherits the project list; a non-nil pointer (including
// an explicit empty slice) replaces it for that repository.

// EffectiveReporterLogins returns the reporter intake allowlist in effect for
// repo, falling back to the Project's ScmSpec list when the repo sets no
// override. A nil repo resolves to the project list.
func EffectiveReporterLogins(proj *Project, repo *Repository) []string {
	var list []string
	if repo != nil && repo.Spec.ReporterLogins != nil {
		list = *repo.Spec.ReporterLogins
	} else if proj != nil && proj.Spec.Scm != nil {
		list = proj.Spec.Scm.ReporterLogins
	}
	return withoutBotLogin(proj, list)
}

// EffectiveMaintainerLogins returns the maintainer/approver allowlist in effect
// for repo. Maintainers are the unified trusted-insider + approver set (issue
// #102): the list that gates approval and the issue #56 autoapprove tier.
func EffectiveMaintainerLogins(proj *Project, repo *Repository) []string {
	var list []string
	if repo != nil && repo.Spec.MaintainerLogins != nil {
		list = *repo.Spec.MaintainerLogins
	} else if proj != nil && proj.Spec.Scm != nil {
		list = proj.Spec.Scm.MaintainerLogins
	}
	return withoutBotLogin(proj, list)
}

// IsAllowedReporter reports whether login may drive issue/comment intake for the
// given project/repo. An empty effective reporter list preserves the historical
// open behavior (any author is accepted). When the list is non-empty the gate is
// active: the bot and the maintainers are always trusted insiders, plus any
// explicitly listed reporter. An empty login fails closed under an active gate.
func IsAllowedReporter(proj *Project, repo *Repository, login string) bool {
	reporters := EffectiveReporterLogins(proj, repo)
	if len(reporters) == 0 {
		return true
	}
	if login == "" {
		return false
	}
	if proj != nil && proj.Spec.Scm != nil && login == proj.Spec.Scm.BotLogin {
		return true
	}
	if containsLogin(EffectiveMaintainerLogins(proj, repo), login) {
		return true
	}
	return containsLogin(reporters, login)
}

// IsTrustedAuthor reports whether login is an explicitly trusted insider (the
// bot, a maintainer, or a listed reporter) for the project/repo. Unlike
// IsAllowedReporter it does NOT treat an empty reporter list as open: it
// requires explicit membership, so it can gate label/reaction-scope bypass
// without opening those gates to third parties when the lists are empty.
func IsTrustedAuthor(proj *Project, repo *Repository, login string) bool {
	if login == "" {
		return false
	}
	if proj != nil && proj.Spec.Scm != nil && login == proj.Spec.Scm.BotLogin {
		return true
	}
	if containsLogin(EffectiveMaintainerLogins(proj, repo), login) {
		return true
	}
	return containsLogin(EffectiveReporterLogins(proj, repo), login)
}

// IsMaintainer reports whether login is a VERIFIED human maintainer for the
// project/repo: a member of the effective MaintainerLogins set that is NOT the
// bot. It is the trust check behind the explicit-maintainer-approval gate.
//
// CLOSED by default: an empty MaintainerLogins set means NOBODY is a maintainer,
// so nothing can be approved and every issue fails closed (this is deliberate -
// a project must name its maintainers before any issue can advance to implement).
// The bot is structurally excluded even if misconfigured into the list, so an
// agent/pod acting AS the bot can never satisfy a maintainer-gated check.
func IsMaintainer(proj *Project, repo *Repository, login string) bool {
	if login == "" {
		return false
	}
	if proj != nil && proj.Spec.Scm != nil && login == proj.Spec.Scm.BotLogin {
		return false
	}
	return containsLogin(EffectiveMaintainerLogins(proj, repo), login)
}

// ResolvedApprovedLabel returns the maintainer-approval label (Scm.ApprovedLabel
// or the "tatara-approved" default). This is the label a maintainer applies to
// an issue to explicitly approve it for implementation; the webhook records a
// verified approval only when a MaintainerLogins member applies exactly this
// label. Kept in sync with the controller's lifecycleLabels default.
func ResolvedApprovedLabel(s *ScmSpec) string {
	if s != nil && s.ApprovedLabel != "" {
		return s.ApprovedLabel
	}
	return "tatara-approved"
}

// withoutBotLogin returns list with the project's bot login removed, so a bot
// login misconfigured into a maintainer/reporter list has no runtime effect.
// The bot's insider trust (IsAllowedReporter/IsTrustedAuthor) does not depend on
// list membership - it is checked separately - so stripping the list is safe.
// Returns list unchanged when no project bot login is set or the list is empty.
func withoutBotLogin(proj *Project, list []string) []string {
	if proj == nil || proj.Spec.Scm == nil || proj.Spec.Scm.BotLogin == "" || len(list) == 0 {
		return list
	}
	bot := proj.Spec.Scm.BotLogin
	out := make([]string, 0, len(list))
	for _, x := range list {
		if x != bot {
			out = append(out, x)
		}
	}
	return out
}

func containsLogin(list []string, login string) bool {
	for _, x := range list {
		if x == login {
			return true
		}
	}
	return false
}
