package v1alpha1

import "testing"

func projWith(bot string, maintainers, reporters []string) *Project {
	return &Project{Spec: ProjectSpec{Scm: &ScmSpec{
		BotLogin:         bot,
		MaintainerLogins: maintainers,
		ReporterLogins:   reporters,
	}}}
}

func repoOverride(reporters, maintainers *[]string) *Repository {
	return &Repository{Spec: RepositorySpec{
		ReporterLogins:   reporters,
		MaintainerLogins: maintainers,
	}}
}

func strs(s ...string) *[]string { return &s }

func TestEffectiveReporterLogins_RepoOverride(t *testing.T) {
	proj := projWith("bot", nil, []string{"alice"})
	// nil override -> inherit project list
	if got := EffectiveReporterLogins(proj, &Repository{}); len(got) != 1 || got[0] != "alice" {
		t.Fatalf("nil override should inherit project list, got %v", got)
	}
	// explicit list -> override
	if got := EffectiveReporterLogins(proj, repoOverride(strs("bob"), nil)); len(got) != 1 || got[0] != "bob" {
		t.Fatalf("explicit override should win, got %v", got)
	}
	// explicit empty -> override to open (empty)
	if got := EffectiveReporterLogins(proj, repoOverride(strs(), nil)); len(got) != 0 {
		t.Fatalf("explicit empty override should clear the list, got %v", got)
	}
	// nil repo -> project list
	if got := EffectiveReporterLogins(proj, nil); len(got) != 1 || got[0] != "alice" {
		t.Fatalf("nil repo should use project list, got %v", got)
	}
}

func TestEffectiveMaintainerLogins_RepoOverride(t *testing.T) {
	proj := projWith("bot", []string{"alice"}, nil)
	if got := EffectiveMaintainerLogins(proj, repoOverride(nil, strs("carol"))); len(got) != 1 || got[0] != "carol" {
		t.Fatalf("maintainer override should win, got %v", got)
	}
	if got := EffectiveMaintainerLogins(proj, &Repository{}); len(got) != 1 || got[0] != "alice" {
		t.Fatalf("nil override should inherit project maintainers, got %v", got)
	}
}

func TestIsTrustedAuthor(t *testing.T) {
	proj := &Project{Spec: ProjectSpec{Scm: &ScmSpec{
		BotLogin:         "szymonrychu-bot",
		MaintainerLogins: []string{"szymonrychu"},
		ReporterLogins:   []string{"szymonrychu"},
	}}}
	open := &Project{Spec: ProjectSpec{Scm: &ScmSpec{BotLogin: "szymonrychu-bot"}}}
	tests := []struct {
		name  string
		proj  *Project
		login string
		want  bool
	}{
		{"bot", proj, "szymonrychu-bot", true},
		{"maintainer", proj, "szymonrychu", true},
		{"third party", proj, "randouser", false},
		{"empty login", proj, "", false},
		{"empty lists do not open", open, "randouser", false},
		{"bot with empty lists", open, "szymonrychu-bot", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTrustedAuthor(tt.proj, nil, tt.login); got != tt.want {
				t.Errorf("IsTrustedAuthor(%q)=%v want %v", tt.login, got, tt.want)
			}
		})
	}
}

func TestIsAllowedReporter(t *testing.T) {
	cases := []struct {
		name      string
		maintain  []string
		reporters []string
		repo      *Repository
		login     string
		wantAllow bool
	}{
		{name: "empty list is open for anyone", login: "stranger", wantAllow: true},
		{name: "empty list is open for empty login", login: "", wantAllow: true},
		{name: "configured: listed reporter allowed", reporters: []string{"alice"}, login: "alice", wantAllow: true},
		{name: "configured: stranger denied", reporters: []string{"alice"}, login: "mallory", wantAllow: false},
		{name: "configured: bot always allowed", reporters: []string{"alice"}, login: "bot", wantAllow: true},
		{name: "configured: maintainer always allowed", maintain: []string{"carol"}, reporters: []string{"alice"}, login: "carol", wantAllow: true},
		{name: "configured: empty login fails closed", reporters: []string{"alice"}, login: "", wantAllow: false},
		{
			name: "repo override opens intake for this repo", reporters: []string{"alice"},
			repo: repoOverride(strs(), nil), login: "mallory", wantAllow: true,
		},
		{
			name: "repo override tightens intake", reporters: []string{"alice"},
			repo: repoOverride(strs("bob"), nil), login: "alice", wantAllow: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			proj := projWith("bot", c.maintain, c.reporters)
			if got := IsAllowedReporter(proj, c.repo, c.login); got != c.wantAllow {
				t.Errorf("IsAllowedReporter(%q) = %v, want %v", c.login, got, c.wantAllow)
			}
		})
	}
}

func TestIsMaintainer(t *testing.T) {
	proj := projWith("bot", []string{"szymon", "alex"}, nil)
	cases := []struct {
		name  string
		login string
		want  bool
	}{
		{"maintainer", "szymon", true},
		{"second maintainer", "alex", true},
		{"bot excluded even if acting", "bot", false},
		{"non-maintainer human", "rando", false},
		{"empty login", "", false},
	}
	for _, c := range cases {
		if got := IsMaintainer(proj, nil, c.login); got != c.want {
			t.Errorf("%s: IsMaintainer(%q) = %v, want %v", c.name, c.login, got, c.want)
		}
	}
	// Closed by default: no maintainers configured => nobody is a maintainer.
	empty := projWith("bot", nil, nil)
	if IsMaintainer(empty, nil, "szymon") {
		t.Error("empty MaintainerLogins must make IsMaintainer closed (fail closed)")
	}
	// Bot is excluded even if misconfigured into the maintainer list.
	botListed := projWith("bot", []string{"bot"}, nil)
	if IsMaintainer(botListed, nil, "bot") {
		t.Error("bot must be structurally excluded from IsMaintainer")
	}
	// Per-repo maintainer override is honored.
	repo := repoOverride(nil, strs("carol"))
	if !IsMaintainer(proj, repo, "carol") {
		t.Error("repo-level maintainer override must be honored")
	}
	if IsMaintainer(proj, repo, "szymon") {
		t.Error("repo-level override replaces the project maintainer list")
	}
}

func TestResolvedApprovedLabel(t *testing.T) {
	if got := ResolvedApprovedLabel(nil); got != "tatara-approved" {
		t.Errorf("nil spec = %q, want default tatara-approved", got)
	}
	if got := ResolvedApprovedLabel(&ScmSpec{}); got != "tatara-approved" {
		t.Errorf("empty ApprovedLabel = %q, want default", got)
	}
	if got := ResolvedApprovedLabel(&ScmSpec{ApprovedLabel: "custom-approved"}); got != "custom-approved" {
		t.Errorf("custom = %q, want custom-approved", got)
	}
}
