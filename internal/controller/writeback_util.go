package controller

import (
	"fmt"
	"strings"

	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// repoSlugFromURL derives the provider-correct repo slug (owner/name for
// GitHub, group/proj path for GitLab) that CloseIssue expects. It delegates to
// the canonical scm.RepoSlugFromURL so the controller (ledger seed/projection)
// and the agent (clone-scope filter) always derive identical slugs for the same
// URL; the provider argument is honoured when explicitly "gitlab"/"github" and
// otherwise inferred from the URL host, matching providerForRemote.
func repoSlugFromURL(repoURL, provider string) (string, string, error) {
	if provider == "gitlab" {
		proj, err := scm.GitLabProjectPath(repoURL)
		return proj, "", err
	}
	if provider == "github" {
		owner, name, err := scm.OwnerRepo(repoURL)
		return owner + "/" + name, "", err
	}
	slug, err := scm.RepoSlugFromURL(repoURL)
	return slug, "", err
}

// commentRef renders the Comment() ref for a thread write. GitHub routes both
// issues and PRs through the same issues/<n>/comments endpoint (isPR is
// irrelevant), but GitLab sigils on '!' for merge requests vs '#' for issues -
// a '#' ref on an MR resolves the wrong (or absent) issue with the same iid,
// which is the reviewpost.go/ownership_announce.go 404 this helper closes.
func commentRef(slug, provider string, number int, isPR bool) string {
	if isPR && provider == "gitlab" {
		return fmt.Sprintf("%s!%d", slug, number)
	}
	return fmt.Sprintf("%s#%d", slug, number)
}

// parseRepoBase returns the scheme+host of repoURL (e.g. "https://gitlab.example.com").
func parseRepoBase(repoURL string) (string, error) {
	if i := strings.Index(repoURL, "://"); i >= 0 {
		rest := repoURL[i+3:]
		if j := strings.IndexByte(rest, '/'); j >= 0 {
			return repoURL[:i+3] + rest[:j], nil
		}
		return repoURL[:i+3] + rest, nil
	}
	return "", fmt.Errorf("no scheme in %q", repoURL)
}
