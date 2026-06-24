package scm

import "strings"

// RepoSlugFromURL derives the provider-correct repo slug from a clone/repo URL:
// "owner/name" for GitHub (rejecting deep/enterprise paths) and the full
// group/subgroup/project path for GitLab. Provider is inferred from the URL host
// (gitlab vs github), matching the controller's providerForRemote default to
// github when neither is present. This is the single canonical slug parser shared
// by the controller (ledger seed/projection) and the agent (clone-scope filter)
// so the two never derive divergent slugs for the same URL.
func RepoSlugFromURL(repoURL string) (string, error) {
	if strings.Contains(strings.ToLower(repoURL), "gitlab") {
		return GitLabProjectPath(repoURL)
	}
	owner, name, err := OwnerRepo(repoURL)
	if err != nil {
		return "", err
	}
	return owner + "/" + name, nil
}
