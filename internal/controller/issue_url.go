package controller

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// issueURLFromRepoURL renders the web URL of issue `number` in `repo`
// (owner/name slug), deriving the host from the Repository's own clone URL so a
// self-hosted forge does not render github.com/gitlab.com links.
func issueURLFromRepoURL(repoURL, provider, repo string, number int) string {
	base := "https://github.com"
	if u, err := parseRepoBase(repoURL); err == nil {
		base = u
	} else if provider == "gitlab" {
		base = "https://gitlab.com"
	}
	if provider == "gitlab" {
		return fmt.Sprintf("%s/%s/-/issues/%d", base, repo, number)
	}
	return fmt.Sprintf("%s/%s/issues/%d", base, repo, number)
}

// parseIssueURL extracts the "owner/repo" slug and issue number from an issue
// web URL. Supports both GitHub (.../owner/repo/issues/N) and GitLab
// (.../group[/subgroup...]/project/-/issues/N, subgroups included) shapes.
// Returns ok=false when itemURL is not a recognizable issue URL.
func parseIssueURL(itemURL string) (repoSlug string, number int, ok bool) {
	u, err := url.Parse(itemURL)
	if err != nil {
		return "", 0, false
	}
	p := strings.Trim(u.Path, "/")
	if idx := strings.Index(p, "/-/issues/"); idx > 0 {
		n, err := strconv.Atoi(p[idx+len("/-/issues/"):])
		if err != nil {
			return "", 0, false
		}
		return p[:idx], n, true
	}
	if idx := strings.Index(p, "/issues/"); idx > 0 {
		n, err := strconv.Atoi(p[idx+len("/issues/"):])
		if err != nil {
			return "", 0, false
		}
		return p[:idx], n, true
	}
	return "", 0, false
}

// parsePRURL extracts the "owner/repo" (or group/subgroup/.../project) slug
// and PR/MR number from a pull-request or merge-request web URL: GitHub
// ".../owner/repo/pull/N" and GitLab ".../group[/subgroup...]/project/-/merge_requests/N".
// Returns ok=false when itemURL is not a recognizable PR/MR URL. Sibling of
// parseIssueURL, which deliberately does NOT match these shapes.
func parsePRURL(itemURL string) (repoSlug string, number int, ok bool) {
	u, err := url.Parse(itemURL)
	if err != nil {
		return "", 0, false
	}
	p := strings.Trim(u.Path, "/")
	if idx := strings.Index(p, "/-/merge_requests/"); idx > 0 {
		n, err := strconv.Atoi(p[idx+len("/-/merge_requests/"):])
		if err != nil {
			return "", 0, false
		}
		return p[:idx], n, true
	}
	if idx := strings.Index(p, "/pull/"); idx > 0 {
		n, err := strconv.Atoi(p[idx+len("/pull/"):])
		if err != nil {
			return "", 0, false
		}
		return p[:idx], n, true
	}
	return "", 0, false
}

// parseSourceURL extracts the owner/repo slug and number from a Task's
// Spec.Source item URL, which is an issue or a PR/MR web URL depending on
// Spec.Source.IsPR.
func parseSourceURL(itemURL string, isPR bool) (repoSlug string, number int, ok bool) {
	if isPR {
		return parsePRURL(itemURL)
	}
	return parseIssueURL(itemURL)
}
