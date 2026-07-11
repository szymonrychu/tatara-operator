package controller

import (
	"net/url"
	"strconv"
	"strings"
)

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
