package ingest

import "strings"

// namespacePath maps a git clone URL to the on-disk subpath:
// owner[/subgroups]/repo, dropping scheme, host, userinfo, and a trailing
// ".git". Keeps the owner.
//
//	https://github.com/szymonrychu/tatara-cli.git   -> szymonrychu/tatara-cli
//	https://gitlab.com/szymonrychu/infra/helmfile    -> szymonrychu/infra/helmfile
//	git@github.com:szymonrychu/tatara-cli.git        -> szymonrychu/tatara-cli
//	ssh://git@host:22/group/sub/repo.git             -> group/sub/repo
func namespacePath(cloneURL string) string {
	s := strings.TrimSpace(cloneURL)

	// Drop scheme (https://, ssh://, git://, ...).
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}

	// scp-like "git@host:owner/repo" has no scheme; split host from path on
	// the first ":" when there is no "/" before it and the colon is NOT
	// followed purely by digits (which would be a port number, not a path).
	if !strings.Contains(s[:firstSlash(s)], "/") {
		if i := strings.Index(s, ":"); i >= 0 && !isPort(s[i+1:]) {
			s = s[:i] + "/" + s[i+1:]
		}
	}

	// Drop userinfo before the host ("git@host", "x-access-token@host").
	if i := strings.Index(s, "@"); i >= 0 && i < firstSlash(s) {
		s = s[i+1:]
	}

	// Drop the host segment (everything up to and including the first slash).
	if i := strings.Index(s, "/"); i >= 0 {
		s = s[i+1:]
	}

	s = strings.Trim(s, "/")
	s = strings.TrimSuffix(s, ".git")
	return s
}

// firstSlash returns the index of the first "/" in s, or len(s) when absent.
func firstSlash(s string) int {
	if i := strings.Index(s, "/"); i >= 0 {
		return i
	}
	return len(s)
}

// isPort reports whether s starts with digits immediately followed by "/" or
// is all digits, indicating a ":NN/" or ":NN" port number rather than an scp
// path separator.
func isPort(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i, c := range s {
		if c == '/' {
			return i > 0 // digits followed by slash -> port
		}
		if c < '0' || c > '9' {
			return false
		}
	}
	return true // all digits
}
