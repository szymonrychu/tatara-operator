// Copyright 2026 tatara authors.

package controller

import (
	"regexp"
	"strconv"
	"strings"
)

// RepoNum identifies a forge issue by its (repo slug, number). Repo is an
// "owner/repo" slug (GitHub) or "group[/subgroup]/project" slug (GitLab),
// never a Repository CR name.
type RepoNum struct {
	Repo   string
	Number int
}

// closeKeywordFrag is the UNION of both forges' close-keyword vocabularies
// (contract C.7), case-insensitive, with the full base/-s/-ed/-ing family:
// close/closes/closed/closing, fix/fixes/fixed/fixing, resolve/resolves/
// resolved/resolving, implement/implements/implemented/implementing. GitLab
// honours the -ing forms and the whole implement* family; GitHub does not -
// and a GitHub-only list would regress coverage on GitLab-hosted repos.
const closeKeywordFrag = `(?:clos(?:e|es|ed|ing)|fix(?:es|ed|ing)?|resolv(?:e|es|ed|ing)|implement(?:s|ed|ing)?)`

// refItemFrag matches one of the four leak-shape reference forms (contract
// C.7): a forge close/issue URL (GitHub "/issues/N", GitLab "/-/issues/N"),
// the "GH-N" shorthand (always same-repo), or a bare "#N"/"owner/repo#N"
// (the owner/repo prefix optional, mirroring the contract's own regex hint).
// The URL branch's repo-path capture is intentionally LAZY (".+?"): a greedy
// capture is ambiguous against the optional GitLab "-/issues/" delimiter (it
// would fold the trailing "-" into the repo slug), while lazy expansion finds
// the shortest, correct split on the first successful match.
const refItemFrag = `(?:https?://[^\s,)/]+/.+?/(?:-/)?issues/\d+` +
	`|GH-\d+` +
	`|(?:[\w.-]+/[\w.-]+)?#\d+)`

// allowlistDirectiveRE matches a keyword-then-reference-LIST directive: the
// keyword, an optional colon, an optional "issue"/"issues" word, then one or
// more refItemFrag references joined by ","/"and" (each optionally preceded
// by its own "issue"/"issues" word) - "Closes #7, #8, owner/repo#9" binds the
// keyword to EVERY reference in the list (today's behaviour, preserved).
var allowlistDirectiveRE = regexp.MustCompile(`(?i)\b` + closeKeywordFrag +
	`\s*:?\s*(?:issues?\s+)?` + refItemFrag +
	`(?:\s*(?:,|and)\s*(?:issues?\s+)?` + refItemFrag + `)*`)

// allowlistRefItemRE extracts each individual reference out of an allowlistDirectiveRE
// match. Applied to the whole directive match (keyword included): the
// keyword vocabulary never itself matches refItemFrag, so this is safe.
var allowlistRefItemRE = regexp.MustCompile(`(?i)` + refItemFrag)

// allowlistURLRefRE decomposes an isolated forge issue URL into its repo-path and
// number. Anchored: it is only ever applied to a single already-extracted
// allowlistRefItemRE token, never to free text.
var allowlistURLRefRE = regexp.MustCompile(`(?i)^https?://[^\s,)/]+/(.+?)/(?:-/)?issues/(\d+)$`)

// resolveRef resolves one extracted reference token to the (repo, number) it
// names. A bare "#N" or "GH-N" resolves against ownRepo (both are same-repo-
// only shorthand on every forge); "owner/repo#N" and a forge URL carry their
// own repo. ok is false only for a token none of the four forms actually
// matched, which cannot happen for a token allowlistRefItemRE itself produced - it
// exists for defensive completeness.
func resolveRef(ref, ownRepo string) (rn RepoNum, ok bool) {
	if m := allowlistURLRefRE.FindStringSubmatch(ref); m != nil {
		n, err := strconv.Atoi(m[2])
		if err != nil {
			return RepoNum{}, false
		}
		return RepoNum{Repo: m[1], Number: n}, true
	}
	if len(ref) > 3 && strings.EqualFold(ref[:3], "GH-") {
		n, err := strconv.Atoi(ref[3:])
		if err != nil {
			return RepoNum{}, false
		}
		return RepoNum{Repo: ownRepo, Number: n}, true
	}
	if idx := strings.IndexByte(ref, '#'); idx >= 0 {
		n, err := strconv.Atoi(ref[idx+1:])
		if err != nil {
			return RepoNum{}, false
		}
		if idx == 0 {
			return RepoNum{Repo: ownRepo, Number: n}, true
		}
		return RepoNum{Repo: ref[:idx], Number: n}, true
	}
	return RepoNum{}, false
}

// FilterCloseDirectives is the C.7 ALLOWLIST: it strips every close directive
// whose (repo, number) is not in allowed. Blocklisting a forge's closing
// grammar is unwinnable (the old neutralizeUnapprovedCloses's own doc-comment
// admits cross-repo forms it deliberately does not match); the allowlist
// makes "unknown" the safe default instead.
//
//   - A directive whose every reference is allowed is left byte-for-byte
//     untouched (preserves the agent's original keyword, casing and spacing).
//   - A directive with at least one disallowed reference is rebuilt
//     reference-by-reference: an allowed reference keeps a "Closes " prefix,
//     a disallowed one loses its keyword and is left as a bare reference (the
//     link survives, the auto-close does not). When EVERY reference under a
//     keyword is disallowed, every rebuilt part is bare, so the keyword goes
//     with them - "Closes #7, #8" with neither approved becomes "#7, #8".
//
// An unknown, unresolvable, or unowned reference (absent from allowed, or
// present with a false value) is treated as disallowed: unknown means
// stripped, which is the entire point of an allowlist.
func FilterCloseDirectives(body string, ownRepo string, allowed map[RepoNum]bool) string {
	return allowlistDirectiveRE.ReplaceAllStringFunc(body, func(m string) string {
		refs := allowlistRefItemRE.FindAllString(m, -1)
		if len(refs) == 0 {
			return m
		}
		ok := make([]bool, len(refs))
		allOK := true
		for i, ref := range refs {
			rn, resolved := resolveRef(ref, ownRepo)
			ok[i] = resolved && allowed[rn]
			if !ok[i] {
				allOK = false
			}
		}
		if allOK {
			return m
		}
		parts := make([]string, len(refs))
		for i, ref := range refs {
			if ok[i] {
				parts[i] = "Closes " + ref
			} else {
				parts[i] = ref
			}
		}
		return strings.Join(parts, ", ")
	})
}
