// Copyright 2026 tatara authors.

package controller

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// corpusAllowedEntry is one (repo, number, approved) row of a corpus case's
// allowed set, as stored in testdata/closedirective/corpus.json.
type corpusAllowedEntry struct {
	Repo     string `json:"repo"`
	Number   int    `json:"number"`
	Approved bool   `json:"approved"`
}

// corpusCase is one golden case: a body, the ownRepo it is being written back
// to, the approved-and-owned set, and the expected filtered body.
type corpusCase struct {
	Name    string               `json:"name"`
	Body    string               `json:"body"`
	OwnRepo string               `json:"ownRepo"`
	Allowed []corpusAllowedEntry `json:"allowed"`
	Want    string               `json:"want"`
}

func loadCorpus(t *testing.T) []corpusCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/closedirective/corpus.json")
	require.NoError(t, err, "read corpus fixture")
	var cases []corpusCase
	require.NoError(t, json.Unmarshal(raw, &cases), "unmarshal corpus fixture")
	require.NotEmpty(t, cases, "corpus fixture must not be empty")
	return cases
}

func allowedMap(entries []corpusAllowedEntry) map[RepoNum]bool {
	m := make(map[RepoNum]bool, len(entries))
	for _, e := range entries {
		m[RepoNum{Repo: e.Repo, Number: e.Number}] = e.Approved
	}
	return m
}

// TestFilterCloseDirectives_Corpus runs the mandatory C.7 golden corpus: 4
// reference forms (bare #N, owner/repo#N, GH-N, forge URL) x {approved,
// unapproved} x {same-repo, cross-repo} x {GitHub, GitLab} = 32 cases, plus a
// comma-list case and a mixed allowed/stripped list case (34 total).
//
// The "cross-repo" dimension means different things per form: owner/repo#N
// and the forge URL genuinely name a different repo (a sibling), while bare
// #N and GH-N are always same-repo-only shorthand on every forge - their
// "cross-repo" cases prove that an approval recorded against a DIFFERENT
// repo's identical issue number never leaks into a same-repo-shorthand
// reference (scoping, not syntax, is what "cross-repo" tests there).
func TestFilterCloseDirectives_Corpus(t *testing.T) {
	for _, c := range loadCorpus(t) {
		t.Run(c.Name, func(t *testing.T) {
			got := FilterCloseDirectives(c.Body, c.OwnRepo, allowedMap(c.Allowed))
			require.Equal(t, c.Want, got)
		})
	}
}

// TestFilterCloseDirectives_UnknownStripped is the C.7 Step 5 mandate made
// explicit: a (repo, number) entirely ABSENT from allowed - i.e. no Issue CR
// exists for it at all, not merely an unapproved one - is stripped exactly
// like an explicit unapproved entry. Unknown means stripped; that is the
// entire point of an allowlist.
func TestFilterCloseDirectives_UnknownStripped(t *testing.T) {
	got := FilterCloseDirectives("Closes #42 today.", "acme/widgets", map[RepoNum]bool{})
	require.Equal(t, "#42 today.", got, "a (repo,number) with no Issue CR at all must be stripped")
}

// TestFilterCloseDirectives_GitLabImplementingURL is the contract's own
// worked example (C.7 Step 5): "Implementing https://gitlab.example/g/p/-/
// issues/7" is stripped when the target is unapproved and survives byte-for-
// byte when it is approved+owned.
func TestFilterCloseDirectives_GitLabImplementingURL(t *testing.T) {
	body := "Implementing https://gitlab.example/g/p/-/issues/7"

	stripped := FilterCloseDirectives(body, "g/p", map[RepoNum]bool{})
	require.Equal(t, "https://gitlab.example/g/p/-/issues/7", stripped)

	survives := FilterCloseDirectives(body, "g/p", map[RepoNum]bool{{Repo: "g/p", Number: 7}: true})
	require.Equal(t, body, survives, "approved+owned reference survives byte-for-byte")
}

// TestFilterCloseDirectives_KeywordFamily proves the full base/-s/-ed/-ing
// keyword family recognizes every inflection the union vocabulary requires
// (contract C.7): a GitHub-only list would regress GitLab's -ing and
// implement* forms, and project-infrastructure is entirely GitLab.
func TestFilterCloseDirectives_KeywordFamily(t *testing.T) {
	keywords := []string{
		"close", "closes", "closed", "closing",
		"fix", "fixes", "fixed", "fixing",
		"resolve", "resolves", "resolved", "resolving",
		"implement", "implements", "implemented", "implementing",
	}
	for _, kw := range keywords {
		t.Run(kw, func(t *testing.T) {
			// Capitalize like an agent would ("Closes #7"), unapproved so the
			// keyword must be stripped - proves the keyword was recognized at all.
			body := strings.ToUpper(kw[:1]) + kw[1:] + " #7"
			got := FilterCloseDirectives(body, "acme/widgets", map[RepoNum]bool{})
			require.Equal(t, "#7", got, "keyword %q must be recognized and stripped", kw)
		})
	}
}

// TestFilterCloseDirectives_NoDirective proves a body with no close directive
// at all - the common case - passes through completely untouched.
func TestFilterCloseDirectives_NoDirective(t *testing.T) {
	body := "This change refactors the widget loader. See #7 for background."
	got := FilterCloseDirectives(body, "acme/widgets", map[RepoNum]bool{{Repo: "acme/widgets", Number: 7}: true})
	require.Equal(t, body, got, "a bare mention with no closing keyword is never a directive")
}

// TestCloseDirectiveResolveRef unit-tests resolveRef's four-form resolution
// directly, independent of the corpus's byte-level ReplaceAllStringFunc
// assertions.
func TestCloseDirectiveResolveRef(t *testing.T) {
	cases := []struct {
		name    string
		ref     string
		ownRepo string
		want    RepoNum
		wantOK  bool
	}{
		{"bare", "#7", "acme/widgets", RepoNum{Repo: "acme/widgets", Number: 7}, true},
		{"owner_repo", "acme/other#9", "acme/widgets", RepoNum{Repo: "acme/other", Number: 9}, true},
		{"gh_shorthand", "GH-7", "acme/widgets", RepoNum{Repo: "acme/widgets", Number: 7}, true},
		{"github_url", "https://github.com/acme/widgets/issues/7", "acme/widgets", RepoNum{Repo: "acme/widgets", Number: 7}, true},
		{"gitlab_url", "https://gitlab.example/g/p/-/issues/7", "g/p", RepoNum{Repo: "g/p", Number: 7}, true},
		{"gitlab_url_nested_group", "https://gitlab.example/g/sub/p/-/issues/7", "g/sub/p", RepoNum{Repo: "g/sub/p", Number: 7}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := resolveRef(c.ref, c.ownRepo)
			require.Equal(t, c.wantOK, ok)
			require.Equal(t, c.want, got)
		})
	}
}
