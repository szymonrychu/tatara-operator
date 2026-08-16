package scm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// THE FORK GUARD NEEDS A HEAD REPO, AND THE WEBHOOK IS WHERE IT COMES FROM.
// AdoptUpgradeMR clause (d) refuses any merge request whose head repo is not
// the base repo, and it fails CLOSED on an empty value - so an adoption event
// built from a delivery that never decoded head.repo can never be admitted.
func TestParseWebhook_GitHubPROpenedCarriesHeadRepo(t *testing.T) {
	const secret = "s"
	body := []byte(`{"action":"opened","pull_request":{"number":7,` +
		`"title":"chore(deps): bump","user":{"login":"tatara-bot"},` +
		`"head":{"sha":"abc","ref":"renovate/dep","repo":{"full_name":"o/r"}},` +
		`"html_url":"https://github.com/o/r/pull/7"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},` +
		`"sender":{"login":"tatara-bot"}}`)
	ev, err := (&GitHub{}).DetectAndVerify(ghHeader("pull_request", secret, body), body, secret)
	require.NoError(t, err)
	require.Equal(t, "o/r", ev.HeadRepo)
}

// A FORK PR REPORTS A DIFFERENT HEAD REPO, and that difference is the entire
// signal: a fork may name its head branch renovate/anything.
func TestParseWebhook_GitHubForkPRReportsTheForkAsHeadRepo(t *testing.T) {
	const secret = "s"
	body := []byte(`{"action":"opened","pull_request":{"number":8,` +
		`"title":"drive-by","user":{"login":"stranger"},` +
		`"head":{"sha":"def","ref":"renovate/evil","repo":{"full_name":"stranger/r"}},` +
		`"html_url":"https://github.com/o/r/pull/8"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},` +
		`"sender":{"login":"stranger"}}`)
	ev, err := (&GitHub{}).DetectAndVerify(ghHeader("pull_request", secret, body), body, secret)
	require.NoError(t, err)
	require.Equal(t, "stranger/r", ev.HeadRepo)
}

// A PAYLOAD WITH NO head.repo LEAVES IT EMPTY, and empty is what every
// consumer fails closed on. It is never defaulted to the base repo: that would
// turn "the forge did not say" into "it is not a fork".
func TestParseWebhook_GitHubPRWithoutHeadRepoLeavesItEmpty(t *testing.T) {
	const secret = "s"
	body := []byte(`{"action":"opened","pull_request":{"number":9,` +
		`"title":"t","user":{"login":"u"},"head":{"sha":"ghi","ref":"b"},` +
		`"html_url":"https://github.com/o/r/pull/9"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},` +
		`"sender":{"login":"u"}}`)
	ev, err := (&GitHub{}).DetectAndVerify(ghHeader("pull_request", secret, body), body, secret)
	require.NoError(t, err)
	require.Equal(t, "", ev.HeadRepo)
}

// GitLab reports the same fact as object_attributes.source.path_with_namespace.
func TestParseWebhook_GitLabMROpenedCarriesHeadRepo(t *testing.T) {
	const token = "t"
	body := []byte(`{"object_kind":"merge_request",` +
		`"user":{"username":"tatara-bot"},` +
		`"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},` +
		`"object_attributes":{"iid":11,"title":"chore(deps): bump","action":"open",` +
		`"source_branch":"renovate/dep","last_commit":{"id":"abc"},` +
		`"source":{"path_with_namespace":"g/p"},` +
		`"url":"https://gitlab.com/g/p/-/merge_requests/11"}}`)
	ev, err := (&GitLab{}).DetectAndVerify(glHeader("Merge Request Hook", token), body, token)
	require.NoError(t, err)
	require.Equal(t, "g/p", ev.HeadRepo)
}

// A GitLab MR whose source project differs from the target reports the fork's
// namespace, matching the GitHub fork case above.
func TestParseWebhook_GitLabForkMRReportsTheForkAsHeadRepo(t *testing.T) {
	const token = "t"
	body := []byte(`{"object_kind":"merge_request",` +
		`"user":{"username":"stranger"},` +
		`"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},` +
		`"object_attributes":{"iid":12,"title":"drive-by","action":"open",` +
		`"source_branch":"renovate/evil","last_commit":{"id":"def"},` +
		`"source":{"path_with_namespace":"stranger/p"},` +
		`"url":"https://gitlab.com/g/p/-/merge_requests/12"}}`)
	ev, err := (&GitLab{}).DetectAndVerify(glHeader("Merge Request Hook", token), body, token)
	require.NoError(t, err)
	require.Equal(t, "stranger/p", ev.HeadRepo)
}

// A GitLab payload with no object_attributes.source leaves HeadRepo empty,
// matching the GitHub no-head.repo case: never defaulted to the base repo.
func TestParseWebhook_GitLabMRWithoutSourceLeavesItEmpty(t *testing.T) {
	const token = "t"
	body := []byte(`{"object_kind":"merge_request",` +
		`"user":{"username":"bob"},` +
		`"project":{"git_http_url":"https://gitlab.com/g/p.git","path_with_namespace":"g/p"},` +
		`"object_attributes":{"iid":13,"title":"t","action":"open",` +
		`"source_branch":"b","last_commit":{"id":"abc"},"url":"u"}}`)
	ev, err := (&GitLab{}).DetectAndVerify(glHeader("Merge Request Hook", token), body, token)
	require.NoError(t, err)
	require.Equal(t, "", ev.HeadRepo)
}
