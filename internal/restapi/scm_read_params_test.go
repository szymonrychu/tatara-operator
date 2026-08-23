package restapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// --- #636. `since` on kind=mr, and NEWEST-first truncation on both lists ----

func mrUpdated(repo string, number int, state string, updated time.Time) *tatarav1alpha1.MergeRequest {
	return mrV2(repo, number, "t1", func(m *tatarav1alpha1.MergeRequest) {
		m.Status.State = state
		u := metav1.NewTime(updated)
		m.Status.UpdatedAt = &u
	})
}

func issueUpdated(repo string, number int, state string, updated time.Time, labels ...string) *tatarav1alpha1.Issue {
	return issueV2(repo, number, "t1", func(i *tatarav1alpha1.Issue) {
		i.Status.State = state
		u := metav1.NewTime(updated)
		i.Status.UpdatedAt = &u
		i.Status.Labels = labels
	})
}

// The reproduction in #636: `since` was advertised for kind=mr, forwarded by
// nothing and read by nothing, so a filtered sweep came back with the OLDEST
// mirrors in the repo. It filters on Status.UpdatedAt - forge activity, the
// same field scmIssues uses - not on lastSyncedAt.
func TestScmRead_MRs_SinceFiltersOnUpdatedAt(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}, reader: panicReader{}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"),
		mrUpdated("tatara-operator", 361, "open", frozenNow.Add(-72*time.Hour)),
		mrUpdated("tatara-operator", 634, "open", frozenNow.Add(-time.Hour)),
	)

	since := frozenNow.Add(-24 * time.Hour).UTC().Format(time.RFC3339)
	w := e.do(t, http.MethodGet,
		"/projects/tatara/scm/mrs?repo=tatara-operator&state=all&since="+since, "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"number":634`)
	require.NotContains(t, w.Body.String(), `"number":361`)
}

// An MR the mirror has never stamped an updatedAt on cannot satisfy a since
// filter: scmIssues drops the same row for the same reason.
func TestScmRead_MRs_SinceDropsRowsWithNoUpdatedAt(t *testing.T) {
	blank := mrV2("tatara-operator", 361, "t1", func(m *tatarav1alpha1.MergeRequest) {
		m.Status.UpdatedAt = nil
	})
	e := buildV2(t, v2Opts{writer: panicForge{}, reader: panicReader{}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"),
		blank,
	)

	since := frozenNow.Add(-24 * time.Hour).UTC().Format(time.RFC3339)
	w := e.do(t, http.MethodGet,
		"/projects/tatara/scm/mrs?repo=tatara-operator&since="+since, "")
	require.Equal(t, http.StatusOK, w.Code)
	require.NotContains(t, w.Body.String(), `"number":361`)
}

func TestScmRead_MRs_SinceRejectsNonRFC3339(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}, reader: panicReader{}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"))

	w := e.do(t, http.MethodGet, "/projects/tatara/scm/mrs?repo=tatara-operator&since=yesterday", "")
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "since must be RFC3339")
}

// limit selects the NEWEST rows by number, not the oldest. Ascending-then-head
// truncation is what returned #361-#373 on a repo sitting at #634.
func TestScmRead_MRs_LimitSelectsTheNewest(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}, reader: panicReader{}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"),
		mrV2("tatara-operator", 361, "t1"), mrV2("tatara-operator", 362, "t1"),
		mrV2("tatara-operator", 633, "t1"), mrV2("tatara-operator", 634, "t1"),
	)

	w := e.do(t, http.MethodGet, "/projects/tatara/scm/mrs?repo=tatara-operator&limit=2", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"number":633`)
	require.Contains(t, w.Body.String(), `"number":634`)
	require.NotContains(t, w.Body.String(), `"number":361`)
	require.NotContains(t, w.Body.String(), `"number":362`)

	var out struct {
		MRs []struct {
			Number int `json:"number"`
		} `json:"mrs"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, []int{633, 634}, []int{out.MRs[0].Number, out.MRs[1].Number},
		"the selected page is still PRESENTED ascending")
}

func TestScmRead_Issues_LimitSelectsTheNewest(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}, reader: panicReader{}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
		issueV2("tatara-operator", 1, "t1"), issueV2("tatara-operator", 2, "t1"),
		issueV2("tatara-operator", 635, "t1"), issueV2("tatara-operator", 636, "t1"),
	)

	w := e.do(t, http.MethodGet, "/projects/tatara/scm/issues?repo=tatara-operator&limit=2", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"number":635`)
	require.Contains(t, w.Body.String(), `"number":636`)
	require.NotContains(t, w.Body.String(), `"number":1,`)
	require.NotContains(t, w.Body.String(), `"number":2,`)

	var out struct {
		Issues []struct {
			Number int `json:"number"`
		} `json:"issues"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	require.Equal(t, []int{635, 636}, []int{out.Issues[0].Number, out.Issues[1].Number},
		"the selected page is still PRESENTED ascending")
}

// createdAt/updatedAt on the MR DTO: without them the since filter's effect is
// invisible in the response, which is why #636's own reproduction had to infer
// staleness from lastSyncedAt.
func TestScmRead_MRs_CarriesCreatedAtAndUpdatedAt(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}, reader: panicReader{}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"),
		mrUpdated("tatara-cli", 80, "open", frozenNow.Add(-time.Hour)),
	)

	w := e.do(t, http.MethodGet, "/projects/tatara/scm/mrs?repo=tatara-cli", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"createdAt":"`)
	require.Contains(t, w.Body.String(),
		fmt.Sprintf(`"updatedAt":"%s"`, frozenNow.Add(-time.Hour).UTC().Format(time.RFC3339)))
}

// --- the anti-drift table (#636 C1) ----------------------------------------

// scmParamCase pins ONE query parameter that tatara-cli's scm_read schema
// advertises to a handler that actually READS it.
//
// It is pinned BEHAVIOURALLY: `with` must produce a different response body
// from `base`. A reflection or grep assertion would pass on a parameter the
// handler parses and then drops, which is exactly half of what #636 was.
//
// This is the AgentVisibleTools idiom (promptguidance/toolnames.go): a
// hand-maintained mirror of a tatara-cli literal, pinned by a test rather than
// fetched. The operator deliberately does not consume the cli's manifest -
// `make test` runs offline, and a fetch that soft-warns on failure is a check
// that can silently stop checking.
//
// The cli spells two of these differently from the wire: `since_days` ->
// `sinceDays`, `is_pr` -> `isPR`. The wire spelling is what is pinned here; the
// cli-side mapping is pinned by tools_scm_test.go.
type scmParamCase struct {
	kind  string
	param string
	base  string
	with  string
}

func scmAdvertisedParams() []scmParamCase {
	since := frozenNow.Add(-24 * time.Hour).UTC().Format(time.RFC3339)
	return []scmParamCase{
		{"issues", "repo", "/projects/tatara/scm/issues?repo=tatara-operator", "/projects/tatara/scm/issues?repo=tatara-cli"},
		{"issues", "state", "/projects/tatara/scm/issues?repo=tatara-operator", "/projects/tatara/scm/issues?repo=tatara-operator&state=all"},
		{"issues", "since", "/projects/tatara/scm/issues?repo=tatara-operator&state=all", "/projects/tatara/scm/issues?repo=tatara-operator&state=all&since=" + since},
		{"issues", "labels", "/projects/tatara/scm/issues?repo=tatara-operator", "/projects/tatara/scm/issues?repo=tatara-operator&labels=bug"},
		{"issues", "limit", "/projects/tatara/scm/issues?repo=tatara-operator", "/projects/tatara/scm/issues?repo=tatara-operator&limit=1"},

		{"mr", "repo", "/projects/tatara/scm/mrs?repo=tatara-operator", "/projects/tatara/scm/mrs?repo=tatara-cli"},
		{"mr", "state", "/projects/tatara/scm/mrs?repo=tatara-operator", "/projects/tatara/scm/mrs?repo=tatara-operator&state=all"},
		{"mr", "since", "/projects/tatara/scm/mrs?repo=tatara-operator&state=all", "/projects/tatara/scm/mrs?repo=tatara-operator&state=all&since=" + since},
		{"mr", "limit", "/projects/tatara/scm/mrs?repo=tatara-operator", "/projects/tatara/scm/mrs?repo=tatara-operator&limit=1"},

		{"comments", "repo", "/projects/tatara/scm/comments?repo=tatara-operator&number=1", "/projects/tatara/scm/comments?repo=tatara-cli&number=1"},
		{"comments", "number", "/projects/tatara/scm/comments?repo=tatara-operator&number=1", "/projects/tatara/scm/comments?repo=tatara-operator&number=2"},
		{"comments", "isPR", "/projects/tatara/scm/comments?repo=tatara-cli&number=80", "/projects/tatara/scm/comments?repo=tatara-cli&number=80&isPR=true"},

		{"commits", "repo", "/projects/tatara/scm/commits?repo=tatara-operator", "/projects/tatara/scm/commits?repo=tatara-cli"},
		{"commits", "sinceDays", "/projects/tatara/scm/commits?repo=tatara-operator", "/projects/tatara/scm/commits?repo=tatara-operator&sinceDays=7"},
		{"commits", "limit", "/projects/tatara/scm/commits?repo=tatara-operator", "/projects/tatara/scm/commits?repo=tatara-operator&limit=1"},

		{"ci", "repo", "/projects/tatara/scm/ci?repo=tatara-operator&number=80", "/projects/tatara/scm/ci?repo=tatara-cli&number=80"},
		{"ci", "number", "/projects/tatara/scm/ci?repo=tatara-operator&number=80", "/projects/tatara/scm/ci?repo=tatara-operator&number=81"},
	}
}

// echoCommitReader renders the (repo, since) it was called with INTO the
// commits it returns, so a dropped sinceDays is visible in the response body
// rather than only in a recorded field.
type echoCommitReader struct {
	scm.SCMReader
}

func (c *echoCommitReader) ListCommits(_ context.Context, _, name string, since time.Time) ([]scm.CommitRef, error) {
	return []scm.CommitRef{
		{SHA: "aaa1", Message: fmt.Sprintf("%s since=%s", name, since.UTC().Format(time.RFC3339)), Author: "szymonrychu", Date: frozenNow},
		{SHA: "bbb2", Message: "second", Author: "szymonrychu", Date: frozenNow},
	}, nil
}

func TestScmRead_EveryAdvertisedQueryParamChangesTheResponse(t *testing.T) {
	old := frozenNow.Add(-72 * time.Hour)
	recent := frozenNow.Add(-time.Hour)

	e := buildV2(t,
		v2Opts{
			reader: &echoCommitReader{},
			ci: &fakeCI{res: scm.CIResult{
				HeadSHA: "abc123", Status: "green", Mergeable: true,
				Checks: []scm.CICheck{{Name: "build", Status: "completed", Conclusion: "success", JobID: "1"}},
			}},
		},
		projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),

		// issues: #1 old + labelled, #2 closed, #3 recent + unlabelled.
		issueWithComment(issueUpdated("tatara-operator", 1, "open", old, "bug"), "c-op-1"),
		issueWithComment(issueUpdated("tatara-operator", 2, "closed", recent), "c-op-2"),
		issueUpdated("tatara-operator", 3, "open", recent),
		issueWithComment(issueUpdated("tatara-cli", 1, "open", recent), "c-cli-1"),
		// tatara-cli#80 exists as BOTH an issue and an MR, so isPR is the only
		// thing separating the two threads.
		issueWithComment(issueUpdated("tatara-cli", 80, "open", recent), "c-cli-issue-80"),

		// mrs: #90 old, #91 merged, #92 recent.
		mrUpdated("tatara-operator", 90, "open", old),
		mrUpdated("tatara-operator", 91, "merged", recent),
		mrUpdated("tatara-operator", 92, "open", recent),
		mrWithComment(mrUpdated("tatara-cli", 80, "open", recent), "c-cli-mr-80"),
	)

	for _, tc := range scmAdvertisedParams() {
		t.Run(tc.kind+"/"+tc.param, func(t *testing.T) {
			base := e.do(t, http.MethodGet, tc.base, "")
			require.Equal(t, http.StatusOK, base.Code, tc.base)
			with := e.do(t, http.MethodGet, tc.with, "")
			require.Equal(t, http.StatusOK, with.Code, tc.with)
			require.NotEqual(t, base.Body.String(), with.Body.String(),
				"scm_read(kind=%s) advertises %q and the handler does not read it", tc.kind, tc.param)
		})
	}
}

func issueWithComment(i *tatarav1alpha1.Issue, id string) *tatarav1alpha1.Issue {
	i.Status.Comments = []tatarav1alpha1.Comment{
		{ExternalID: id, Author: "human", Body: id, CreatedAt: metav1.NewTime(frozenNow)},
	}
	i.Status.CommentCount = 1
	return i
}

func mrWithComment(m *tatarav1alpha1.MergeRequest, id string) *tatarav1alpha1.MergeRequest {
	m.Status.Comments = []tatarav1alpha1.Comment{
		{ExternalID: id, Author: "human", Body: id, CreatedAt: metav1.NewTime(frozenNow)},
	}
	m.Status.CommentCount = 1
	return m
}
