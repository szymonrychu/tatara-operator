package webhook_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// prOpenedBody returns a GitHub pull_request "opened" webhook body for the given
// repo full-name / clone-url, PR number, head branch, author, and (optional) label.
func prOpenedBody(fullName, cloneURL string, number int, headRef, author string) []byte {
	return []byte(`{"action":"opened","sender":{"login":"` + author + `"},` +
		`"pull_request":{"number":` + itoa(number) + `,"title":"PR","body":"body",` +
		`"user":{"login":"` + author + `"},"labels":[{"name":"tatara"}],` +
		`"html_url":"https://github.com/` + fullName + `/pull/` + itoa(number) + `",` +
		`"head":{"sha":"deadbeef","ref":"` + headRef + `"}},` +
		`"repository":{"clone_url":"` + cloneURL + `","full_name":"` + fullName + `"}}`)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestPRCreate_JoinsExistingStreamReviewUmbrella is U-C (a): a PR-create for a repo
// whose PR shares the stream's head branch JOINS the existing stream review
// umbrella - the PR is added to that Task's ledger as role:openedPR and NO new
// per-PR review Task (QueuedEvent) is created.
func TestPRCreate_JoinsExistingStreamReviewUmbrella(t *testing.T) {
	proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
	repo1 := repository("scmrepo", proj.Name, "https://github.com/o/r.git", "main")
	repo2 := repository("scmrepo2", proj.Name, "https://github.com/o/r2.git", "main")
	review := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "stream-review", Namespace: ns},
		Spec: tatarav1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: "scmrepo", Kind: "review",
			Source: &tatarav1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
		},
	}
	srv, c := newWebhookServer(t, proj, repo1, repo2, review)
	h := srv.Server.Handler()

	// Seed the review umbrella's ledger: it already spans o/r#9 on branch "feature".
	var rt tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "stream-review"}, &rt))
	rt.Status.Phase = "Running"
	rt.Status.WorkItems = []tatarav1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1.WorkItemPR, Role: tatarav1.RoleOpenedPR, State: tatarav1.WIOpen, HeadBranch: "feature"},
	}
	require.NoError(t, c.Status().Update(context.Background(), &rt))

	body := prOpenedBody("o/r2", "https://github.com/o/r2.git", 21, "feature", "tatara-bot")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign("whsec", body))

	w := post(t, h, proj.Name, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	// No new per-PR review Task was queued.
	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Empty(t, qel.Items, "joining an umbrella must not create a per-PR review task")

	// The PR is now a role:openedPR member of the umbrella review Task.
	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "stream-review"}, &got))
	var found *tatarav1.WorkItemRef
	for i := range got.Status.WorkItems {
		if got.Status.WorkItems[i].Repo == "o/r2" && got.Status.WorkItems[i].Number == 21 {
			found = &got.Status.WorkItems[i]
		}
	}
	require.NotNil(t, found, "the joined PR must be added to the umbrella ledger")
	require.Equal(t, tatarav1.RoleOpenedPR, found.Role)
	require.Equal(t, tatarav1.WorkItemPR, found.Kind)
	require.Equal(t, "feature", found.HeadBranch)
}

// TestPRCreate_LinkedIssueOnlyDoesNotJoinStream is finding #5: a PR whose body only
// cites the umbrella's source issue ("Closes #5") but does NOT share the stream head
// branch must NOT auto-join the collective stream review. Stream membership is
// tightened to the STRONG branch-match signal; a linked-issue-only match falls
// through to the normal per-PR review path (its own review Task), never sweeping the
// PR into the collective approve/withhold.
func TestPRCreate_LinkedIssueOnlyDoesNotJoinStream(t *testing.T) {
	proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
	repo1 := repository("scmrepo", proj.Name, "https://github.com/o/r.git", "main")
	review := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "stream-review", Namespace: ns},
		Spec: tatarav1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: "scmrepo", Kind: "review",
			Source: &tatarav1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
		},
	}
	srv, c := newWebhookServer(t, proj, repo1, review)
	h := srv.Server.Handler()

	// The stream review spans branch "feature" (role:openedPR o/r#9) and Source o/r#5.
	var rt tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "stream-review"}, &rt))
	rt.Status.Phase = "Running"
	rt.Status.WorkItems = []tatarav1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1.WorkItemPR, Role: tatarav1.RoleOpenedPR, State: tatarav1.WIOpen, HeadBranch: "feature"},
	}
	require.NoError(t, c.Status().Update(context.Background(), &rt))

	// A human PR on a DIFFERENT branch whose body cites the umbrella's source issue.
	body := []byte(`{"action":"opened","sender":{"login":"octocat"},` +
		`"pull_request":{"number":77,"title":"PR","body":"Closes #5",` +
		`"user":{"login":"octocat"},"labels":[{"name":"tatara"}],` +
		`"html_url":"https://github.com/o/r/pull/77",` +
		`"head":{"sha":"cafef00d","ref":"human-branch"}},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign("whsec", body))

	w := post(t, h, proj.Name, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	// The PR did NOT join the stream review umbrella.
	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "stream-review"}, &got))
	for i := range got.Status.WorkItems {
		require.False(t, got.Status.WorkItems[i].Repo == "o/r" && got.Status.WorkItems[i].Number == 77,
			"a linked-issue-only PR on a different branch must NOT join the stream review")
	}

	// It fell through to the normal per-PR review path (its own review Task).
	qe := singleQueuedEvent(t, c, proj.Name)
	require.Equal(t, "review", qe.Spec.Payload.Kind)
	require.NotNil(t, qe.Spec.Payload.Source)
	require.True(t, qe.Spec.Payload.Source.IsPR, "per-PR review Source is the PR itself")
	require.Equal(t, 77, qe.Spec.Payload.Source.Number)
}

// TestPRCreate_SpawnsSingleStreamReviewFromImplementUmbrella verifies the first
// PR-create for a stream that has only an implement umbrella spawns ONE stream
// review Task carrying the umbrella's originating issue as Spec.Source and the
// shared branch as AnnReviewHeadBranch (not a per-PR review of the PR itself).
func TestPRCreate_SpawnsSingleStreamReviewFromImplementUmbrella(t *testing.T) {
	proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
	repo1 := repository("scmrepo", proj.Name, "https://github.com/o/r.git", "main")
	repo2 := repository("scmrepo2", proj.Name, "https://github.com/o/r2.git", "main")
	impl := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "stream-impl", Namespace: ns},
		Spec: tatarav1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: "scmrepo", Kind: "implement",
			Source: &tatarav1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
		},
	}
	srv, c := newWebhookServer(t, proj, repo1, repo2, impl)
	h := srv.Server.Handler()

	// The implement umbrella opened PRs on branch "feature" (role:openedPR).
	var it tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "stream-impl"}, &it))
	it.Status.Phase = "Succeeded"
	it.Status.WorkItems = []tatarav1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 5, Kind: tatarav1.WorkItemIssue, Role: tatarav1.RoleSource, State: tatarav1.WIOpen},
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1.WorkItemPR, Role: tatarav1.RoleOpenedPR, State: tatarav1.WIOpen, HeadBranch: "feature"},
	}
	require.NoError(t, c.Status().Update(context.Background(), &it))

	body := prOpenedBody("o/r", "https://github.com/o/r.git", 9, "feature", "tatara-bot")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign("whsec", body))

	w := post(t, h, proj.Name, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	qe := singleQueuedEvent(t, c, proj.Name)
	require.Equal(t, "review", qe.Spec.Payload.Kind)
	require.NotNil(t, qe.Spec.Payload.Source)
	require.False(t, qe.Spec.Payload.Source.IsPR, "stream review Source is the originating issue, not the PR")
	require.Equal(t, "o/r#5", qe.Spec.Payload.Source.IssueRef)
	require.Equal(t, "feature", qe.Spec.Payload.Annotations[tatarav1.AnnReviewHeadBranch])
}
