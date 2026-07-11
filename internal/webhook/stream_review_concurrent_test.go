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

// TestPRCreate_ConcurrentLoserJoinsMaterializedStreamReview is the finding-4
// contract: two PR-creates for one shared stream branch must BOTH end up in the
// single stream review span - the deterministic-name collapse must not silently
// drop the loser. Once the winner's review Task has materialized, the loser's
// PR-create joins it (no second review Task, no dropped PR). Modelled here by a
// materialized stream review Task plus the umbrella; the second repo's PR-create
// joins the existing review span.
func TestPRCreate_ConcurrentLoserJoinsMaterializedStreamReview(t *testing.T) {
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
	// The winner's stream review Task, already materialized, keyed on branch "feature".
	review := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "stream-review", Namespace: ns,
			Annotations: map[string]string{tatarav1.AnnReviewHeadBranch: "feature"},
		},
		Spec: tatarav1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: "scmrepo", Kind: "review",
			Source: &tatarav1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
		},
	}
	srv, c := newWebhookServer(t, proj, repo1, repo2, impl, review)
	h := srv.Server.Handler()

	// The winner PR (o/r#9) is already in the review span; the loser PR (o/r2#21) is not.
	var rt tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "stream-review"}, &rt))
	rt.Status.Phase = "Running"
	rt.Status.WorkItems = []tatarav1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1.WorkItemPR, Role: tatarav1.RoleOpenedPR, State: tatarav1.WIOpen, HeadBranch: "feature"},
	}
	require.NoError(t, c.Status().Update(context.Background(), &rt))

	// Loser PR-create for the second repo on the SAME branch.
	body := prOpenedBody("o/r2", "https://github.com/o/r2.git", 21, "feature", "tatara-bot")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign("whsec", body))

	w := post(t, h, proj.Name, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	// No second review Task/QueuedEvent: the stream collapses to one review.
	require.Empty(t, allQEs(t, c, proj.Name), "the loser PR-create must not spawn a second review task")

	// The loser PR joined the single stream review span (not dropped).
	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "stream-review"}, &got))
	var joined bool
	for i := range got.Status.WorkItems {
		if got.Status.WorkItems[i].Repo == "o/r2" && got.Status.WorkItems[i].Number == 21 {
			joined = true
			require.Equal(t, tatarav1.RoleOpenedPR, got.Status.WorkItems[i].Role)
		}
	}
	require.True(t, joined, "the concurrent loser PR must be joined to the stream review span, never dropped")
}
