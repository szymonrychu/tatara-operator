package webhook_test

// Task 4d: the human pull_request_review path. postReview/reviewBody render a
// GitHub pull_request_review delivery; reviewTask/reviewMR seed the owning
// Task + owned MergeRequest CR the same way primary_mint_test.go's owned-mirror
// tests do (own.AddPlainOwner + own.HandOverController).

import (
	"context"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
)

// reviewProject builds a Project with a GitHub bot login and a maintainer
// allowlist, for the review-routing tests.
func reviewProject(name, secretRef, bot string, maintainers []string) *tatarav1.Project {
	return &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: secretRef,
			Scm: &tatarav1.ScmSpec{
				Provider: "github", Owner: "o", BotLogin: bot,
				MaintainerLogins: maintainers,
			},
		},
	}
}

// reviewTask builds a Task in reviewing, of the given kind, for the
// review-routing tests.
func reviewTask(name, projName, kind string) *tatarav1.Task {
	task := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       tatarav1.TaskSpec{ProjectRef: projName, Kind: kind, Goal: "g"},
	}
	task.Status.Stage = tatarav1.StageReviewing
	ent := metav1.Now()
	task.Status.StageEnteredAt = &ent
	return task
}

// reviewMR builds an open MergeRequest CR, controller-owned by task, for the
// review-routing tests.
func reviewMR(name, projName, repoName string, number int, task *tatarav1.Task) *tatarav1.MergeRequest {
	mr := &tatarav1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       tatarav1.MergeRequestSpec{RepositoryRef: repoName, Number: number, ProjectRef: projName},
	}
	mr.Status.State = "open"
	own.AddPlainOwner(mr, task)
	if err := own.HandOverController(mr, nil, task); err != nil {
		panic(err)
	}
	return mr
}

// reviewBody renders a pull_request_review.<action> delivery: state on the
// review object, id as the review id, reviewer as both the review's and the
// event's actor, targeting PR number.
func reviewBody(action, state string, id int, reviewer string, number int) []byte {
	return reviewBodyWithText(action, state, id, reviewer, number, "")
}

// reviewBodyWithText is reviewBody plus an explicit review.body, for cases
// (e.g. commented) where the review text itself matters.
func reviewBodyWithText(action, state string, id int, reviewer string, number int, text string) []byte {
	n := strconv.Itoa(number)
	idStr := strconv.Itoa(id)
	return []byte(`{"action":"` + action + `",
		"review":{"id":` + idStr + `,"state":"` + state + `","commit_id":"deadbeef","body":"` + text + `","user":{"login":"` + reviewer + `"}},
		"pull_request":{"number":` + n + `,"user":{"login":"alice"},"head":{"sha":"deadbeef","ref":"fix"},"html_url":"u"},
		"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},
		"sender":{"login":"` + reviewer + `"}}`)
}

// postReview signs and delivers a pull_request_review webhook, asserting a 202
// (every path - acted or ignored - accepts with 202; only routing/auth
// failures upstream of handleReview would 4xx/5xx, and none of these tests
// exercise those).
func postReview(t *testing.T, h http.Handler, projName, secretVal string, body []byte) {
	t.Helper()
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request_review")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))
	w := post(t, h, projName, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)
}

func getTask(t *testing.T, c client.Client, name string) *tatarav1.Task {
	t.Helper()
	got := &tatarav1.Task{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got))
	return got
}

// A maintainer's changes_requested on a Tatara-owned unmerged MR re-enters
// implementing.
func TestReview_ChangesRequested_ReentersImplementing(t *testing.T) {
	const secretVal = "whsec-rv1"
	proj := reviewProject("rv1", "rv1-scm", "tatara-bot", []string{"maint"})
	repo := repository("repo-rv1", "rv1", "https://github.com/o/r.git", "main")
	task := reviewTask("rv1-task", "rv1", "clarify")
	mr := reviewMR(tatarav1.MergeRequestName(repo.Name, 42), "rv1", repo.Name, 42, task)
	c := seedClient(t, proj, secret("rv1-scm", secretVal), repo, task, mr)
	h, _ := newServer(t, c)

	postReview(t, h, "rv1", secretVal, reviewBody("submitted", "changes_requested", 900, "maint", 42))

	got := getTask(t, c, task.Name)
	require.Equal(t, tatarav1.StageImplementing, got.Status.Stage)
}

// A non-maintainer review is ignored.
func TestReview_NonMaintainer_Ignored(t *testing.T) {
	const secretVal = "whsec-rv2"
	proj := reviewProject("rv2", "rv2-scm", "tatara-bot", []string{"maint"})
	repo := repository("repo-rv2", "rv2", "https://github.com/o/r.git", "main")
	task := reviewTask("rv2-task", "rv2", "clarify")
	mr := reviewMR(tatarav1.MergeRequestName(repo.Name, 43), "rv2", repo.Name, 43, task)
	c := seedClient(t, proj, secret("rv2-scm", secretVal), repo, task, mr)
	h, _ := newServer(t, c)

	postReview(t, h, "rv2", secretVal, reviewBody("submitted", "changes_requested", 901, "rando", 43))

	got := getTask(t, c, task.Name)
	require.Equal(t, tatarav1.StageReviewing, got.Status.Stage, "a non-maintainer's review must have no effect")
}

// A maintainer approval enters merging.
func TestReview_Approved_EntersMerging(t *testing.T) {
	const secretVal = "whsec-rv3"
	proj := reviewProject("rv3", "rv3-scm", "tatara-bot", []string{"maint"})
	repo := repository("repo-rv3", "rv3", "https://github.com/o/r.git", "main")
	task := reviewTask("rv3-task", "rv3", "clarify")
	mr := reviewMR(tatarav1.MergeRequestName(repo.Name, 44), "rv3", repo.Name, 44, task)
	mr.Status.PendingReview = &tatarav1.PendingReview{Round: 1} // bot review still owed
	c := seedClient(t, proj, secret("rv3-scm", secretVal), repo, task, mr)
	h, _ := newServer(t, c)

	postReview(t, h, "rv3", secretVal, reviewBody("submitted", "approved", 902, "maint", 44))

	got := getTask(t, c, task.Name)
	require.Equal(t, tatarav1.StageMerging, got.Status.Stage)

	var gotMR tatarav1.MergeRequest
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: mr.Name}, &gotMR))
	require.Nil(t, gotMR.Status.PendingReview, "maintainer approval short-circuits the pending bot review")
	require.Equal(t, "approved", gotMR.Status.Status)
}

// changes_requested on an adopted human PR (owning Task Kind=review) does NOT
// drive implementing.
func TestReview_ChangesRequested_ReviewKind_NotDriven(t *testing.T) {
	const secretVal = "whsec-rv4"
	proj := reviewProject("rv4", "rv4-scm", "tatara-bot", []string{"maint"})
	repo := repository("repo-rv4", "rv4", "https://github.com/o/r.git", "main")
	task := reviewTask("rv4-task", "rv4", "review")
	mr := reviewMR(tatarav1.MergeRequestName(repo.Name, 45), "rv4", repo.Name, 45, task)
	c := seedClient(t, proj, secret("rv4-scm", secretVal), repo, task, mr)
	h, _ := newServer(t, c)

	postReview(t, h, "rv4", secretVal, reviewBody("submitted", "changes_requested", 903, "maint", 45))

	got := getTask(t, c, task.Name)
	require.Equal(t, tatarav1.StageReviewing, got.Status.Stage, "a kind=review Task is only reviewed, never driven to implementing")
}

// The SAME (review.id, state) delivered twice fires the transition once. A
// Task at merging has a legal edge back to implementing on changes_requested
// (Task 4b); after the first delivery flips it there, the Task is manually
// pushed back to merging (simulating independent progress) so a SECOND,
// otherwise-legal delivery of the identical event would visibly re-fire if
// the (review.id, state) dedup were not honored.
func TestReview_DedupOnReviewIDState(t *testing.T) {
	const secretVal = "whsec-rv5"
	proj := reviewProject("rv5", "rv5-scm", "tatara-bot", []string{"maint"})
	repo := repository("repo-rv5", "rv5", "https://github.com/o/r.git", "main")
	task := reviewTask("rv5-task", "rv5", "clarify")
	task.Status.Stage = tatarav1.StageMerging
	mr := reviewMR(tatarav1.MergeRequestName(repo.Name, 46), "rv5", repo.Name, 46, task)
	c := seedClient(t, proj, secret("rv5-scm", secretVal), repo, task, mr)
	h, _ := newServer(t, c)

	postReview(t, h, "rv5", secretVal, reviewBody("submitted", "changes_requested", 904, "maint", 46))
	got := getTask(t, c, task.Name)
	require.Equal(t, tatarav1.StageImplementing, got.Status.Stage, "first delivery re-enters implementing")

	// Simulate independent progress back to merging - the same edge that just
	// fired is legal again, so only the dedup marker can stop a re-fire.
	got.Status.Stage = tatarav1.StageMerging
	require.NoError(t, c.Status().Update(context.Background(), got))

	postReview(t, h, "rv5", secretVal, reviewBody("submitted", "changes_requested", 904, "maint", 46))
	got2 := getTask(t, c, task.Name)
	require.Equal(t, tatarav1.StageMerging, got2.Status.Stage, "the second identical (review.id,state) delivery must not re-fire")
}

// A maintainer's commented review folds to the pending-event path (contract
// E.3) carrying the review TEXT along, so the review agent picking up the
// Task's pendingEvents actually sees what the maintainer said instead of an
// empty Body.
func TestReview_Commented_CarriesBodyToPendingEvent(t *testing.T) {
	const secretVal = "whsec-rv7"
	proj := reviewProject("rv7", "rv7-scm", "tatara-bot", []string{"maint"})
	repo := repository("repo-rv7", "rv7", "https://github.com/o/r.git", "main")
	task := reviewTask("rv7-task", "rv7", "clarify")
	mr := reviewMR(tatarav1.MergeRequestName(repo.Name, 48), "rv7", repo.Name, 48, task)
	c := seedClient(t, proj, secret("rv7-scm", secretVal), repo, task, mr)
	h, _ := newServer(t, c)

	postReview(t, h, "rv7", secretVal, reviewBodyWithText("submitted", "commented", 906, "maint", 48, "please rename this var"))

	got := getTask(t, c, task.Name)
	require.Len(t, got.Status.PendingEvents, 1)
	require.Equal(t, "please rename this var", got.Status.PendingEvents[0].Body)
}

// A terminal (failed) owning Task is never resurrected: changes_requested on it
// is ignored, the stage is untouched (server.go TaskDone guard).
func TestReview_TerminalTask_Ignored(t *testing.T) {
	const secretVal = "whsec-rv8"
	proj := reviewProject("rv8", "rv8-scm", "tatara-bot", []string{"maint"})
	repo := repository("repo-rv8", "rv8", "https://github.com/o/r.git", "main")
	task := reviewTask("rv8-task", "rv8", "clarify")
	task.Status.Stage = tatarav1.StageFailed
	task.Status.StageReason = "turn-budget-exhausted"
	mr := reviewMR(tatarav1.MergeRequestName(repo.Name, 49), "rv8", repo.Name, 49, task)
	c := seedClient(t, proj, secret("rv8-scm", secretVal), repo, task, mr)
	h, _ := newServer(t, c)

	postReview(t, h, "rv8", secretVal, reviewBody("submitted", "changes_requested", 907, "maint", 49))

	got := getTask(t, c, task.Name)
	require.Equal(t, tatarav1.StageFailed, got.Status.Stage, "a terminal Task is never resurrected")
}

// changes_requested on a parked(merge-timeout) Task resumes MERGING (F1), routed
// by the park reason - not implementing.
func TestReview_ChangesRequested_ParkedMergeTimeout_ResumesMerging(t *testing.T) {
	const secretVal = "whsec-rv9"
	proj := reviewProject("rv9", "rv9-scm", "tatara-bot", []string{"maint"})
	repo := repository("repo-rv9", "rv9", "https://github.com/o/r.git", "main")
	task := reviewTask("rv9-task", "rv9", "clarify")
	task.Status.Stage = tatarav1.StageParked
	task.Status.StageReason = "merge-timeout"
	mr := reviewMR(tatarav1.MergeRequestName(repo.Name, 50), "rv9", repo.Name, 50, task)
	c := seedClient(t, proj, secret("rv9-scm", secretVal), repo, task, mr)
	h, _ := newServer(t, c)

	postReview(t, h, "rv9", secretVal, reviewBody("submitted", "changes_requested", 908, "maint", 50))

	got := getTask(t, c, task.Name)
	require.Equal(t, tatarav1.StageMerging, got.Status.Stage, "merge-timeout re-enters merging, never implementing")
	require.Equal(t, 1, got.Status.MergeReentries)
}

// changes_requested on a parked(review-loop-exhausted) Task folds to the
// pending-event path (the review is not lost) and does NOT re-enter (F1).
func TestReview_ChangesRequested_ParkedReviewLoopExhausted_Folds(t *testing.T) {
	const secretVal = "whsec-rv10"
	proj := reviewProject("rv10", "rv10-scm", "tatara-bot", []string{"maint"})
	repo := repository("repo-rv10", "rv10", "https://github.com/o/r.git", "main")
	task := reviewTask("rv10-task", "rv10", "clarify")
	task.Status.Stage = tatarav1.StageParked
	task.Status.StageReason = "review-loop-exhausted"
	mr := reviewMR(tatarav1.MergeRequestName(repo.Name, 51), "rv10", repo.Name, 51, task)
	c := seedClient(t, proj, secret("rv10-scm", secretVal), repo, task, mr)
	h, _ := newServer(t, c)

	postReview(t, h, "rv10", secretVal, reviewBody("submitted", "changes_requested", 909, "maint", 51))

	got := getTask(t, c, task.Name)
	require.Equal(t, tatarav1.StageParked, got.Status.Stage, "review-loop-exhausted must not re-enter on a human review")
	require.Len(t, got.Status.PendingEvents, 1, "the review folds to the pending-event path, not lost")
}

// dismissed / edited actions are ignored (Action != submitted).
func TestReview_Dismissed_Ignored(t *testing.T) {
	const secretVal = "whsec-rv6"
	proj := reviewProject("rv6", "rv6-scm", "tatara-bot", []string{"maint"})
	repo := repository("repo-rv6", "rv6", "https://github.com/o/r.git", "main")
	task := reviewTask("rv6-task", "rv6", "clarify")
	mr := reviewMR(tatarav1.MergeRequestName(repo.Name, 47), "rv6", repo.Name, 47, task)
	c := seedClient(t, proj, secret("rv6-scm", secretVal), repo, task, mr)
	h, _ := newServer(t, c)

	postReview(t, h, "rv6", secretVal, reviewBody("dismissed", "dismissed", 905, "maint", 47))

	got := getTask(t, c, task.Name)
	require.Equal(t, tatarav1.StageReviewing, got.Status.Stage, "a dismissed review must have no effect")
}
