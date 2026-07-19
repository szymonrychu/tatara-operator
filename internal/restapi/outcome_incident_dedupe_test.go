package restapi_test

import (
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func incidentTracker(repo string, number int, labels map[string]string) *tatarav1alpha1.Issue {
	return &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.IssueName(repo, number), Namespace: ns, Labels: labels,
		},
		Spec: tatarav1alpha1.IssueSpec{RepositoryRef: repo, Number: number, ProjectRef: "tatara"},
	}
}

// action=comment_issue appends the agent's evidence to an EXISTING tracker (no
// new issue) and terminates the Task at rejected(tracked-elsewhere). The gate
// passes because the target Issue CR carries an incident rule-key label.
func TestOutcome_Incident_CommentIssueAppendsToTracker(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	tracker := incidentTracker("tatara-memory", 7, map[string]string{queue.LabelAlertRuleKey: "abc123def4567890"}) //gitleaks:allow
	e := buildV2(t, v2Opts{metrics: metrics}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-memory", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"), tracker)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"comment_issue","alertRules":["cnpg-connections-high"],"reason":"same CNPG outage as tracker",
	  "comment":{"repo":"tatara-memory","number":7,"body":"fresh evidence: connections still saturated"}}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	require.Len(t, e.forge.comments, 1, "the evidence must be appended to the tracker")
	require.Equal(t, "acme/tatara-memory#7", e.forge.comments[0].Ref)
	require.Contains(t, e.forge.comments[0].Body, "connections still saturated")
	require.Empty(t, e.forge.createdReqs, "comment_issue files NO new issue")

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StageRejected, got.Status.Stage)
	require.Equal(t, "tracked-elsewhere", got.Status.StageReason)
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.IncidentTrackerCommentCounter("posted")))
}

// The gate: comment_issue on an Issue that is NOT an incident tracker (no
// rule/group label) is refused, so an incident agent cannot comment on an
// arbitrary human thread.
func TestOutcome_Incident_CommentIssueRejectsNonTracker(t *testing.T) {
	plain := incidentTracker("tatara-memory", 7, nil) // no alert label => not a tracker
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-memory", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"), plain)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"comment_issue","alertRules":["x"],"reason":"r",
	  "comment":{"repo":"tatara-memory","number":7,"body":"evidence"}}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Empty(t, e.forge.comments, "no comment may be posted to a non-tracker issue")
	require.Equal(t, tatarav1alpha1.StageInvestigating, e.task(t, "t1").Status.Stage)
}

// comment_issue with a missing comment body is a 400; a comment on a
// false_positive action is a 400 (unexpected field).
func TestOutcome_Incident_CommentIssueValidation(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"comment_issue","alertRules":["x"],"reason":"r",
	  "comment":{"repo":"tatara-operator","number":7,"body":""}}}`)
	require.Equal(t, http.StatusBadRequest, w.Code, "empty comment.body must be rejected")

	w = e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"false_positive","alertRules":["x"],"reason":"r",
	  "comment":{"repo":"tatara-operator","number":7,"body":"b"}}}`)
	require.Equal(t, http.StatusBadRequest, w.Code, "comment is only for action=comment_issue")
}

// A file_issue whose incident Task shares a GROUP key with an OLDER open sibling
// tracker (different rule) is AUTO-LINKED under that sibling as a sub-issue, even
// though the agent named no parent - collapsing a co-firing storm into one tree.
func TestOutcome_Incident_FileIssueAutoLinksGroupSibling(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	groupKey := "group0123456789a"
	sibling := incidentTracker("tatara-memory", 7, map[string]string{
		queue.LabelAlertGroupKey: groupKey, queue.LabelAlertRuleKey: "rulekeyB0000000"})
	task := taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident")
	task.Spec.DedupKey = "rulekeyA0000000"
	task.Spec.GroupKey = groupKey
	e := buildV2(t, v2Opts{metrics: metrics}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-memory", "tatara"), task, sibling)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["postgres-down"],"reason":"co-firing with CNPG tracker",
	  "issue":{"repo":"tatara-operator","title":"postgres down","body":"trace"}}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	require.Len(t, e.forge.subIssueCalls, 1, "the new tracker must be auto-linked under the group sibling")
	require.Equal(t, "acme/tatara-memory#7", e.forge.subIssueCalls[0].ParentRef)
	require.Equal(t, 101, e.forge.subIssueCalls[0].ChildNumber)
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.IncidentGroupLinkedCounter("linked")))

	// The minted tracker also carries the group-key label so a later co-firing
	// sibling links under it too.
	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	require.Equal(t, groupKey, iss.Labels[queue.LabelAlertGroupKey])
}
