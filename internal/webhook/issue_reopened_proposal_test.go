package webhook_test

// O9 reachability: the leader's reopen-undo for a RETAINED declined brainstorm
// proposal gates on Issue.Status.State == "open". handleIssueClosed was the only
// webhook writer of that field, and MintForItem writes it only when it CREATES
// the CR - which it never does for a retained mirror, because that mirror still
// exists and is still controller-owned. So without the stamp below the gate can
// never fire, the undo is dead code, and a reopened proposal is wedged: never
// counted, never minted, and finally cascaded by its owner's reap.
//
// This test exists to pin the WRITER, not the undo's logic (that is covered in
// internal/controller). It asserts the exact precondition the leader gate reads.

import (
	"context"
	"net/http"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/webhook"
)

// noopSpiller satisfies objbudget.Spiller. The shared newServer helper leaves
// Config.Spiller nil, which makes every mirror status write in this package a
// silent no-op - fine for the tests that only assert on mints, useless here,
// since the mirror write IS what is under test.
type noopSpiller struct{}

func (noopSpiller) Spill(context.Context, string, string, any) (string, error) { return "track-1", nil }

func newServerWithSpiller(c client.Client) http.Handler {
	return webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Seq:       &queue.SeqSource{Client: c, Namespace: ns},
		Spiller:   noopSpiller{},
	}).Handler()
}

// issueReopenedByOther renders an issues.reopened delivery whose ISSUE author is
// author and whose SENDER is actor - the real shape of a maintainer reopening a
// bot-filed proposal. issueOpenedBy makes both the same login, which cannot
// model it.
func issueReopenedByOther(author, actor string, number int) []byte {
	n := strconv.Itoa(number)
	return []byte(`{"action":"reopened","issue":{"number":` + n +
		`,"title":"an idea","body":"<!-- tatara-proposed-by:brainstorm -->\n\nan idea worth having",` +
		`"user":{"login":"` + author + `"},` +
		`"html_url":"https://github.com/o/r/issues/` + n + `"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},` +
		`"sender":{"login":"` + actor + `"}}`)
}

func TestIssueReopened_MirrorsOpenStateOntoARetainedProposal(t *testing.T) {
	const secretVal = "whsec-reopen1"
	const number = 77

	body := "<!-- tatara-proposed-by:brainstorm -->\n\nan idea worth having"
	yes := true
	task := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "clarify-1", Namespace: ns},
		Spec:       tatarav1.TaskSpec{ProjectRef: "rp", Kind: "brainstorm"},
	}
	// The exact shape recordProposalDecline leaves behind.
	iss := &tatarav1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1.IssueName("repo-reopen", number), Namespace: ns,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: tatarav1.GroupVersion.String(), Kind: "Task",
				Name: task.Name, Controller: &yes,
			}},
		},
		Spec: tatarav1.IssueSpec{
			RepositoryRef: "repo-reopen", Number: number, ProjectRef: "rp",
			ProposalKind:     tatarav1.ProposalKindBrainstorm,
			ProposalBodyHash: tatarav1.ComputeProposalContentHash(body),
		},
	}
	iss.Status.State, iss.Status.Status = "closed", "rejected"
	iss.Status.Author, iss.Status.Body = "tatara-bot", body

	c := seedClient(t,
		projectWithReporters("rp", "rp-scm", "tatara", "tatara-bot", nil),
		secret("rp-scm", secretVal),
		repository("repo-reopen", "rp", "https://github.com/o/r.git", "main"),
		task, iss,
	)
	h := newServerWithSpiller(c)
	postIssueOpened(t, h, "rp", secretVal, issueReopenedByOther("tatara-bot", "alice", number))

	var got tatarav1.Issue
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: ns, Name: iss.Name}, &got))
	require.Equal(t, "open", got.Status.State,
		"the reopen must reach the mirror, or the leader's reopen-undo gate can never fire")
	require.Equal(t, "rejected", got.Status.Status,
		"the webhook only mirrors forge state; clearing the verdict is the leader's job")
}
