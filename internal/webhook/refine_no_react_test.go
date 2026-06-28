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

// TestRefineEditOnBacklogIssueSpawnsNothing guards the refiner's "no reacting
// agents" contract: the refiner edits backlog proposals (brainstorming-labelled,
// no trigger label) as the bot. Such an issues event must spawn no task - the
// handler ignores issue events that do not carry the trigger label, so a bot
// edit on a backlog proposal cascades into nothing.
func TestRefineEditOnBacklogIssueSpawnsNothing(t *testing.T) {
	const secretVal = "whsec"
	proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
	proj.Name = "refine-noreact-proj"
	proj.Namespace = ns
	proj.Spec.ScmSecretRef = "refine-noreact-scm"
	proj.Spec.TriggerLabel = "tatara"

	c := seedClient(t,
		proj,
		&tatarav1.Repository{
			ObjectMeta: metav1.ObjectMeta{Name: "refine-noreact-repo", Namespace: ns},
			Spec:       tatarav1.RepositorySpec{ProjectRef: "refine-noreact-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main"},
		},
		secret("refine-noreact-scm", secretVal),
	)
	h, _ := newServer(t, c)

	// Bot edits a backlog proposal: action=edited, sender=bot, labels carry only
	// tatara-brainstorming (NOT the trigger label "tatara").
	body := []byte(`{"action":"edited","sender":{"login":"tatara-bot"},"issue":{"number":7,"title":"Narrowed scope","body":"refined","user":{"login":"tatara-bot"},"labels":[{"name":"tatara-brainstorming"}],"html_url":"https://github.com/o/r/issues/7"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "refine-noreact-proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	matching := 0
	for _, qe := range qel.Items {
		if qe.Spec.ProjectRef == "refine-noreact-proj" {
			matching++
		}
	}
	require.Equal(t, 0, matching,
		"a bot edit on a non-trigger-labelled backlog issue must spawn no task")
}
