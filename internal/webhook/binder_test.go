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

// TestLabeledIssueWebhookCreatesIssueLifecycleImplement asserts that when an
// issue webhook arrives with the triggerLabel, the created Task has
// Kind=issueLifecycle and Status.LifecycleState=Implement.
// (kind switch: was "implement", now "issueLifecycle"; migration note: in-flight
// "implement" tasks created before this deploy still complete via old writeback arm)
func TestLabeledIssueWebhookCreatesIssueLifecycleImplement(t *testing.T) {
	const secretVal = "whsec"
	proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
	proj.Name = "lc-issue-proj"
	proj.Namespace = ns
	proj.Spec.ScmSecretRef = "lc-issue-scm"
	proj.Spec.TriggerLabel = "tatara"

	c := seedClient(t,
		proj,
		&tatarav1.Repository{
			ObjectMeta: metav1.ObjectMeta{Name: "lc-issue-repo", Namespace: ns},
			Spec:       tatarav1.RepositorySpec{ProjectRef: "lc-issue-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main"},
		},
		secret("lc-issue-scm", secretVal),
	)
	h, _ := newServer(t, c)

	body := []byte(`{"action":"labeled","sender":{"login":"alice"},"label":{"name":"tatara"},"issue":{"number":7,"title":"Fix","body":"please fix","user":{"login":"alice"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/7"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "lc-issue-proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	var matching []tatarav1.QueuedEvent
	for _, qe := range qel.Items {
		if qe.Spec.ProjectRef == "lc-issue-proj" {
			matching = append(matching, qe)
		}
	}
	require.Len(t, matching, 1)
	qe := matching[0]
	require.Equal(t, "issueLifecycle", qe.Spec.Payload.Kind, "labeled issue must create issueLifecycle kind")
	// Entry state is now carried by the lifecycle-entry annotation (FIX 3+5).
	require.Equal(t, "Implement", qe.Spec.Payload.Annotations["tatara.dev/lifecycle-entry"], "trigger-labeled issue must have lifecycle-entry=Implement annotation")
}

// TestBotPRWebhookCreatesIssueLifecycleMRCI asserts that a webhook for a
// bot-authored PR creates Kind=issueLifecycle with LifecycleState=MRCI and
// PRNumber set.
// (kind switch: was "selfImprove", now "issueLifecycle"; migration note: in-flight
// "selfImprove" tasks created before this deploy still complete via old writeback arm)
func TestBotPRWebhookCreatesIssueLifecycleMRCI(t *testing.T) {
	const secretVal = "whsec"
	proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
	proj.Name = "lc-botpr-proj"
	proj.Namespace = ns
	proj.Spec.ScmSecretRef = "lc-botpr-scm"
	proj.Spec.TriggerLabel = "tatara"

	c := seedClient(t,
		proj,
		&tatarav1.Repository{
			ObjectMeta: metav1.ObjectMeta{Name: "lc-botpr-repo", Namespace: ns},
			Spec:       tatarav1.RepositorySpec{ProjectRef: "lc-botpr-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main"},
		},
		secret("lc-botpr-scm", secretVal),
	)
	h, _ := newServer(t, c)

	// Bot-authored PR with trigger label
	body := []byte(`{"action":"opened","sender":{"login":"tatara-bot"},"pull_request":{"number":9,"title":"PR","body":"Closes #5\ndescription","user":{"login":"tatara-bot"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/9","head":{"sha":"deadbeef","ref":"feature"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "lc-botpr-proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	var matching []tatarav1.QueuedEvent
	for _, qe := range qel.Items {
		if qe.Spec.ProjectRef == "lc-botpr-proj" {
			matching = append(matching, qe)
		}
	}
	require.Len(t, matching, 1)
	qe := matching[0]
	require.Equal(t, "issueLifecycle", qe.Spec.Payload.Kind, "bot PR must create issueLifecycle kind")
	// Entry state is now carried by the lifecycle-entry annotation (FIX 3+5).
	require.Equal(t, "MRCI", qe.Spec.Payload.Annotations["tatara.dev/lifecycle-entry"], "bot PR must have lifecycle-entry=MRCI annotation")
	require.Equal(t, 9, qe.Spec.Payload.Source.Number, "Spec.Source.Number must be PR number")
}

// ----- FIX 3 + FIX 5: atomic lifecycle entry via create-time annotation -----

// TestLabeledIssueWebhook_EntryAnnotationImplement asserts that a trigger-labeled
// issue creates a Task whose tatara.dev/lifecycle-entry annotation is "Implement",
// so reconcileLifecycle can initialize LifecycleState atomically from the annotation
// without a post-create status update.
func TestLabeledIssueWebhook_EntryAnnotationImplement(t *testing.T) {
	const secretVal = "whsec"
	proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
	proj.Name = "lc-ann-issue-proj"
	proj.Namespace = ns
	proj.Spec.ScmSecretRef = "lc-ann-issue-scm"
	proj.Spec.TriggerLabel = "tatara"

	c := seedClient(t,
		proj,
		&tatarav1.Repository{
			ObjectMeta: metav1.ObjectMeta{Name: "lc-ann-issue-repo", Namespace: ns},
			Spec:       tatarav1.RepositorySpec{ProjectRef: "lc-ann-issue-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main"},
		},
		secret("lc-ann-issue-scm", secretVal),
	)
	h, _ := newServer(t, c)

	body := []byte(`{"action":"labeled","sender":{"login":"alice"},"label":{"name":"tatara"},"issue":{"number":3,"title":"Fix","body":"please fix","user":{"login":"alice"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/3"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "lc-ann-issue-proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	var matching []tatarav1.QueuedEvent
	for _, qe := range qel.Items {
		if qe.Spec.ProjectRef == "lc-ann-issue-proj" {
			matching = append(matching, qe)
		}
	}
	require.Len(t, matching, 1)
	qe := matching[0]
	require.Equal(t, "Implement", qe.Spec.Payload.Annotations["tatara.dev/lifecycle-entry"],
		"trigger-labeled issue task must have lifecycle-entry=Implement annotation at create time")
	// Payload.Source must be set at create time (atomic with the QueuedEvent object).
	require.NotNil(t, qe.Spec.Payload.Source)
	require.Equal(t, 3, qe.Spec.Payload.Source.Number)
}

// TestBotPRWebhook_EntryAnnotationMRCIAndSpecSource asserts that a bot-PR webhook
// creates a Task with lifecycle-entry=MRCI annotation AND Spec.Source populated
// with PR number, IsPR=true, and URL - all set atomically at create time, not via
// a post-create status update.
func TestBotPRWebhook_EntryAnnotationMRCIAndSpecSource(t *testing.T) {
	const secretVal = "whsec"
	proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
	proj.Name = "lc-ann-botpr-proj"
	proj.Namespace = ns
	proj.Spec.ScmSecretRef = "lc-ann-botpr-scm"
	proj.Spec.TriggerLabel = "tatara"

	c := seedClient(t,
		proj,
		&tatarav1.Repository{
			ObjectMeta: metav1.ObjectMeta{Name: "lc-ann-botpr-repo", Namespace: ns},
			Spec:       tatarav1.RepositorySpec{ProjectRef: "lc-ann-botpr-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main"},
		},
		secret("lc-ann-botpr-scm", secretVal),
	)
	h, _ := newServer(t, c)

	body := []byte(`{"action":"opened","sender":{"login":"tatara-bot"},"pull_request":{"number":15,"title":"PR","body":"Closes #7\ndescription","user":{"login":"tatara-bot"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/15","head":{"sha":"deadbeef","ref":"feature"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "lc-ann-botpr-proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	var matching []tatarav1.QueuedEvent
	for _, qe := range qel.Items {
		if qe.Spec.ProjectRef == "lc-ann-botpr-proj" {
			matching = append(matching, qe)
		}
	}
	require.Len(t, matching, 1)
	qe := matching[0]
	require.Equal(t, "MRCI", qe.Spec.Payload.Annotations["tatara.dev/lifecycle-entry"],
		"bot-PR task must have lifecycle-entry=MRCI annotation at create time")
	require.NotNil(t, qe.Spec.Payload.Source, "Payload.Source must be set at create time")
	require.Equal(t, 15, qe.Spec.Payload.Source.Number, "Source.Number must be PR number")
	require.True(t, qe.Spec.Payload.Source.IsPR, "Source.IsPR must be true for bot PR")
	require.Equal(t, "https://github.com/o/r/pull/15", qe.Spec.Payload.Source.URL)
}

// ----- FIX 4: bot-PR webhook dedup key uses linked issue number -----

// TestBotPRWebhook_DedupKeyLinkedIssue asserts that when a bot-PR webhook body
// contains "Closes #7", the created Task's tatara.io/source-number dedup label
// is "7" (the linked issue number), matching what mrScan would produce for the
// same PR body, preventing duplicate lifecycle Tasks.
func TestBotPRWebhook_DedupKeyLinkedIssue(t *testing.T) {
	const secretVal = "whsec"
	proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
	proj.Name = "lc-dedup-proj"
	proj.Namespace = ns
	proj.Spec.ScmSecretRef = "lc-dedup-scm"
	proj.Spec.TriggerLabel = "tatara"

	c := seedClient(t,
		proj,
		&tatarav1.Repository{
			ObjectMeta: metav1.ObjectMeta{Name: "lc-dedup-repo", Namespace: ns},
			Spec:       tatarav1.RepositorySpec{ProjectRef: "lc-dedup-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main"},
		},
		secret("lc-dedup-scm", secretVal),
	)
	h, _ := newServer(t, c)

	// Bot-authored PR body includes "Closes #7"
	body := []byte(`{"action":"opened","sender":{"login":"tatara-bot"},"pull_request":{"number":21,"title":"PR","body":"Closes #7\nsome description","user":{"login":"tatara-bot"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/21","head":{"sha":"abc","ref":"feature"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "lc-dedup-proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	var matching []tatarav1.QueuedEvent
	for _, qe := range qel.Items {
		if qe.Spec.ProjectRef == "lc-dedup-proj" {
			matching = append(matching, qe)
		}
	}
	require.Len(t, matching, 1)
	qe := matching[0]
	require.Equal(t, "7", qe.Spec.Payload.Labels["tatara.io/source-number"],
		"bot-PR with Closes #7 must have source-number=7 (linked issue), matching mrScan's dedup key")
}

// TestHumanPRWebhookStillCreatesReview asserts that human-authored PRs still
// create Kind=review (unchanged behavior).
func TestHumanPRWebhookStillCreatesReview(t *testing.T) {
	const secretVal = "whsec"
	proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
	proj.Name = "lc-humanpr-proj"
	proj.Namespace = ns
	proj.Spec.ScmSecretRef = "lc-humanpr-scm"
	proj.Spec.TriggerLabel = "tatara"

	c := seedClient(t,
		proj,
		&tatarav1.Repository{
			ObjectMeta: metav1.ObjectMeta{Name: "lc-humanpr-repo", Namespace: ns},
			Spec:       tatarav1.RepositorySpec{ProjectRef: "lc-humanpr-proj", URL: "https://github.com/o/r.git", DefaultBranch: "main"},
		},
		secret("lc-humanpr-scm", secretVal),
	)
	h, _ := newServer(t, c)

	// Human-authored PR with trigger label
	body := []byte(`{"action":"opened","sender":{"login":"alice"},"pull_request":{"number":11,"title":"PR","body":"body","user":{"login":"alice"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/11","head":{"sha":"abc","ref":"fix"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "lc-humanpr-proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	var matching []tatarav1.QueuedEvent
	for _, qe := range qel.Items {
		if qe.Spec.ProjectRef == "lc-humanpr-proj" {
			matching = append(matching, qe)
		}
	}
	require.Len(t, matching, 1)
	require.Equal(t, "review", matching[0].Spec.Payload.Kind, "human PR must still create review kind")
}
