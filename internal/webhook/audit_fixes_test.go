package webhook_test

// Tests for audit findings in internal/webhook/server.go.
// Each test is named after its finding number and must fail before the fix and
// pass after.

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/webhook"
)

// --- Finding 1: GitLab MR IssueRef uses '!' not '#'; LastIndex returns -1 -> panic ---

// TestFinding1_GitLabNoteOnMR_NoPanic verifies that a GitLab Note Hook for an
// MR (IssueRef = "group/proj!42") processed via handleIssueComment does NOT
// panic at createLifecycleTaskAtTriage's repoSlug derivation.
const glNoteOnMRPayload = `{
  "object_kind": "note",
  "user": {"username": "alice"},
  "project": {"git_http_url": "https://gitlab.com/group/proj.git", "path_with_namespace": "group/proj"},
  "object_attributes": {
    "note": "looks good", "url": "https://gitlab.com/group/proj/-/notes/1",
    "action": "create"
  },
  "merge_request": {"iid": 43, "source_branch": "feature"},
  "issue": {"iid": 0},
  "labels": []
}`

func TestFinding1_GitLabNoteOnMR_NoPanic(t *testing.T) {
	const secretVal = "gl-note-secret"
	proj := &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "glnoteproj", Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: "glnoteproj-scm",
			TriggerLabel: "tatara",
			Scm: &tatarav1.ScmSpec{
				Provider: "gitlab", Owner: "group", BotLogin: "tatara-bot",
			},
		},
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "glnoteproj-scm", Namespace: ns},
		Data: map[string][]byte{
			"webhookSecret": []byte(secretVal),
			"token":         []byte("pat"),
		},
	}
	repo := &tatarav1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "glnoterepo", Namespace: ns},
		Spec:       tatarav1.RepositorySpec{ProjectRef: "glnoteproj", URL: "https://gitlab.com/group/proj.git", DefaultBranch: "main"},
	}

	c := seedClient(t, proj, sec, repo)
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	body := []byte(glNoteOnMRPayload)
	hdr := http.Header{}
	hdr.Set("X-Gitlab-Event", "Note Hook")
	hdr.Set("X-Gitlab-Token", secretVal)

	// Must not panic.
	w := post(t, h, "glnoteproj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code, "GitLab Note on MR must not panic")
}

// --- Finding 3: Empty webhookSecret passes GitHub HMAC verification ---

// TestFinding3_EmptyWebhookSecret_Rejected verifies that when the webhookSecret
// key is present but empty, the webhook returns 500 (secret error) rather than
// accepting requests signed with HMAC("", body).
func TestFinding3_EmptyWebhookSecret_Rejected(t *testing.T) {
	proj := project("emptysec-proj", "emptysec-scm", "tatara")
	// Secret key present but value is empty.
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "emptysec-scm", Namespace: ns},
		Data:       map[string][]byte{"webhookSecret": []byte("")},
	}
	c := seedClient(t, proj, sec)
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	// Sign with an empty key (what an attacker would send).
	body := []byte(`{"ref":"refs/heads/main","after":"sha","repository":{"clone_url":"https://github.com/o/r.git"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "push")
	hdr.Set("X-Hub-Signature-256", ghSign("", body))

	w := post(t, h, "emptysec-proj", hdr, body)
	// Must reject: empty secret is a misconfiguration, not a valid secret.
	require.Equal(t, http.StatusInternalServerError, w.Code,
		"empty webhookSecret must cause 500 (secret error), not accept the request")
}

// --- Finding 7: Nil Metrics in NewServer causes nil-deref in count() ---

// TestFinding7_NilMetricsPanicsAtNewServer verifies that constructing a Server
// with nil Metrics panics immediately with a clear message, rather than silently
// accepting a nil pointer that explodes on the first webhook event.
func TestFinding7_NilMetricsPanicsAtNewServer(t *testing.T) {
	require.Panics(t, func() {
		webhook.NewServer(webhook.Config{
			Client:    seedClient(t),
			Namespace: ns,
			Metrics:   nil,
		})
	}, "NewServer with nil Metrics must panic immediately")
}

// --- R2 Finding 1: Bot-PR dedup uses PR number not linked-issue number ---

// --- R2 Finding 3: GitHub action is unnormalized in metric label ---

// TestR2Finding3_GitHubUnusualAction_NormalizedToOther verifies that a GitHub
// event with an unusual action (e.g. "assigned") produces metric label action="other"
// rather than "assigned", keeping the label set closed.
func TestR2Finding3_GitHubUnusualAction_NormalizedToOther(t *testing.T) {
	const secretVal = "whsec-r2f3"
	proj := project("r2f3proj", "r2f3proj-scm", "tatara")
	sec := secret("r2f3proj-scm", secretVal)
	repo := repository("r2f3repo", "r2f3proj", "https://github.com/o/r.git", "main")

	c := seedClient(t, proj, sec, repo)
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	// GitHub "issues" event with action="assigned" - not in the normalized set.
	body := []byte(`{"action":"assigned","issue":{"number":5,"title":"fix","body":"body","labels":[],"html_url":"https://github.com/o/r/issues/5"},"assignee":{"login":"alice"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},"sender":{"login":"alice"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "r2f3proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	// The metric action label must be "other", not "assigned".
	otherCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "other", "result": "ignored"})
	assignedCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "assigned", "result": "ignored"})
	require.Equal(t, 1.0, otherCount, "unusual GitHub action must be normalized to 'other' in metric")
	require.Equal(t, 0.0, assignedCount, "raw 'assigned' action must not appear as metric label")
}

// --- R2 Finding 5: readBody silently truncates payloads > 5 MiB ---

// TestR2Finding5_OversizedBody_Returns413 verifies that a payload larger than 5 MiB
// returns 413 (not a 401 signature mismatch or 200 with a corrupted payload).
func TestR2Finding5_OversizedBody_Returns413(t *testing.T) {
	const secretVal = "whsec-r2f5"
	proj := project("r2f5proj", "r2f5proj-scm", "tatara")
	sec := secret("r2f5proj-scm", secretVal)

	c := seedClient(t, proj, sec)
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	// Build a body that is just over 5 MiB (5<<20 + 1 bytes).
	// The body itself needn't be valid JSON; we just want readBody to detect overflow.
	oversized := bytes.Repeat([]byte("x"), (5<<20)+1)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "push")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, oversized))

	req, err := makePostRequest("/operator/webhooks/r2f5proj", hdr, oversized)
	require.NoError(t, err)

	rr := newRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, rr.Code,
		"payload > 5 MiB must return 413, not 401 or other status")

	tooLargeCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "unknown", "kind": "other", "action": "other", "result": "too_large"})
	require.Equal(t, 1.0, tooLargeCount, "oversized body must increment result=too_large metric")
}

// --- R2 Finding 6: Provider mismatch not detected ---

// TestR2Finding6_ProviderMismatch_Returns400 verifies that a GitHub-style webhook
// delivery to a GitLab-configured project returns 400 (provider_mismatch), not a
// confusing 401 bad_signature from the wrong HMAC scheme.
func TestR2Finding6_ProviderMismatch_Returns400(t *testing.T) {
	const secretVal = "whsec-r2f6"
	// Project configured for GitLab.
	proj := &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "r2f6proj", Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: "r2f6proj-scm",
			TriggerLabel: "tatara",
			Scm: &tatarav1.ScmSpec{
				Provider: "gitlab", Owner: "group", BotLogin: "tatara-bot",
			},
		},
	}
	sec := secret("r2f6proj-scm", secretVal)

	c := seedClient(t, proj, sec)
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	// Deliver a GitHub-style webhook (X-GitHub-Event header).
	body := []byte(`{"action":"labeled","issue":{"number":1,"title":"x","body":"y","labels":[],"html_url":"https://github.com/g/r/issues/1"},"repository":{"clone_url":"https://github.com/g/r.git","full_name":"g/r"},"sender":{"login":"alice"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "r2f6proj", hdr, body)
	require.Equal(t, http.StatusBadRequest, w.Code,
		"GitHub delivery to GitLab-configured project must return 400 (provider_mismatch), not 401")

	mismatchCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "other", "action": "other", "result": "provider_mismatch"})
	require.Equal(t, 1.0, mismatchCount, "provider mismatch must increment result=provider_mismatch metric")
}

// makePostRequest and newRecorder are helpers for tests that can't use the post()
// helper directly (e.g. when the body is a []byte not a string).
func makePostRequest(path string, hdr http.Header, body []byte) (*http.Request, error) {
	req, err := http.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	return req, nil
}

type testRecorder struct {
	Code   int
	Body   strings.Builder
	header http.Header
}

func newRecorder() *testRecorder {
	return &testRecorder{Code: http.StatusOK, header: http.Header{}}
}

func (r *testRecorder) Header() http.Header         { return r.header }
func (r *testRecorder) WriteHeader(code int)        { r.Code = code }
func (r *testRecorder) Write(b []byte) (int, error) { return r.Body.WriteString(string(b)) }
