package webhook_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/webhook"
)

const ns = "tatara"

func ghSign(secret string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(body)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}

func newScheme(t *testing.T) *runtime.Scheme {
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, tatarav1.AddToScheme(s))
	return s
}

func seedClient(t *testing.T, objs ...client.Object) client.Client {
	return fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(objs...).Build()
}

func project(name, secretRef, trigger string) *tatarav1.Project {
	return &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       tatarav1.ProjectSpec{ScmSecretRef: secretRef, TriggerLabel: trigger},
	}
}

func secret(name, webhookSecret string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       map[string][]byte{"webhookSecret": []byte(webhookSecret), "token": []byte("pat")},
	}
}

func repository(name, projectRef, url, branch string) *tatarav1.Repository {
	return &tatarav1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       tatarav1.RepositorySpec{ProjectRef: projectRef, URL: url, DefaultBranch: branch},
	}
}

func newServer(t *testing.T, c client.Client) (http.Handler, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	return srv.Handler(), reg
}

func post(t *testing.T, h http.Handler, projName string, hdr http.Header, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/operator/webhooks/"+projName, strings.NewReader(string(body)))
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func counterValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			match := true
			for _, lp := range m.GetLabel() {
				if want, ok := labels[lp.GetName()]; ok && want != lp.GetValue() {
					match = false
				}
			}
			if match && len(m.GetLabel()) == len(labels) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestPushSetsReingestAnnotation(t *testing.T) {
	const secretVal = "whsec"
	c := seedClient(t,
		project("proj1", "proj1-scm", "tatara"),
		secret("proj1-scm", secretVal),
		repository("repo1", "proj1", "https://github.com/o/r.git", "main"),
	)
	h, reg := newServer(t, c)

	body := []byte(`{"ref":"refs/heads/main","after":"sha1","repository":{"clone_url":"https://github.com/o/r.git"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "push")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "proj1", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var got tatarav1.Repository
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "repo1"}, &got))
	ts := got.Annotations[tatarav1.ReingestRequestedAnnotation]
	require.NotEmpty(t, ts)
	_, err := time.Parse(time.RFC3339, ts)
	require.NoError(t, err)

	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total", map[string]string{"provider": "github", "kind": "push", "action": "", "result": "accepted"}))
}

func TestPushNonDefaultBranchNoMutation(t *testing.T) {
	const secretVal = "whsec"
	c := seedClient(t,
		project("proj1", "proj1-scm", "tatara"),
		secret("proj1-scm", secretVal),
		repository("repo1", "proj1", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)
	body := []byte(`{"ref":"refs/heads/feature","after":"sha2","repository":{"clone_url":"https://github.com/o/r.git"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "push")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "proj1", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)
	var got tatarav1.Repository
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "repo1"}, &got))
	require.Empty(t, got.Annotations[tatarav1.ReingestRequestedAnnotation])
}

// TestIssueWithTriggerLabelStubbed was the M2 stub test asserting NO Task is
// created and result=accepted. Removed in M5: the handler now creates a Task
// (result=task_created). See workitem_test.go for the replacement assertions.

func TestUnknownProject404(t *testing.T) {
	c := seedClient(t)
	h, reg := newServer(t, c)
	body := []byte(`{"ref":"refs/heads/main","after":"x","repository":{"clone_url":"https://github.com/o/r.git"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "push")
	hdr.Set("X-Hub-Signature-256", ghSign("whatever", body))
	w := post(t, h, "ghost", hdr, body)
	require.Equal(t, http.StatusNotFound, w.Code)
	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total", map[string]string{"provider": "github", "kind": "other", "action": "other", "result": "unknown_project"}))
}

func TestBadSignature401NoMutation(t *testing.T) {
	const secretVal = "whsec"
	c := seedClient(t,
		project("proj1", "proj1-scm", "tatara"),
		secret("proj1-scm", secretVal),
		repository("repo1", "proj1", "https://github.com/o/r.git", "main"),
	)
	h, reg := newServer(t, c)
	body := []byte(`{"ref":"refs/heads/main","after":"x","repository":{"clone_url":"https://github.com/o/r.git"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "push")
	hdr.Set("X-Hub-Signature-256", ghSign("wrong", body))
	w := post(t, h, "proj1", hdr, body)
	require.Equal(t, http.StatusUnauthorized, w.Code)
	var got tatarav1.Repository
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "repo1"}, &got))
	require.Empty(t, got.Annotations[tatarav1.ReingestRequestedAnnotation])
	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total", map[string]string{"provider": "github", "kind": "other", "action": "other", "result": "bad_signature"}))
}
