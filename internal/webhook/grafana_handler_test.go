package webhook

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func grafanaRouter(t *testing.T, objs ...client.Object) (*chi.Mux, client.Client) {
	t.Helper()
	sch := runtime.NewScheme()
	_ = tatarav1.AddToScheme(sch)
	_ = corev1.AddToScheme(sch)
	fc := fake.NewClientBuilder().WithScheme(sch).WithObjects(objs...).
		WithStatusSubresource(&tatarav1.Project{}, &tatarav1.Task{}).Build()
	s := NewServer(Config{Client: fc, Namespace: "tatara", Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())})
	r := chi.NewRouter()
	s.Mount(r)
	return r, fc
}

func grafanaProject(name string) *tatarav1.Project {
	p := &tatarav1.Project{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"}}
	p.Spec.Grafana = &tatarav1.GrafanaSpec{Enabled: true, URL: "http://g", SecretRef: name + "-grafana", CooldownSeconds: 3600}
	return p
}

func grafanaSecret(name string) *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-grafana", Namespace: "tatara"},
		Data: map[string][]byte{"webhookSecret": []byte("tok"), "serviceAccountToken": []byte("sa")}}
}

func postGrafana(r *chi.Mux, project, bearer, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/operator/webhooks/"+project+"/grafana", strings.NewReader(body))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func listIncidentTasks(t *testing.T, fc client.Client) []tatarav1.Task {
	t.Helper()
	var tl tatarav1.TaskList
	_ = fc.List(t.Context(), &tl)
	var out []tatarav1.Task
	for _, x := range tl.Items {
		if x.Spec.Kind == "incident" {
			out = append(out, x)
		}
	}
	return out
}

func TestGrafana_FiringCreatesIncident(t *testing.T) {
	r, fc := grafanaRouter(t, grafanaProject("p1"), grafanaSecret("p1"))
	w := postGrafana(r, "p1", "tok", grafanaFiring)
	if w.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", w.Code, w.Body.String())
	}
	if n := len(listIncidentTasks(t, fc)); n != 1 {
		t.Fatalf("want 1 incident task, got %d", n)
	}
}

func TestGrafana_BadBearer401(t *testing.T) {
	r, _ := grafanaRouter(t, grafanaProject("p2"), grafanaSecret("p2"))
	if w := postGrafana(r, "p2", "wrong", grafanaFiring); w.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", w.Code)
	}
}

func TestGrafana_ResolvedIgnored(t *testing.T) {
	r, fc := grafanaRouter(t, grafanaProject("p3"), grafanaSecret("p3"))
	w := postGrafana(r, "p3", "tok", grafanaResolved)
	if w.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d", w.Code)
	}
	if n := len(listIncidentTasks(t, fc)); n != 0 {
		t.Fatalf("resolved must create no task, got %d", n)
	}
}

func TestGrafana_DedupInFlight(t *testing.T) {
	r, fc := grafanaRouter(t, grafanaProject("p4"), grafanaSecret("p4"))
	_ = postGrafana(r, "p4", "tok", grafanaFiring)
	_ = postGrafana(r, "p4", "tok", grafanaFiring) // same groupKey, in-flight
	if n := len(listIncidentTasks(t, fc)); n != 1 {
		t.Fatalf("dedup failed: want 1 incident task, got %d", n)
	}
}

func TestGrafana_DisabledProject(t *testing.T) {
	p := &tatarav1.Project{ObjectMeta: metav1.ObjectMeta{Name: "p5", Namespace: "tatara"}} // no Grafana
	r, _ := grafanaRouter(t, p)
	if w := postGrafana(r, "p5", "tok", grafanaFiring); w.Code != http.StatusNotFound {
		t.Fatalf("disabled project must 404, got %d", w.Code)
	}
}
