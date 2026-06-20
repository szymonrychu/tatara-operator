package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
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
		WithStatusSubresource(&tatarav1.Project{}, &tatarav1.Task{}, &tatarav1.QueuedEvent{}).Build()
	seq := &queue.SeqSource{Client: fc, Namespace: "tatara"}
	s := NewServer(Config{Client: fc, Namespace: "tatara", Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()), Seq: seq})
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

func listIncidentQueuedEvents(t *testing.T, fc client.Client) []tatarav1.QueuedEvent {
	t.Helper()
	var qel tatarav1.QueuedEventList
	_ = fc.List(context.Background(), &qel)
	var out []tatarav1.QueuedEvent
	for _, x := range qel.Items {
		if x.Spec.Kind == "incident" {
			out = append(out, x)
		}
	}
	return out
}

func TestHandleGrafanaAlert_EnqueuesAlertEvent(t *testing.T) {
	r, fc := grafanaRouter(t, grafanaProject("p0"), grafanaSecret("p0"))
	w := postGrafana(r, "p0", "tok", grafanaFiring)
	if w.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", w.Code, w.Body.String())
	}
	qes := listIncidentQueuedEvents(t, fc)
	if len(qes) != 1 {
		t.Fatalf("want 1 incident QueuedEvent, got %d", len(qes))
	}
	qe := qes[0]
	if qe.Spec.Class != tatarav1.QueueClassAlert {
		t.Fatalf("want class=%q, got %q", tatarav1.QueueClassAlert, qe.Spec.Class)
	}
	if qe.Spec.Payload.Kind != "incident" {
		t.Fatalf("want payload.Kind=incident, got %q", qe.Spec.Payload.Kind)
	}
	if qe.Spec.ProjectRef != "p0" {
		t.Fatalf("want ProjectRef=p0, got %q", qe.Spec.ProjectRef)
	}
	// No Task should be created
	var tl tatarav1.TaskList
	_ = fc.List(context.Background(), &tl)
	if len(tl.Items) != 0 {
		t.Fatalf("want 0 Tasks, got %d", len(tl.Items))
	}
}

func TestGrafana_FiringCreatesIncident(t *testing.T) {
	r, fc := grafanaRouter(t, grafanaProject("p1"), grafanaSecret("p1"))
	w := postGrafana(r, "p1", "tok", grafanaFiring)
	if w.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d: %s", w.Code, w.Body.String())
	}
	if n := len(listIncidentQueuedEvents(t, fc)); n != 1 {
		t.Fatalf("want 1 incident QueuedEvent, got %d", n)
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
	if n := len(listIncidentQueuedEvents(t, fc)); n != 0 {
		t.Fatalf("resolved must create no QueuedEvent, got %d", n)
	}
}

func TestGrafana_DedupInFlight(t *testing.T) {
	r, fc := grafanaRouter(t, grafanaProject("p4"), grafanaSecret("p4"))
	_ = postGrafana(r, "p4", "tok", grafanaFiring)
	_ = postGrafana(r, "p4", "tok", grafanaFiring) // same groupKey, in-flight
	if n := len(listIncidentQueuedEvents(t, fc)); n != 1 {
		t.Fatalf("dedup failed: want 1 incident QueuedEvent, got %d", n)
	}
}

func TestGrafana_DedupEmitsDuplicateMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	sch := runtime.NewScheme()
	_ = tatarav1.AddToScheme(sch)
	_ = corev1.AddToScheme(sch)
	fc := fake.NewClientBuilder().WithScheme(sch).WithObjects(grafanaProject("p6"), grafanaSecret("p6")).
		WithStatusSubresource(&tatarav1.Project{}, &tatarav1.Task{}, &tatarav1.QueuedEvent{}).Build()
	seq := &queue.SeqSource{Client: fc, Namespace: "tatara"}
	s := NewServer(Config{Client: fc, Namespace: "tatara", Metrics: obs.NewOperatorMetrics(reg), Seq: seq})
	r := chi.NewRouter()
	s.Mount(r)

	// First firing: creates the QueuedEvent (result=created).
	w1 := postGrafana(r, "p6", "tok", grafanaFiring)
	if w1.Code != http.StatusAccepted {
		t.Fatalf("first fire: want 202, got %d", w1.Code)
	}
	// Second firing: same alert group, QueuedEvent already exists (result=duplicate).
	w2 := postGrafana(r, "p6", "tok", grafanaFiring)
	if w2.Code != http.StatusAccepted {
		t.Fatalf("second fire: want 202, got %d", w2.Code)
	}

	// Queue must still have exactly 1 incident QueuedEvent.
	if n := len(listIncidentQueuedEvents(t, fc)); n != 1 {
		t.Fatalf("dedup failed: want 1 incident QueuedEvent, got %d", n)
	}

	// The duplicate counter must have been incremented.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	var dupCount float64
	for _, mf := range mfs {
		if mf.GetName() != "operator_webhook_events_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["result"] == "duplicate" && labels["provider"] == "grafana" {
				dupCount = m.GetCounter().GetValue()
			}
		}
	}
	if dupCount != 1 {
		t.Fatalf("want duplicate metric count=1, got %g", dupCount)
	}
}

func TestGrafana_DisabledProject(t *testing.T) {
	p := &tatarav1.Project{ObjectMeta: metav1.ObjectMeta{Name: "p5", Namespace: "tatara"}} // no Grafana
	r, _ := grafanaRouter(t, p)
	if w := postGrafana(r, "p5", "tok", grafanaFiring); w.Code != http.StatusNotFound {
		t.Fatalf("disabled project must 404, got %d", w.Code)
	}
}
