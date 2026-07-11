package webhook

import (
	"testing"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// An alert whose label value names a project component repo (service=tatara-operator),
// with a distinct groupKey so alert-group dedup does NOT fire (this is a DIFFERENT
// alert than any prior, testing the repo-in-flight dedup, not the re-fire dedup).
const grafanaFiringImplicatingRepo = `{"status":"firing","groupKey":"{}/{alertname=\"OperatorErrors\"}","commonLabels":{"alertname":"OperatorErrors","service":"tatara-operator","severity":"critical"},"commonAnnotations":{"summary":"errors"},"externalURL":"http://grafana:3000","alerts":[{"status":"firing","labels":{"alertname":"OperatorErrors","service":"tatara-operator"},"annotations":{"summary":"errors"},"startsAt":"2026-06-19T00:00:00Z","generatorURL":"http://grafana:3000/alerting/rule","fingerprint":"opx1"}]}`

func inflightRepo(project, name, slug string) *tatarav1.Repository {
	r := &tatarav1.Repository{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"}}
	r.Spec.ProjectRef = project
	r.Spec.URL = "https://github.com/szymonrychu/" + slug + ".git"
	r.Spec.DefaultBranch = "main"
	return r
}

func inflightTask(name, project, issueRef string, number int) *tatarav1.Task {
	return &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec: tatarav1.TaskSpec{
			ProjectRef: project, Kind: "implement", Goal: "g",
			Source: &tatarav1.TaskSource{Provider: "github", IssueRef: issueRef, Number: number},
		},
		Status: tatarav1.TaskStatus{Phase: "Running"},
	}
}

// TestGrafana_InflightRepo_SkipsCompetingIncident: a firing alert implicating a
// repo that already has a non-terminal Task must NOT spawn a competing incident
// (finding #6): the mid-flight work owns that repo.
func TestGrafana_InflightRepo_SkipsCompetingIncident(t *testing.T) {
	r, fc := grafanaRouter(t,
		grafanaProject("pif"), grafanaSecret("pif"),
		inflightRepo("pif", "pif-operator", "tatara-operator"),
		inflightTask("pif-live", "pif", "szymonrychu/tatara-operator#5", 5),
	)
	w := postGrafana(r, "pif", "tok", grafanaFiringImplicatingRepo)
	if w.Code != 202 {
		t.Fatalf("want 202, got %d: %s", w.Code, w.Body.String())
	}
	qes := listIncidentQueuedEvents(t, fc)
	if len(qes) != 0 {
		t.Fatalf("implicated repo has in-flight work: want 0 incident QueuedEvents, got %d", len(qes))
	}
}

// TestGrafana_InflightRepo_SpawnsWhenNoLiveTask: the same alert DOES spawn an
// incident when the implicated repo has no non-terminal Task.
func TestGrafana_InflightRepo_SpawnsWhenNoLiveTask(t *testing.T) {
	r, fc := grafanaRouter(t,
		grafanaProject("pif2"), grafanaSecret("pif2"),
		inflightRepo("pif2", "pif2-operator", "tatara-operator"),
	)
	w := postGrafana(r, "pif2", "tok", grafanaFiringImplicatingRepo)
	if w.Code != 202 {
		t.Fatalf("want 202, got %d: %s", w.Code, w.Body.String())
	}
	qes := listIncidentQueuedEvents(t, fc)
	if len(qes) != 1 {
		t.Fatalf("no in-flight work: want 1 incident QueuedEvent, got %d", len(qes))
	}
}

// TestGrafana_InflightRepo_TerminalTaskDoesNotBlock: a terminal Task on the
// implicated repo does not block a new incident.
func TestGrafana_InflightRepo_TerminalTaskDoesNotBlock(t *testing.T) {
	done := inflightTask("pif3-done", "pif3", "szymonrychu/tatara-operator#5", 5)
	done.Status.Phase = "Succeeded"
	r, fc := grafanaRouter(t,
		grafanaProject("pif3"), grafanaSecret("pif3"),
		inflightRepo("pif3", "pif3-operator", "tatara-operator"),
		done,
	)
	w := postGrafana(r, "pif3", "tok", grafanaFiringImplicatingRepo)
	if w.Code != 202 {
		t.Fatalf("want 202, got %d: %s", w.Code, w.Body.String())
	}
	if qes := listIncidentQueuedEvents(t, fc); len(qes) != 1 {
		t.Fatalf("terminal task must not block: want 1 incident QueuedEvent, got %d", len(qes))
	}
}
