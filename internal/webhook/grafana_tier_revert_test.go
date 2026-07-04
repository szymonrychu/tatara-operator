package webhook

import (
	"testing"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/incident"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const grafanaTierQualityFiring = `{"status":"firing","groupKey":"{}/{alertname=\"TierQualityReview\"}","commonLabels":{"alertname":"TierQualityReview","tatara_tier_quality":"true","kind":"review","model":"claude-sonnet-5","project":"tatara"},"commonAnnotations":{"summary":"review find-rate near zero"},"externalURL":"http://grafana:3000","alerts":[{"status":"firing","labels":{"alertname":"TierQualityReview","kind":"review","model":"claude-sonnet-5"},"annotations":{"summary":"review find-rate near zero"},"startsAt":"2026-07-04T00:00:00Z","generatorURL":"http://grafana:3000/alerting/rule","fingerprint":"tqr1"}]}`

func tierRevertRepo(project string) *tatarav1.Repository {
	r := &tatarav1.Repository{ObjectMeta: metav1.ObjectMeta{Name: project + "-helmfile", Namespace: "tatara"}}
	r.Spec.ProjectRef = project
	r.Spec.URL = "https://github.com/szymonrychu/tatara-helmfile"
	return r
}

func TestGrafana_TierQualityAlert_UsesTierRevertGoal(t *testing.T) {
	r, fc := grafanaRouter(t, grafanaProject("p7"), grafanaSecret("p7"), tierRevertRepo("p7"))
	w := postGrafana(r, "p7", "tok", grafanaTierQualityFiring)
	if w.Code != 202 {
		t.Fatalf("want 202, got %d: %s", w.Code, w.Body.String())
	}
	qes := listIncidentQueuedEvents(t, fc)
	if len(qes) != 1 {
		t.Fatalf("want 1 incident QueuedEvent, got %d", len(qes))
	}
	want := incident.GoalTierRevert("p7", "review", "claude-sonnet-5")
	if got := qes[0].Spec.Payload.Goal; got != want {
		t.Fatalf("goal mismatch\nwant: %s\ngot:  %s", want, got)
	}
}

func TestGrafana_NonTierQualityAlert_UsesProjectGoal(t *testing.T) {
	r, fc := grafanaRouter(t, grafanaProject("p8"), grafanaSecret("p8"))
	w := postGrafana(r, "p8", "tok", grafanaFiring)
	if w.Code != 202 {
		t.Fatalf("want 202, got %d: %s", w.Code, w.Body.String())
	}
	qes := listIncidentQueuedEvents(t, fc)
	if len(qes) != 1 {
		t.Fatalf("want 1 incident QueuedEvent, got %d", len(qes))
	}
	if got := qes[0].Spec.Payload.Goal; got == incident.GoalTierRevert("p8", "review", "claude-sonnet-5") {
		t.Fatalf("non-marked alert must not use tier-revert goal, got: %s", got)
	}
}
