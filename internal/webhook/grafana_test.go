package webhook

import (
	"strings"
	"testing"
)

const grafanaFiring = `{"status":"firing","groupKey":"{}/{alertname=\"HighCPU\"}","commonLabels":{"alertname":"HighCPU","severity":"critical"},"commonAnnotations":{"summary":"CPU high"},"externalURL":"http://grafana:3000","alerts":[{"status":"firing","labels":{"alertname":"HighCPU","instance":"node1"},"annotations":{"summary":"CPU high on node1"},"startsAt":"2026-06-19T00:00:00Z","generatorURL":"http://grafana:3000/alerting/rule","fingerprint":"abc123"}]}`

const grafanaResolved = `{"status":"resolved","groupKey":"g","alerts":[]}`

func TestParseGrafanaAlert(t *testing.T) {
	a, err := parseGrafanaAlert([]byte(grafanaFiring))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if a.Status != "firing" || a.GroupKey == "" || len(a.Alerts) != 1 {
		t.Fatalf("parsed wrong: %+v", a)
	}
	if a.Alerts[0].GeneratorURL == "" || a.CommonLabels["severity"] != "critical" {
		t.Fatalf("fields missing: %+v", a)
	}
}

func TestRenderAlertContext(t *testing.T) {
	a, _ := parseGrafanaAlert([]byte(grafanaFiring))
	ctx := renderAlertContext(a)
	for _, kw := range []string{"firing", "HighCPU", "generatorURL", "http://grafana:3000"} {
		if !strings.Contains(ctx, kw) {
			t.Fatalf("rendered ctx missing %q:\n%s", kw, ctx)
		}
	}
}

func TestAlertGroupHash_StableAndDistinct(t *testing.T) {
	a, _ := parseGrafanaAlert([]byte(grafanaFiring))
	b, _ := parseGrafanaAlert([]byte(grafanaResolved))
	if alertGroupHash(a) == "" || len(alertGroupHash(a)) != 16 {
		t.Fatalf("hash must be 16 hex: %q", alertGroupHash(a))
	}
	if alertGroupHash(a) == alertGroupHash(b) {
		t.Fatalf("distinct groupKeys must hash differently")
	}
}
