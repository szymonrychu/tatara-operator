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

func TestAlertRuleName_AlertnameThenGroupKey(t *testing.T) {
	a := GrafanaAlert{CommonLabels: map[string]string{"alertname": "HighCPU"}, GroupKey: "gk"}
	if got := alertRuleName(a); got != "HighCPU" {
		t.Fatalf("want HighCPU, got %q", got)
	}
	b := GrafanaAlert{GroupKey: "gk-only"}
	if got := alertRuleName(b); got != "gk-only" {
		t.Fatalf("fallback: want gk-only, got %q", got)
	}
}
