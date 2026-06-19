package v1alpha1

import "testing"

func TestGrafanaSpec_Fields(t *testing.T) {
	g := GrafanaSpec{Enabled: true, URL: "http://grafana:3000", SecretRef: "proj-grafana", CooldownSeconds: 1800}
	if !g.Enabled || g.URL == "" || g.SecretRef == "" || g.CooldownSeconds != 1800 {
		t.Fatalf("GrafanaSpec fields not wired: %+v", g)
	}
	p := ProjectSpec{Grafana: &g}
	if p.Grafana == nil || !p.Grafana.Enabled {
		t.Fatalf("ProjectSpec.Grafana not wired")
	}
	st := ProjectStatus{Grafana: &GrafanaStatus{Phase: "Ready", Endpoint: "http://grafana-mcp-x.ns.svc:8000"}}
	if st.Grafana == nil || st.Grafana.Phase != "Ready" {
		t.Fatalf("ProjectStatus.Grafana not wired")
	}
}
