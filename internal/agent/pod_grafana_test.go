package agent

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func podEnvMap(t *testing.T, project *tatarav1alpha1.Project) map[string]string {
	t.Helper()
	task := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "t1"}, Spec: tatarav1alpha1.TaskSpec{ProjectRef: project.Name, Kind: "incident"}}
	pod := BuildPod(project, nil, task, nil, "http://mem", PodConfig{Namespace: "tatara"})
	m := map[string]string{}
	for _, e := range pod.Spec.Containers[0].Env {
		m[e.Name] = e.Value
	}
	return m
}

func TestBuildPod_GrafanaMCPURL_WhenEnabled(t *testing.T) {
	p := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "acme"}}
	p.Spec.Grafana = &tatarav1alpha1.GrafanaSpec{Enabled: true, URL: "http://g", SecretRef: "s"}
	m := podEnvMap(t, p)
	if m["TATARA_GRAFANA_MCP_URL"] != "http://grafana-mcp-acme.tatara.svc:8000/mcp" {
		t.Fatalf("grafana mcp url env wrong/missing: %q", m["TATARA_GRAFANA_MCP_URL"])
	}
}

func TestBuildPod_NoGrafanaMCPURL_WhenDisabled(t *testing.T) {
	p := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "acme"}}
	m := podEnvMap(t, p)
	if _, ok := m["TATARA_GRAFANA_MCP_URL"]; ok {
		t.Fatalf("grafana mcp url env must be absent when feature off")
	}
}

func TestBuildPod_NoGrafanaMCPURL_WhenExplicitlyDisabled(t *testing.T) {
	p := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "acme"}}
	p.Spec.Grafana = &tatarav1alpha1.GrafanaSpec{Enabled: false, URL: "http://g", SecretRef: "s"}
	m := podEnvMap(t, p)
	if _, ok := m["TATARA_GRAFANA_MCP_URL"]; ok {
		t.Fatalf("grafana mcp url env must be absent when grafana spec present but disabled")
	}
}
