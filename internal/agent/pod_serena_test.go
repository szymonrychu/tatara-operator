package agent

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// podEnvMapWithCfg builds a pod with the given PodConfig and returns its env as
// a name->value map. Mirrors podEnvMap in pod_grafana_test.go but allows a
// caller-supplied cfg to test operator-level env switches.
func podEnvMapWithCfg(t *testing.T, project *tatarav1alpha1.Project, cfg PodConfig) map[string]string {
	t.Helper()
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t1"},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: project.Name, Kind: "brainstorm"},
	}
	pod := BuildPod(project, nil, task, nil, "http://mem", cfg)
	m := map[string]string{}
	for _, e := range pod.Spec.Containers[0].Env {
		m[e.Name] = e.Value
	}
	return m
}

// TestPodEnv_SerenaURLSetWhenConfigured verifies that when SerenaURL is set in
// PodConfig the agent pod receives TATARA_SERENA_URL with that value. The
// wrapper reads this env to register Serena as an HTTP MCP server (Phase 2).
func TestPodEnv_SerenaURLSetWhenConfigured(t *testing.T) {
	p := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "acme"}}
	cfg := PodConfig{
		Namespace: "tatara",
		SerenaURL: "http://serena.tatara.svc:9000",
	}
	m := podEnvMapWithCfg(t, p, cfg)
	if m["TATARA_SERENA_URL"] != "http://serena.tatara.svc:9000" {
		t.Fatalf("TATARA_SERENA_URL env wrong/missing: %q", m["TATARA_SERENA_URL"])
	}
}

// TestPodEnv_SerenaURLAbsentWhenUnset verifies that when SerenaURL is empty no
// TATARA_SERENA_URL env var is injected (off by default, Phase 1 code-path only).
func TestPodEnv_SerenaURLAbsentWhenUnset(t *testing.T) {
	p := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "acme"}}
	cfg := PodConfig{Namespace: "tatara"}
	m := podEnvMapWithCfg(t, p, cfg)
	if _, ok := m["TATARA_SERENA_URL"]; ok {
		t.Fatal("TATARA_SERENA_URL must be absent when SerenaURL is not configured")
	}
}
