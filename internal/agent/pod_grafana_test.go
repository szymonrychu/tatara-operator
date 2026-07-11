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

func TestPodEnv_SetsBotGitIdentity(t *testing.T) {
	p := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "gitid"}}
	p.Spec.Scm = &tatarav1alpha1.ScmSpec{
		BotLogin: "szymonrychu-bot",
		BotEmail: "143486966+szymonrychu-bot@users.noreply.github.com",
	}
	m := podEnvMap(t, p)
	if m["GIT_USER_NAME"] != "szymonrychu-bot" {
		t.Fatalf("GIT_USER_NAME=%q", m["GIT_USER_NAME"])
	}
	if m["GIT_USER_EMAIL"] != "143486966+szymonrychu-bot@users.noreply.github.com" {
		t.Fatalf("GIT_USER_EMAIL=%q", m["GIT_USER_EMAIL"])
	}
}

func TestPodEnv_OmitsGitEmailWhenUnset(t *testing.T) {
	p := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "gitid2"}}
	p.Spec.Scm = &tatarav1alpha1.ScmSpec{
		BotLogin: "szymonrychu-bot",
		BotEmail: "",
	}
	m := podEnvMap(t, p)
	if _, ok := m["GIT_USER_EMAIL"]; ok {
		t.Fatal("GIT_USER_EMAIL must be omitted when BotEmail empty")
	}
}

func incidentTask(groupHash string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "inc-" + groupHash},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "incident", DedupKey: groupHash},
	}
}

// TestIncidentPodSuffix_ContainsIncident verifies the suffix starts with "incident".
func TestIncidentPodSuffix_ContainsIncident(t *testing.T) {
	task := incidentTask("abc123")
	suffix := podNameSuffix(task)
	if len(suffix) < len("incident") || suffix[:8] != "incident" {
		t.Fatalf("suffix %q does not start with 'incident'", suffix)
	}
}

// TestIncidentPodSuffix_UniquePerAlertGroup verifies two tasks with different
// Spec.DedupKey values produce different pod name suffixes (and thus different
// pod names), preventing incident Tasks from colliding on the same pod name.
func TestIncidentPodSuffix_UniquePerAlertGroup(t *testing.T) {
	taskA := incidentTask("group-hash-aaa")
	taskB := incidentTask("group-hash-bbb")
	suffixA := podNameSuffix(taskA)
	suffixB := podNameSuffix(taskB)
	if suffixA == suffixB {
		t.Fatalf("incident tasks with different alert groups must produce different suffixes; both got %q", suffixA)
	}
}

// TestIncidentPodSuffix_NoLabelFallback verifies a graceful fallback when
// DedupKey is absent.
func TestIncidentPodSuffix_NoLabelFallback(t *testing.T) {
	task := incidentTask("")
	suffix := podNameSuffix(task)
	if suffix != "incident" {
		t.Fatalf("incident task with no alert-group label should produce 'incident', got %q", suffix)
	}
}
