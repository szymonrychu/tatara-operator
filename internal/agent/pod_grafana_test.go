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

// TestIncidentPodIDSegment_IsDedupKey verifies the id segment equals the
// task's DedupKey.
func TestIncidentPodIDSegment_IsDedupKey(t *testing.T) {
	task := incidentTask("abc123")
	id := podNameIDSegment(task)
	if id != "abc123" {
		t.Fatalf("id segment = %q, want 'abc123'", id)
	}
}

// TestIncidentPodIDSegment_UniquePerAlertGroup verifies two tasks with
// different Spec.DedupKey values produce different pod name id segments (and
// thus different pod names), preventing incident Tasks from colliding on the
// same pod name.
func TestIncidentPodIDSegment_UniquePerAlertGroup(t *testing.T) {
	taskA := incidentTask("group-hash-aaa")
	taskB := incidentTask("group-hash-bbb")
	idA := podNameIDSegment(taskA)
	idB := podNameIDSegment(taskB)
	if idA == idB {
		t.Fatalf("incident tasks with different alert groups must produce different id segments; both got %q", idA)
	}
}

// TestIncidentPodIDSegment_NoLabelFallback verifies a graceful fallback (empty
// id segment, dropped by BuildPodName) when DedupKey is absent.
func TestIncidentPodIDSegment_NoLabelFallback(t *testing.T) {
	task := incidentTask("")
	id := podNameIDSegment(task)
	if id != "" {
		t.Fatalf("incident task with no alert-group label should produce empty id segment, got %q", id)
	}
}
