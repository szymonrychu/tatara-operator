package agent

import (
	"encoding/json"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildPod_ExtraMCPServers_WhenSet(t *testing.T) {
	p := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "mtg"}}
	p.Spec.Agent.MCPServers = []tatarav1alpha1.AgentMCPServer{
		{Name: "spellslinger", URL: "http://spellslinger-mcp-http.spellslinger-development.svc.cluster.local:8080/mcp", Type: "http"},
	}
	m := podEnvMap(t, p)
	raw, ok := m["TATARA_EXTRA_MCP_SERVERS"]
	if !ok {
		t.Fatal("TATARA_EXTRA_MCP_SERVERS absent when servers set")
	}
	var got []map[string]string
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("env is not JSON: %v (%q)", err, raw)
	}
	if len(got) != 1 || got[0]["name"] != "spellslinger" || got[0]["type"] != "http" ||
		got[0]["url"] != "http://spellslinger-mcp-http.spellslinger-development.svc.cluster.local:8080/mcp" {
		t.Fatalf("bad payload: %v", got)
	}
}

func TestBuildPod_ExtraMCPServers_DefaultsTypeHTTP(t *testing.T) {
	p := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "mtg"}}
	p.Spec.Agent.MCPServers = []tatarav1alpha1.AgentMCPServer{{Name: "x", URL: "http://x/mcp"}}
	m := podEnvMap(t, p)
	var got []map[string]string
	_ = json.Unmarshal([]byte(m["TATARA_EXTRA_MCP_SERVERS"]), &got)
	if got[0]["type"] != "http" {
		t.Fatalf("type not defaulted: %v", got)
	}
}

func TestBuildPod_NoExtraMCPServers_WhenEmpty(t *testing.T) {
	p := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "mtg"}}
	m := podEnvMap(t, p)
	if _, ok := m["TATARA_EXTRA_MCP_SERVERS"]; ok {
		t.Fatal("TATARA_EXTRA_MCP_SERVERS must be absent when no servers set")
	}
}
