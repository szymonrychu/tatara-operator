package v1alpha1

import "testing"

func TestAgentMCPServer_DeepCopyRoundTrip(t *testing.T) {
	in := AgentSpec{MCPServers: []AgentMCPServer{{Name: "spellslinger", URL: "http://x:8080/mcp", Type: "http"}}}
	out := in.DeepCopy()
	if len(out.MCPServers) != 1 || out.MCPServers[0].Name != "spellslinger" || out.MCPServers[0].URL != "http://x:8080/mcp" {
		t.Fatalf("deepcopy lost data: %+v", out.MCPServers)
	}
	// Mutating the copy must not touch the original (independent backing array).
	out.MCPServers[0].Name = "changed"
	if in.MCPServers[0].Name != "spellslinger" {
		t.Fatal("deepcopy shares backing array")
	}
}
