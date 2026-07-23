package v1alpha1

import "testing"

func TestPromptAppendFor(t *testing.T) {
	a := AgentSpec{PromptAppendByKind: map[string]string{"*": "W", "review": "R"}}
	if got := a.PromptAppendFor("review"); got != "W\n\nR" {
		t.Fatalf("review: got %q", got)
	}
	if got := a.PromptAppendFor("implement"); got != "W" {
		t.Fatalf("implement (wildcard only): got %q", got)
	}
	if got := (AgentSpec{}).PromptAppendFor("review"); got != "" {
		t.Fatalf("empty spec: got %q", got)
	}
	if got := (AgentSpec{PromptAppendByKind: map[string]string{"review": "R"}}).PromptAppendFor("review"); got != "R" {
		t.Fatalf("kind only: got %q", got)
	}
}
