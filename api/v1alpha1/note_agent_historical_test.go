package v1alpha1

import (
	"os"
	"strings"
	"testing"
)

// A note records WHO WROTE IT. #521 deleted clarify as a live agent kind, but
// notes already in etcd say clarify and are immutable, so the value must stay
// accepted or the apiserver rejects every status write to a Task carrying one -
// appending a note revalidates its siblings, and ratcheting only exempts
// unchanged keypaths. That is what broke enforce-live-pod-ceiling in v2.0.0.
func TestNoteAgentEnumStillToleratesClarify(t *testing.T) {
	b, err := os.ReadFile("../../charts/tatara-operator/crd-bases/tatara.dev_tasks.yaml")
	if err != nil {
		t.Fatalf("read CRD: %v", err)
	}
	crd := string(b)
	i := strings.Index(crd, "notes:")
	if i < 0 {
		t.Fatal("notes property not found in the generated CRD")
	}
	// The agent enum is the first one after the notes property.
	j := strings.Index(crd[i:], "agent:")
	if j < 0 {
		t.Fatal("notes[].agent not found in the generated CRD")
	}
	window := crd[i+j : i+j+600]
	if !strings.Contains(window, "clarify") {
		t.Fatalf("notes[].agent enum dropped clarify; historical notes become unwritable.\n%s", window)
	}
}
