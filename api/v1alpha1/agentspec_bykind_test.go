package v1alpha1

import "testing"

// TestAgentSpecDeepCopy_ByKindMapsIndependent asserts the generated deepcopy
// makes the by-kind maps independent (mutating the source after a DeepCopy
// must not mutate the copy). This fails to COMPILE until the fields exist and
// fails at runtime until controller-gen regenerates the map-copy loops.
func TestAgentSpecDeepCopy_ByKindMapsIndependent(t *testing.T) {
	in := &AgentSpec{
		Model:        "claude-opus-4-8",
		Effort:       "high",
		ModelByKind:  map[string]string{"review": "claude-sonnet-5"},
		EffortByKind: map[string]string{"review": "medium"},
	}
	out := in.DeepCopy()
	in.ModelByKind["review"] = "mutated"
	in.EffortByKind["review"] = "mutated"
	if out.ModelByKind["review"] != "claude-sonnet-5" {
		t.Fatalf("ModelByKind not deep-copied: got %q", out.ModelByKind["review"])
	}
	if out.EffortByKind["review"] != "medium" {
		t.Fatalf("EffortByKind not deep-copied: got %q", out.EffortByKind["review"])
	}
}
