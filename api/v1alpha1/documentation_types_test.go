package v1alpha1

import "testing"

func TestDocumentationSpec_Fields(t *testing.T) {
	d := DocumentationSpec{Enabled: true, Repo: "https://github.com/szymonrychu/tatara-documentation"}
	if !d.Enabled || d.Repo == "" {
		t.Fatalf("DocumentationSpec fields not wired: %+v", d)
	}
	p := ProjectSpec{Documentation: &d}
	if p.Documentation == nil || !p.Documentation.Enabled {
		t.Fatalf("ProjectSpec.Documentation not wired")
	}
}

// TestDocumentationSpec_InertByDefault verifies the zero value (no
// kubebuilder:default) is Enabled=false, so a Project that never sets
// Documentation, and a Project that sets it with Enabled unset, both read as
// disabled - never gate on == default (MEMORY trap).
func TestDocumentationSpec_InertByDefault(t *testing.T) {
	var d DocumentationSpec
	if d.Enabled {
		t.Fatalf("zero-value DocumentationSpec must be Enabled=false")
	}
}
