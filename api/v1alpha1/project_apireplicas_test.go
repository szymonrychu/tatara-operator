package v1alpha1

import "testing"

// TestCRDAPIReplicasIsBoundedInt32 pins the four facets that together stop an
// out-of-range apiReplicas from silently scaling a Project's memory API to zero.
//
// The field is narrowed to int32 on the way to the Deployment. If the Go type is
// `int`, controller-gen emits `type: integer` with NO `format: int32`, so the
// apiserver admits any int64 and the narrowing wraps:
//
//	apiReplicas: 4294967296 -> replicas 0            (memory API scaled to zero, no error)
//	apiReplicas: 2147483648 -> replicas -2147483648  (Deployment rejected, Project stuck Failed)
//
// `format: int32` alone still admits 2147483647, so Maximum is load-bearing too.
func TestCRDAPIReplicasIsBoundedInt32(t *testing.T) {
	node := crdSchemaAt(t, "tatara.dev_projects.yaml", "spec", "memory", "apiReplicas")

	if got, _ := node["type"].(string); got != "integer" {
		t.Errorf("type = %q, want integer", got)
	}
	if got, _ := node["format"].(string); got != "int32" {
		t.Errorf("format = %q, want int32: without it the apiserver admits int64 and the int32 narrowing wraps to 0", got)
	}
	if got, ok := node["default"].(float64); !ok || got != 1 {
		t.Errorf("default = %v, want 1: existing Projects must be unchanged", node["default"])
	}
	if got, ok := node["minimum"].(float64); !ok || got != 1 {
		t.Errorf("minimum = %v, want 1", node["minimum"])
	}
	max, ok := node["maximum"].(float64)
	if !ok {
		t.Fatalf("no maximum: format int32 still admits 2147483647 replicas")
	}
	if max != 10 {
		t.Errorf("maximum = %v, want 10", max)
	}
}
