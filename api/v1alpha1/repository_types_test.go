package v1alpha1

import (
	"encoding/json"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRepositorySpec_ReingestScheduleField(t *testing.T) {
	r := &Repository{}
	r.Spec.ReingestSchedule = "0 6 * * *"
	if r.Spec.ReingestSchedule != "0 6 * * *" {
		t.Fatalf("ReingestSchedule = %q, want %q", r.Spec.ReingestSchedule, "0 6 * * *")
	}
}

func TestRepositoryStatus_LastScheduledReingestField(t *testing.T) {
	r := &Repository{}
	now := metav1.NewTime(time.Now())
	r.Status.LastScheduledReingest = &now
	if r.Status.LastScheduledReingest == nil {
		t.Fatal("LastScheduledReingest should round-trip a *metav1.Time")
	}
}

func TestRepository_DeepCopyCopiesLastScheduledReingest(t *testing.T) {
	now := metav1.NewTime(time.Now())
	r := &Repository{}
	r.Spec.ReingestSchedule = "0 6 * * *"
	r.Status.LastScheduledReingest = &now

	cp := r.DeepCopy()
	if cp.Spec.ReingestSchedule != "0 6 * * *" {
		t.Errorf("deepcopy lost ReingestSchedule: %q", cp.Spec.ReingestSchedule)
	}
	if cp.Status.LastScheduledReingest == nil {
		t.Fatal("deepcopy lost LastScheduledReingest")
	}
	// Must be a distinct pointer (deep, not shallow).
	if cp.Status.LastScheduledReingest == r.Status.LastScheduledReingest {
		t.Error("deepcopy must allocate a new LastScheduledReingest pointer")
	}
}

// boolPtr is a test-local helper to take the address of a bool literal.
func boolPtr(v bool) *bool { return &v }

func TestRepositorySpec_SemanticIngestField(t *testing.T) {
	r := &Repository{}
	r.Spec.SemanticIngest = boolPtr(true)
	if !BoolVal(r.Spec.SemanticIngest, true) {
		t.Fatalf("SemanticIngest = %v, want true", r.Spec.SemanticIngest)
	}
}

func TestRepository_DeepCopyCopiesSemanticIngest(t *testing.T) {
	r := &Repository{}
	r.Spec.SemanticIngest = boolPtr(true)
	cp := r.DeepCopy()
	if !BoolVal(cp.Spec.SemanticIngest, true) {
		t.Errorf("deepcopy lost SemanticIngest: %v", cp.Spec.SemanticIngest)
	}
}

func TestRepository_DeepCopyIndependentSemanticIngest(t *testing.T) {
	r := &Repository{}
	r.Spec.SemanticIngest = boolPtr(true)
	cp := r.DeepCopy()
	*cp.Spec.SemanticIngest = false
	if !BoolVal(r.Spec.SemanticIngest, true) {
		t.Fatal("mutating the copy's SemanticIngest changed the original; deepcopy must deep-copy the pointer")
	}
}

// TestRepositorySpec_IngestEnabledFalseRoundTrips verifies that an explicit
// false value survives JSON round-trip (the core bug: bool+omitempty drops false
// so the apiserver re-applies the +kubebuilder:default=true and silently
// re-enables a deliberately disabled repo). With *bool the marshaled JSON
// contains `"ingestEnabled":false` and the field round-trips correctly.
func TestRepositorySpec_IngestEnabledFalseRoundTrips(t *testing.T) {
	r := &Repository{}
	f := false
	r.Spec.IngestEnabled = &f
	r.Spec.SemanticIngest = &f
	r.Spec.ReingestSchedule = "0 6 * * *"

	b, err := json.Marshal(&r.Spec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	js := string(b)

	// With *bool + omitempty the false pointer IS marshaled as false.
	// With plain bool + omitempty the key is omitted entirely.
	if !contains(js, `"ingestEnabled":false`) {
		t.Errorf("ingestEnabled:false not preserved in JSON: %s", js)
	}
	if !contains(js, `"semanticIngest":false`) {
		t.Errorf("semanticIngest:false not preserved in JSON: %s", js)
	}

	var spec2 RepositorySpec
	if err := json.Unmarshal(b, &spec2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if spec2.IngestEnabled == nil || *spec2.IngestEnabled {
		t.Errorf("ingestEnabled round-trip failed: got %v", spec2.IngestEnabled)
	}
	if spec2.SemanticIngest == nil || *spec2.SemanticIngest {
		t.Errorf("semanticIngest round-trip failed: got %v", spec2.SemanticIngest)
	}
}

// TestBoolVal covers the nil-safe helper used by consumers.
func TestBoolVal(t *testing.T) {
	tr := true
	fa := false
	if !BoolVal(&tr, false) {
		t.Error("BoolVal(*true, false) should return true")
	}
	if BoolVal(&fa, true) {
		t.Error("BoolVal(*false, true) should return false")
	}
	if !BoolVal(nil, true) {
		t.Error("BoolVal(nil, true) should return true")
	}
	if BoolVal(nil, false) {
		t.Error("BoolVal(nil, false) should return false")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
