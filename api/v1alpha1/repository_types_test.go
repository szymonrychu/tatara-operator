package v1alpha1

import (
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

func TestRepositorySpec_SemanticIngestField(t *testing.T) {
	r := &Repository{}
	r.Spec.SemanticIngest = true
	if !r.Spec.SemanticIngest {
		t.Fatalf("SemanticIngest = %v, want true", r.Spec.SemanticIngest)
	}
}

func TestRepository_DeepCopyCopiesSemanticIngest(t *testing.T) {
	r := &Repository{}
	r.Spec.SemanticIngest = true
	cp := r.DeepCopy()
	if !cp.Spec.SemanticIngest {
		t.Errorf("deepcopy lost SemanticIngest: %v", cp.Spec.SemanticIngest)
	}
}
