package v1alpha1_test

import (
	"testing"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestGroupVersion(t *testing.T) {
	if v1alpha1.GroupVersion.Group != "tatara.dev" {
		t.Fatalf("Group = %q, want tatara.dev", v1alpha1.GroupVersion.Group)
	}
	if v1alpha1.GroupVersion.Version != "v1alpha1" {
		t.Fatalf("Version = %q, want v1alpha1", v1alpha1.GroupVersion.Version)
	}
}

func TestAddToScheme(t *testing.T) {
	if v1alpha1.SchemeBuilder.GroupVersion.String() != "tatara.dev/v1alpha1" {
		t.Fatalf("SchemeBuilder.GroupVersion = %q, want tatara.dev/v1alpha1", v1alpha1.SchemeBuilder.GroupVersion.String())
	}
	if v1alpha1.AddToScheme == nil {
		t.Fatal("AddToScheme is nil")
	}
}
