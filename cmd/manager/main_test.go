package main

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	apiv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestNewScheme_RegistersAllKinds(t *testing.T) {
	s := newScheme()
	for _, kind := range []string{"Project", "Repository", "Task", "Subtask"} {
		if !s.Recognizes(apiv1alpha1.GroupVersion.WithKind(kind)) {
			t.Fatalf("scheme does not recognize %s", kind)
		}
	}
}

func TestNewScheme_HasCoreTypes(t *testing.T) {
	s := newScheme()
	gvk := runtime.NewScheme()
	_ = gvk
	// core/v1 Pod must be registered so the manager client can read Secrets/Pods later.
	if !s.Recognizes(corePodGVK()) {
		t.Fatal("scheme does not recognize core/v1 Pod")
	}
}
