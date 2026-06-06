package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

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
	if !s.Recognizes(corev1.SchemeGroupVersion.WithKind("Pod")) {
		t.Fatal("scheme does not recognize core/v1 Pod")
	}
}
