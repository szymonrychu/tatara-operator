package main

import (
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	apiv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/config"
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

func TestManagerOptions_LeaderElection(t *testing.T) {
	opts := managerOptions(config.Config{Namespace: "tatara", LeaderElection: true}, newScheme())
	if !opts.LeaderElection {
		t.Fatal("managerOptions did not enable leader election")
	}
	if opts.LeaderElectionID != "tatara-operator-leader" {
		t.Fatalf("LeaderElectionID = %q, want tatara-operator-leader", opts.LeaderElectionID)
	}
	if opts.LeaderElectionNamespace != "tatara" {
		t.Fatalf("LeaderElectionNamespace = %q, want tatara", opts.LeaderElectionNamespace)
	}
}

func TestManagerOptions_LeaderElectionDisabled(t *testing.T) {
	opts := managerOptions(config.Config{Namespace: "tatara", LeaderElection: false}, newScheme())
	if opts.LeaderElection {
		t.Fatal("managerOptions enabled leader election when config flag was false")
	}
}

func TestNewScheme_RegistersCNPGCluster(t *testing.T) {
	s := newScheme()
	gvk := schema.GroupVersionKind{
		Group:   "postgresql.cnpg.io",
		Version: "v1",
		Kind:    "Cluster",
	}
	if !s.Recognizes(gvk) {
		t.Fatalf("scheme does not recognize cnpg Cluster %v", gvk)
	}
	obj, err := s.New(gvk)
	if err != nil {
		t.Fatalf("scheme.New(%v): %v", gvk, err)
	}
	if _, ok := obj.(*cnpgv1.Cluster); !ok {
		t.Fatalf("scheme returned %T, want *cnpgv1.Cluster", obj)
	}
}
