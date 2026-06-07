package controller

import (
	"context"
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestCNPGClusterCRDInstalled proves the vendored cnpg Cluster CRD is loaded
// by envtest and the cnpgv1 type is in the suite scheme, so later
// provisioning tests can create Cluster objects.
func TestCNPGClusterCRDInstalled(t *testing.T) {
	ctx := context.Background()
	c := &cnpgv1.Cluster{}
	c.Name = "crd-probe"
	c.Namespace = testNS
	c.Spec.Instances = 1
	c.Spec.StorageConfiguration = cnpgv1.StorageConfiguration{Size: "1Gi"}
	if err := k8sClient.Create(ctx, c); err != nil {
		t.Fatalf("create cnpg Cluster (CRD not installed or type not registered?): %v", err)
	}
	got := &cnpgv1.Cluster{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "crd-probe"}, got); err != nil {
		t.Fatalf("get cnpg Cluster: %v", err)
	}
	if got.Spec.Instances != 1 {
		t.Fatalf("instances = %d, want 1", got.Spec.Instances)
	}
}
