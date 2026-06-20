package controller

import (
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

// newTestScheme builds a *runtime.Scheme with core + tatara + cnpg types
// registered, suitable for use with the controller-runtime fake client.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("newTestScheme: clientgo: %v", err)
	}
	if err := tatarav1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("newTestScheme: tatarav1alpha1: %v", err)
	}
	if err := cnpgv1.AddToScheme(s); err != nil {
		t.Fatalf("newTestScheme: cnpgv1: %v", err)
	}
	return s
}
