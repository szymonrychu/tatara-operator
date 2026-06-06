// Package v1alpha1 contains API Schema definitions for the tatara.dev v1alpha1 API group.
// +kubebuilder:object:generate=true
// +groupName=tatara.dev
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is group version used to register these objects.
var GroupVersion = schema.GroupVersion{Group: "tatara.dev", Version: "v1alpha1"}

// SchemeBuilder is used to add go types to the GroupVersionKind scheme.
var SchemeBuilder = runtime.NewSchemeBuilder(func(s *runtime.Scheme) error {
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
})

// AddToScheme adds the types in this group-version to the given scheme.
var AddToScheme = SchemeBuilder.AddToScheme
