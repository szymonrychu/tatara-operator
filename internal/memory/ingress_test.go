package memory

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testProject(name string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: name, UID: "uid-1"}}
}

func TestIngress_NilWhenNoHost(t *testing.T) {
	if Ingress(testProject("alpha"), Config{Namespace: "tatara"}) != nil {
		t.Fatal("expected nil ingress when IngressHost is empty")
	}
}

func TestIngress_MemoryPathOnly(t *testing.T) {
	cfg := Config{Namespace: "tatara", IngressHost: "tatara.szymonrichert.pl", IngressClassName: "nginx", IngressRewriteTarget: "/$2", MemoryPathPrefix: "/api/v1/memory"}
	ing := Ingress(testProject("alpha"), cfg)
	if ing == nil {
		t.Fatal("expected non-nil ingress")
	}
	if ing.Name != "alpha" || ing.Namespace != "tatara" {
		t.Fatalf("meta: got %s/%s", ing.Namespace, ing.Name)
	}
	if ing.Annotations["nginx.ingress.kubernetes.io/rewrite-target"] != "/$2" {
		t.Fatalf("rewrite annotation missing: %v", ing.Annotations)
	}
	if *ing.Spec.IngressClassName != "nginx" {
		t.Fatalf("class: %v", ing.Spec.IngressClassName)
	}
	if len(ing.OwnerReferences) != 1 || ing.OwnerReferences[0].Name != "alpha" {
		t.Fatalf("owner ref: %v", ing.OwnerReferences)
	}
	paths := ing.Spec.Rules[0].HTTP.Paths
	if len(paths) != 1 {
		t.Fatalf("expected 1 path (memory only), got %d", len(paths))
	}
	if paths[0].Path != "/api/v1/memory/alpha(/|$)(.*)" {
		t.Fatalf("memory path: %s", paths[0].Path)
	}
	if paths[0].Backend.Service.Name != "mem-alpha" || paths[0].Backend.Service.Port.Number != 8080 {
		t.Fatalf("memory backend: %+v", paths[0].Backend.Service)
	}
	if ing.Spec.Rules[0].Host != "tatara.szymonrichert.pl" {
		t.Fatalf("host: %s", ing.Spec.Rules[0].Host)
	}
}

func TestIngress_AddsChatPath(t *testing.T) {
	cfg := Config{Namespace: "tatara", IngressHost: "h", MemoryPathPrefix: "/api/v1/memory", ChatPathPrefix: "/api/v1/chat"}
	ing := Ingress(testProject("alpha"), cfg)
	paths := ing.Spec.Rules[0].HTTP.Paths
	if len(paths) != 2 {
		t.Fatalf("expected memory+chat paths, got %d", len(paths))
	}
	if paths[1].Path != "/api/v1/chat/alpha(/|$)(.*)" || paths[1].Backend.Service.Name != "chat-alpha" {
		t.Fatalf("chat path/backend: %s %s", paths[1].Path, paths[1].Backend.Service.Name)
	}
}

// TestIngress_NoRewriteWhenUnset asserts the nginx-specific rewrite-target
// annotation is NOT emitted when IngressRewriteTarget is empty (cluster-agnostic,
// rule 14): a non-nginx controller must not be handed nginx annotations.
func TestIngress_NoRewriteWhenUnset(t *testing.T) {
	cfg := Config{Namespace: "tatara", IngressHost: "h", MemoryPathPrefix: "/api/v1/memory"}
	ing := Ingress(testProject("alpha"), cfg)
	if ing == nil {
		t.Fatal("expected non-nil ingress")
	}
	if _, ok := ing.Annotations["nginx.ingress.kubernetes.io/rewrite-target"]; ok {
		t.Fatalf("rewrite annotation must be absent when unset: %v", ing.Annotations)
	}
}

// TestIngress_ClassNameNilWhenUnset asserts IngressClassName is left nil (let the
// cluster default IngressClass apply) rather than a pointer-to-empty-string when
// unconfigured.
func TestIngress_ClassNameNilWhenUnset(t *testing.T) {
	cfg := Config{Namespace: "tatara", IngressHost: "h", MemoryPathPrefix: "/api/v1/memory"}
	ing := Ingress(testProject("alpha"), cfg)
	if ing.Spec.IngressClassName != nil {
		t.Fatalf("IngressClassName must be nil when unset, got %q", *ing.Spec.IngressClassName)
	}
}

// TestIngress_CustomRewriteTarget asserts a configured non-nginx-default rewrite
// target is honored verbatim.
func TestIngress_CustomRewriteTarget(t *testing.T) {
	cfg := Config{Namespace: "tatara", IngressHost: "h", IngressRewriteTarget: "/$2", MemoryPathPrefix: "/api/v1/memory"}
	ing := Ingress(testProject("alpha"), cfg)
	if ing.Annotations["nginx.ingress.kubernetes.io/rewrite-target"] != "/$2" {
		t.Fatalf("rewrite annotation = %v", ing.Annotations)
	}
}

func TestExternalMemoryURL(t *testing.T) {
	cfg := Config{IngressHost: "h", MemoryPathPrefix: "/api/v1/memory"}
	if got := ExternalMemoryURL("alpha", cfg); got != "https://h/api/v1/memory/alpha" {
		t.Fatalf("url: %s", got)
	}
	if ExternalMemoryURL("alpha", Config{}) != "" {
		t.Fatal("expected empty url when host unset")
	}
}
