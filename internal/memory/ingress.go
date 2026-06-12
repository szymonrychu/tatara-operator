package memory

import (
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Ingress builds the per-Project Ingress exposing this project's memory (and,
// when ChatPathPrefix is set, chat) on cfg.IngressHost under a project-scoped
// path with a rewrite that strips the prefix. Returns nil when cfg.IngressHost
// is empty (external exposure disabled). Owner-ref'd to the Project; no TLS
// block (the host cert is owned by the operator Ingress) and no nginx auth
// annotations (memory/chat enforce OIDC at the app).
func Ingress(p *tatarav1alpha1.Project, cfg Config) *networkingv1.Ingress {
	if cfg.IngressHost == "" {
		return nil
	}
	n := NamesFor(p.Name)
	pt := networkingv1.PathTypeImplementationSpecific
	paths := []networkingv1.HTTPIngressPath{{
		Path:     cfg.MemoryPathPrefix + "/" + p.Name + "(/|$)(.*)",
		PathType: &pt,
		Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
			Name: n.Memory, Port: networkingv1.ServiceBackendPort{Number: 8080}}},
	}}
	if cfg.ChatPathPrefix != "" {
		paths = append(paths, networkingv1.HTTPIngressPath{
			Path:     cfg.ChatPathPrefix + "/" + p.Name + "(/|$)(.*)",
			PathType: &pt,
			Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
				Name: "chat-" + p.Name, Port: networkingv1.ServiceBackendPort{Number: 8080}}},
		})
	}
	className := cfg.IngressClassName
	meta := objectMeta(p, cfg, p.Name)
	meta.Annotations = map[string]string{"nginx.ingress.kubernetes.io/rewrite-target": "/$2"}
	return &networkingv1.Ingress{
		TypeMeta:   metav1.TypeMeta{APIVersion: "networking.k8s.io/v1", Kind: "Ingress"},
		ObjectMeta: meta,
		Spec: networkingv1.IngressSpec{
			IngressClassName: &className,
			Rules: []networkingv1.IngressRule{{
				Host:             cfg.IngressHost,
				IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{Paths: paths}},
			}},
		},
	}
}

// ExternalMemoryURL is the external URL of a Project's memory, or "" if not exposed.
func ExternalMemoryURL(project string, cfg Config) string {
	if cfg.IngressHost == "" {
		return ""
	}
	return "https://" + cfg.IngressHost + cfg.MemoryPathPrefix + "/" + project
}
