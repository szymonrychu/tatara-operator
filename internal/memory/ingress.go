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
		// The path is project-scoped so external clients address chat per project,
		// but chat is a single shared service, so every project's path routes to the
		// one ChatServiceName backend (not a per-project chat-<project>).
		paths = append(paths, networkingv1.HTTPIngressPath{
			Path:     cfg.ChatPathPrefix + "/" + p.Name + "(/|$)(.*)",
			PathType: &pt,
			Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
				Name: ChatServiceName, Port: networkingv1.ServiceBackendPort{Number: 8080}}},
		})
	}
	meta := objectMeta(p, cfg, p.Name)
	// Emit nginx-specific annotations only when rewrite-target is configured,
	// so a non-nginx controller is not handed nginx annotations (rule 14).
	// use-regex must accompany rewrite-target: the path above is itself a regex
	// (MemoryPathPrefix + "/" + name + "(/|$)(.*)"), and without use-regex nginx
	// treats it as a literal path - query/search_entities 404 at the ingress.
	if cfg.IngressRewriteTarget != "" {
		meta.Annotations = map[string]string{
			"nginx.ingress.kubernetes.io/rewrite-target": cfg.IngressRewriteTarget,
			"nginx.ingress.kubernetes.io/use-regex":      "true",
		}
	}
	// Leave IngressClassName nil (cluster default IngressClass applies) when
	// unconfigured, rather than a pointer-to-empty-string.
	var className *string
	if cfg.IngressClassName != "" {
		c := cfg.IngressClassName
		className = &c
	}
	return &networkingv1.Ingress{
		TypeMeta:   metav1.TypeMeta{APIVersion: "networking.k8s.io/v1", Kind: "Ingress"},
		ObjectMeta: meta,
		Spec: networkingv1.IngressSpec{
			IngressClassName: className,
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
