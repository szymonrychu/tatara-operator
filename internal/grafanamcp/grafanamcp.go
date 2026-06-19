// Package grafanamcp holds pure builder functions producing the per-Project
// read-only grafana-mcp workload (grafana/mcp-grafana, streamable-http,
// --disable-write). No function performs a client call; the ProjectReconciler
// server-side-applies the returned objects (mirrors internal/memory).
package grafanamcp

import (
	"fmt"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Config is the operator-level (non-per-Project) input the builders need.
type Config struct {
	Namespace       string
	Image           string
	ImagePullSecret string
}

// Name returns the Deployment/Service name for a project.
func Name(project string) string { return "grafana-mcp-" + project }

// Endpoint is the in-cluster base URL of a project's grafana-mcp.
func Endpoint(project, namespace string) string {
	return fmt.Sprintf("http://grafana-mcp-%s.%s.svc:8000", project, namespace)
}

// MCPURL is the streamable-http MCP endpoint the agent connects to.
func MCPURL(project, namespace string) string { return Endpoint(project, namespace) + "/mcp" }

func labels(project string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "grafana-mcp",
		"app.kubernetes.io/instance": Name(project),
		"tatara.dev/project":         project,
	}
}

func ownerRef(p *tatarav1alpha1.Project) metav1.OwnerReference {
	return *metav1.NewControllerRef(p, tatarav1alpha1.GroupVersion.WithKind("Project"))
}

func objectMeta(p *tatarav1alpha1.Project, cfg Config, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:            name,
		Namespace:       cfg.Namespace,
		Labels:          labels(p.Name),
		OwnerReferences: []metav1.OwnerReference{ownerRef(p)},
	}
}
