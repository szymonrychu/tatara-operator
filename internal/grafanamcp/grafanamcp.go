// Package grafanamcp holds pure builder functions producing the per-Project
// read-only grafana-mcp workload (grafana/mcp-grafana, streamable-http,
// --disable-write). No function performs a client call; the ProjectReconciler
// server-side-applies the returned objects (mirrors internal/memory).
package grafanamcp

import (
	"fmt"
	"strings"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"golang.org/x/mod/semver"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// minImageVersion is the first mcp-grafana release that understands
// GRAFANA_SERVICE_ACCOUNT_TOKEN_FILE. Images below this version silently
// ignore the _FILE env var and send every Grafana API request unauthenticated.
const minImageVersion = "v0.16.0"

// ValidateImage returns an error when image contains a parseable semver tag
// that is older than minImageVersion. Non-semver tags (latest, digests, "")
// are accepted (fail-open) because we cannot determine their version.
func ValidateImage(image string) error {
	tag := imageTag(image)
	if tag == "" {
		return nil
	}
	// Normalise: golang.org/x/mod/semver requires a "v" prefix.
	v := tag
	if !strings.HasPrefix(v, "v") {
		v = "v" + v
	}
	if !semver.IsValid(v) {
		return nil // unparseable (e.g. "latest") -> fail-open
	}
	if semver.Compare(v, minImageVersion) < 0 {
		return fmt.Errorf("grafanamcp: image %q has tag %s which is older than the minimum required %s; GRAFANA_SERVICE_ACCOUNT_TOKEN_FILE is not supported, upgrade the image", image, tag, minImageVersion)
	}
	return nil
}

// imageTag extracts the tag portion from an image reference, or "" when
// the reference uses a digest or has no tag.
func imageTag(image string) string {
	// digest reference: image@sha256:... -> no parseable semver tag
	if strings.Contains(image, "@") {
		return ""
	}
	idx := strings.LastIndex(image, ":")
	if idx < 0 {
		return ""
	}
	return image[idx+1:]
}

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
