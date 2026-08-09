package grafanamcp

import (
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func proj() *tatarav1alpha1.Project {
	p := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "acme", Namespace: "tatara"}}
	p.Spec.Grafana = &tatarav1alpha1.GrafanaSpec{Enabled: true, URL: "http://grafana:3000", SecretRef: "acme-grafana"}
	return p
}

func TestDeployment_ReadOnlyStreamableHTTP(t *testing.T) {
	d := Deployment(proj(), Config{Namespace: "tatara", Image: "grafana/mcp-grafana:v0.1.0"})
	if d.Name != "grafana-mcp-acme" {
		t.Fatalf("name: %s", d.Name)
	}
	c := d.Spec.Template.Spec.Containers[0]
	args := strings.Join(c.Args, " ")
	if !strings.Contains(args, "streamable-http") || !strings.Contains(args, "--disable-write") {
		t.Fatalf("args must be read-only streamable-http: %v", c.Args)
	}
	if c.Ports[0].ContainerPort != 8000 {
		t.Fatalf("port: %d", c.Ports[0].ContainerPort)
	}
	var url, tokenFile string
	for _, e := range c.Env {
		if e.Name == "GRAFANA_URL" {
			url = e.Value
		}
		if e.Name == "GRAFANA_SERVICE_ACCOUNT_TOKEN_FILE" {
			tokenFile = e.Value
		}
	}
	if url != "http://grafana:3000" {
		t.Fatalf("GRAFANA_URL: %q", url)
	}
	if tokenFile != "/etc/grafana/token" {
		t.Fatalf("token file env: %q", tokenFile)
	}
	if c.SecurityContext == nil || c.SecurityContext.RunAsUser == nil {
		t.Fatalf("container needs a numeric runAsUser (runAsNonRoot incident)")
	}
	// token mounted from the project's grafana secret, key serviceAccountToken.
	vol := d.Spec.Template.Spec.Volumes[0]
	if vol.Secret == nil || vol.Secret.SecretName != "acme-grafana" {
		t.Fatalf("token volume must project secret acme-grafana: %+v", vol)
	}
	if vol.Secret.Items[0].Key != "serviceAccountToken" || vol.Secret.Items[0].Path != "token" {
		t.Fatalf("token volume item must be serviceAccountToken->token: %+v", vol.Secret.Items)
	}
}

func TestService_ClusterIP8000(t *testing.T) {
	s := Service(proj(), Config{Namespace: "tatara"})
	if s.Name != "grafana-mcp-acme" || s.Spec.Ports[0].Port != 8000 {
		t.Fatalf("service: %s :%d", s.Name, s.Spec.Ports[0].Port)
	}
}

func TestEndpointAndMCPURL(t *testing.T) {
	if Endpoint("acme", "tatara") != "http://grafana-mcp-acme.tatara.svc:8000" {
		t.Fatalf("endpoint: %s", Endpoint("acme", "tatara"))
	}
	if MCPURL("acme", "tatara") != "http://grafana-mcp-acme.tatara.svc:8000/mcp" {
		t.Fatalf("mcp url: %s", MCPURL("acme", "tatara"))
	}
}

// TestDeployment_NoPlacementWhenUnset asserts a zero-value Config stamps a
// Deployment with no placement at all, so an operator deployed from the
// cluster-agnostic chart default (rule 14) behaves exactly as it does today.
func TestDeployment_NoPlacementWhenUnset(t *testing.T) {
	d := Deployment(proj(), Config{Namespace: "tatara", Image: "grafana/mcp-grafana:v0.1.0"})
	ps := d.Spec.Template.Spec
	if ps.NodeSelector != nil {
		t.Fatalf("nodeSelector must be nil when unset: %+v", ps.NodeSelector)
	}
	if ps.Tolerations != nil {
		t.Fatalf("tolerations must be nil when unset: %+v", ps.Tolerations)
	}
	if ps.Affinity != nil {
		t.Fatalf("affinity must be nil when unset: %+v", ps.Affinity)
	}
}

// TestDeployment_AppliesScheduling asserts the cluster-specific placement the
// deploying helmfile supplies via MCP_SCHEDULING reaches the stamped PodSpec.
// This is the fix for two grafana-mcp Deployments landing on the CI-runner node
// because the operator gave them no scheduling constraints at all.
func TestDeployment_AppliesScheduling(t *testing.T) {
	aff := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "node-role.kubernetes.io/control-plane",
						Operator: corev1.NodeSelectorOpExists,
					}},
				}},
			},
		},
	}
	cfg := Config{
		Namespace:    "tatara",
		Image:        "grafana/mcp-grafana:v0.1.0",
		NodeSelector: map[string]string{"kubernetes.io/os": "linux"},
		Tolerations:  []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}},
		Affinity:     aff,
	}

	ps := Deployment(proj(), cfg).Spec.Template.Spec
	if ps.NodeSelector["kubernetes.io/os"] != "linux" {
		t.Fatalf("nodeSelector not applied: %+v", ps.NodeSelector)
	}
	if len(ps.Tolerations) != 1 || ps.Tolerations[0].Key != "dedicated" {
		t.Fatalf("tolerations not applied: %+v", ps.Tolerations)
	}
	if ps.Affinity == nil || ps.Affinity.NodeAffinity == nil {
		t.Fatalf("affinity not applied: %+v", ps.Affinity)
	}
	terms := ps.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if terms[0].MatchExpressions[0].Key != "node-role.kubernetes.io/control-plane" {
		t.Fatalf("node affinity term not applied: %+v", terms)
	}
}

func TestValidateImage_RejectsBelowFloor(t *testing.T) {
	cases := []struct {
		image   string
		wantErr bool
	}{
		// parseable semver below 0.16.0 -> must error
		{"grafana/mcp-grafana:0.11.4", true},
		{"grafana/mcp-grafana:0.15.2", true},
		{"grafana/mcp-grafana:v0.1.0", true},
		// at or above floor -> ok
		{"grafana/mcp-grafana:0.16.0", false},
		{"grafana/mcp-grafana:0.17.0", false},
		{"grafana/mcp-grafana:v0.16.0", false},
		// unparseable / digest -> fail-open (don't block)
		{"grafana/mcp-grafana:latest", false},
		{"grafana/mcp-grafana@sha256:abc123", false},
		{"", false},
	}
	for _, tc := range cases {
		err := ValidateImage(tc.image)
		if tc.wantErr && err == nil {
			t.Errorf("image %q: expected error (below _FILE floor), got nil", tc.image)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("image %q: unexpected error: %v", tc.image, err)
		}
	}
}
