package grafanamcp

import (
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
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
