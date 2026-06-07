package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testProject(name string) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara", UID: "uid-123"},
	}
}

func TestNames(t *testing.T) {
	n := memory.NamesFor("acme")
	require.Equal(t, "mem-acme-pg", n.PGCluster)
	require.Equal(t, "mem-acme-pg-rw", n.PGService)
	require.Equal(t, "mem-acme-pg-app", n.PGAppSecret)
	require.Equal(t, "mem-acme-neo4j", n.Neo4j)
	require.Equal(t, "mem-acme-neo4j", n.Neo4jSecret)
	require.Equal(t, "mem-acme-lightrag", n.Lightrag)
	require.Equal(t, "mem-acme-lightrag-data", n.LightragPVC)
	require.Equal(t, "mem-acme", n.Memory)
}

func TestEndpoint(t *testing.T) {
	require.Equal(t, "http://mem-acme.tatara.svc:8080", memory.Endpoint("acme", "tatara"))
	require.Equal(t, "http://mem-foo.other.svc:8080", memory.Endpoint("foo", "other"))
}
