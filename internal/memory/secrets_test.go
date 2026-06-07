package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/szymonrychu/tatara-operator/internal/memory"
)

func TestNeo4jPasswordSecret(t *testing.T) {
	p := testProject("acme")
	s := memory.Neo4jPasswordSecret(p, testCfg(), "s3cret")

	require.Equal(t, "mem-acme-neo4j", s.Name)
	require.Equal(t, "tatara", s.Namespace)
	require.Equal(t, "tatara-memory", s.Labels["app.kubernetes.io/name"])
	require.Len(t, s.OwnerReferences, 1)
	require.True(t, *s.OwnerReferences[0].Controller)

	require.Equal(t, "s3cret", s.StringData["password"])
	require.Equal(t, "neo4j/s3cret", s.StringData["NEO4J_AUTH"])
}
