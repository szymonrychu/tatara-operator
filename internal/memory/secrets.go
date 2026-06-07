package memory

import (
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Neo4jPasswordSecret builds the generated neo4j password Secret. The caller
// (ProjectReconciler, N2) generates password once and guards on existence;
// this builder is pure. Key "password" feeds lightrag's NEO4J_PASSWORD; key
// "NEO4J_AUTH" (neo4j/<password>) feeds the neo4j StatefulSet.
func Neo4jPasswordSecret(p *tatarav1alpha1.Project, cfg Config, password string) *corev1.Secret {
	n := NamesFor(p.Name)
	return &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: objectMeta(p, cfg, n.Neo4jSecret),
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"password":   password,
			"NEO4J_AUTH": "neo4j/" + password,
		},
	}
}
