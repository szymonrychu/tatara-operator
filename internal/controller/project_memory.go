package controller

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// ensureNeo4jPassword returns the neo4j password for the Project's memory
// stack, generating a random one and persisting it to the mem-<proj>-neo4j
// Secret on first reconcile. On subsequent reconciles it reads the existing
// Secret back so the password is never rotated.
func (r *ProjectReconciler) ensureNeo4jPassword(ctx context.Context, p *tataradevv1alpha1.Project) (string, error) {
	names := memory.NamesFor(p.Name)
	var existing corev1.Secret
	key := types.NamespacedName{Namespace: r.MemoryConfig.Namespace, Name: names.Neo4jSecret}
	err := r.Get(ctx, key, &existing)
	switch {
	case err == nil:
		pw := string(existing.Data["password"])
		if pw == "" {
			return "", fmt.Errorf("neo4j secret %s missing password key", names.Neo4jSecret)
		}
		return pw, nil
	case !apierrors.IsNotFound(err):
		return "", fmt.Errorf("get neo4j secret: %w", err)
	}

	pw, err := randomPassword(32)
	if err != nil {
		return "", fmt.Errorf("generate neo4j password: %w", err)
	}
	sec := memory.Neo4jPasswordSecret(p, r.MemoryConfig, pw)
	if err := r.Create(ctx, sec); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Lost a race; read the winner back.
			if err := r.Get(ctx, key, &existing); err != nil {
				return "", fmt.Errorf("get neo4j secret after race: %w", err)
			}
			return string(existing.Data["password"]), nil
		}
		return "", fmt.Errorf("create neo4j secret: %w", err)
	}
	return pw, nil
}

// randomPassword returns a URL-safe base64 string with at least nBytes of
// entropy.
func randomPassword(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
