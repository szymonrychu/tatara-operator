// Package harness provides a per-project ConfigMap-backed key/value store with
// compare-and-swap semantics for durable agent harness state (the brainstorm
// LENS_CYCLE rotation register, incident alert dedup markers). It lives in the
// operator namespace, not the memory service, so it survives a memory-stack
// outage (which has deadlocked the fleet before). It mirrors queue.SeqSource:
// one ConfigMap per project, any replica mutates concurrently, optimistic
// concurrency via the whole-ConfigMap resourceVersion. Harness writes are
// single-writer-per-turn (brainstorm is one-per-project-per-cycle), so a
// whole-CM CAS token is adequate and no per-key generation is needed.
package harness

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/szymonrychu/tatara-operator/internal/slug"
)

const cmPrefix = "harness-state-"

// ErrConflict is returned by CAS when the caller's version does not match the
// backing ConfigMap resourceVersion (another writer advanced the state first),
// or when a create/update race was lost. Callers re-Get and retry.
var ErrConflict = errors.New("harness: version conflict")

// Store reads and compare-and-swaps per-project harness-state keys held in a
// single ConfigMap per project (harness-state-<sanitized-project>). Each key is
// one Data entry; the whole-ConfigMap resourceVersion is the CAS token.
type Store struct {
	Client    client.Client
	Namespace string
}

// cmName sanitizes the project to a DNS-1123 label so the "harness-state-"
// prefix (14 chars) keeps the full name within the 63-char limit.
func cmName(project string) string {
	return cmPrefix + slug.SanitizeDNS1123(project, 49)
}

// Entry is one harness-state value plus its CAS token (the backing ConfigMap
// resourceVersion). Version is "" when the ConfigMap does not yet exist; a
// first-run caller passes that empty version back to CAS to create the state.
type Entry struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Version string `json:"version"`
}

// Get returns the value for key and the CAS token. A missing ConfigMap yields
// an empty value + empty version (never an error): first-run callers treat
// empty as "no state yet".
func (s *Store) Get(ctx context.Context, project, key string) (Entry, error) {
	var cm corev1.ConfigMap
	err := s.Client.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: cmName(project)}, &cm)
	if apierrors.IsNotFound(err) {
		return Entry{Key: key, Value: "", Version: ""}, nil
	}
	if err != nil {
		return Entry{}, fmt.Errorf("harness: get state for project %s: %w", project, err)
	}
	return Entry{Key: key, Value: cm.Data[key], Version: cm.ResourceVersion}, nil
}

// CAS sets key=value only if version matches the current backing ConfigMap
// resourceVersion. An empty version means "expect no ConfigMap yet" (first
// write, creates it). Returns ErrConflict on a version mismatch or a lost
// create/update race.
func (s *Store) CAS(ctx context.Context, project, key, value, version string) (Entry, error) {
	name := cmName(project)
	var cm corev1.ConfigMap
	err := s.Client.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: name}, &cm)
	if apierrors.IsNotFound(err) {
		if version != "" {
			return Entry{}, ErrConflict
		}
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.Namespace},
			Data:       map[string]string{key: value},
		}
		if cerr := s.Client.Create(ctx, newCM); cerr != nil {
			if apierrors.IsAlreadyExists(cerr) {
				return Entry{}, ErrConflict
			}
			return Entry{}, fmt.Errorf("harness: create state for project %s: %w", project, cerr)
		}
		return Entry{Key: key, Value: value, Version: newCM.ResourceVersion}, nil
	}
	if err != nil {
		return Entry{}, fmt.Errorf("harness: get state for project %s: %w", project, err)
	}
	if cm.ResourceVersion != version {
		return Entry{}, ErrConflict
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[key] = value
	if uerr := s.Client.Update(ctx, &cm); uerr != nil {
		if apierrors.IsConflict(uerr) {
			return Entry{}, ErrConflict
		}
		return Entry{}, fmt.Errorf("harness: update state for project %s: %w", project, uerr)
	}
	return Entry{Key: key, Value: value, Version: cm.ResourceVersion}, nil
}
