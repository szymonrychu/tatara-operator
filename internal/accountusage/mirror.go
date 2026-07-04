package accountusage

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const mirrorDataKey = "snapshot.json"

// Mirror persists the fleet-wide Snapshot to a ConfigMap so a manager restart
// (or a newly-elected leader) restores the last-known windows instead of
// starting from zero and re-blocking every kind until the first live poll.
type Mirror struct {
	Client    client.Client
	Namespace string
	Name      string
}

// Save JSON-encodes snap into the mirror ConfigMap's single data key,
// creating the ConfigMap if it does not exist yet.
func (m *Mirror) Save(ctx context.Context, snap Snapshot) error {
	body, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("accountusage: marshal snapshot: %w", err)
	}
	var cm corev1.ConfigMap
	err = m.Client.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Name}, &cm)
	if apierrors.IsNotFound(err) {
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: m.Name, Namespace: m.Namespace},
			Data:       map[string]string{mirrorDataKey: string(body)},
		}
		if createErr := m.Client.Create(ctx, newCM); createErr != nil {
			return fmt.Errorf("accountusage: create mirror configmap: %w", createErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("accountusage: get mirror configmap: %w", err)
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[mirrorDataKey] = string(body)
	if err := m.Client.Update(ctx, &cm); err != nil {
		return fmt.Errorf("accountusage: update mirror configmap: %w", err)
	}
	return nil
}

// Load decodes the mirrored Snapshot. A missing ConfigMap or data key returns
// a zero Snapshot and nil error, so a fresh cluster (no prior poll) starts
// cleanly rather than failing manager startup.
func (m *Mirror) Load(ctx context.Context) (Snapshot, error) {
	var cm corev1.ConfigMap
	err := m.Client.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Name}, &cm)
	if apierrors.IsNotFound(err) {
		return Snapshot{}, nil
	}
	if err != nil {
		return Snapshot{}, fmt.Errorf("accountusage: get mirror configmap: %w", err)
	}
	body, ok := cm.Data[mirrorDataKey]
	if !ok || body == "" {
		return Snapshot{}, nil
	}
	var snap Snapshot
	if err := json.Unmarshal([]byte(body), &snap); err != nil {
		return Snapshot{}, fmt.Errorf("accountusage: decode mirror configmap: %w", err)
	}
	return snap, nil
}
