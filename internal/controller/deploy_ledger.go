package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/szymonrychu/tatara-operator/internal/slug"
)

// ledgerRetry mirrors queue.seqRetry: a richer backoff than retry.DefaultRetry
// (5 steps) so the per-project deploy-ledger CAS survives a burst of Deploying
// Tasks contending on one ConfigMap without surfacing a Conflict.
var ledgerRetry = wait.Backoff{
	Duration: 5 * time.Millisecond,
	Factor:   1.5,
	Jitter:   0.2,
	Steps:    30,
}

const (
	deployLedgerPrefix = "deploy-ledger-"
	deployLedgerKey    = "entries"
)

// Deploy-ledger entry states.
const (
	DeployStateDeploying = "deploying"
	DeployStateApplied   = "applied"
	DeployStateFailed    = "failed"
)

// DeployLedgerEntry is one Deploying Task's record in the per-Project deploy
// ledger: the artifact it published, the version driving toward the cluster, the
// Task and originating issue it belongs to, and its head SHA. The apply-outcome
// sweep matches applied pins against {Artifact, Version} and resolves every
// matching Task in one pass (8a.3), so N Tasks converging on one helmfile apply
// share one watcher and resolve together.
type DeployLedgerEntry struct {
	Artifact      string `json:"artifact"`
	Version       string `json:"version"`
	SourceTaskRef string `json:"sourceTaskRef"`
	IssueRef      string `json:"issueRef,omitempty"`
	HeadSHA       string `json:"headSHA,omitempty"`
	// +kubebuilder:validation:Enum=deploying;applied;failed
	State string `json:"state"`
}

// DeployLedger is the per-Project deploy ledger: a ConfigMap (CAS) holding the
// JSON array of DeployLedgerEntry under key "entries". Modelled EXACTLY on
// queue.SeqSource - any replica mutates concurrently, RetryOnConflict + the
// ConfigMap's own resourceVersion serialise read-modify-write across replicas
// with no leader and no in-memory state. The ConfigMap is intentionally NOT
// owner-referenced to the Project (ProjectReconciler uses Owns(&ConfigMap{}), so
// an owned CM would storm a reconcile on every CAS Update); it is orphaned when
// its Project is deleted, which is acceptable.
type DeployLedger struct {
	Client    client.Client
	Namespace string
}

// deployLedgerName returns the per-project ledger ConfigMap name, sanitising the
// project to a DNS-1123 label (same near-identity treatment as queue.SeqSource).
func deployLedgerName(project string) string {
	return deployLedgerPrefix + sanitizeDNS1123Label(project)
}

// sanitizeDNS1123Label lowercases s, collapses every run of non-[a-z0-9] into a
// single '-', trims leading/trailing '-', and caps the result so the
// "deploy-ledger-" prefix (14 chars) keeps the name within the 63-char limit.
func sanitizeDNS1123Label(s string) string {
	return slug.SanitizeDNS1123(s, 49)
}

// mutate runs fn against the current entries under a CAS loop and persists the
// result. fn receives a copy of the decoded entries and returns the new slice;
// it MUST be pure (no side effects) because RetryOnConflict re-invokes it on a
// lost race. A missing ConfigMap is created with fn(nil)'s result.
func (l *DeployLedger) mutate(ctx context.Context, project string, fn func([]DeployLedgerEntry) ([]DeployLedgerEntry, error)) error {
	name := deployLedgerName(project)
	err := retry.RetryOnConflict(ledgerRetry, func() error {
		var cm corev1.ConfigMap
		getErr := l.Client.Get(ctx, client.ObjectKey{Namespace: l.Namespace, Name: name}, &cm)
		if apierrors.IsNotFound(getErr) {
			next, fnErr := fn(nil)
			if fnErr != nil {
				return fnErr
			}
			encoded, encErr := encodeEntries(next)
			if encErr != nil {
				return encErr
			}
			newCM := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: l.Namespace},
				Data:       map[string]string{deployLedgerKey: encoded},
			}
			createErr := l.Client.Create(ctx, newCM)
			if apierrors.IsAlreadyExists(createErr) {
				// Lost the create race; synthetic Conflict re-Gets the winner.
				return apierrors.NewConflict(corev1.Resource("configmaps"), name, createErr)
			}
			return createErr
		}
		if getErr != nil {
			return getErr
		}
		cur, decErr := decodeEntries(cm.Data[deployLedgerKey])
		if decErr != nil {
			return fmt.Errorf("deploy-ledger: corrupt entries for project %s: %w", project, decErr)
		}
		next, fnErr := fn(cur)
		if fnErr != nil {
			return fnErr
		}
		encoded, encErr := encodeEntries(next)
		if encErr != nil {
			return encErr
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[deployLedgerKey] = encoded
		return l.Client.Update(ctx, &cm)
	})
	if err != nil {
		return fmt.Errorf("deploy-ledger: mutate project %s: %w", project, err)
	}
	return nil
}

// Add records (or refreshes) a Deploying Task's entry. The entry is keyed by
// SourceTaskRef: a Task re-reconciling upserts in place rather than duplicating,
// so the ledger stays one-entry-per-Task across repeated reconciles.
func (l *DeployLedger) Add(ctx context.Context, project string, e DeployLedgerEntry) error {
	return l.mutate(ctx, project, func(es []DeployLedgerEntry) ([]DeployLedgerEntry, error) {
		for i := range es {
			if es[i].SourceTaskRef == e.SourceTaskRef {
				es[i] = e
				return es, nil
			}
		}
		return append(es, e), nil
	})
}

// SetState updates the state of the entry for sourceTaskRef. A no-op if absent.
func (l *DeployLedger) SetState(ctx context.Context, project, sourceTaskRef, state string) error {
	return l.mutate(ctx, project, func(es []DeployLedgerEntry) ([]DeployLedgerEntry, error) {
		for i := range es {
			if es[i].SourceTaskRef == sourceTaskRef {
				es[i].State = state
				return es, nil
			}
		}
		return es, nil
	})
}

// List returns the current ledger entries (empty when the ledger is absent).
func (l *DeployLedger) List(ctx context.Context, project string) ([]DeployLedgerEntry, error) {
	var cm corev1.ConfigMap
	err := l.Client.Get(ctx, client.ObjectKey{Namespace: l.Namespace, Name: deployLedgerName(project)}, &cm)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("deploy-ledger: list project %s: %w", project, err)
	}
	entries, decErr := decodeEntries(cm.Data[deployLedgerKey])
	if decErr != nil {
		return nil, fmt.Errorf("deploy-ledger: corrupt entries for project %s: %w", project, decErr)
	}
	return entries, nil
}

// MatchEntries returns the entries whose artifact and version both equal the
// applied pin. Pure helper for the apply-outcome sweep: every returned entry's
// SourceTaskRef is a Task to resolve in this pass (N Tasks => one sweep).
func MatchEntries(entries []DeployLedgerEntry, artifact, version string) []DeployLedgerEntry {
	var out []DeployLedgerEntry
	for _, e := range entries {
		if e.Artifact == artifact && e.Version == version {
			out = append(out, e)
		}
	}
	return out
}

// encodeEntries marshals entries to the canonical JSON array stored under the
// ledger key. nil/empty entries encode to "[]" so the key is always present.
func encodeEntries(entries []DeployLedgerEntry) (string, error) {
	if entries == nil {
		entries = []DeployLedgerEntry{}
	}
	b, err := json.Marshal(entries)
	if err != nil {
		return "", fmt.Errorf("deploy-ledger: marshal entries: %w", err)
	}
	return string(b), nil
}

// decodeEntries parses the stored JSON array. An empty/missing value is an empty
// ledger (not an error); a present-but-unparseable value is a hard error.
func decodeEntries(raw string) ([]DeployLedgerEntry, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var entries []DeployLedgerEntry
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("unmarshal %q: %w", raw, err)
	}
	return entries, nil
}
