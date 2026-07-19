package controller

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// TestProjectReconcile_StatusUpdateRetriesOnConflict is the regression guard for
// tatara-operator#387: the Project reconcile's status write (project_controller.go)
// was the lone status-write site doing a raw r.Status().Update with no conflict
// handling, so a routine optimistic-concurrency 409 (amplified during operator
// rollouts) bubbled up as a "Reconciler error" log line. It must now retry the
// conflict and complete without returning an error, while NOT reverting a status
// field it does not compute (TokenBudget, owned by the turncallback/#189 path) to
// its pre-conflict value.
func TestProjectReconcile_StatusUpdateRetriesOnConflict(t *testing.T) {
	ctx := context.Background()
	mkSecret(t, "conflict-scm", map[string][]byte{
		"token":         []byte("ghp_x"),
		"webhookSecret": []byte("hmac"),
	})
	p := &tataradevv1alpha1.Project{}
	p.Name = "proj-conflict"
	p.Namespace = testNS
	p.Spec.ScmSecretRef = "conflict-scm"
	require.NoError(t, k8sClient.Create(ctx, p))

	// Seed a status field this reconcile does NOT compute (owned by the #189
	// token-budget write path). A whole-status copy on a conflict retry would
	// clobber it back to its pre-read value; the surgical re-apply must preserve it.
	fresh := getProject(t, "proj-conflict")
	fresh.Status.TokenBudget = &tataradevv1alpha1.TokenBudgetStatus{WindowTokens: 4242}
	require.NoError(t, k8sClient.Status().Update(ctx, fresh))

	var calls atomic.Int32
	reg := prometheus.NewRegistry()
	r := &ProjectReconciler{
		Client:              &conflictOnceProjectClient{Client: k8sClient, calls: &calls},
		Scheme:              k8sClient.Scheme(),
		Metrics:             obs.NewOperatorMetrics(reg),
		ExternalWebhookBase: "https://tatara.example/operator/webhooks",
		MemoryConfig: memory.Config{
			Namespace:        testNS,
			MemoryImage:      "harbor.example/tatara-memory:test",
			LightragImage:    "harbor.example/lightrag:test",
			Neo4jImage:       "neo4j:5-community",
			OpenAISecretName: "openai-shared",
			OIDCIssuer:       "https://keycloak.example/realms/tatara",
			OIDCAudience:     "tatara-memory",
		},
	}

	_, err := r.Reconcile(logf.IntoContext(ctx, logf.Log), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: "proj-conflict"},
	})
	require.NoError(t, err, "status-update conflict must be retried, not surfaced as a Reconciler error")
	require.GreaterOrEqual(t, calls.Load(), int32(2), "must have retried the status write at least once")

	got := getProject(t, "proj-conflict")
	// Computed fields landed.
	require.NotEmpty(t, got.Status.WebhookURL, "computed WebhookURL must persist after the retry")
	require.NotNil(t, apimeta.FindStatusCondition(got.Status.Conditions, "Ready"),
		"computed Ready condition must persist after the retry")
	// Other-path field preserved, not reverted by the retry.
	require.NotNil(t, got.Status.TokenBudget, "TokenBudget must not be clobbered by the status-write retry")
	require.Equal(t, int64(4242), got.Status.TokenBudget.WindowTokens,
		"retry must re-apply only computed fields, leaving TokenBudget intact")
}
