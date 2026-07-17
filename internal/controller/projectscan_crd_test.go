package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestProjectCronFieldsRoundTrip(t *testing.T) {
	ctx := context.Background()
	mkSecret(t, "cron-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	p := &tataradevv1alpha1.Project{}
	p.Name = "cron-proj"
	p.Namespace = testNS
	p.Spec.ScmSecretRef = "cron-scm"
	p.Spec.Scm = &tataradevv1alpha1.ScmSpec{
		Provider: "github", Owner: "o", BotLogin: "bot",
		PriorityLabel: "tatara/priority",
		Cron: &tataradevv1alpha1.ScmCron{
			IssueScan: tataradevv1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 1},
			Brainstorm: tataradevv1alpha1.BrainstormActivity{
				Enabled: true, Schedule: "0 6 * * *", MaxPerCycle: 1,
				Sources: []string{"docs", "memory", "internet"},
			},
		},
	}
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create project: %v", err)
	}
	got := &tataradevv1alpha1.Project{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "cron-proj"}, got); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if got.Spec.Scm.PriorityLabel != "tatara/priority" || got.Spec.Scm.Cron.IssueScan.MaxPerRepo != 1 {
		t.Fatalf("cron fields not persisted: %+v", got.Spec.Scm)
	}
	if !got.Spec.Scm.Cron.Brainstorm.Enabled || got.Spec.Scm.Cron.Brainstorm.Sources[2] != "internet" {
		t.Fatalf("brainstorm fields not persisted: %+v", got.Spec.Scm.Cron.Brainstorm)
	}
	now := metav1.Now()
	got.Status.LastIssueScan = &now
	got.Status.LastBrainstorm = &now
	if err := k8sClient.Status().Update(ctx, got); err != nil {
		t.Fatalf("status update: %v", err)
	}
}

func TestTaskKindEnum(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"brainstorm", "incident", "clarify", "refine", "review", "documentation"} {
		tk := &tataradevv1alpha1.Task{}
		tk.Name = "enum-" + kind
		tk.Namespace = testNS
		repoRef := ""
		if kind == "documentation" {
			repoRef = "r"
		}
		tk.Spec = tataradevv1alpha1.TaskSpec{
			ProjectRef: "p", RepositoryRef: repoRef, Goal: "g", Kind: kind,
		}
		if err := k8sClient.Create(ctx, tk); err != nil {
			t.Fatalf("create task kind=%s: %v", kind, err)
		}
	}
	// The retired kinds are REJECTED by the CRD enum.
	for _, kind := range []string{"implement", "issueLifecycle", "triageIssue", "selfImprove", "healthCheck"} {
		tk := &tataradevv1alpha1.Task{}
		tk.Name = "enum-bad-" + strings.ToLower(kind)
		tk.Namespace = testNS
		tk.Spec = tataradevv1alpha1.TaskSpec{ProjectRef: "p", RepositoryRef: "r", Goal: "g", Kind: kind}
		if err := k8sClient.Create(ctx, tk); err == nil {
			t.Fatalf("kind=%s must be rejected by the CRD enum", kind)
		}
	}
}

// SPEC TEST 11. A live Project still carrying a stale mrScan block must apply
// CLEANLY once the field is gone. Structural-schema pruning drops an unknown
// field from an incoming CR with NO error, which is why the operator and
// tatara-helmfile may ship in either order with no coordinated cutover.
func TestProjectCR_StaleMRScanBlockIsPrunedNotRejected(t *testing.T) {
	ctx := context.Background()
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "tatara.dev/v1alpha1",
		"kind":       "Project",
		"metadata":   map[string]any{"name": "stale-mrscan", "namespace": testNS},
		"spec": map[string]any{
			"scmSecretRef": "tatara-scm",
			"scm": map[string]any{
				"provider": "github",
				"owner":    "o",
				"botLogin": "bot",
				"cron": map[string]any{
					"mrScan":    map[string]any{"maxPerRepo": int64(1), "schedule": "0 */2 * * *"},
					"issueScan": map[string]any{"maxPerRepo": int64(1), "schedule": "0 */4 * * *"},
				},
			},
		},
	}}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "tatara.dev", Version: "v1alpha1", Kind: "Project"})
	require.NoError(t, k8sClient.Create(ctx, obj), "a stale mrScan block must be PRUNED, not rejected")

	var got unstructured.Unstructured
	got.SetGroupVersionKind(obj.GroupVersionKind())
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), &got))
	cron, found, err := unstructured.NestedMap(got.Object, "spec", "scm", "cron")
	require.NoError(t, err)
	require.True(t, found)
	require.NotContains(t, cron, "mrScan", "the field must be pruned away")
	require.Contains(t, cron, "issueScan", "its live peer must survive")
}
