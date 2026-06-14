package agent

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// DeleteWrapper best-effort deletes the wrapper Pod and Service for a task in
// the given namespace. Idempotent: a missing object (IsNotFound) is not an
// error. Shared by the controller (terminate, resetAgentRun, terminal
// lifecycle transitions) and the webhook server (reactivateTask), so the
// Pod+Service teardown lives in exactly one place despite the two different
// receiver types.
func DeleteWrapper(ctx context.Context, c client.Client, namespace string, task *tatarav1alpha1.Task) error {
	name := PodName(task)
	pod := &corev1.Pod{}
	pod.Name = name
	pod.Namespace = namespace
	if err := c.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete wrapper pod: %w", err)
	}
	svc := &corev1.Service{}
	svc.Name = name
	svc.Namespace = namespace
	if err := c.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete wrapper service: %w", err)
	}
	return nil
}
