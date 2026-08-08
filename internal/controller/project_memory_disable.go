package controller

import (
	"context"
	"fmt"
	"strings"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// disableMemory is the spec.memory.enabled=false branch of reconcileMemory.
//
// It tears the memory stack's COMPUTE and MONITORING objects down and leaves
// the Project in the terminal Disabled phase. It does NOT touch the data: every
// PersistentVolumeClaim (and every cnpg-generated Secret) is retained first, so
// re-enabling picks the same volumes back up. See retainMemoryData for why that
// is mandatory rather than nice-to-have.
//
// The teardown itself runs at most ONCE per Project generation. A disabled
// Project is still reconciled on every watch event forever, and re-issuing a
// dozen deletes per pass against objects that are already gone is a pointless
// apiserver load with a real failure mode (each one is a write the apiserver
// audits). status.memory.disabledGeneration is the marker.
func (r *ProjectReconciler) disableMemory(ctx context.Context, p *tataradevv1alpha1.Project) error {
	l := log.FromContext(ctx)
	st := p.Status.Memory

	if st.Phase != tataradevv1alpha1.MemoryPhaseDisabled || st.DisabledGeneration != p.Generation {
		// Retain BEFORE deleting: the cnpg PVCs carry an ownerRef to the cnpg
		// Cluster, so the Cluster delete below is itself the cascade that would
		// destroy them.
		retained, err := r.retainMemoryData(ctx, p)
		if err != nil {
			return r.failMemory(p, "RetainError", err)
		}
		if err := r.tearDownMemoryStack(ctx, p); err != nil {
			return r.failMemory(p, "TeardownError", err)
		}
		st.DisabledGeneration = p.Generation
		l.Info("memory stack disabled: compute and monitoring torn down, data retained",
			"action", "memory_disable",
			"resource_id", p.Name,
			"generation", p.Generation,
			"retained", strings.Join(retained, ","))
	}

	// A disabled stack has no endpoint and no readiness clocks. Clearing them
	// keeps MemoryStablyReady false and stops a stale ReadySince from making a
	// torn-down stack look serving.
	st.Phase = tataradevv1alpha1.MemoryPhaseDisabled
	st.Endpoint = ""
	st.ExternalEndpoint = ""
	st.ReadySince = nil
	st.ProvisioningSince = nil
	st.NotReady = nil
	st.PgReadyInstances = 0
	st.PgWantInstances = 0
	st.PgPrimary = ""
	r.clearTransientApply(p.Name)
	delete(r.memoryUnhealthyCycles, p.Name)

	meta.SetStatusCondition(&p.Status.Conditions, metav1.Condition{
		Type:   "MemoryReady",
		Status: metav1.ConditionFalse,
		Reason: "Disabled",
		Message: "memory is disabled for this project (spec.memory.enabled=false); " +
			"the stack's volumes are retained and labelled " +
			memory.RetainedForProjectLabel + "=" + p.Name,
		ObservedGeneration: p.Generation,
	})
	return nil
}

// tearDownMemoryStack deletes every COMPUTE and MONITORING object of the
// Project's memory stack. Deleting is not optional: merely ceasing to reconcile
// leaves the Deployments running and - worse - leaves the stack's own
// PrometheusRule loaded, firing against backends that no longer exist. This is
// the same argument deleteScheduledBackup already makes for the cnpg
// ScheduledBackup.
//
// Deliberately NOT deleted: the PVCs (retained by retainMemoryData) and the
// neo4j password Secret, which a re-enabled stack must reuse or the retained
// neo4j volume cannot be opened.
func (r *ProjectReconciler) tearDownMemoryStack(ctx context.Context, p *tataradevv1alpha1.Project) error {
	ns := r.MemoryConfig.Namespace
	n := memory.NamesFor(p.Name)

	objs := []client.Object{
		&appsv1.Deployment{ObjectMeta: objMeta(ns, n.Memory)},
		&corev1.Service{ObjectMeta: objMeta(ns, n.Memory)},
		&corev1.ConfigMap{ObjectMeta: objMeta(ns, n.Memory)},
		&appsv1.Deployment{ObjectMeta: objMeta(ns, n.Lightrag)},
		&corev1.Service{ObjectMeta: objMeta(ns, n.Lightrag)},
		&appsv1.StatefulSet{ObjectMeta: objMeta(ns, n.Neo4j)},
		&corev1.Service{ObjectMeta: objMeta(ns, n.Neo4j)},
		// The ingress is named after the Project, not the mem-* family.
		&networkingv1.Ingress{ObjectMeta: objMeta(ns, p.Name)},
		// Monitors last-but-one: a stale ServiceMonitor/PodMonitor would keep the
		// scrape targets registered and a stale PrometheusRule would keep the
		// per-project memory alerts loaded against a stack that is gone.
		&monitoringv1.ServiceMonitor{ObjectMeta: objMeta(ns, n.Memory)},
		&monitoringv1.PodMonitor{ObjectMeta: objMeta(ns, n.PGCluster)},
		&monitoringv1.PrometheusRule{ObjectMeta: objMeta(ns, n.Memory)},
		// The cnpg Cluster goes LAST: it is the object whose cascade would reach
		// the PVCs, so nothing else may fail after it.
		&cnpgv1.Cluster{ObjectMeta: objMeta(ns, n.PGCluster)},
	}
	for _, obj := range objs {
		if err := r.Delete(ctx, obj); !memoryDeleteTolerable(err) {
			return fmt.Errorf("delete %T %s: %w", obj, obj.GetName(), err)
		}
	}
	return r.deleteScheduledBackup(ctx, p)
}

// memoryDeleteTolerable reports whether a teardown Delete outcome means "there
// is definitively nothing to remove".
//
// NotFound is the steady state on a second pass. A no-kind-match means the CRD
// is not installed on this cluster (prometheus-operator absent, the same case
// MonitorEnabled=false covers on the apply side), and a not-registered type
// means the scheme this client was built with does not carry the kind. Neither
// can be hiding a live object, so failing the whole teardown over them would
// only strand the objects that come after in the list.
func memoryDeleteTolerable(err error) bool {
	return err == nil ||
		apierrors.IsNotFound(err) ||
		meta.IsNoMatchError(err) ||
		runtime.IsNotRegisteredError(err)
}

// retainMemoryData makes the memory stack's persistent state survive the
// teardown, and returns the names it retained.
//
// This is the load-bearing safety property of the whole feature.
// spec.memory.enabled=false is a CONFIGURATION change - "this project does not
// use recall" - and must never be a data-destruction request. There is no
// retention annotation, no finalizer and no reclaim-policy override anywhere in
// this operator; memoryBackup.enabled is off by default and its restore path is
// not called automatically by anything. So an unhandled cascade here is
// unrecoverable loss, not an inconvenience.
//
// Two things are done to each retained object:
//
//   - ALL ownerReferences are stripped, not only the Project's. The cnpg PVCs
//     are owned by the cnpg *Cluster*, and the Cluster is exactly what the
//     teardown deletes, so leaving that reference in place would destroy the
//     data no matter what we did about the Project reference.
//   - a RetainedForProjectLabel is stamped, because an object with no
//     ownerReferences is otherwise unreachable from the Project. It is what the
//     re-enable path re-adopts by, and what a human greps for.
//
// The consequence is deliberate: DELETING a Project whose memory is already
// disabled leaves its retained volumes behind rather than cascading them away.
// That is the safe direction (the data is still there, and the retention label
// finds it), and the alternative - re-attaching a controller reference just so
// a later delete can destroy it - reintroduces exactly the cascade this exists
// to prevent. Deleting an ENABLED Project cascades normally: reconcileMemory
// returns on DeletionTimestamp before ever reaching this path.
//
// The neo4j PVC is found by NAME PREFIX: it comes from the StatefulSet's
// volumeClaimTemplate, whose metadata carries no labels and cannot be given any
// (volumeClaimTemplates are immutable on a live StatefulSet, so adding labels
// there would break every existing deployment's apply).
func (r *ProjectReconciler) retainMemoryData(ctx context.Context, p *tataradevv1alpha1.Project) ([]string, error) {
	ns := r.MemoryConfig.Namespace
	n := memory.NamesFor(p.Name)
	neo4jPVCPrefix := "data-" + n.Neo4j + "-"

	var retained []string

	var pvcs corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcs, client.InNamespace(ns)); err != nil {
		return nil, fmt.Errorf("list pvcs for retention: %w", err)
	}
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		if pvc.Labels[memory.ProjectLabel] != p.Name &&
			pvc.Labels[cnpgClusterLabel] != n.PGCluster &&
			!strings.HasPrefix(pvc.Name, neo4jPVCPrefix) {
			continue
		}
		if err := r.retainObject(ctx, p, pvc, &corev1.PersistentVolumeClaim{}); err != nil {
			return nil, err
		}
		retained = append(retained, "pvc/"+pvc.Name)
	}

	// cnpg generates the app/TLS Secrets and owns them via the Cluster, so they
	// cascade with it. Retaining them keeps the app role's credentials matching
	// the retained PGDATA on re-enable instead of relying on cnpg re-deriving
	// them. Best-effort by label: a cnpg build that does not label its generated
	// Secrets simply matches nothing here.
	var secrets corev1.SecretList
	if err := r.List(ctx, &secrets, client.InNamespace(ns),
		client.MatchingLabels{cnpgClusterLabel: n.PGCluster}); err != nil {
		return nil, fmt.Errorf("list cnpg secrets for retention: %w", err)
	}
	for i := range secrets.Items {
		sec := &secrets.Items[i]
		if err := r.retainObject(ctx, p, sec, &corev1.Secret{}); err != nil {
			return nil, err
		}
		retained = append(retained, "secret/"+sec.Name)
	}

	return retained, nil
}

// retainObject strips every ownerReference from obj and stamps the retention
// label, under conflict retry (the objects are concurrently written by cnpg and
// the StatefulSet controller right up until their controllers are deleted).
// into is an empty object of the same type used to re-read on conflict.
func (r *ProjectReconciler) retainObject(ctx context.Context, p *tataradevv1alpha1.Project,
	obj client.Object, into client.Object) error {
	key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := r.Get(ctx, key, into); err != nil {
			return err
		}
		if len(into.GetOwnerReferences()) == 0 &&
			into.GetLabels()[memory.RetainedForProjectLabel] == p.Name {
			return nil // already retained
		}
		into.SetOwnerReferences(nil)
		labels := into.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels[memory.RetainedForProjectLabel] = p.Name
		into.SetLabels(labels)
		return r.Update(ctx, into)
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("retain %T %s: %w", obj, obj.GetName(), err)
	}
	return nil
}

// adoptRetainedMemoryPVCs is the inverse of retainMemoryData, run once on the
// Disabled -> enabled edge before the stack is re-applied: the retained objects
// drop the retention label and get a Project ownerReference back, so they are
// re-adopted BY NAME rather than left orphaned beside a freshly provisioned
// duplicate set.
//
// The restored reference is deliberately NOT a controller reference: cnpg sets
// its own controller ownerRef on the PVCs and Secrets it re-adopts, and an
// object may carry only one. A plain (non-controller) reference still makes the
// object garbage-collected with the Project, which is the property that matters.
func (r *ProjectReconciler) adoptRetainedMemoryPVCs(ctx context.Context, p *tataradevv1alpha1.Project) error {
	ns := r.MemoryConfig.Namespace
	sel := client.MatchingLabels{memory.RetainedForProjectLabel: p.Name}

	var pvcs corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcs, client.InNamespace(ns), sel); err != nil {
		return fmt.Errorf("list retained pvcs: %w", err)
	}
	for i := range pvcs.Items {
		if err := r.adoptRetainedObject(ctx, p, &pvcs.Items[i], &corev1.PersistentVolumeClaim{}); err != nil {
			return err
		}
	}

	var secrets corev1.SecretList
	if err := r.List(ctx, &secrets, client.InNamespace(ns), sel); err != nil {
		return fmt.Errorf("list retained secrets: %w", err)
	}
	for i := range secrets.Items {
		if err := r.adoptRetainedObject(ctx, p, &secrets.Items[i], &corev1.Secret{}); err != nil {
			return err
		}
	}

	log.FromContext(ctx).Info("re-adopted retained memory volumes",
		"action", "memory_reenable_adopt",
		"resource_id", p.Name,
		"pvcs", len(pvcs.Items),
		"secrets", len(secrets.Items))
	return nil
}

func (r *ProjectReconciler) adoptRetainedObject(ctx context.Context, p *tataradevv1alpha1.Project,
	obj client.Object, into client.Object) error {
	key := types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := r.Get(ctx, key, into); err != nil {
			return err
		}
		labels := into.GetLabels()
		delete(labels, memory.RetainedForProjectLabel)
		into.SetLabels(labels)
		if !hasProjectOwnerRef(into, p) {
			into.SetOwnerReferences(append(into.GetOwnerReferences(), metav1.OwnerReference{
				APIVersion: tataradevv1alpha1.GroupVersion.String(),
				Kind:       "Project",
				Name:       p.Name,
				UID:        p.UID,
			}))
		}
		return r.Update(ctx, into)
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("adopt retained %T %s: %w", obj, obj.GetName(), err)
	}
	return nil
}

func hasProjectOwnerRef(obj client.Object, p *tataradevv1alpha1.Project) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID == p.UID {
			return true
		}
	}
	return false
}
