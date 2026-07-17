package controller

import (
	"strings"
	"testing"

	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// newTestManager builds a real manager against the envtest control plane
// (never Started - SetupWithManager only needs to register, not run) with
// its network-facing servers disabled so it never binds a port in a test
// run.
func newTestManager(t *testing.T) ctrl.Manager {
	t.Helper()
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme.Scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return mgr
}

// callSetup invokes SetupWithManager and tolerates ONLY the
// controller-runtime "controller with name X already exists" error: that
// global name registry is process-wide (not per-manager), so a second test in
// this same binary that also builds a "project" controller collides on
// Complete(), AFTER the self-wire this test cares about has already run (the
// nil-check runs before ctrl.NewControllerManagedBy(...).Complete(r), so the
// field mutation under test is unaffected by that later registration error).
// Any OTHER error still fails the test.
func callSetup(t *testing.T, r *ProjectReconciler, mgr ctrl.Manager) {
	t.Helper()
	if err := r.SetupWithManager(mgr); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("SetupWithManager: %v", err)
	}
}

// Finding 1: a wire.go that forgets ProjectReconciler.APIReader must not
// silently fall back to the cached Client for driveUnparks' ApplyUnpark Get -
// SetupWithManager must self-wire it from the manager, same idiom as
// DispatcherReconciler (queue_controller.go).
func TestProjectReconciler_SetupWithManager_SelfWiresAPIReader(t *testing.T) {
	mgr := newTestManager(t)
	r := &ProjectReconciler{Client: k8sClient, Scheme: scheme.Scheme}
	if r.APIReader != nil {
		t.Fatalf("precondition: APIReader must start nil")
	}
	callSetup(t, r, mgr)
	if r.APIReader == nil {
		t.Fatal("SetupWithManager left APIReader nil; driveUnparks' ApplyUnpark Get silently falls back to the cached Client")
	}
}

// A wire.go that DOES set APIReader must be respected, not clobbered by the
// self-wire (the nil check is a fallback, not an override).
func TestProjectReconciler_SetupWithManager_RespectsPresetAPIReader(t *testing.T) {
	mgr := newTestManager(t)
	preset := k8sClient
	r := &ProjectReconciler{Client: k8sClient, Scheme: scheme.Scheme, APIReader: preset}
	callSetup(t, r, mgr)
	if r.APIReader != preset {
		t.Fatal("SetupWithManager overwrote a pre-set APIReader; it must only fill in a nil one")
	}
}
