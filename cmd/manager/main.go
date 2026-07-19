package main

import (
	"context"
	"log/slog"
	"os"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	apiv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/config"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/pushmetrics"
	"github.com/szymonrychu/tatara-operator/internal/version"
)

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilRuntimeMust(clientgoscheme.AddToScheme(s))
	utilRuntimeMust(apiv1alpha1.AddToScheme(s))
	utilRuntimeMust(cnpgv1.AddToScheme(s))
	// ServiceMonitor + PrometheusRule the operator emits per provisioned memory
	// stack (issue #200) are server-side-applied as typed objects, so their GVK
	// must be in the client scheme.
	utilRuntimeMust(monitoringv1.AddToScheme(s))
	return s
}

func utilRuntimeMust(err error) {
	if err != nil {
		panic(err)
	}
}

// managerOptions builds the controller-runtime manager options. Split out from
// buildManager so the leader-election wiring is unit-testable without a live
// API server.
func managerOptions(cfg config.Config, scheme *runtime.Scheme) manager.Options {
	return manager.Options{
		Scheme: scheme,
		// The operator is namespace-scoped (all CRDs + spawned workloads live in
		// cfg.Namespace), and the chart grants a namespaced Role. Scope the cache
		// to that namespace so list/watch stays within the granted RBAC.
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{cfg.Namespace: {}},
		},
		Metrics: metricsserver.Options{
			BindAddress: cfg.MetricsAddr,
		},
		HealthProbeBindAddress: cfg.HealthAddr,
		// Guard against two managers reconciling concurrently during a
		// rolling-update surge (replicaCount: 1, maxSurge rounds up to 1).
		// The lease lives in the same namespace the cache/RBAC are scoped to.
		LeaderElection:          cfg.LeaderElection,
		LeaderElectionID:        "tatara-operator-leader",
		LeaderElectionNamespace: cfg.Namespace,
		// Release the lease on graceful shutdown instead of holding it for the
		// full lease duration. Without this the outgoing leader during a
		// rollout holds its lease through SIGTERM, so the new leader waits out
		// the lease before it can start dispatching - part of the 7m22s
		// alert-admission gap in issue #395.
		LeaderElectionReleaseOnCancel: true,
	}
}

// getConfig is a seam over ctrl.GetConfigOrDie so tests can substitute a
// rest.Config without a live API server or kubeconfig.
var getConfig = ctrl.GetConfigOrDie

// restConfig returns the REST config the manager is built from, raised from
// client-go's default QPS=5/Burst=10 to QPS=50/Burst=100. The default throttles
// the manager's cold-start informer cache fill during a rollout/leader-handoff
// burst, contributing to the 7m22s alert-admission gap in issue #395.
func restConfig() *rest.Config {
	cfg := getConfig()
	cfg.QPS = 50
	cfg.Burst = 100
	return cfg
}

func buildManager(cfg config.Config, scheme *runtime.Scheme) (manager.Manager, error) {
	return ctrl.NewManager(restConfig(), managerOptions(cfg, scheme))
}

func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := obs.NewLogger(os.Stdout, obs.ParseLevel(cfg.LogLevel))
	slog.SetDefault(logger)
	ctrl.SetLogger(slogToLogr(logger))
	mgr, err := buildManager(cfg, newScheme())
	if err != nil {
		return err
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}
	operatorMetrics := obs.NewOperatorMetrics(ctrlmetrics.Registry)
	// Push-receiver for short-lived wrapper pods: aggregates their pushed
	// series and re-exposes them on the operator's own /metrics registry.
	pushReceiver := pushmetrics.New(cfg.PushMetricsTTL, cfg.PushMetricsAllowedPrefixes)
	ctrlmetrics.Registry.MustRegister(pushReceiver)
	seqAlloc, err := addReconcilers(mgr, cfg, operatorMetrics, pushReceiver)
	if err != nil {
		return err
	}
	if err := addWebhookServer(ctx, mgr, cfg, operatorMetrics, seqAlloc); err != nil {
		return err
	}
	logger.Info("starting manager",
		slog.String("action", "manager_start"),
		slog.String("version", version.String()),
		slog.String("metrics_addr", cfg.MetricsAddr),
	)
	return mgr.Start(ctx)
}

func main() {
	bootstrap := obs.NewLogger(os.Stdout, slog.LevelInfo)
	slog.SetDefault(bootstrap)
	ctrl.SetLogger(slogToLogr(bootstrap))
	if err := run(ctrl.SetupSignalHandler()); err != nil {
		bootstrap.Error("manager exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}
