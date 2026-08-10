package main

import (
	"context"
	"io"
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
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
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
		//
		// RE-DECIDED under #538 and KEPT. #538 named this the amplifier of the
		// benign-shutdown ERROR burst: with replicaCount 3 and PDB
		// maxUnavailable 1 the freed lease is often won by another OLD-ReplicaSet
		// pod that is itself next in the termination order, which starts a full
		// reconcile sweep and is SIGTERM'd ~12s later (three leaders in 27s on
		// 2026-08-07). Turning it off was considered and REFUSED: it does not
		// fix the ERROR lines (the LAST old leader still dies mid-sweep, and one
		// aborted sweep is already enough to trip the >=2/1h rule), and it
		// reinstates the #395 admission gap on every rollout - trading a real
		// availability regression for a partial noise reduction. Nothing here can
		// choose WHICH pod wins a released lease; that is kube-scheduler and the
		// PDB. The defect was never the handoff, it was that an aborted reconcile
		// was reported as a failure - fixed at the Reconcile boundary instead
		// (controller.classifyReconcileShutdown).
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

// setupLogging installs the process-wide logger and is the ONLY caller of
// ctrl.SetLogger in this binary.
//
// That "only" is load-bearing, not stylistic. controller-runtime's root logger
// is a delegating sink whose promise is nil'd by the first Fulfill
// (pkg/log/deleg.go:79,194), so a second ctrl.SetLogger is a silent no-op. Until
// #579 this binary called it twice - once with an unconfigured bootstrap logger
// from main(), then again here - which meant every dependency-emitted line
// (controller-runtime, klog, client-go through the reconcile-scoped contextual
// logger) went to the BOOTSTRAP logger: LOG_LEVEL never applied to them, and
// wrapping only this logger would have shipped a no-op fix.
func setupLogging(w io.Writer, level slog.Level, gate *obs.ShutdownGate) *slog.Logger {
	logger := obs.NewLogger(w, level, gate)
	slog.SetDefault(logger)
	ctrl.SetLogger(slogToLogr(logger))
	return logger
}

func run(ctx context.Context, gate *obs.ShutdownGate) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := setupLogging(os.Stdout, obs.ParseLevel(cfg.LogLevel), gate)
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
	// The A.7 byte-budget guard's recorder. objbudget defaults to a no-op, and
	// nothing installed a real one, so operator_object_size_bytes,
	// operator_object_too_large_total and operator_comment_spill_total were
	// registered by nobody and exported by nothing - every guarded write was
	// unmeasured. Install it here, before any reconciler can run.
	objbudget.SetMetrics(obs.NewObjBudgetMetrics(ctrlmetrics.Registry))
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
	// NO MIGRATION RUNS HERE, and that is a RULING, not an omission. #521 removed
	// status.stage from the CRD, and CRD structural pruning applies on the READ
	// path: once the narrowed schema is served, no GET returns the legacy field
	// on any object, so a one-shot migrator has nothing to read and cannot work.
	// The accepted consequence is that every pre-#521 Task boots STATELESS and
	// re-derives through the ordinary create edge in TaskReconciler, guarded
	// against resurrecting terminal work by controller.terminalResetTarget.
	// See MEMORY.md 2026-08-08.
	logger.Info("starting manager",
		slog.String("action", "manager_start"),
		slog.String("version", version.String()),
		slog.String("metrics_addr", cfg.MetricsAddr),
	)
	return mgr.Start(ctx)
}

func main() {
	// Built before the signal context exists and armed straight after, so the gate
	// is live for the whole process lifetime including a shutdown that arrives
	// during startup.
	gate := obs.NewShutdownGate()
	// Covers the window before config.Load() succeeds, and the terminal line
	// below. Gated like the configured logger, because mgr.Start can return a
	// cancellation-derived error on a normal drain and that exit line is an ERROR
	// on exactly the shutdown path this change exists to quieten. A grace-period
	// overrun wraps context.DeadlineExceeded, not Canceled, so it keeps its ERROR.
	// It deliberately does NOT call ctrl.SetLogger - see setupLogging.
	bootstrap := obs.NewLogger(os.Stdout, slog.LevelInfo, gate)
	slog.SetDefault(bootstrap)
	ctx := ctrl.SetupSignalHandler()
	gate.Arm(ctx)
	if err := run(ctx, gate); err != nil {
		// slog.Any, not err.Error(): the downgrade predicate needs the live error
		// to run errors.Is against. The rendered JSON is the same string.
		bootstrap.Error("manager exited with error", slog.Any("error", err))
		os.Exit(1)
	}
}
