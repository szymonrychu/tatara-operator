package main

import (
	"context"
	"log/slog"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	apiv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/config"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/version"
)

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilRuntimeMust(clientgoscheme.AddToScheme(s))
	utilRuntimeMust(apiv1alpha1.AddToScheme(s))
	return s
}

func corePodGVK() schema.GroupVersionKind {
	return corev1.SchemeGroupVersion.WithKind("Pod")
}

func utilRuntimeMust(err error) {
	if err != nil {
		panic(err)
	}
}

func buildManager(cfg config.Config, scheme *runtime.Scheme) (manager.Manager, error) {
	return ctrl.NewManager(ctrl.GetConfigOrDie(), manager.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: cfg.MetricsAddr,
		},
		HealthProbeBindAddress: cfg.InternalAddr,
	})
}

func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
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
	// No reconcilers registered in M0; controllers land in M1-M4.
	logger.Info("starting manager",
		slog.String("action", "manager_start"),
		slog.String("version", version.String()),
		slog.String("metrics_addr", cfg.MetricsAddr),
	)
	return mgr.Start(ctx)
}

func main() {
	logger := obs.NewLogger(os.Stdout, slog.LevelInfo)
	ctrl.SetLogger(slogToLogr(logger))
	if err := run(ctrl.SetupSignalHandler(), logger); err != nil {
		logger.Error("manager exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}
