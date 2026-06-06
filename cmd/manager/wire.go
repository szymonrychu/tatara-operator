package main

import (
	"fmt"

	"github.com/szymonrychu/tatara-operator/internal/config"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/ingest"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	ctrl "sigs.k8s.io/controller-runtime"
)

// ingestConfigFromConfig maps the operator config to the ingest Job builder
// config. memoryAudience is the OIDC audience the ingester presents to
// tatara-memory.
func ingestConfigFromConfig(cfg config.Config, memoryAudience string) ingest.Config {
	return ingest.Config{
		IngesterImage:    cfg.IngesterImage,
		MemoryBaseURL:    cfg.MemoryBaseURL,
		OIDCIssuer:       cfg.OIDCIssuer,
		OIDCClientID:     cfg.OperatorOIDCClientID,
		OIDCClientSecret: cfg.OperatorOIDCClientSecret,
		OIDCAudience:     memoryAudience,
		Namespace:        cfg.Namespace,
	}
}

// addReconcilers constructs and registers the M1 reconcilers with mgr.
func addReconcilers(mgr ctrl.Manager, cfg config.Config, metrics *obs.OperatorMetrics) error {
	if err := (&controller.ProjectReconciler{
		Client:              mgr.GetClient(),
		Scheme:              mgr.GetScheme(),
		Metrics:             metrics,
		ExternalWebhookBase: cfg.ExternalWebhookBase,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup ProjectReconciler: %w", err)
	}
	if err := (&controller.RepositoryReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Metrics:      metrics,
		IngestConfig: ingestConfigFromConfig(cfg, "tatara-memory"),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup RepositoryReconciler: %w", err)
	}
	return nil
}
