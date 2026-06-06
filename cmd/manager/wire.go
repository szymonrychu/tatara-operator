package main

import (
	"context"
	"fmt"

	"github.com/go-chi/chi/v5"
	"github.com/szymonrychu/tatara-operator/internal/auth"
	"github.com/szymonrychu/tatara-operator/internal/config"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/ingest"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
	"github.com/szymonrychu/tatara-operator/internal/webhook"
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

// addWebhookServer builds the shared HTTP listener that serves both the M2
// SCM webhook routes and the M3 OIDC-gated REST API on HTTP_ADDR. Both route
// groups are mounted onto one chi.Mux and wrapped in a single HandlerRunnable.
//
// Webhook routes (/operator/webhooks/...) are unauthenticated - HMAC
// verification happens inside the handler. REST routes are OIDC-gated.
func addWebhookServer(ctx context.Context, mgr ctrl.Manager, cfg config.Config, metrics *obs.OperatorMetrics) error {
	httpMux := chi.NewRouter()

	// M2 webhook routes - unauthenticated, HMAC-verified inside the handler.
	webhook.NewServer(webhook.Config{
		Client:    mgr.GetClient(),
		Namespace: cfg.Namespace,
		Metrics:   metrics,
	}).Mount(httpMux)

	// M3 REST API - OIDC-gated. Discovery failures at startup are fatal so
	// misconfiguration is caught before the manager starts accepting requests.
	verifier, err := auth.NewVerifier(ctx, auth.Config{
		Issuer:   cfg.OIDCIssuer,
		Audience: cfg.OIDCAudience,
	})
	if err != nil {
		return fmt.Errorf("build OIDC verifier: %w", err)
	}
	restapi.NewServer(restapi.Config{
		Client:    mgr.GetClient(),
		Namespace: cfg.Namespace,
	}).Mount(httpMux, auth.Middleware(verifier))

	return mgr.Add(webhook.NewHandlerRunnable(httpMux, cfg.HTTPAddr))
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
