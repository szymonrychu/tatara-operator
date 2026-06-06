package main

import (
	"context"
	"fmt"

	"github.com/go-chi/chi/v5"
	"github.com/szymonrychu/tatara-operator/internal/agent"
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

// podConfigFromConfig maps operator config to the wrapper Pod/Service builder
// config.
func podConfigFromConfig(cfg config.Config) agent.PodConfig {
	return agent.PodConfig{
		Namespace:           cfg.Namespace,
		InternalAddr:        cfg.InternalAddr,
		AnthropicSecretName: cfg.AnthropicSecretName,
		CLIOIDCSecretName:   cfg.CLIOIDCSecretName,
	}
}

// addReconcilers constructs and registers all reconcilers with mgr, and adds
// the turn-complete callback server as a manager Runnable.
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

	wrapperTokens := auth.NewTokenSource(auth.TokenSourceConfig{
		TokenURL:     cfg.OIDCIssuer + "/protocol/openid-connect/token",
		ClientID:     cfg.OperatorOIDCClientID,
		ClientSecret: cfg.OperatorOIDCClientSecret,
		Audience:     "tatara-claude-code-wrapper",
	})
	if err := (&controller.TaskReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Metrics:   metrics,
		Session:   agent.NewHTTPSession(wrapperTokens.Token),
		PodConfig: podConfigFromConfig(cfg),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup TaskReconciler: %w", err)
	}

	cbServer := &controller.CallbackServer{
		Client:    mgr.GetClient(),
		Metrics:   metrics,
		Session:   agent.NewHTTPSession(wrapperTokens.Token),
		Namespace: cfg.Namespace,
	}
	if err := mgr.Add(callbackRunnable{srv: cbServer, addr: normalizeAddr(cfg.InternalAddr)}); err != nil {
		return fmt.Errorf("add callback server: %w", err)
	}
	return nil
}

type callbackRunnable struct {
	srv  *controller.CallbackServer
	addr string
}

func (c callbackRunnable) Start(ctx context.Context) error {
	return c.srv.Start(ctx, c.addr)
}

// normalizeAddr converts INTERNAL_ADDR (which may be a full URL like
// http://tatara-operator-internal.tatara.svc:9090 used as DEFAULT_CALLBACK_URL
// base, or a raw listen address like :8081) into a net listen address (:port).
// INTERNAL_ADDR is dual-purpose: the full URL drives the wrapper pod env;
// the listen port is what the callback server binds to.
func normalizeAddr(internalAddr string) string {
	// If it starts with a scheme, strip everything before the last colon-group.
	if len(internalAddr) > 0 && internalAddr[0] != ':' {
		// Find the last colon to extract :port.
		last := -1
		for i := 0; i < len(internalAddr); i++ {
			if internalAddr[i] == ':' {
				last = i
			}
		}
		if last >= 0 {
			return internalAddr[last:]
		}
	}
	return internalAddr
}
