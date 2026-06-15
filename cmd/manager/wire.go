package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/auth"
	"github.com/szymonrychu/tatara-operator/internal/config"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/ingest"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/pushmetrics"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/webhook"
	ctrl "sigs.k8s.io/controller-runtime"
)

// memoryConfigFromConfig maps operator config to the per-project memory stack
// builder config. The audience is always the memory-service audience
// (tatara-memory), not the operator's own API audience.
func memoryConfigFromConfig(cfg config.Config) memory.Config {
	return memory.Config{
		Namespace:        cfg.Namespace,
		MemoryImage:      cfg.MemoryImage,
		LightragImage:    cfg.LightragImage,
		Neo4jImage:       cfg.Neo4jImage,
		OpenAISecretName: cfg.OpenAISecretName,
		OIDCIssuer:       cfg.OIDCIssuer,
		OIDCAudience:     "tatara-memory",
		ImagePullSecret:  cfg.ImagePullSecret,
		IngressHost:      cfg.IngressHost,
		IngressClassName: cfg.IngressClassName,
		MemoryPathPrefix: cfg.MemoryPathPrefix,
		ChatPathPrefix:   cfg.ChatPathPrefix,
		ChatImage:        cfg.ChatImage,
	}
}

// ingestConfigFromConfig maps the operator config to the ingest Job builder
// config. memoryAudience is the OIDC audience the ingester presents to
// tatara-memory.
func ingestConfigFromConfig(cfg config.Config, memoryAudience string) ingest.Config {
	return ingest.Config{
		IngesterImage:    cfg.IngesterImage,
		OIDCIssuer:       cfg.OIDCIssuer,
		OIDCClientID:     cfg.OperatorOIDCClientID,
		OIDCSecretName:   cfg.OperatorOIDCSecretName,
		OIDCAudience:     memoryAudience,
		Namespace:        cfg.Namespace,
		ImagePullSecret:  cfg.ImagePullSecret,
		OpenAISecretName: cfg.OpenAISecretName,
		SemanticModel:    cfg.SemanticModel,
	}
}

// newWebhookMux returns a chi.Mux pre-wired with the observability middleware
// stack: RequestID (correlation) and Recoverer (panic -> 500 instead of
// closed connection, satisfying hard rules 12 and 13). Routes are mounted by
// the callers.
func newWebhookMux() *chi.Mux {
	mux := chi.NewRouter()
	mux.Use(middleware.RequestID)
	mux.Use(middleware.Recoverer)
	return mux
}

// addWebhookServer builds the shared HTTP listener that serves both the M2
// SCM webhook routes and the M3 OIDC-gated REST API on HTTP_ADDR. Both route
// groups are mounted onto one chi.Mux and wrapped in a single HandlerRunnable.
//
// Webhook routes (/operator/webhooks/...) are unauthenticated - HMAC
// verification happens inside the handler. REST routes are OIDC-gated.
func addWebhookServer(ctx context.Context, mgr ctrl.Manager, cfg config.Config, metrics *obs.OperatorMetrics) error {
	httpMux := newWebhookMux()

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
		SCMFor: func(provider string) (scm.SCMWriter, error) {
			return scm.ByProvider(provider)
		},
		Logger:  slog.Default(),
		Metrics: metrics,
	}).Mount(httpMux, auth.Middleware(verifier))

	return mgr.Add(webhook.NewHandlerRunnable(httpMux, cfg.HTTPAddr))
}

// podConfigFromConfig maps operator config to the wrapper Pod/Service builder
// config.
func podConfigFromConfig(cfg config.Config) agent.PodConfig {
	return agent.PodConfig{
		Namespace:           cfg.Namespace,
		CallbackURL:         cfg.CallbackURL,
		OIDCIssuer:          cfg.OIDCIssuer,
		AnthropicSecretName: cfg.AnthropicSecretName,
		CLIOIDCSecretName:   cfg.CLIOIDCSecretName,
		ImagePullSecret:     cfg.ImagePullSecret,
		OperatorURL:         cfg.OperatorURL,
	}
}

// addReconcilers constructs and registers all reconcilers with mgr, and adds
// the turn-complete callback server as a manager Runnable.
func addReconcilers(mgr ctrl.Manager, cfg config.Config, metrics *obs.OperatorMetrics, lifecycleMetrics *obs.LifecycleMetrics, pushReceiver *pushmetrics.Receiver) error {
	// Fail fast at startup if any wrapper-pod resource quantity is malformed,
	// rather than silently dropping it on every reconcile.
	if err := agent.ValidatePodResourceQuantities(podConfigFromConfig(cfg)); err != nil {
		return fmt.Errorf("invalid wrapper pod resource config: %w", err)
	}
	if err := (&controller.ProjectReconciler{
		Client:              mgr.GetClient(),
		Scheme:              mgr.GetScheme(),
		Metrics:             metrics,
		LifecycleMetrics:    lifecycleMetrics,
		ExternalWebhookBase: cfg.ExternalWebhookBase,
		MemoryConfig:        memoryConfigFromConfig(cfg),
		ReaderFor: func(provider, token string) (scm.SCMReader, error) {
			return scm.ReaderByProvider(provider, token)
		},
		SCMFor: func(provider string) (scm.SCMWriter, error) {
			return scm.ByProvider(provider)
		},
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
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		Metrics:          metrics,
		LifecycleMetrics: lifecycleMetrics,
		Session:          agent.NewHTTPSessionWithMetrics(wrapperTokens.Token, metrics),
		PodConfig:        podConfigFromConfig(cfg),
		SCMFor: func(provider string) (scm.SCMWriter, error) {
			return scm.ByProvider(provider)
		},
		ReaderFor: func(provider, token string) (scm.SCMReader, error) {
			return scm.ReaderByProvider(provider, token)
		},
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup TaskReconciler: %w", err)
	}

	cbServer := &controller.CallbackServer{
		Client:      mgr.GetClient(),
		Metrics:     metrics,
		Session:     agent.NewHTTPSessionWithMetrics(wrapperTokens.Token, metrics),
		Namespace:   cfg.Namespace,
		PushMetrics: pushReceiver.PushHandler(),
	}
	if err := mgr.Add(callbackRunnable{srv: cbServer, addr: cfg.InternalAddr}); err != nil {
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
