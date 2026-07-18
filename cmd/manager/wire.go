package main

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/accountusage"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/auth"
	"github.com/szymonrychu/tatara-operator/internal/config"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/grafanamcp"
	"github.com/szymonrychu/tatara-operator/internal/ingest"
	"github.com/szymonrychu/tatara-operator/internal/memclient"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/pushmetrics"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/webhook"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// memoryConfigFromConfig maps operator config to the per-project memory stack
// builder config. The audience is always the memory-service audience
// (tatara-memory), not the operator's own API audience.
func memoryConfigFromConfig(cfg config.Config) memory.Config {
	return memory.Config{
		Namespace:            cfg.Namespace,
		MemoryImage:          cfg.MemoryImage,
		LightragImage:        cfg.LightragImage,
		Neo4jImage:           cfg.Neo4jImage,
		OpenAISecretName:     cfg.OpenAISecretName,
		OIDCIssuer:           cfg.OIDCIssuer,
		OIDCAudience:         "tatara-memory",
		ImagePullSecret:      cfg.ImagePullSecret,
		IngressHost:          cfg.IngressHost,
		IngressClassName:     cfg.IngressClassName,
		IngressRewriteTarget: cfg.IngressRewriteTarget,
		MemoryPathPrefix:     cfg.MemoryPathPrefix,
		MonitorEnabled:       cfg.MemoryMonitoringEnabled,
		MonitorLabels:        cfg.MemoryMonitorLabels,
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
		CallbackURL:      cfg.CallbackURL,
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
func addWebhookServer(ctx context.Context, mgr ctrl.Manager, cfg config.Config, metrics *obs.OperatorMetrics, seq *queue.SeqSource) error {
	httpMux := newWebhookMux()

	// M2 webhook routes - unauthenticated, HMAC-verified inside the handler.
	// SpillerFor resolves the A.7 spill client per Project (the E.3 pendingEvents
	// mirror + the identity-unverified re-verify both write through objbudget); an
	// unset SpillerFor left reverifyParked dead (fix W1).
	webhook.NewServer(webhook.Config{
		Client:                          mgr.GetClient(),
		APIReader:                       mgr.GetAPIReader(),
		Namespace:                       cfg.Namespace,
		Metrics:                         metrics,
		Seq:                             seq,
		SpillerFor:                      newSpillerFor(mgr, cfg),
		IncidentDedupVolatileLabels:     cfg.IncidentDedupVolatileLabels,
		IncidentRefireCommentCooldown:   cfg.IncidentRefireCommentCooldown,
		IncidentCorrelationLabels:       cfg.IncidentCorrelationLabels,
		IncidentEscalateRefireThreshold: cfg.IncidentEscalateRefireThreshold,
		IncidentEscalateStaleAge:        cfg.IncidentEscalateStaleAge,
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
		ReaderFor: func(provider, token string) (scm.SCMReader, error) {
			return scm.ReaderByProvider(provider, token)
		},
		Logger:  slog.Default(),
		Metrics: metrics,
		// Approval is the C.6 grammar seam. Before this was wired the field was
		// nil, so verifyApprovalScope FAILED CLOSED on every clarify
		// decision=implement and the platform could never implement anything
		// (fix W1). GrammarVerifier runs the real maintainer-identity + anchored-
		// phrase check against the Issue CR's mirrored comments.
		Approval: &controller.GrammarVerifier{Client: mgr.GetClient()},
	}).Mount(httpMux, auth.Middleware(verifier, metrics))

	return mgr.Add(webhook.NewHandlerRunnable(httpMux, cfg.HTTPAddr))
}

// newSpillerFor builds the per-project A.7 spill-client resolver: the
// tatara-memory client for that Project's status.memory.endpoint. The endpoint
// is per-project, so the Spiller is resolved per write, not captured once. It is
// the SAME construction the mirror reconcilers use (addReconcilers), shared so
// the webhook pendingEvents path spills to the identical per-project endpoint.
func newSpillerFor(mgr ctrl.Manager, cfg config.Config) func(*tataradevv1alpha1.Project) objbudget.Spiller {
	memoryTokens := auth.NewTokenSource(auth.TokenSourceConfig{
		TokenURL:     cfg.OIDCIssuer + "/protocol/openid-connect/token",
		ClientID:     cfg.OperatorOIDCClientID,
		ClientSecret: cfg.OperatorOIDCClientSecret,
		Audience:     "tatara-memory",
	})
	return func(proj *tataradevv1alpha1.Project) objbudget.Spiller {
		endpoint := ""
		if proj.Status.Memory != nil {
			endpoint = proj.Status.Memory.Endpoint
		}
		return memclient.New(endpoint, memoryTokens.Token, nil)
	}
}

// podConfigFromConfig maps operator config to the wrapper Pod/Service builder
// config, including resource/securityContext scalars and the cluster-specific
// scheduling placement. cfg.Scheduling is parsed once by config.Load and
// stored on the Config so no re-parse (and no discarded error) is needed here.
func podConfigFromConfig(cfg config.Config) agent.PodConfig {
	return agent.PodConfig{
		Namespace:              cfg.Namespace,
		CallbackURL:            cfg.CallbackURL,
		OIDCIssuer:             cfg.OIDCIssuer,
		AnthropicSecretName:    cfg.AnthropicSecretName,
		CLIOIDCSecretName:      cfg.CLIOIDCSecretName,
		ImagePullSecret:        cfg.ImagePullSecret,
		OperatorURL:            cfg.OperatorURL,
		CPURequest:             cfg.AgentCPURequest,
		CPULimit:               cfg.AgentCPULimit,
		MemoryRequest:          cfg.AgentMemoryRequest,
		MemoryLimit:            cfg.AgentMemoryLimit,
		RunAsNonRoot:           cfg.AgentRunAsNonRoot,
		RunAsUser:              cfg.AgentRunAsUser,
		FSGroup:                cfg.AgentFSGroup,
		NodeSelector:           cfg.Scheduling.NodeSelector,
		Tolerations:            cfg.Scheduling.Tolerations,
		Affinity:               cfg.Scheduling.Affinity,
		CallbackHMACSecretName: cfg.CallbackHMACSecretName,
		SerenaURL:              cfg.SerenaURL,
	}
}

// secretTokenSource returns a func() (string, error) that reads the named data
// key from the named Secret on every call via reader (the manager's uncached
// API reader, so a rotated OAuth token is picked up without a cache watch).
// Used by the account-usage client, which needs a fresh Anthropic OAuth token
// on each poll rather than one captured at startup.
func secretTokenSource(reader client.Reader, namespace, secretName, key string) func() (string, error) {
	return func() (string, error) {
		var sec corev1.Secret
		if err := reader.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: secretName}, &sec); err != nil {
			return "", fmt.Errorf("accountusage: get secret %s/%s: %w", namespace, secretName, err)
		}
		v, ok := sec.Data[key]
		if !ok {
			return "", fmt.Errorf("accountusage: secret %s/%s missing key %q", namespace, secretName, key)
		}
		return string(v), nil
	}
}

// usageBundleAccessors returns Load/Save closures over an OAuth credential
// Secret for the self-refreshing usage-poller token source. Load reads via the
// uncached API reader (fresh resourceVersion); Save re-reads then Updates so the
// rotated refresh token is persisted under optimistic concurrency (single leader
// polls, so contention is nil, but the re-read keeps the version current). Keys:
// oauth-access-token, oauth-refresh-token, oauth-expires-at (unix seconds).
func usageBundleAccessors(reader client.Reader, writer client.Client, namespace, name string) (
	func(context.Context) (accountusage.SecretBundle, error),
	func(context.Context, accountusage.SecretBundle) error,
) {
	load := func(ctx context.Context) (accountusage.SecretBundle, error) {
		var sec corev1.Secret
		if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &sec); err != nil {
			return accountusage.SecretBundle{}, fmt.Errorf("accountusage: get usage secret %s/%s: %w", namespace, name, err)
		}
		b := accountusage.SecretBundle{
			AccessToken:  strings.TrimSpace(string(sec.Data["oauth-access-token"])),
			RefreshToken: strings.TrimSpace(string(sec.Data["oauth-refresh-token"])),
		}
		if v := strings.TrimSpace(string(sec.Data["oauth-expires-at"])); v != "" {
			if unix, err := strconv.ParseInt(v, 10, 64); err == nil {
				b.ExpiresAt = time.Unix(unix, 0)
			}
		}
		return b, nil
	}
	save := func(ctx context.Context, b accountusage.SecretBundle) error {
		var sec corev1.Secret
		if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &sec); err != nil {
			return fmt.Errorf("accountusage: get usage secret %s/%s: %w", namespace, name, err)
		}
		if sec.Data == nil {
			sec.Data = map[string][]byte{}
		}
		sec.Data["oauth-access-token"] = []byte(b.AccessToken)
		sec.Data["oauth-refresh-token"] = []byte(b.RefreshToken)
		sec.Data["oauth-expires-at"] = []byte(strconv.FormatInt(b.ExpiresAt.Unix(), 10))
		return writer.Update(ctx, &sec)
	}
	return load, save
}

// addReconcilers constructs and registers all reconcilers with mgr, and adds
// the turn-complete callback server as a manager Runnable. It returns the
// shared SeqSource so callers can pass it to addWebhookServer.
func addReconcilers(mgr ctrl.Manager, cfg config.Config, metrics *obs.OperatorMetrics, pushReceiver *pushmetrics.Receiver) (*queue.SeqSource, error) {
	// Fail fast at startup if any wrapper-pod resource quantity is malformed,
	// rather than silently dropping it on every reconcile.
	if err := agent.ValidatePodResourceQuantities(podConfigFromConfig(cfg)); err != nil {
		return nil, fmt.Errorf("invalid wrapper pod resource config: %w", err)
	}

	// Durable per-project seq source: webhook and cron producers all share this
	// stateless allocator. Each project's counter lives in its own ConfigMap
	// (queue-seq-<project>) updated via CAS, so any replica allocates safely with
	// no leader dependency and no in-memory state.
	seq := &queue.SeqSource{Client: mgr.GetClient(), Namespace: cfg.Namespace}

	// Memory-audience token source for the operator's authenticated retrieval
	// probe (updateMemoryRetrievalProbe). Mints via the same shared OIDC client as
	// the wrapper token below, for the "tatara-memory" audience; the source caches
	// across the per-cycle probes.
	memoryTokens := auth.NewTokenSource(auth.TokenSourceConfig{
		TokenURL:     cfg.OIDCIssuer + "/protocol/openid-connect/token",
		ClientID:     cfg.OperatorOIDCClientID,
		ClientSecret: cfg.OperatorOIDCClientSecret,
		Audience:     "tatara-memory",
	})

	if err := (&controller.ProjectReconciler{
		Client:              mgr.GetClient(),
		APIReader:           mgr.GetAPIReader(),
		Scheme:              mgr.GetScheme(),
		Metrics:             metrics,
		ExternalWebhookBase: cfg.ExternalWebhookBase,
		MemoryConfig:        memoryConfigFromConfig(cfg),
		MemoryToken:         memoryTokens.Token,
		OperatorURL:         cfg.OperatorURL,
		GrafanaConfig: grafanamcp.Config{
			Namespace:       cfg.Namespace,
			Image:           cfg.GrafanaMCPImage,
			ImagePullSecret: cfg.ImagePullSecret,
		},
		ReaderFor: func(provider, token string) (scm.SCMReader, error) {
			return scm.ReaderByProvider(provider, token)
		},
		SCMFor: func(provider string) (scm.SCMWriter, error) {
			return scm.ByProvider(provider)
		},
		Seq: seq,
	}).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("setup ProjectReconciler: %w", err)
	}

	// Fleet-wide Claude account usage poller (claudeSubscription mode). The
	// store is nil-safe on the reconciler side, but is always built here so the
	// gate has live data as soon as the poller (a leader-elected Runnable)
	// completes its first poll. The mirror ConfigMap restores the last-known
	// snapshot across a restart/re-election; it is marked unhealthy until the
	// first live poll replaces it, so a stale restore never masks a real outage.
	usageStore := &accountusage.Store{}
	// The poller is gated: off by default so deploying the operator does NOT start
	// polling the undocumented /api/oauth/usage with a possibly-wrong auth header
	// (the Task 0 spike confirms bearer vs x-api-key). When off, usageStore stays
	// empty and the claudeSubscription gate reads 0% = fail-open (nothing held).
	if cfg.UsageEnabled {
		metrics.SetAccountUsagePollerEnabled(true)
		usageMirror := &accountusage.Mirror{Client: mgr.GetClient(), Namespace: cfg.Namespace, Name: "tatara-account-usage"}
		if snap, err := usageMirror.Load(context.Background()); err != nil {
			// Load already swallows a missing ConfigMap to a nil error, so a non-nil err
			// is a real read/decode failure that must not be dropped silently (rule 12).
			slog.Warn("accountusage mirror load failed", "action", "usage_mirror_load", "error", err)
		} else {
			snap.Healthy = false
			usageStore.Set(snap)
		}
		// Token source: when a UsageSecretName is set, use the self-refreshing OAuth
		// source (the real deploy path - the shared fleet setup-token lacks the
		// user:profile scope /api/oauth/usage needs, so the poller runs on an
		// interactive-login token that expires ~hourly and must be refreshed +
		// persisted). Otherwise fall back to the static oauth-token key.
		var usageTokenSource func() (string, error)
		if cfg.UsageSecretName != "" {
			load, save := usageBundleAccessors(mgr.GetAPIReader(), mgr.GetClient(), cfg.Namespace, cfg.UsageSecretName)
			usageTokenSource = accountusage.NewRefreshingTokenSource(accountusage.RefreshingTokenSourceConfig{
				Load:    load,
				Save:    save,
				Refresh: accountusage.OAuthRefreshConfig{TokenURL: cfg.UsageTokenURL, ClientID: cfg.UsageOAuthClientID, UserAgent: cfg.UsageUserAgent},
				Margin:  cfg.UsageRefreshMargin,
			})
		} else {
			usageTokenSource = secretTokenSource(mgr.GetAPIReader(), cfg.Namespace, cfg.AnthropicSecretName, "oauth-token")
		}
		usageClient := accountusage.NewClient(accountusage.ClientConfig{
			BaseURL:     cfg.UsageBaseURL,
			TokenSource: usageTokenSource,
			UserAgent:   cfg.UsageUserAgent,
			AuthMode:    cfg.UsageAuthMode,
		})
		usagePoller := &accountusage.Poller{
			Fetcher:          usageClient,
			Store:            usageStore,
			Metrics:          metrics,
			Interval:         cfg.UsagePollInterval,
			FailureThreshold: 3,
			Now:              time.Now,
		}
		// onUpdate mirrors the snapshot to the ConfigMap only; all account-usage
		// metrics (utilization, reset, overage, poll-health, failures) are produced by
		// the poller itself so failures and staleness stay observable (F3).
		usagePoller.SetOnUpdate(func(s accountusage.Snapshot) {
			if err := usageMirror.Save(context.Background(), s); err != nil {
				slog.Warn("accountusage mirror save failed", "action", "usage_mirror_save", "error", err)
			}
		})
		if err := mgr.Add(usagePoller); err != nil {
			return nil, fmt.Errorf("add usage poller: %w", err)
		}
	} else {
		metrics.SetAccountUsagePollerEnabled(false)
		slog.Info("account-usage poller disabled (USAGE_ENABLED=false); claudeSubscription gate reads an empty store, fail-open",
			"action", "usage_poller_disabled")
	}

	if err := (&controller.DispatcherReconciler{
		Client:            mgr.GetClient(),
		Scheme:            mgr.GetScheme(),
		Metrics:           metrics,
		BudgetDefaults:    cfg.BudgetDefaults(),
		Usage:             usageStore,
		UsagePollInterval: cfg.UsagePollInterval,
	}).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("setup DispatcherReconciler: %w", err)
	}
	if err := (&controller.RepositoryReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		Metrics:      metrics,
		IngestConfig: ingestConfigFromConfig(cfg, "tatara-memory"),
	}).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("setup RepositoryReconciler: %w", err)
	}

	wrapperTokens := auth.NewTokenSource(auth.TokenSourceConfig{
		TokenURL:     cfg.OIDCIssuer + "/protocol/openid-connect/token",
		ClientID:     cfg.OperatorOIDCClientID,
		ClientSecret: cfg.OperatorOIDCClientSecret,
		Audience:     "tatara-claude-code-wrapper",
	})
	spillerFor := func(proj *tataradevv1alpha1.Project) objbudget.Spiller {
		endpoint := ""
		if proj.Status.Memory != nil {
			endpoint = proj.Status.Memory.Endpoint
		}
		return memclient.New(endpoint, memoryTokens.Token, nil)
	}

	if err := (&controller.TaskReconciler{
		Client:        mgr.GetClient(),
		APIReader:     mgr.GetAPIReader(),
		Scheme:        mgr.GetScheme(),
		Metrics:       metrics,
		SpillerFor:    spillerFor,
		Seq:           seq,
		BundleMetrics: obs.NewBundleMetrics(ctrlmetrics.Registry),
		Session:       agent.NewHTTPSessionWithMetrics(wrapperTokens.Token, metrics),
		PodConfig:     podConfigFromConfig(cfg),
		SCMFor: func(provider string) (scm.SCMWriter, error) {
			return scm.ByProvider(provider)
		},
		ReaderFor: func(provider, token string) (scm.SCMReader, error) {
			return scm.ReaderByProvider(provider, token)
		},
	}).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("setup TaskReconciler: %w", err)
	}

	// The MIRROR (contract B.4/C.1). Both reconcilers write through the A.7
	// byte-budget guard, whose Spiller is the per-project tatara-memory client
	// built from that Project's status.memory.endpoint. The endpoint is
	// per-project, so the Spiller is resolved per write, not captured once. A
	// project whose memory stack is not up yet has an empty endpoint: memclient
	// then fails the spill, and objbudget ABORTS the write rather than dropping
	// comments (SPILL FIRST, DROP ONLY ON SPILL SUCCESS).
	// THE OPERATOR EGRESS (contract C.4, C.5). One driver, shared by the three
	// reconcilers that own a piece of it: the mirror reconcilers drain the durable
	// review/comment intents the REST + MCP layers persist, and the stage
	// reconciler drives the two pod-less operator stages (merging, deploying). It
	// is the SOLE merge caller and the SOLE review poster.
	stageDriver := &controller.StageDriver{
		Client:  mgr.GetClient(),
		Metrics: metrics,
		SCMFor: func(provider string) (scm.SCMWriter, error) {
			return scm.ByProvider(provider)
		},
		ReaderFor: func(provider, token string) (scm.SCMReader, error) {
			return scm.ReaderByProvider(provider, token)
		},
		SpillerFor: spillerFor,
		Now:        time.Now,
	}
	if err := (&controller.IssueReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		SCMFor: func(provider string) (scm.SCMWriter, error) {
			return scm.ByProvider(provider)
		},
		ReaderFor: func(provider, token string) (scm.SCMReader, error) {
			return scm.ReaderByProvider(provider, token)
		},
		SpillerFor: spillerFor,
		Driver:     stageDriver,
	}).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("setup IssueReconciler: %w", err)
	}
	if err := (&controller.MergeRequestReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		ReaderFor: func(provider, token string) (scm.SCMReader, error) {
			return scm.ReaderByProvider(provider, token)
		},
		SpillerFor: spillerFor,
		Driver:     stageDriver,
	}).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("setup MergeRequestReconciler: %w", err)
	}
	if err := (&controller.StageReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Driver: stageDriver,
	}).SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("setup StageReconciler: %w", err)
	}

	cbServer := &controller.CallbackServer{
		Client:           mgr.GetClient(),
		Metrics:          metrics,
		Session:          agent.NewHTTPSessionWithMetrics(wrapperTokens.Token, metrics),
		Namespace:        cfg.Namespace,
		PushMetrics:      pushReceiver.PushHandler(),
		CallbackSecret:   cfg.CallbackHMACSecret,
		IdlePodReapAfter: cfg.IdlePodReapAfter,
		BudgetDefaults:   cfg.BudgetDefaults(),
	}
	if err := mgr.Add(callbackRunnable{srv: cbServer, addr: cfg.InternalAddr}); err != nil {
		return nil, fmt.Errorf("add callback server: %w", err)
	}
	if err := mgr.Add(maintenanceRunnable{srv: cbServer}); err != nil {
		return nil, fmt.Errorf("add maintenance runnable: %w", err)
	}
	return seq, nil
}

type callbackRunnable struct {
	srv  *controller.CallbackServer
	addr string
}

func (c callbackRunnable) Start(ctx context.Context) error {
	return c.srv.Start(ctx, c.addr)
}

// NeedLeaderElection implements manager.LeaderElectionRunnable. The callback
// and push-metrics server is stateless and must start on every replica,
// independent of leadership.
func (c callbackRunnable) NeedLeaderElection() bool { return false }

// maintenanceRunnable runs the CallbackServer's poll-backstop + orphan-reaper
// ticker. Unlike the callback HTTP server (callbackRunnable, every replica),
// this is LEADER-ONLY: NeedLeaderElection true so only the elected leader
// polls for missed turn callbacks and reaps orphan pods. When leader election
// is disabled (single replica), controller-runtime still runs it.
type maintenanceRunnable struct {
	srv *controller.CallbackServer
}

func (m maintenanceRunnable) Start(ctx context.Context) error {
	return m.srv.RunMaintenance(ctx)
}

func (m maintenanceRunnable) NeedLeaderElection() bool { return true }
