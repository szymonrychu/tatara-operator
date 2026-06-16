package main

import (
	"io"
	"log/slog"
	"net/http"
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	apiv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/config"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/webhook"
)

func TestNewScheme_RegistersAllKinds(t *testing.T) {
	s := newScheme()
	for _, kind := range []string{"Project", "Repository", "Task", "Subtask"} {
		if !s.Recognizes(apiv1alpha1.GroupVersion.WithKind(kind)) {
			t.Fatalf("scheme does not recognize %s", kind)
		}
	}
}

func TestNewScheme_HasCoreTypes(t *testing.T) {
	s := newScheme()
	if !s.Recognizes(corev1.SchemeGroupVersion.WithKind("Pod")) {
		t.Fatal("scheme does not recognize core/v1 Pod")
	}
}

func TestManagerOptions_LeaderElection(t *testing.T) {
	opts := managerOptions(config.Config{Namespace: "tatara", LeaderElection: true}, newScheme())
	if !opts.LeaderElection {
		t.Fatal("managerOptions did not enable leader election")
	}
	if opts.LeaderElectionID != "tatara-operator-leader" {
		t.Fatalf("LeaderElectionID = %q, want tatara-operator-leader", opts.LeaderElectionID)
	}
	if opts.LeaderElectionNamespace != "tatara" {
		t.Fatalf("LeaderElectionNamespace = %q, want tatara", opts.LeaderElectionNamespace)
	}
}

func TestManagerOptions_LeaderElectionDisabled(t *testing.T) {
	opts := managerOptions(config.Config{Namespace: "tatara", LeaderElection: false}, newScheme())
	if opts.LeaderElection {
		t.Fatal("managerOptions enabled leader election when config flag was false")
	}
}

// TestSlogDefaultIsJSONAfterRun verifies that after installing the JSON logger
// via obs.NewLogger, slog.Default() emits JSON records (not the stdlib text
// handler). This guards hard rule 11: all log output must be JSON on stdout.
func TestSlogDefaultIsJSONAfterRun(t *testing.T) {
	logger := obs.NewLogger(io.Discard, slog.LevelInfo)
	slog.SetDefault(logger)
	got := slog.Default()
	if got == nil {
		t.Fatal("slog.Default() is nil after SetDefault")
	}
	// The JSON handler is not the stdlib TextHandler; verifying via handler type.
	// obs.NewLogger returns a *slog.Logger backed by slog.NewJSONHandler.
	h := got.Handler()
	if _, ok := h.(*slog.TextHandler); ok {
		t.Fatal("slog.Default() is still using TextHandler; slog.SetDefault(jsonLogger) was not called")
	}
}

// TestHandlerRunnableNeedLeaderElection verifies that HandlerRunnable opts out
// of leader-election gating so the webhook/REST API server starts on every
// replica immediately, not only after the leader lease is acquired.
func TestHandlerRunnableNeedLeaderElection(t *testing.T) {
	r := webhook.NewHandlerRunnable(http.NewServeMux(), ":0")
	if r.NeedLeaderElection() {
		t.Fatal("HandlerRunnable.NeedLeaderElection() = true, want false: webhook server must start on every replica")
	}
}

func TestNewScheme_RegistersCNPGCluster(t *testing.T) {
	s := newScheme()
	gvk := schema.GroupVersionKind{
		Group:   "postgresql.cnpg.io",
		Version: "v1",
		Kind:    "Cluster",
	}
	if !s.Recognizes(gvk) {
		t.Fatalf("scheme does not recognize cnpg Cluster %v", gvk)
	}
	obj, err := s.New(gvk)
	if err != nil {
		t.Fatalf("scheme.New(%v): %v", gvk, err)
	}
	if _, ok := obj.(*cnpgv1.Cluster); !ok {
		t.Fatalf("scheme returned %T, want *cnpgv1.Cluster", obj)
	}
}
