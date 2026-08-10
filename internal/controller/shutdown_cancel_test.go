package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// cancelClient is a client whose every Get fails with err, so a Reconcile is
// aborted at its very first read - exactly where a SIGTERM lands on a reconcile
// that is mid-flight against the API server or an SCM.
func cancelClient(t *testing.T, err error) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(mirrorScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return err
			},
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return err
			},
		}).
		Build()
}

// loggingCancelClient is cancelClient plus the half #557 could not see: before
// returning the cancellation, every read logs it at ERROR through the
// reconcile-scoped contextual logger, exactly as k8s.io/client-go
// rest/request.go:1173-1200 does on a cancelled io.ReadAll(resp.Body).
func loggingCancelClient(t *testing.T, err error) client.Client {
	t.Helper()
	logAndFail := func(ctx context.Context) error {
		logf.FromContext(ctx).Error(context.Canceled, "Unexpected error when reading response body")
		return err
	}
	return fake.NewClientBuilder().
		WithScheme(mirrorScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
				return logAndFail(ctx)
			},
			List: func(ctx context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return logAndFail(ctx)
			},
		}).
		Build()
}

// shutdownReconcilers returns every reconciler the manager registers, keyed by
// its controller name. The root cause of #538 is that NONE of them classified
// context.Canceled, so a rolling restart logged benign shutdown aborts at ERROR
// and re-fired the "Tatara operator error recurring" rule.
func shutdownReconcilers(c client.Client) map[string]reconcile.Reconciler {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	return map[string]reconcile.Reconciler{
		"issue":        &IssueReconciler{Client: c},
		"mergerequest": &MergeRequestReconciler{Client: c},
		"project":      &ProjectReconciler{Client: c, APIReader: c, Metrics: m},
		"repository":   &RepositoryReconciler{Client: c, Metrics: m},
		"task":         &TaskReconciler{Client: c, APIReader: c, Metrics: m},
		"podclocks":    &PodWatchReconciler{Client: c, Metrics: m},
		"taskstage":    &StageReconciler{Client: c, Driver: &StageDriver{Client: c, Metrics: m}},
		"dispatcher":   &DispatcherReconciler{Client: c, Metrics: m},
	}
}

var shutdownReq = ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "anything"}}

// TestReconcile_ShutdownCancellationIsNotAnError is the #538 regression. On
// origin/main every one of these reconcilers returns the raw wrapped
// context.Canceled, controller-runtime logs it as ERROR msg="Reconciler error",
// and two such lines in an hour fire the alert. A cancellation caused by the
// manager shutting down is not a failure: nothing is lost, and the requeue the
// error would buy is never serviced because the manager is stopping.
func TestReconcile_ShutdownCancellationIsNotAnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := cancelClient(t, fmt.Errorf("github: do request: %w", context.Canceled))
	for name, r := range shutdownReconcilers(c) {
		t.Run(name, func(t *testing.T) {
			res, err := r.Reconcile(ctx, shutdownReq)
			if err != nil {
				t.Fatalf("%s: Reconcile returned %v; a manager-shutdown cancellation must not surface as a reconcile error", name, err)
			}
			if res.RequeueAfter != 0 || res.Requeue { //nolint:staticcheck // Requeue is still part of the result contract
				t.Fatalf("%s: Reconcile asked for a requeue (%v) on a shutting-down manager", name, res)
			}
		})
	}
}

// TestReconcile_ShutdownCancellationEmitsZeroErrorLines is #579 action item 3,
// and the assertion #557 did not make: "Reconcile returned nil" is not the same
// claim as "the reconcile published no ERROR". The Loki rule counts ERROR LINES,
// so the guarantee has to be stated in those terms or a dependency writing its
// own ERROR from inside the reconcile re-fires the same alert under a new msg.
func TestReconcile_ShutdownCancellationEmitsZeroErrorLines(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	gate := obs.NewShutdownGate()
	gate.Arm(ctx)
	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo, gate)
	ctx = logf.IntoContext(ctx, logr.FromSlogHandler(logger.Handler()))

	c := loggingCancelClient(t, fmt.Errorf(
		"unexpected error when reading response body. Please retry. Original error: %w", context.Canceled))
	for name, r := range shutdownReconcilers(c) {
		if _, err := r.Reconcile(ctx, shutdownReq); err != nil {
			t.Fatalf("%s: Reconcile returned %v", name, err)
		}
	}

	var downgraded int
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line is not valid JSON: %v (%q)", err, line)
		}
		if entry["level"] == "ERROR" {
			t.Fatalf("a shutdown-cancelled reconcile published an ERROR line: %s", line)
		}
		if entry["shutdown_downgrade"] != nil {
			downgraded++
		}
	}
	if downgraded == 0 {
		t.Fatal("no line was downgraded; the test did not exercise the dependency-emitted ERROR path")
	}
}

// TestReconcile_RealErrorDuringShutdownStillSurfaces is the other half of the
// contract: the classifier must not become a blanket error swallower. A genuine
// failure that happens to land while the manager is stopping is still an error.
func TestReconcile_RealErrorDuringShutdownStillSurfaces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := cancelClient(t, errors.New("etcdserver: request timed out"))
	for name, r := range shutdownReconcilers(c) {
		t.Run(name, func(t *testing.T) {
			if _, err := r.Reconcile(ctx, shutdownReq); err == nil {
				t.Fatalf("%s: a real error during shutdown was swallowed", name)
			}
		})
	}
}

// TestReconcile_CancellationOnALiveManagerStillSurfaces guards the narrowest
// case: a per-call cancellation (a caller-scoped context, a cancelled sub-request)
// while the MANAGER is still running is a real error and must keep its ERROR line.
func TestReconcile_CancellationOnALiveManagerStillSurfaces(t *testing.T) {
	c := cancelClient(t, fmt.Errorf("github: do request: %w", context.Canceled))
	for name, r := range shutdownReconcilers(c) {
		t.Run(name, func(t *testing.T) {
			if _, err := r.Reconcile(context.Background(), shutdownReq); err == nil {
				t.Fatalf("%s: a cancellation with a LIVE manager context was swallowed", name)
			}
		})
	}
}
