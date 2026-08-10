package obs_test

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

	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// The two production lines this whole file exists for, copied verbatim from the
// dependencies that emit them. Both are ERROR, both are written from INSIDE the
// process during a normal graceful shutdown, and neither is reachable from the
// Reconcile-boundary classifier in internal/controller/labels.go.
const (
	msgBodyRead    = "Unexpected error when reading response body"    // client-go rest/request.go:1196
	msgStopEngaged = "error received after stop sequence was engaged" // controller-runtime pkg/manager/internal.go:533
)

// emit drives a record through the exact adapter cmd/manager/logr.go uses
// (logr.FromSlogHandler over the logger's handler), so the test exercises the
// same path client-go and controller-runtime take, not a direct slog call.
func emit(logger *slog.Logger, f func(logr.Logger)) {
	f(logr.FromSlogHandler(logger.Handler()))
}

func loggerWithGate(t *testing.T, buf *bytes.Buffer, gate *obs.ShutdownGate) *slog.Logger {
	t.Helper()
	return obs.NewLogger(buf, slog.LevelInfo, gate)
}

func engagedGate() *obs.ShutdownGate {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g := obs.NewShutdownGate()
	g.Arm(ctx)
	return g
}

func liveGate() *obs.ShutdownGate {
	g := obs.NewShutdownGate()
	g.Arm(context.Background())
	return g
}

func lastEntry(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 0 || lines[0] == "" {
		t.Fatal("no log line was emitted")
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &entry); err != nil {
		t.Fatalf("log line is not valid JSON: %v (%q)", err, lines[len(lines)-1])
	}
	return entry
}

func TestShutdownDowngrade_ClientGoBodyReadCancellation(t *testing.T) {
	var buf bytes.Buffer
	logger := loggerWithGate(t, &buf, engagedGate())
	emit(logger, func(l logr.Logger) {
		l.Error(context.Canceled, msgBodyRead)
	})

	entry := lastEntry(t, &buf)
	if entry["level"] != "INFO" {
		t.Fatalf("level = %v, want INFO: a shutdown cancellation must not be published at ERROR", entry["level"])
	}
	if entry["msg"] != msgBodyRead {
		t.Fatalf("msg = %v, want %q", entry["msg"], msgBodyRead)
	}
	if entry["shutdown_downgrade"] != "cancelled_request" {
		t.Fatalf("shutdown_downgrade = %v, want cancelled_request", entry["shutdown_downgrade"])
	}
	if entry["original_level"] != "ERROR" {
		t.Fatalf("original_level = %v, want ERROR", entry["original_level"])
	}
}

// Hard rule 12 puts an "action" field on essentially every line this operator
// logs, and slog's JSON handler does not dedupe keys - last wins on the Loki
// side. A marker written to "action" would therefore make the downgraded line
// unfindable by its OWN action, turning a re-levelled line into a lost one.
func TestShutdownDowngrade_DoesNotClobberCallerAttrs(t *testing.T) {
	var buf bytes.Buffer
	logger := loggerWithGate(t, &buf, engagedGate())
	emit(logger, func(l logr.Logger) {
		l.Error(context.Canceled, "reaper: failed to reap orphan wrapper pod",
			"action", "reap_orphan", "resource_id", "pod-1")
	})

	line := strings.TrimSpace(buf.String())
	if strings.Count(line, `"action"`) != 1 {
		t.Fatalf("duplicate action key in %q", line)
	}
	entry := lastEntry(t, &buf)
	if entry["action"] != "reap_orphan" {
		t.Fatalf("action = %v, want reap_orphan: the decorator overwrote the caller's action", entry["action"])
	}
	if entry["resource_id"] != "pod-1" {
		t.Fatalf("resource_id lost: %v", entry)
	}
	if entry["shutdown_downgrade"] != "cancelled_request" {
		t.Fatalf("shutdown_downgrade = %v, want cancelled_request", entry["shutdown_downgrade"])
	}
}

// The production error is the client-go wrap, not a bare context.Canceled.
func TestShutdownDowngrade_WrappedCancellation(t *testing.T) {
	var buf bytes.Buffer
	logger := loggerWithGate(t, &buf, engagedGate())
	wrapped := fmt.Errorf("unexpected error when reading response body. Please retry. Original error: %w", context.Canceled)
	emit(logger, func(l logr.Logger) {
		l.Error(wrapped, msgBodyRead)
	})

	if got := lastEntry(t, &buf)["level"]; got != "INFO" {
		t.Fatalf("level = %v, want INFO for a wrapped context.Canceled", got)
	}
}

func TestShutdownDowngrade_LeaderElectionLost(t *testing.T) {
	var buf bytes.Buffer
	logger := loggerWithGate(t, &buf, engagedGate())
	emit(logger, func(l logr.Logger) {
		l.Error(errors.New("leader election lost"), msgStopEngaged)
	})

	entry := lastEntry(t, &buf)
	if entry["level"] != "INFO" {
		t.Fatalf("level = %v, want INFO: LeaderElectionReleaseOnCancel makes this line certain on every graceful shutdown of the leaseholder", entry["level"])
	}
	if entry["shutdown_downgrade"] != "leader_election_lost" {
		t.Fatalf("shutdown_downgrade = %v, want leader_election_lost", entry["shutdown_downgrade"])
	}
}

// Nothing is downgraded on a live manager: an in-flight request cancelled while
// the process is healthy is a real error, exactly as labels.go:47-51 argues.
func TestShutdownDowngrade_LiveProcessKeepsError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		msg  string
	}{
		{"body_read_cancel", context.Canceled, msgBodyRead},
		{"leader_election_lost", errors.New("leader election lost"), msgStopEngaged},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := loggerWithGate(t, &buf, liveGate())
			emit(logger, func(l logr.Logger) { l.Error(tc.err, tc.msg) })

			entry := lastEntry(t, &buf)
			if entry["level"] != "ERROR" {
				t.Fatalf("level = %v, want ERROR while the process is not shutting down", entry["level"])
			}
			if _, ok := entry["shutdown_downgrade"]; ok {
				t.Fatalf("untouched record carries shutdown_downgrade=%v", entry["shutdown_downgrade"])
			}
		})
	}
}

// The blind spot the issue names: a GENUINE failure that merely lands inside the
// shutdown window must keep its ERROR line, or the rule goes deaf.
func TestShutdownDowngrade_GenuineFailureDuringShutdownKeepsError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		msg  string
	}{
		{"etcd_timeout_under_body_read_msg", errors.New("etcdserver: request timed out"), msgBodyRead},
		{"deadline_exceeded", context.DeadlineExceeded, msgBodyRead},
		{"other_runnable_failure_under_stop_msg", errors.New("webhook server Shutdown: context deadline exceeded"), msgStopEngaged},
		{"cancel_message_without_cancel_error", errors.New("context canceled"), msgStopEngaged},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := loggerWithGate(t, &buf, engagedGate())
			emit(logger, func(l logr.Logger) { l.Error(tc.err, tc.msg) })

			if got := lastEntry(t, &buf)["level"]; got != "ERROR" {
				t.Fatalf("level = %v, want ERROR: a real failure during shutdown must stay countable", got)
			}
		})
	}
}

// The client-go line arrives through the reconcile-scoped contextual logger, so
// it carries controller/reconcileID. Downgrading must not drop them.
func TestShutdownDowngrade_PreservesReconcileScopedValues(t *testing.T) {
	var buf bytes.Buffer
	logger := loggerWithGate(t, &buf, engagedGate())
	emit(logger, func(l logr.Logger) {
		l.WithValues("controller", "queuedevent", "reconcileID", "a140311c").
			Error(context.Canceled, msgBodyRead)
	})

	entry := lastEntry(t, &buf)
	if entry["level"] != "INFO" {
		t.Fatalf("level = %v, want INFO", entry["level"])
	}
	if entry["controller"] != "queuedevent" || entry["reconcileID"] != "a140311c" {
		t.Fatalf("reconcile-scoped attrs lost: %v", entry)
	}
	if entry["err"] == nil {
		t.Fatalf("err attr lost: %v", entry)
	}
}

func TestShutdownDowngrade_NonErrorRecordsUntouched(t *testing.T) {
	var buf bytes.Buffer
	logger := loggerWithGate(t, &buf, engagedGate())
	logger.Info("Stopping and waiting for caches", slog.String("action", "manager_stop"))

	entry := lastEntry(t, &buf)
	if entry["level"] != "INFO" {
		t.Fatalf("level = %v, want INFO", entry["level"])
	}
	if entry["action"] != "manager_stop" {
		t.Fatalf("action = %v, want manager_stop: the decorator overwrote a caller attr", entry["action"])
	}
	if _, ok := entry["shutdown_downgrade"]; ok {
		t.Fatal("a non-ERROR record was marked as downgraded")
	}
}

// A nil gate is the bootstrap/test path: no wrapper, no behaviour change.
func TestNewLogger_NilGateDoesNotDowngrade(t *testing.T) {
	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo, nil)
	emit(logger, func(l logr.Logger) { l.Error(context.Canceled, msgBodyRead) })

	if got := lastEntry(t, &buf)["level"]; got != "ERROR" {
		t.Fatalf("level = %v, want ERROR with no gate installed", got)
	}
}

func TestShutdownGate_Engaged(t *testing.T) {
	if (*obs.ShutdownGate)(nil).Engaged() {
		t.Fatal("a nil gate must never report engaged")
	}
	if obs.NewShutdownGate().Engaged() {
		t.Fatal("an unarmed gate must not report engaged")
	}
	if liveGate().Engaged() {
		t.Fatal("a gate armed with a live context must not report engaged")
	}
	if !engagedGate().Engaged() {
		t.Fatal("a gate armed with a cancelled context must report engaged")
	}
}

// A downgraded record must not slip past a raised LOG_LEVEL: at warn the INFO
// replacement is below threshold and is dropped, not printed.
func TestShutdownDowngrade_RespectsRaisedLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelWarn, engagedGate())
	emit(logger, func(l logr.Logger) { l.Error(context.Canceled, msgBodyRead) })

	if strings.TrimSpace(buf.String()) != "" {
		t.Fatalf("downgraded record emitted below the configured level: %q", buf.String())
	}
}
