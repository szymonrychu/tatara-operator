package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// TestSetupLogging_GateReachesControllerRuntimeRootLogger is the #579 wiring
// lock. The ERROR lines the gate exists to re-level are not written by this
// binary: client-go and controller-runtime write them, through the logger
// ctrl.SetLogger installed. Wrapping only slog.Default() would leave them
// untouched, so the assertion is made against ctrl.Log - the delegating root
// every dependency and every reconcile-scoped contextual logger derives from.
//
// ctrl.SetLogger is process-global and first-Fulfill-wins, so this test must be
// the only one in this package that installs a logger.
func TestSetupLogging_GateReachesControllerRuntimeRootLogger(t *testing.T) {
	shutdown, cancel := context.WithCancel(context.Background())
	cancel()
	gate := obs.NewShutdownGate()
	gate.Arm(shutdown)

	var buf bytes.Buffer
	setupLogging(&buf, slog.LevelInfo, gate)

	ctrl.Log.Error(context.Canceled, "Unexpected error when reading response body")

	line := strings.TrimSpace(buf.String())
	if line == "" {
		t.Fatal("ctrl.Log did not reach the logger setupLogging installed")
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("log line is not valid JSON: %v (%q)", err, line)
	}
	if entry["level"] != "INFO" {
		t.Fatalf("level = %v, want INFO: the shutdown gate did not reach controller-runtime's logger", entry["level"])
	}
	if entry["shutdown_downgrade"] != "cancelled_request" {
		t.Fatalf("shutdown_downgrade = %v, want cancelled_request", entry["shutdown_downgrade"])
	}
}
