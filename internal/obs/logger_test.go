package obs_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/obs"
)

func TestNewLogger_EmitsJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo)
	logger.Info("hello", slog.String("action", "test"))

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("log line is not valid JSON: %v (%q)", err, buf.String())
	}
	if entry["msg"] != "hello" {
		t.Fatalf("msg = %v, want hello", entry["msg"])
	}
	if entry["action"] != "test" {
		t.Fatalf("action = %v, want test", entry["action"])
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		in   string
		want slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"", slog.LevelInfo},
		{"bogus", slog.LevelInfo},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if got := obs.ParseLevel(tt.in); got != tt.want {
				t.Fatalf("ParseLevel(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
