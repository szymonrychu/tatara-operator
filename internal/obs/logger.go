package obs

import (
	"io"
	"log/slog"
	"strings"
)

// NewLogger returns a JSON-format slog.Logger writing to w at the given level.
//
// gate may be nil (bootstrap, tests), in which case the handler is the bare
// JSON handler. When non-nil, the handler is wrapped so ERROR records that a
// dependency emits during the process's own shutdown can be re-levelled - see
// shutdown_log.go.
func NewLogger(w io.Writer, level slog.Level, gate *ShutdownGate) *slog.Logger {
	var h slog.Handler = slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	if gate != nil {
		h = &shutdownDowngradeHandler{inner: h, gate: gate}
	}
	return slog.New(h)
}

// ParseLevel maps a config string to an slog.Level, defaulting to Info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
