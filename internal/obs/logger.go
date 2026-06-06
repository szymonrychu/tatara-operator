package obs

import (
	"io"
	"log/slog"
	"strings"
)

// NewLogger returns a JSON-format slog.Logger writing to w at the given level.
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
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
