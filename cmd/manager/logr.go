package main

import (
	"log/slog"

	"github.com/go-logr/logr"
)

func slogToLogr(l *slog.Logger) logr.Logger {
	return logr.FromSlogHandler(l.Handler())
}
