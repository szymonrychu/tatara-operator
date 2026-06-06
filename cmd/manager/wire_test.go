package main

import (
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/config"
	"github.com/szymonrychu/tatara-operator/internal/ingest"
)

func TestIngestConfigFromConfig(t *testing.T) {
	cfg := config.Config{
		MemoryBaseURL:            "http://mem:8080",
		IngesterImage:            "img:1",
		OIDCIssuer:               "https://kc/realms/t",
		OperatorOIDCClientID:     "tatara-operator",
		OperatorOIDCClientSecret: "secret",
		Namespace:                "tatara",
	}
	got := ingestConfigFromConfig(cfg, "tatara-memory")
	want := ingest.Config{
		IngesterImage:    "img:1",
		MemoryBaseURL:    "http://mem:8080",
		OIDCIssuer:       "https://kc/realms/t",
		OIDCClientID:     "tatara-operator",
		OIDCClientSecret: "secret",
		OIDCAudience:     "tatara-memory",
		Namespace:        "tatara",
	}
	if got != want {
		t.Errorf("ingestConfigFromConfig = %+v, want %+v", got, want)
	}
}
