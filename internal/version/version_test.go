package version_test

import (
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/version"
)

func TestString(t *testing.T) {
	tests := []struct {
		name    string
		version string
		commit  string
		date    string
		want    string
	}{
		{name: "defaults", version: "dev", commit: "unknown", date: "unknown", want: "dev (unknown, unknown)"},
		{name: "release", version: "v1.2.3", commit: "abc123", date: "2026-06-06T00:00:00Z", want: "v1.2.3 (abc123, 2026-06-06T00:00:00Z)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version.Version = tt.version
			version.Commit = tt.commit
			version.Date = tt.date
			if got := version.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
