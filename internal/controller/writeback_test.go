package controller

import (
	"context"
	"testing"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func TestProviderForRemote(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	tests := []struct {
		remote string
		want   string
	}{
		{"https://gitlab.com/org/repo.git", "gitlab"},
		{"https://self-hosted.gitlab.example.com/org/repo.git", "gitlab"},
		{"https://github.com/org/repo.git", "github"},
		{"https://github.example.com/org/repo.git", "github"},
		{"https://internal.example.com/org/repo.git", "github"}, // unknown -> defaults to github
	}
	for _, tc := range tests {
		if got := providerForRemote(ctx, tc.remote); got != tc.want {
			t.Errorf("providerForRemote(%q) = %q, want %q", tc.remote, got, tc.want)
		}
	}
}

func TestFirstLine(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", "tatara automated change"},
		{"one line", "one line"},
		{"first\nsecond", "first"},
	}
	for _, tc := range tests {
		if got := firstLine(tc.in); got != tc.want {
			t.Errorf("firstLine(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
