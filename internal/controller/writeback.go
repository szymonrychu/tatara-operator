package controller

import (
	"context"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// Writer is the SCM egress contract the reconciler uses. It is the full
// scm.SCMWriter; SCMFor returns it and tests fake it.
type Writer = scm.SCMWriter

// providerForRemote is a best-effort heuristic used only when
// Task.spec.source.provider is unset. Prefer setting that field explicitly.
func providerForRemote(ctx context.Context, remote string) string {
	lower := strings.ToLower(remote)
	if strings.Contains(lower, "gitlab") {
		return "gitlab"
	}
	if strings.Contains(lower, "github") {
		return "github"
	}
	log.FromContext(ctx).Info("writeback: provider unknown from remote URL, defaulting to github",
		"remote", remote)
	return "github"
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 72 {
		s = s[:72]
	}
	if s == "" {
		return "tatara automated change"
	}
	return s
}
