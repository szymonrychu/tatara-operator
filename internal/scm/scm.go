package scm

import (
	"context"
	"fmt"
	"net/http"
)

// WebhookEvent is the provider-agnostic parse of an inbound SCM webhook.
type WebhookEvent struct {
	Kind     string // "push" | "issue" | "mr" | "other"
	Repo     string // remote URL
	Branch   string // for push
	Labels   []string
	Title    string
	Body     string
	IssueRef string // owner/repo#123 (github) or group/proj!iid (gitlab)
	URL      string
}

// Client is the per-provider SCM adapter. M2 implements DetectAndVerify;
// OpenChange and Comment are implemented in M5.
type Client interface {
	Provider() string
	DetectAndVerify(h http.Header, payload []byte, secret string) (WebhookEvent, error)
	OpenChange(ctx context.Context, repoURL, token, sourceBranch, targetBranch, title, body string) (url string, err error)
	Comment(ctx context.Context, token, issueRef, body string) error
}

// HTTPError is returned when an SCM REST call responds 4xx/5xx.
type HTTPError struct {
	Status int
	Body   string
	Path   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("scm: %s -> %d: %s", e.Path, e.Status, e.Body)
}

// GitHub implements Client for GitHub.
type GitHub struct {
	apiBase string
}

// GitLab implements Client for GitLab.
type GitLab struct {
	apiBase string
}

func (*GitHub) Provider() string { return "github" }
func (*GitLab) Provider() string { return "gitlab" }

// DetectAndVerify is implemented per provider in github.go and gitlab.go.
// OpenChange and Comment are implemented per provider in github.go and gitlab.go.
