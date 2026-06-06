package scm

import (
	"context"
	"errors"
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

var errNotImplemented = errors.New("not implemented: M5")

// GitHub implements Client for GitHub.
type GitHub struct{}

// GitLab implements Client for GitLab.
type GitLab struct{}

func (*GitHub) Provider() string { return "github" }
func (*GitLab) Provider() string { return "gitlab" }

// OpenChange is implemented in M5 (SCM write-back).
func (*GitHub) OpenChange(context.Context, string, string, string, string, string, string) (string, error) {
	return "", errNotImplemented
}

// Comment is implemented in M5 (SCM write-back).
func (*GitHub) Comment(context.Context, string, string, string) error { return errNotImplemented }

// OpenChange is implemented in M5 (SCM write-back).
func (*GitLab) OpenChange(context.Context, string, string, string, string, string, string) (string, error) {
	return "", errNotImplemented
}

// Comment is implemented in M5 (SCM write-back).
func (*GitLab) Comment(context.Context, string, string, string) error { return errNotImplemented }

// DetectAndVerify is implemented per provider in github.go / gitlab.go.
// GitLab placeholder remains until Task 3.
func (*GitLab) DetectAndVerify(http.Header, []byte, string) (WebhookEvent, error) {
	return WebhookEvent{}, errNotImplemented
}
