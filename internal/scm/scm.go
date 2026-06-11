package scm

import (
	"context"
	"fmt"
	"net/http"
)

// WebhookEvent is the provider-agnostic parse of an inbound SCM webhook.
type WebhookEvent struct {
	Kind         string // "push" | "issue" | "mr" | "other"
	Repo         string // remote URL
	Branch       string // for push
	Labels       []string
	Title        string
	Body         string
	IssueRef     string // owner/repo#123 (github) or group/proj!iid (gitlab)
	URL          string
	AuthorLogin  string // login of the issue/PR/MR author (the resource author)
	ActorLogin   string // login of the user who triggered the event (the sender)
	Action       string // opened|labeled|unlabeled|closed|synchronize|submitted|created|other
	Number       int    // issue/PR/MR number (github) or iid (gitlab)
	IsPR         bool   // true for mr/pull_request events
	HeadSHA      string // PR/MR head commit (for CI lookup)
	HeadBranch   string // PR/MR source branch (for selfImprove push target)
	ChangedLabel string // for labeled/unlabeled: the single label added/removed
}

// IssueReq is the payload for creating an issue.
type IssueReq struct {
	Title  string
	Body   string
	Labels []string
}

// CreatedIssue identifies a created issue (internal return type, not a wire type).
type CreatedIssue struct {
	Ref string // owner/repo#n (github) or group/proj#iid (gitlab)
	URL string // html/web url
}

// PRState is the inspected state of a PR/MR.
type PRState struct {
	Author     string
	HeadSHA    string
	HeadBranch string
	Mergeable  bool
	CIStatus   string // "" none | pending | success | failure
}

// Suggestion is one inline code suggestion on a PR/MR.
type Suggestion struct {
	Path string
	Line int
	Body string
}

// BoardRef identifies a project board (GitHub Projects v2 or GitLab issue board).
type BoardRef struct {
	Provider            string
	Owner               string
	GitHubProjectNumber int
	GitLabBoardID       int
	StatusField         string // GH single-select field; default "Status"
}

// SCMWriter is what controller.SCMFor returns; *GitHub and *GitLab satisfy it.
type SCMWriter interface {
	OpenChange(ctx context.Context, repoURL, token, sourceBranch, targetBranch, title, body string) (string, error)
	Comment(ctx context.Context, token, issueRef, body string) error
	CreateIssue(ctx context.Context, repoURL, token string, req IssueReq) (CreatedIssue, error)
	AddLabel(ctx context.Context, token, issueRef, label string) error
	RemoveLabel(ctx context.Context, token, issueRef, label string) error
	GetPRState(ctx context.Context, repoURL, token string, number int) (PRState, error)
	Approve(ctx context.Context, repoURL, token string, number int, body string) error
	RequestChanges(ctx context.Context, repoURL, token string, number int, body string) error
	Suggest(ctx context.Context, repoURL, token string, number int, sugg []Suggestion) error
	Merge(ctx context.Context, repoURL, token string, number int, method string) error
	ClosePR(ctx context.Context, repoURL, token string, number int, body string) error
	AddBoardItem(ctx context.Context, token string, board BoardRef, itemURL string) error
	SetBoardColumn(ctx context.Context, token string, board BoardRef, itemURL, column string) error
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
	apiBase     string
	graphQLBase string
}

// GitLab implements Client for GitLab.
type GitLab struct {
	apiBase string
}

func (*GitHub) Provider() string { return "github" }
func (*GitLab) Provider() string { return "gitlab" }

// DetectAndVerify is implemented per provider in github.go and gitlab.go.
// OpenChange and Comment are implemented per provider in github.go and gitlab.go.
