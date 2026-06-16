package scm

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

// WebhookEvent is the provider-agnostic parse of an inbound SCM webhook.
type WebhookEvent struct {
	Kind         string // "push" | "issue" | "mr" | "other"
	Repo         string // remote URL
	Branch       string // for push
	Labels       []string
	Title        string
	Body         string // issue/PR/MR description body
	CommentBody  string // the comment text for issue_comment/note (created) events
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

// PRRef is one open PR/MR listed for cron MR-triage.
type PRRef struct {
	Repo      string    `json:"repo"`
	Number    int       `json:"number"`
	Author    string    `json:"author"`
	HeadSHA   string    `json:"headSha"`
	Labels    []string  `json:"labels,omitempty"`
	Body      string    `json:"body,omitempty"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// IssueRef is one open issue listed for cron issue-triage.
type IssueRef struct {
	Repo      string    `json:"repo"`
	Number    int       `json:"number"`
	Title     string    `json:"title,omitempty"`
	Author    string    `json:"author,omitempty"` // issue author login; drives the author-tiered autoapprove gate
	Labels    []string  `json:"labels,omitempty"`
	UpdatedAt time.Time `json:"updatedAt"`
	IsPR      bool      `json:"isPr"` // GitHub /issues returns PRs; filter these out
}

// BoardItem is one project-board item listed for cron issue-triage.
type BoardItem struct {
	Repo      string    `json:"repo"`
	Number    int       `json:"number"` // 0 for draft/non-issue items -> skipped
	Column    string    `json:"column"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// PRState is the inspected state of a PR/MR.
// Author is the SCM login of the PR author. An empty Author means the account
// was deleted or GitHub returned user:null; callers MUST treat Author==""
// as "not the bot" and never allow it to pass an equality gate.
type PRState struct {
	Author     string
	HeadSHA    string
	HeadBranch string
	CIStatus   string // "" none | pending | success | failure
	Merged     bool   // true when the PR/MR is already merged
}

// ErrMergeConflict is returned by Merge when the SCM signals the PR is not
// mergeable or a conflict prevents the merge (GitHub 405/409, GitLab 405/406/409).
// Callers should use errors.Is(err, ErrMergeConflict) and re-triage rather than
// hard-erroring.
var ErrMergeConflict = fmt.Errorf("scm: merge conflict or PR not mergeable")

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
	Merge(ctx context.Context, repoURL, token string, number int, method string) (string, error)
	ClosePR(ctx context.Context, repoURL, token string, number int, body string) error
	AddBoardItem(ctx context.Context, token string, board BoardRef, itemURL string) error
	SetBoardColumn(ctx context.Context, token string, board BoardRef, itemURL, column string) error
	CloseIssue(ctx context.Context, token, repo string, number int, comment string) error
}

// IssueComment is one human comment on an issue, ordered oldest-first.
type IssueComment struct {
	Author    string
	Body      string
	CreatedAt time.Time
}

// IssueContent holds the title and body of an issue fetched from the SCM.
type IssueContent struct {
	Title string
	Body  string
}

// SCMReader lists open work for the cron scan loop; *GitHub and *GitLab satisfy it.
type SCMReader interface {
	ListOpenPRs(ctx context.Context, owner, repo string) ([]PRRef, error)
	ListOpenIssues(ctx context.Context, owner, repo string) ([]IssueRef, error)
	ListBoardItems(ctx context.Context, board BoardRef) ([]BoardItem, error)
	// GetCommitCIStatus returns the CI status for a commit sha.
	// Returns "" (none) | "pending" | "success" | "failure".
	GetCommitCIStatus(ctx context.Context, owner, repo, sha string) (string, error)
	// ListIssueComments returns the human comments on an issue, oldest-first.
	// For GitLab owner carries the full project path; repo is unused.
	ListIssueComments(ctx context.Context, owner, repo string, number int) ([]IssueComment, error)
	// GetIssue returns the title and body of an issue.
	// For GitLab owner carries the full project path; repo is unused.
	GetIssue(ctx context.Context, owner, repo string, number int) (IssueContent, error)
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

const httpErrorBodyLimit = 200

func (e *HTTPError) Error() string {
	body := e.Body
	if len(body) > httpErrorBodyLimit {
		body = body[:httpErrorBodyLimit] + "...[truncated]"
	}
	return fmt.Sprintf("scm: %s -> %d: %s", e.Path, e.Status, body)
}

// GitHub implements Client for GitHub.
type GitHub struct {
	apiBase     string
	graphQLBase string
	token       string // bound for reader calls; empty for writer/webhook use
}

// GitLab implements Client for GitLab.
type GitLab struct {
	apiBase string
	token   string // bound for reader calls; empty for writer/webhook use
}

func (*GitHub) Provider() string { return "github" }
func (*GitLab) Provider() string { return "gitlab" }

// DetectAndVerify is implemented per provider in github.go and gitlab.go.
// OpenChange and Comment are implemented per provider in github.go and gitlab.go.

// reClosesIssue matches "Closes #N" (case-insensitive) in a PR body.
var reClosesIssue = regexp.MustCompile(`(?i)closes\s+#(\d+)`)

// LinkedIssueNumber parses the first "Closes #N" reference from a PR body.
// Returns (n, true) on match, (0, false) otherwise. Shared by the webhook
// binder and the cron mrScan so their dedup keys are consistent.
func LinkedIssueNumber(body string) (int, bool) {
	m := reClosesIssue.FindStringSubmatch(body)
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	return n, true
}
