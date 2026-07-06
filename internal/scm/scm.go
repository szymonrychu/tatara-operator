package scm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	CommentID    int    // provider comment id for idempotency (0 when not a comment event)
	IsComment    bool   // true only for issue_comment/Note-Hook events (not label/state events)
	IssueRef     string // owner/repo#123 (github) or group/proj!iid (gitlab)
	URL          string
	AuthorLogin  string // login of the issue/PR/MR author (the resource author)
	ActorLogin   string // login of the user who triggered the event (the sender)
	Action       string // opened|labeled|unlabeled|closed|synchronize|submitted|created|other
	Number       int    // issue/PR/MR number (github) or iid (gitlab)
	IsPR         bool   // true for mr/pull_request events
	HeadSHA      string // PR/MR head commit (for CI lookup); push after-SHA (documentation agent diff head)
	BaseSHA      string // push before-SHA (documentation agent diff base); empty for non-push events
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
	Repo       string    `json:"repo"`
	Number     int       `json:"number"`
	Author     string    `json:"author"`
	HeadSHA    string    `json:"headSha"`
	HeadBranch string    `json:"headBranch,omitempty"` // source/head branch; set when available from list API
	Labels     []string  `json:"labels,omitempty"`
	Body       string    `json:"body,omitempty"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// IssueRef is one open issue listed for cron issue-triage.
type IssueRef struct {
	Repo      string    `json:"repo"`
	Number    int       `json:"number"`
	Title     string    `json:"title,omitempty"`
	Author    string    `json:"author,omitempty"` // issue author login; drives the author-tiered autoapprove gate
	Labels    []string  `json:"labels,omitempty"`
	CreatedAt time.Time `json:"createdAt,omitempty"` // issue creation time; detects a non-comment edit/reaction (UpdatedAt after CreatedAt)
	UpdatedAt time.Time `json:"updatedAt"`
	IsPR      bool      `json:"isPr"`            // GitHub /issues returns PRs; filter these out
	State     string    `json:"state,omitempty"` // open | closed
	ClosedAt  time.Time `json:"closedAt,omitempty"`
}

// CommitRef is one default-branch commit, for refiner implemented-detection.
type CommitRef struct {
	SHA     string    `json:"sha"`
	Message string    `json:"message"`
	Author  string    `json:"author,omitempty"`
	Date    time.Time `json:"date"`
}

// EditIssueReq is a PATCH: only non-nil fields are sent.
type EditIssueReq struct {
	Title  *string
	Body   *string
	Labels *[]string
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
	Closed     bool   // true when the PR/MR is closed (not merged, state=closed)
}

// ErrMergeConflict is returned by Merge when the SCM signals the PR is not
// mergeable or a conflict prevents the merge (GitHub 405/409, GitLab 405/406/409).
// Callers should use errors.Is(err, ErrMergeConflict) and re-triage rather than
// hard-erroring.
var ErrMergeConflict = fmt.Errorf("scm: merge conflict or PR not mergeable")

// MergeState is the provider-neutral mergeability of a PR/MR, mapped from
// GitHub REST mergeable_state and GitLab merge_status. Callers switch on it
// exhaustively at the merge-gate / conflict-sweep decision point.
type MergeState string

const (
	MergeStateUnknown MergeState = "unknown" // not yet computed (recompute in flight)
	MergeStateClean   MergeState = "clean"   // mergeable, no conflict
	MergeStateDirty   MergeState = "dirty"   // conflict with the base branch
	MergeStateBlocked MergeState = "blocked" // mergeable-blocked (draft, failing required checks)
	MergeStateBehind  MergeState = "behind"  // behind base; needs an update but no conflict
)

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
	// EnableAutoMerge turns on the forge's native auto-merge for the PR/MR at
	// prURL so it merges itself once required checks pass. Best-effort: a forge
	// that disallows auto-merge returns an error callers treat as non-fatal.
	EnableAutoMerge(ctx context.Context, repoURL, token, prURL, method string) error
	ClosePR(ctx context.Context, repoURL, token string, number int, body string) error
	AddBoardItem(ctx context.Context, token string, board BoardRef, itemURL string) error
	SetBoardColumn(ctx context.Context, token string, board BoardRef, itemURL, column string) error
	CloseIssue(ctx context.Context, token, repo string, number int, comment string) error
	// EditIssue updates an issue with only the non-nil fields in req.
	// A 404 (issue gone) is treated as benign and returns nil.
	EditIssue(ctx context.Context, token, repo string, number int, req EditIssueReq) error
	// EnsureLabel ensures a label exists on the repo with the given hex color
	// (6 hex digits, no '#'), creating it or updating its color. Idempotent.
	EnsureLabel(ctx context.Context, repoURL, token, name, color string) error
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
	// GetDefaultBranchHeadSHA resolves the default branch HEAD commit sha.
	// For GitLab owner carries the full project path; repo is unused. Paired
	// with GetCommitCIStatus to report per-repo main-branch CI health.
	GetDefaultBranchHeadSHA(ctx context.Context, owner, repo string) (string, error)
	// ListClosedIssues returns issues closed at/after since (PRs filtered out).
	ListClosedIssues(ctx context.Context, owner, repo string, since time.Time) ([]IssueRef, error)
	// ListCommits returns recent default-branch commits since the given time.
	ListCommits(ctx context.Context, owner, repo string, since time.Time) ([]CommitRef, error)
}

// PRCommentLister is an optional SCMReader capability: list the conversation
// comments/notes on a pull/merge request, oldest-first. It is separate from
// SCMReader because the providers diverge - on GitHub a PR is an issue so its
// conversation comments are issue comments, while GitLab merge requests live in a
// distinct IID namespace with their own /merge_requests/:iid/notes endpoint, so
// reusing ListIssueComments for an MR would read the wrong (or no) notes. The
// scan-time bot-last-word gate (issue #188) uses this for PR/MR candidates and
// falls back to ListIssueComments for readers that do not implement it (the
// GitHub-compatible default). Both concrete readers implement it.
type PRCommentLister interface {
	ListPRComments(ctx context.Context, owner, repo string, number int) ([]IssueComment, error)
}

// WorkflowRun is one CI workflow run (GitHub Actions run / GitLab pipeline) used
// by push-CD deploy-supervision to detect a tatara-helmfile apply outcome.
type WorkflowRun struct {
	HeadSHA    string
	Status     string // queued | in_progress | completed (GitHub) | pending | running | success | failed (GitLab)
	Conclusion string // success | failure | cancelled | ... ("" until completed)
	HTMLURL    string
	CreatedAt  time.Time
}

// DeployWatcher is an optional SCMReader capability used by push-CD
// deploy-supervision to read the terminal tatara-helmfile repo's CD apply
// pipeline and applied pin state. It is separate from SCMReader because only the
// GitHub adapter implements it (the cd-release cascade is GitHub-only; the GitLab
// "infrastructure" project is not part of it), and deploy-supervision degrades
// gracefully when a reader does not satisfy it.
type DeployWatcher interface {
	// LatestWorkflowRun returns the most recent run of the named workflow file
	// (e.g. "apply.yaml") on branch, or ok=false when none exist.
	LatestWorkflowRun(ctx context.Context, owner, repo, workflowFile, branch string) (WorkflowRun, bool, error)
	// GetFileContent returns the decoded UTF-8 content of path at ref (a commit SHA
	// or branch name). A 404 (file absent at ref) returns ("", nil) so callers can
	// probe a set of candidate pin files without erroring on the misses.
	GetFileContent(ctx context.Context, owner, repo, path, ref string) (string, error)
	// LatestSemverTag returns the highest vX.Y.Z (or X.Y.Z) tag on the repo, with
	// ok=false when the repo carries no semver tag yet. Used to learn the version a
	// merged component repo just cut before its pin is applied.
	LatestSemverTag(ctx context.Context, owner, repo string) (string, bool, error)
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

// createOrUpdateOnConflict runs create; if create fails with an *HTTPError
// whose Status equals conflictStatus, it runs update instead and returns its
// result. Any other create error, or any update error, is returned as-is.
// Path/color/body construction stays in each provider's create/update
// closures - do not fold that in here.
func createOrUpdateOnConflict(create func() error, conflictStatus int, update func() error) error {
	err := create()
	if err == nil {
		return nil
	}
	var he *HTTPError
	if errors.As(err, &he) && he.Status == conflictStatus {
		return update()
	}
	return err
}

// ErrorStatus classifies an SCM error into a metrics label: the HTTP status code
// (e.g. "401", "403", "429", "500") when the call reached the server and got a
// non-2xx, or "network" for connect/DNS/timeout failures that never got a reply.
// Returns "" for a nil error.
func ErrorStatus(err error) string {
	if err == nil {
		return ""
	}
	var he *HTTPError
	if errors.As(err, &he) {
		return strconv.Itoa(he.Status)
	}
	return "network"
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

// doPagedGET performs a single GET to fullURL, decodes a non-error JSON body
// into out, and returns the value of peekHeader (the pagination cursor
// header: "Link" for GitHub, "X-Next-Page" for GitLab). headers carries both
// the auth header and the Accept header - callers supply their own values so
// this stays provider-agnostic. errPrefix ("github"/"gitlab") preserves each
// provider's existing wrapped-error text verbatim. errPath is the value
// stored on HTTPError.Path on a 4xx/5xx response; GitHub's call site passes
// the same fullURL it always has, GitLab's passes the relative path it
// always has - this helper does not unify that difference. The pagination
// loop (page-advance, self-reference, and page-count guards) lives in the
// callers and is untouched by this extraction.
func doPagedGET(ctx context.Context, fullURL, errPrefix, errPath string, headers map[string]string, peekHeader string, out any) (peeked string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return "", fmt.Errorf("%s: build request: %w", errPrefix, err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := scmHTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("%s: do request: %w", errPrefix, err)
	}
	defer func() { _ = resp.Body.Close() }()
	peeked = resp.Header.Get(peekHeader)
	if resp.StatusCode >= 400 {
		buf, _ := io.ReadAll(resp.Body)
		return "", &HTTPError{Status: resp.StatusCode, Body: string(buf), Path: errPath}
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil && err != io.EOF {
			return "", fmt.Errorf("%s: decode response: %w", errPrefix, err)
		}
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	return peeked, nil
}

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
