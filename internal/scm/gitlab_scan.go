package scm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type glMR struct {
	IID          int    `json:"iid"`
	SHA          string `json:"sha"`
	SourceBranch string `json:"source_branch"` // head/source branch name
	Description  string `json:"description"`
	Author       struct {
		Username string `json:"username"`
	} `json:"author"`
	Labels    []string  `json:"labels"`
	UpdatedAt time.Time `json:"updated_at"`
}

type glIssueItem struct {
	IID    int    `json:"iid"`
	Title  string `json:"title"`
	Author struct {
		Username string `json:"username"`
	} `json:"author"`
	Labels    []string  `json:"labels"`
	UpdatedAt time.Time `json:"updated_at"`
	State     string    `json:"state"`
	ClosedAt  time.Time `json:"closed_at"`
}

// glProjectID builds the GitLab project identifier from a (owner, repo) pair.
// It accepts both calling conventions: a split pair ("g", "p") joins to "g/p",
// and a full-path-in-owner pair ("g/p", "") returns "g/p" without a trailing
// slash (a trailing slash 404s). Controller callers derive coordinates via
// scm.OwnerRepo (split) for GitHub-shaped two-segment URLs and via
// scm.GitLabProjectPath (full path, empty repo) for subgroup-aware paths; both
// must resolve to the same project id here.
func glProjectID(owner, repo string) string {
	if repo == "" {
		return owner
	}
	return owner + "/" + repo
}

// ListOpenPRs lists opened merge requests for the project path owner/repo. All pages are fetched.
func (c *GitLab) ListOpenPRs(ctx context.Context, owner, repo string) ([]PRRef, error) {
	proj := glProjectID(owner, repo)
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests?state=opened&per_page=100"
	raw, err := glDoPaged[glMR](ctx, c.base(), path, c.token)
	if err != nil {
		return nil, err
	}
	out := make([]PRRef, 0, len(raw))
	for _, m := range raw {
		out = append(out, PRRef{
			Repo: proj, Number: m.IID, Author: m.Author.Username,
			HeadSHA: m.SHA, HeadBranch: m.SourceBranch, Body: m.Description, Labels: m.Labels, UpdatedAt: m.UpdatedAt,
		})
	}
	return out, nil
}

// ListOpenIssues lists opened issues for the project path owner/repo. All pages are fetched.
func (c *GitLab) ListOpenIssues(ctx context.Context, owner, repo string) ([]IssueRef, error) {
	proj := glProjectID(owner, repo)
	path := "/projects/" + url.PathEscape(proj) + "/issues?state=opened&per_page=100"
	raw, err := glDoPaged[glIssueItem](ctx, c.base(), path, c.token)
	if err != nil {
		return nil, err
	}
	out := make([]IssueRef, 0, len(raw))
	for _, i := range raw {
		out = append(out, IssueRef{
			Repo: proj, Number: i.IID, Title: i.Title, Author: i.Author.Username, Labels: i.Labels, UpdatedAt: i.UpdatedAt, IsPR: false, State: "open",
		})
	}
	return out, nil
}

// ListClosedIssues lists recently-closed issues for the project path owner/repo.
func (c *GitLab) ListClosedIssues(ctx context.Context, owner, repo string, since time.Time) ([]IssueRef, error) {
	proj := glProjectID(owner, repo)
	path := "/projects/" + url.PathEscape(proj) + "/issues?state=closed&updated_after=" + url.QueryEscape(since.UTC().Format(time.RFC3339)) + "&per_page=100"
	raw, err := glDoPaged[glIssueItem](ctx, c.base(), path, c.token)
	if err != nil {
		return nil, err
	}
	out := make([]IssueRef, 0, len(raw))
	for _, i := range raw {
		out = append(out, IssueRef{
			Repo: proj, Number: i.IID, Title: i.Title, Author: i.Author.Username, Labels: i.Labels,
			UpdatedAt: i.UpdatedAt, IsPR: false, State: "closed", ClosedAt: i.ClosedAt,
		})
	}
	return out, nil
}

type glCommitItem struct {
	ID         string    `json:"id"`
	Message    string    `json:"message"`
	AuthorName string    `json:"author_name"`
	CreatedAt  time.Time `json:"created_at"`
}

// ListCommits returns recent default-branch commits for the project path owner/repo since the given time.
func (c *GitLab) ListCommits(ctx context.Context, owner, repo string, since time.Time) ([]CommitRef, error) {
	proj := glProjectID(owner, repo)
	path := "/projects/" + url.PathEscape(proj) + "/repository/commits?since=" + url.QueryEscape(since.UTC().Format(time.RFC3339)) + "&per_page=100"
	raw, err := glDoPaged[glCommitItem](ctx, c.base(), path, c.token)
	if err != nil {
		return nil, err
	}
	out := make([]CommitRef, 0, len(raw))
	for _, c := range raw {
		out = append(out, CommitRef{
			SHA:     c.ID,
			Message: c.Message,
			Author:  c.AuthorName,
			Date:    c.CreatedAt,
		})
	}
	return out, nil
}

// ListBoardItems for GitLab is a documented no-op: GitLab issue boards are
// label-driven; the board view is already covered by ListOpenIssues.
// Returns nil to satisfy the SCMReader interface without a second source of truth.
func (c *GitLab) ListBoardItems(_ context.Context, _ BoardRef) ([]BoardItem, error) {
	return nil, nil
}

// glNote is the JSON shape of a GitLab issue note.
type glNote struct {
	Author struct {
		Username string `json:"username"`
	} `json:"author"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
	System    bool      `json:"system"`
}

// GetIssue returns the title and body of an issue. The (owner, repo) pair is
// resolved to a project id by glProjectID, accepting both the split and
// full-path calling conventions.
func (c *GitLab) GetIssue(ctx context.Context, owner, repo string, number int) (IssueContent, error) {
	var raw struct {
		Title       string `json:"title"`
		Description string `json:"description"`
	}
	path := "/projects/" + url.PathEscape(glProjectID(owner, repo)) + "/issues/" + strconv.Itoa(number)
	if err := glDo(ctx, c.base(), http.MethodGet, path, c.token, nil, &raw); err != nil {
		return IssueContent{}, err
	}
	return IssueContent{Title: raw.Title, Body: raw.Description}, nil
}

// GetDefaultBranchHeadSHA resolves the default branch HEAD commit sha. owner
// carries the full project path; repo is unused (matches GetCommitCIStatus).
func (c *GitLab) GetDefaultBranchHeadSHA(ctx context.Context, owner, _ /*repo*/ string) (string, error) {
	esc := url.PathEscape(owner)
	var meta struct {
		DefaultBranch string `json:"default_branch"`
	}
	if err := glDo(ctx, c.base(), http.MethodGet, "/projects/"+esc, c.token, nil, &meta); err != nil {
		return "", fmt.Errorf("gitlab: get project meta %s: %w", owner, err)
	}
	if meta.DefaultBranch == "" {
		return "", fmt.Errorf("gitlab: empty default_branch for %s", owner)
	}
	var branch struct {
		Commit struct {
			ID string `json:"id"`
		} `json:"commit"`
	}
	path := "/projects/" + esc + "/repository/branches/" + url.PathEscape(meta.DefaultBranch)
	if err := glDo(ctx, c.base(), http.MethodGet, path, c.token, nil, &branch); err != nil {
		return "", fmt.Errorf("gitlab: get default branch head %s@%s: %w", owner, meta.DefaultBranch, err)
	}
	return branch.Commit.ID, nil
}

// ListIssueComments returns non-system notes on issue number, oldest-first. All
// pages are fetched. The (owner, repo) pair is resolved to a project id by
// glProjectID, accepting both the split and full-path calling conventions.
func (c *GitLab) ListIssueComments(ctx context.Context, owner, repo string, number int) ([]IssueComment, error) {
	path := "/projects/" + url.PathEscape(glProjectID(owner, repo)) + "/issues/" + strconv.Itoa(number) + "/notes?per_page=100"
	raw, err := glDoPaged[glNote](ctx, c.base(), path, c.token)
	if err != nil {
		return nil, err
	}
	out := make([]IssueComment, 0, len(raw))
	for _, n := range raw {
		if n.System {
			continue
		}
		out = append(out, IssueComment{Author: n.Author.Username, Body: n.Body, CreatedAt: n.CreatedAt})
	}
	// Defensive sort: GitLab returns notes newest-first by default; sort guards
	// ordering within the fetched set regardless of server-side default.
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// CloseIssue posts a note then PUTs the issue state_event=close. Unlike the
// reader methods, repo here is the already-joined full project path (callers
// pass a complete slug), so it is used directly as the project id.
func (c *GitLab) CloseIssue(ctx context.Context, token, repo string, number int, comment string) error {
	proj := repo
	if comment != "" {
		npath := "/projects/" + url.PathEscape(proj) + "/issues/" + strconv.Itoa(number) + "/notes"
		if err := glDo(ctx, c.base(), http.MethodPost, npath, token, map[string]string{"body": comment}, nil); err != nil {
			return err
		}
	}
	ipath := "/projects/" + url.PathEscape(proj) + "/issues/" + strconv.Itoa(number)
	return glDo(ctx, c.base(), http.MethodPut, ipath, token, map[string]string{"state_event": "close"}, nil)
}

// EditIssue updates an issue with only the non-nil fields in req (PUT semantics on GitLab).
// A 404 (issue gone) is treated as benign and returns nil.
func (c *GitLab) EditIssue(ctx context.Context, token, repo string, number int, req EditIssueReq) error {
	proj := repo
	body := map[string]any{}
	if req.Title != nil {
		body["title"] = *req.Title
	}
	if req.Body != nil {
		body["body"] = *req.Body
	}
	if req.Labels != nil {
		labels := make([]string, 0, len(*req.Labels))
		labels = append(labels, *req.Labels...)
		body["labels"] = strings.Join(labels, ",")
	}
	if len(body) == 0 {
		return nil
	}
	ipath := "/projects/" + url.PathEscape(proj) + "/issues/" + strconv.Itoa(number)
	err := glDo(ctx, c.base(), http.MethodPut, ipath, token, body, nil)
	var he *HTTPError
	if errors.As(err, &he) && he.Status == http.StatusNotFound {
		return nil
	}
	return err
}
