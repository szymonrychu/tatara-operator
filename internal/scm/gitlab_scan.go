package scm

import (
	"context"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"
)

type glMR struct {
	IID    int    `json:"iid"`
	SHA    string `json:"sha"`
	Author struct {
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
}

// ListOpenPRs lists opened merge requests for the project path owner/repo. All pages are fetched.
func (c *GitLab) ListOpenPRs(ctx context.Context, owner, repo string) ([]PRRef, error) {
	proj := owner + "/" + repo
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests?state=opened&per_page=100"
	raw, err := glDoPaged[glMR](ctx, c.base(), path, c.token)
	if err != nil {
		return nil, err
	}
	out := make([]PRRef, 0, len(raw))
	for _, m := range raw {
		out = append(out, PRRef{
			Repo: proj, Number: m.IID, Author: m.Author.Username,
			HeadSHA: m.SHA, Labels: m.Labels, UpdatedAt: m.UpdatedAt,
		})
	}
	return out, nil
}

// ListOpenIssues lists opened issues for the project path owner/repo. All pages are fetched.
func (c *GitLab) ListOpenIssues(ctx context.Context, owner, repo string) ([]IssueRef, error) {
	proj := owner + "/" + repo
	path := "/projects/" + url.PathEscape(proj) + "/issues?state=opened&per_page=100"
	raw, err := glDoPaged[glIssueItem](ctx, c.base(), path, c.token)
	if err != nil {
		return nil, err
	}
	out := make([]IssueRef, 0, len(raw))
	for _, i := range raw {
		out = append(out, IssueRef{
			Repo: proj, Number: i.IID, Title: i.Title, Author: i.Author.Username, Labels: i.Labels, UpdatedAt: i.UpdatedAt, IsPR: false,
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

// GetIssue returns the title and body of an issue.
// For GitLab owner carries the full project path; repo is unused.
func (c *GitLab) GetIssue(ctx context.Context, owner, _ string, number int) (IssueContent, error) {
	var raw struct {
		Title       string `json:"title"`
		Description string `json:"description"`
	}
	path := "/projects/" + url.PathEscape(owner) + "/issues/" + strconv.Itoa(number)
	if err := glDo(ctx, c.base(), http.MethodGet, path, c.token, nil, &raw); err != nil {
		return IssueContent{}, err
	}
	return IssueContent{Title: raw.Title, Body: raw.Description}, nil
}

// ListIssueComments returns non-system notes on issue number, oldest-first. All pages are fetched.
// For GitLab owner carries the full project path (group/sub/project); repo is unused.
func (c *GitLab) ListIssueComments(ctx context.Context, owner, _ string, number int) ([]IssueComment, error) {
	path := "/projects/" + url.PathEscape(owner) + "/issues/" + strconv.Itoa(number) + "/notes?per_page=100"
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

// CloseIssue posts a note then PUTs the issue state_event=close.
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
