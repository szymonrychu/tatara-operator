package scm

import (
	"context"
	"net/http"
	"net/url"
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
	IID       int       `json:"iid"`
	Title     string    `json:"title"`
	Labels    []string  `json:"labels"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ListOpenPRs lists opened merge requests for the project path owner/repo.
func (c *GitLab) ListOpenPRs(ctx context.Context, owner, repo string) ([]PRRef, error) {
	proj := owner + "/" + repo
	var raw []glMR
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests?state=opened"
	if err := glDo(ctx, c.base(), http.MethodGet, path, c.token, nil, &raw); err != nil {
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

// ListOpenIssues lists opened issues for the project path owner/repo.
func (c *GitLab) ListOpenIssues(ctx context.Context, owner, repo string) ([]IssueRef, error) {
	proj := owner + "/" + repo
	var raw []glIssueItem
	path := "/projects/" + url.PathEscape(proj) + "/issues?state=opened"
	if err := glDo(ctx, c.base(), http.MethodGet, path, c.token, nil, &raw); err != nil {
		return nil, err
	}
	out := make([]IssueRef, 0, len(raw))
	for _, i := range raw {
		out = append(out, IssueRef{
			Repo: proj, Number: i.IID, Title: i.Title, Labels: i.Labels, UpdatedAt: i.UpdatedAt, IsPR: false,
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
