package scm

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

type ghPR struct {
	Number int `json:"number"`
	User   struct {
		Login string `json:"login"`
	} `json:"user"`
	Head struct {
		SHA string `json:"sha"`
	} `json:"head"`
	Labels    []ghLabel `json:"labels"`
	UpdatedAt time.Time `json:"updated_at"`
}

type ghIssueItem struct {
	Number      int       `json:"number"`
	Labels      []ghLabel `json:"labels"`
	UpdatedAt   time.Time `json:"updated_at"`
	PullRequest *struct {
		URL string `json:"url"`
	} `json:"pull_request"`
}

func ghLabelNames(in []ghLabel) []string {
	out := make([]string, 0, len(in))
	for _, l := range in {
		out = append(out, l.Name)
	}
	return out
}

// ListOpenPRs lists open pull requests for owner/repo.
func (c *GitHub) ListOpenPRs(ctx context.Context, owner, repo string) ([]PRRef, error) {
	var raw []ghPR
	path := fmt.Sprintf("/repos/%s/%s/pulls?state=open", owner, repo)
	if err := ghDo(ctx, c.base(), http.MethodGet, path, c.token, nil, &raw); err != nil {
		return nil, err
	}
	slug := owner + "/" + repo
	out := make([]PRRef, 0, len(raw))
	for _, p := range raw {
		out = append(out, PRRef{
			Repo: slug, Number: p.Number, Author: p.User.Login,
			HeadSHA: p.Head.SHA, Labels: ghLabelNames(p.Labels), UpdatedAt: p.UpdatedAt,
		})
	}
	return out, nil
}

// ListOpenIssues lists open issues for owner/repo. GitHub returns PRs in the
// issues feed; IsPR is set so the caller can filter.
func (c *GitHub) ListOpenIssues(ctx context.Context, owner, repo string) ([]IssueRef, error) {
	var raw []ghIssueItem
	path := fmt.Sprintf("/repos/%s/%s/issues?state=open", owner, repo)
	if err := ghDo(ctx, c.base(), http.MethodGet, path, c.token, nil, &raw); err != nil {
		return nil, err
	}
	slug := owner + "/" + repo
	out := make([]IssueRef, 0, len(raw))
	for _, i := range raw {
		out = append(out, IssueRef{
			Repo: slug, Number: i.Number, Labels: ghLabelNames(i.Labels),
			UpdatedAt: i.UpdatedAt, IsPR: i.PullRequest != nil,
		})
	}
	return out, nil
}

// ListBoardItems lists ProjectV2 board items via GraphQL, dual user/org query.
func (c *GitHub) ListBoardItems(ctx context.Context, board BoardRef) ([]BoardItem, error) {
	type itemNode struct {
		UpdatedAt        time.Time `json:"updatedAt"`
		FieldValueByName *struct {
			Name string `json:"name"`
		} `json:"fieldValueByName"`
		Content struct {
			Number     int `json:"number"`
			Repository struct {
				NameWithOwner string `json:"nameWithOwner"`
			} `json:"repository"`
		} `json:"content"`
	}
	type projectV2Items struct {
		Items struct {
			Nodes []itemNode `json:"nodes"`
		} `json:"items"`
	}
	var resp struct {
		User struct {
			ProjectV2 projectV2Items `json:"projectV2"`
		} `json:"user"`
		Organization struct {
			ProjectV2 projectV2Items `json:"projectV2"`
		} `json:"organization"`
	}
	field := board.StatusField
	if field == "" {
		field = "Status"
	}
	sel := fmt.Sprintf(
		`projectV2(number:%d){ items(first:100){ nodes { updatedAt fieldValueByName(name:%q){ ... on ProjectV2ItemFieldSingleSelectValue { name } } content { ... on Issue { number repository { nameWithOwner } } ... on PullRequest { number repository { nameWithOwner } } } } } }`,
		board.GitHubProjectNumber, field,
	)
	q := fmt.Sprintf(`query { user(login:%q){ %s } organization(login:%q){ %s } }`, board.Owner, sel, board.Owner, sel)
	if err := c.ghGraphQL(ctx, c.token, q, nil, &resp); err != nil {
		return nil, err
	}
	nodes := resp.Organization.ProjectV2.Items.Nodes
	if len(resp.User.ProjectV2.Items.Nodes) > 0 {
		nodes = resp.User.ProjectV2.Items.Nodes
	}
	out := make([]BoardItem, 0, len(nodes))
	for _, n := range nodes {
		col := ""
		if n.FieldValueByName != nil {
			col = n.FieldValueByName.Name
		}
		out = append(out, BoardItem{
			Repo: n.Content.Repository.NameWithOwner, Number: n.Content.Number,
			Column: col, UpdatedAt: n.UpdatedAt,
		})
	}
	return out, nil
}

// CloseIssue posts a comment then PATCHes the issue state to closed.
func (c *GitHub) CloseIssue(ctx context.Context, repo string, number int, comment string) error {
	owner, name, err := ghOwnerRepoFromSlug(repo)
	if err != nil {
		return err
	}
	if comment != "" {
		cpath := fmt.Sprintf("/repos/%s/%s/issues/%d/comments", owner, name, number)
		if err := ghDo(ctx, c.base(), http.MethodPost, cpath, c.token, map[string]string{"body": comment}, nil); err != nil {
			return fmt.Errorf("github: close issue comment: %w", err)
		}
	}
	ipath := fmt.Sprintf("/repos/%s/%s/issues/%d", owner, name, number)
	return ghDo(ctx, c.base(), http.MethodPatch, ipath, c.token, map[string]string{"state": "closed"}, nil)
}

func ghOwnerRepoFromSlug(slug string) (string, string, error) {
	for i := len(slug) - 1; i >= 0; i-- {
		if slug[i] == '/' {
			if i == 0 || i == len(slug)-1 {
				break
			}
			return slug[:i], slug[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("github: malformed repo slug %q", slug)
}
