package scm

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"
)

type ghPR struct {
	Number int `json:"number"`
	User   struct {
		Login string `json:"login"`
	} `json:"user"`
	Head struct {
		SHA string `json:"sha"`
		Ref string `json:"ref"` // head branch name (source branch)
	} `json:"head"`
	Body      string    `json:"body"`
	Labels    []ghLabel `json:"labels"`
	UpdatedAt time.Time `json:"updated_at"`
}

type ghIssueItem struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
	Body   string `json:"body"`
	User   struct {
		Login string `json:"login"`
	} `json:"user"`
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

// ListOpenPRs lists open pull requests for owner/repo. All pages are fetched.
func (c *GitHub) ListOpenPRs(ctx context.Context, owner, repo string) ([]PRRef, error) {
	path := fmt.Sprintf("/repos/%s/%s/pulls?state=open&per_page=100", owner, repo)
	raw, err := ghDoPaged[ghPR](ctx, c.base(), path, c.token)
	if err != nil {
		return nil, err
	}
	slug := owner + "/" + repo
	out := make([]PRRef, 0, len(raw))
	for _, p := range raw {
		out = append(out, PRRef{
			Repo: slug, Number: p.Number, Author: p.User.Login,
			HeadSHA: p.Head.SHA, HeadBranch: p.Head.Ref, Body: p.Body, Labels: ghLabelNames(p.Labels), UpdatedAt: p.UpdatedAt,
		})
	}
	return out, nil
}

// ListOpenIssues lists open issues for owner/repo. GitHub returns PRs in the
// issues feed; IsPR is set so the caller can filter. All pages are fetched.
func (c *GitHub) ListOpenIssues(ctx context.Context, owner, repo string) ([]IssueRef, error) {
	path := fmt.Sprintf("/repos/%s/%s/issues?state=open&per_page=100", owner, repo)
	raw, err := ghDoPaged[ghIssueItem](ctx, c.base(), path, c.token)
	if err != nil {
		return nil, err
	}
	slug := owner + "/" + repo
	out := make([]IssueRef, 0, len(raw))
	for _, i := range raw {
		out = append(out, IssueRef{
			Repo: slug, Number: i.Number, Title: i.Title, Author: i.User.Login, Labels: ghLabelNames(i.Labels),
			UpdatedAt: i.UpdatedAt, IsPR: i.PullRequest != nil,
		})
	}
	return out, nil
}

// ListBoardItems lists ProjectV2 board items via GraphQL, dual user/org query.
// All pages are fetched by following pageInfo.hasNextPage/endCursor.
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
	type pageInfo struct {
		HasNextPage bool   `json:"hasNextPage"`
		EndCursor   string `json:"endCursor"`
	}
	type projectV2Items struct {
		Items struct {
			PageInfo pageInfo   `json:"pageInfo"`
			Nodes    []itemNode `json:"nodes"`
		} `json:"items"`
	}
	type respShape struct {
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

	const q = `query($owner:String!,$number:Int!,$field:String!,$after:String){
		user(login:$owner){ projectV2(number:$number){ items(first:100,after:$after){ pageInfo{ hasNextPage endCursor } nodes { updatedAt fieldValueByName(name:$field){ ... on ProjectV2ItemFieldSingleSelectValue { name } } content { ... on Issue { number repository { nameWithOwner } } } } } } }
		organization(login:$owner){ projectV2(number:$number){ items(first:100,after:$after){ pageInfo{ hasNextPage endCursor } nodes { updatedAt fieldValueByName(name:$field){ ... on ProjectV2ItemFieldSingleSelectValue { name } } content { ... on Issue { number repository { nameWithOwner } } } } } } }
	}`

	var out []BoardItem
	var cursor any // nil means no after-cursor (first page)
	prevCursor := ""
	for pageNum := 0; pageNum < maxPaginationPages; pageNum++ {
		vars := map[string]any{
			"owner":  board.Owner,
			"number": board.GitHubProjectNumber,
			"field":  field,
			"after":  cursor,
		}
		var resp respShape
		if err := c.ghGraphQL(ctx, c.token, q, vars, &resp); err != nil {
			return nil, err
		}
		// Prefer user nodes if present, else org.
		pv := resp.Organization.ProjectV2
		if len(resp.User.ProjectV2.Items.Nodes) > 0 || resp.User.ProjectV2.Items.PageInfo.HasNextPage {
			pv = resp.User.ProjectV2
		}
		for _, n := range pv.Items.Nodes {
			col := ""
			if n.FieldValueByName != nil {
				col = n.FieldValueByName.Name
			}
			out = append(out, BoardItem{
				Repo: n.Content.Repository.NameWithOwner, Number: n.Content.Number,
				Column: col, UpdatedAt: n.UpdatedAt,
			})
		}
		if !pv.Items.PageInfo.HasNextPage {
			return out, nil
		}
		next := pv.Items.PageInfo.EndCursor
		if next == "" {
			return nil, fmt.Errorf("github: board pagination: hasNextPage=true but endCursor is empty")
		}
		if next == prevCursor {
			return nil, fmt.Errorf("github: board pagination stuck: endCursor %q did not advance", next)
		}
		prevCursor = next
		cursor = next // non-nil string triggers $after on next page
	}
	return nil, fmt.Errorf("github: board pagination exceeded %d pages", maxPaginationPages)
}

// CloseIssue posts a comment then PATCHes the issue state to closed.
func (c *GitHub) CloseIssue(ctx context.Context, token, repo string, number int, comment string) error {
	owner, name, err := ghOwnerRepoFromSlug(repo)
	if err != nil {
		return err
	}
	if comment != "" {
		cpath := fmt.Sprintf("/repos/%s/%s/issues/%d/comments", owner, name, number)
		if err := ghDo(ctx, c.base(), http.MethodPost, cpath, token, map[string]string{"body": comment}, nil); err != nil {
			return fmt.Errorf("github: close issue comment: %w", err)
		}
	}
	ipath := fmt.Sprintf("/repos/%s/%s/issues/%d", owner, name, number)
	return ghDo(ctx, c.base(), http.MethodPatch, ipath, token, map[string]string{"state": "closed"}, nil)
}

// ghIssueComment is the JSON shape of a GitHub issue comment.
type ghIssueComment struct {
	User struct {
		Login string `json:"login"`
	} `json:"user"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

// ListIssueComments returns the comments on issue number, oldest-first. All pages are fetched.
func (c *GitHub) ListIssueComments(ctx context.Context, owner, repo string, number int) ([]IssueComment, error) {
	path := fmt.Sprintf("/repos/%s/%s/issues/%d/comments?per_page=100", owner, repo, number)
	raw, err := ghDoPaged[ghIssueComment](ctx, c.base(), path, c.token)
	if err != nil {
		return nil, err
	}
	out := make([]IssueComment, 0, len(raw))
	for _, c := range raw {
		out = append(out, IssueComment{Author: c.User.Login, Body: c.Body, CreatedAt: c.CreatedAt})
	}
	// Defensive sort: GitHub returns comments oldest-first by default, but sort
	// guards against any ordering surprise within the fetched set.
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// GetIssue returns the title and body of an issue.
func (c *GitHub) GetIssue(ctx context.Context, owner, repo string, number int) (IssueContent, error) {
	var raw ghIssueItem
	path := fmt.Sprintf("/repos/%s/%s/issues/%d", owner, repo, number)
	if err := ghDo(ctx, c.base(), http.MethodGet, path, c.token, nil, &raw); err != nil {
		return IssueContent{}, err
	}
	return IssueContent{Title: raw.Title, Body: raw.Body}, nil
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
