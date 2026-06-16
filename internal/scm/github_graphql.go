package scm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

func (c *GitHub) graphQLEndpoint() string {
	if c.graphQLBase != "" {
		return c.graphQLBase
	}
	return "https://api.github.com/graphql"
}

// ghGraphQL posts a GraphQL query and decodes data into out.
func (c *GitHub) ghGraphQL(ctx context.Context, token, query string, vars map[string]any, out any) error {
	payload := map[string]any{"query": query}
	if vars != nil {
		payload["variables"] = vars
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("github: encode graphql: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.graphQLEndpoint(), bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("github: build graphql request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := scmHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("github: do graphql request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		buf, _ := io.ReadAll(resp.Body)
		return &HTTPError{Status: resp.StatusCode, Body: string(buf), Path: "/graphql"}
	}
	var env struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return fmt.Errorf("github: decode graphql: %w", err)
	}
	if len(env.Errors) > 0 {
		msgs := make([]string, len(env.Errors))
		for i, e := range env.Errors {
			msgs[i] = e.Message
		}
		return fmt.Errorf("github: graphql error: %s", strings.Join(msgs, "; "))
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(env.Data, out); err != nil {
		return fmt.Errorf("github: decode graphql data: %w", err)
	}
	return nil
}

// AddBoardItem resolves the project by org+number, the issue node by URL, and
// adds the item to the Projects v2 board.
func (c *GitHub) AddBoardItem(ctx context.Context, token string, board BoardRef, itemURL string) error {
	projectID, err := c.ghProjectID(ctx, token, board)
	if err != nil {
		return err
	}
	contentID, err := c.ghResourceID(ctx, token, itemURL)
	if err != nil {
		return err
	}
	const q = `mutation($projectId:ID!,$contentId:ID!){ addProjectV2ItemById(input:{projectId:$projectId, contentId:$contentId}) { item { id } } }`
	return c.ghGraphQL(ctx, token, q, map[string]any{
		"projectId": projectID,
		"contentId": contentID,
	}, nil)
}

// SetBoardColumn sets the Status single-select field of the board item for itemURL.
func (c *GitHub) SetBoardColumn(ctx context.Context, token string, board BoardRef, itemURL, column string) error {
	field := board.StatusField
	if field == "" {
		field = "Status"
	}
	type projectV2 struct {
		ID    string `json:"id"`
		Field struct {
			ID      string `json:"id"`
			Options []struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"options"`
		} `json:"field"`
	}
	var proj struct {
		User struct {
			ProjectV2 projectV2 `json:"projectV2"`
		} `json:"user"`
		Organization struct {
			ProjectV2 projectV2 `json:"projectV2"`
		} `json:"organization"`
	}
	// The board owner may be a user or an organization; query both aliased
	// roots and pick whichever resolves non-null.
	const pq = `query($owner:String!,$number:Int!,$field:String!){
		user(login:$owner){ projectV2(number:$number){ id field(name:$field){ ... on ProjectV2SingleSelectField { id options { id name } } } } }
		organization(login:$owner){ projectV2(number:$number){ id field(name:$field){ ... on ProjectV2SingleSelectField { id options { id name } } } } }
	}`
	if err := c.ghGraphQL(ctx, token, pq, map[string]any{
		"owner":  board.Owner,
		"number": board.GitHubProjectNumber,
		"field":  field,
	}, &proj); err != nil {
		return err
	}
	pv := proj.Organization.ProjectV2
	if proj.User.ProjectV2.ID != "" {
		pv = proj.User.ProjectV2
	}
	if pv.ID == "" {
		return fmt.Errorf("github: project %d not found for owner %q", board.GitHubProjectNumber, board.Owner)
	}
	optionID := ""
	for _, o := range pv.Field.Options {
		if o.Name == column {
			optionID = o.ID
			break
		}
	}
	if optionID == "" {
		return fmt.Errorf("github: board column %q not found in field %q", column, field)
	}
	itemID, err := c.ghProjectItemID(ctx, token, itemURL, pv.ID)
	if err != nil {
		return err
	}
	const mq = `mutation($projectId:ID!,$itemId:ID!,$fieldId:ID!,$optionId:String!){ updateProjectV2ItemFieldValue(input:{projectId:$projectId, itemId:$itemId, fieldId:$fieldId, value:{singleSelectOptionId:$optionId}}) { clientMutationId } }`
	return c.ghGraphQL(ctx, token, mq, map[string]any{
		"projectId": pv.ID,
		"itemId":    itemID,
		"fieldId":   pv.Field.ID,
		"optionId":  optionID,
	}, nil)
}

func (c *GitHub) ghProjectID(ctx context.Context, token string, board BoardRef) (string, error) {
	var out struct {
		User struct {
			ProjectV2 struct {
				ID string `json:"id"`
			} `json:"projectV2"`
		} `json:"user"`
		Organization struct {
			ProjectV2 struct {
				ID string `json:"id"`
			} `json:"projectV2"`
		} `json:"organization"`
	}
	// The board owner may be a user or an organization; query both aliased
	// roots and pick whichever resolves non-null.
	const q = `query($owner:String!,$number:Int!){ user(login:$owner){ projectV2(number:$number){ id } } organization(login:$owner){ projectV2(number:$number){ id } } }`
	if err := c.ghGraphQL(ctx, token, q, map[string]any{
		"owner":  board.Owner,
		"number": board.GitHubProjectNumber,
	}, &out); err != nil {
		return "", err
	}
	if out.User.ProjectV2.ID != "" {
		return out.User.ProjectV2.ID, nil
	}
	if out.Organization.ProjectV2.ID != "" {
		return out.Organization.ProjectV2.ID, nil
	}
	return "", fmt.Errorf("github: project %d not found for owner %q", board.GitHubProjectNumber, board.Owner)
}

// validGitHubItemURL rejects itemURL values that are not https github.com
// resources. Parameterised GraphQL prevents injection, but unvalidated
// attacker-controlled URLs would still be forwarded to GitHub's resolver.
func (c *GitHub) validGitHubItemURL(itemURL string) error {
	u, err := url.Parse(itemURL)
	if err != nil || u.Scheme != "https" {
		return fmt.Errorf("github: itemURL must be https: %q", itemURL)
	}
	// Allow the configured graphQL host (for testing) or api.github.com / github.com.
	allowedHosts := []string{"github.com", "api.github.com"}
	if c.graphQLBase != "" {
		if gu, err2 := url.Parse(c.graphQLBase); err2 == nil {
			allowedHosts = append(allowedHosts, gu.Host)
		}
	}
	for _, h := range allowedHosts {
		if u.Host == h {
			return nil
		}
	}
	return fmt.Errorf("github: itemURL host %q not allowed", u.Host)
}

func (c *GitHub) ghResourceID(ctx context.Context, token, itemURL string) (string, error) {
	if err := c.validGitHubItemURL(itemURL); err != nil {
		return "", err
	}
	var out struct {
		Resource struct {
			ID string `json:"id"`
		} `json:"resource"`
	}
	const q = `query($url:URI!){ resource(url:$url) { ... on Issue { id } ... on PullRequest { id } } }`
	if err := c.ghGraphQL(ctx, token, q, map[string]any{"url": itemURL}, &out); err != nil {
		return "", err
	}
	if out.Resource.ID == "" {
		return "", fmt.Errorf("github: resource not found for url %q", itemURL)
	}
	return out.Resource.ID, nil
}

func (c *GitHub) ghProjectItemID(ctx context.Context, token, itemURL, projectID string) (string, error) {
	if err := c.validGitHubItemURL(itemURL); err != nil {
		return "", err
	}
	var out struct {
		Resource struct {
			ProjectItems struct {
				Nodes []struct {
					ID      string `json:"id"`
					Project struct {
						ID string `json:"id"`
					} `json:"project"`
				} `json:"nodes"`
			} `json:"projectItems"`
		} `json:"resource"`
	}
	const q = `query($url:URI!){ resource(url:$url) { ... on Issue { projectItems(first:20) { nodes { id project { id } } } } ... on PullRequest { projectItems(first:20) { nodes { id project { id } } } } } }`
	if err := c.ghGraphQL(ctx, token, q, map[string]any{"url": itemURL}, &out); err != nil {
		return "", err
	}
	for _, n := range out.Resource.ProjectItems.Nodes {
		if n.Project.ID == projectID {
			return n.ID, nil
		}
	}
	return "", fmt.Errorf("github: item for %q not on project %q", itemURL, projectID)
}
