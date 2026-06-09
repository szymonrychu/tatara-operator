package scm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	resp, err := http.DefaultClient.Do(req)
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
		return fmt.Errorf("github: graphql error: %s", env.Errors[0].Message)
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
	q := fmt.Sprintf(`mutation { addProjectV2ItemById(input:{projectId:%q, contentId:%q}) { item { id } } }`, projectID, contentID)
	return c.ghGraphQL(ctx, token, q, nil, nil)
}

// SetBoardColumn sets the Status single-select field of the board item for itemURL.
func (c *GitHub) SetBoardColumn(ctx context.Context, token string, board BoardRef, itemURL, column string) error {
	field := board.StatusField
	if field == "" {
		field = "Status"
	}
	var proj struct {
		Organization struct {
			ProjectV2 struct {
				ID    string `json:"id"`
				Field struct {
					ID      string `json:"id"`
					Options []struct {
						ID   string `json:"id"`
						Name string `json:"name"`
					} `json:"options"`
				} `json:"field"`
			} `json:"projectV2"`
		} `json:"organization"`
	}
	pq := fmt.Sprintf(`query { organization(login:%q) { projectV2(number:%d) { id field(name:%q) { ... on ProjectV2SingleSelectField { id options { id name } } } } } }`, board.Owner, board.GitHubProjectNumber, field)
	if err := c.ghGraphQL(ctx, token, pq, nil, &proj); err != nil {
		return err
	}
	optionID := ""
	for _, o := range proj.Organization.ProjectV2.Field.Options {
		if o.Name == column {
			optionID = o.ID
			break
		}
	}
	if optionID == "" {
		return fmt.Errorf("github: board column %q not found in field %q", column, field)
	}
	itemID, err := c.ghProjectItemID(ctx, token, itemURL, proj.Organization.ProjectV2.ID)
	if err != nil {
		return err
	}
	mq := fmt.Sprintf(`mutation { updateProjectV2ItemFieldValue(input:{projectId:%q, itemId:%q, fieldId:%q, value:{singleSelectOptionId:%q}}) { clientMutationId } }`,
		proj.Organization.ProjectV2.ID, itemID, proj.Organization.ProjectV2.Field.ID, optionID)
	return c.ghGraphQL(ctx, token, mq, nil, nil)
}

func (c *GitHub) ghProjectID(ctx context.Context, token string, board BoardRef) (string, error) {
	var out struct {
		Organization struct {
			ProjectV2 struct {
				ID string `json:"id"`
			} `json:"projectV2"`
		} `json:"organization"`
	}
	q := fmt.Sprintf(`query { organization(login:%q) { projectV2(number:%d) { id } } }`, board.Owner, board.GitHubProjectNumber)
	if err := c.ghGraphQL(ctx, token, q, nil, &out); err != nil {
		return "", err
	}
	if out.Organization.ProjectV2.ID == "" {
		return "", fmt.Errorf("github: project %d not found for org %q", board.GitHubProjectNumber, board.Owner)
	}
	return out.Organization.ProjectV2.ID, nil
}

func (c *GitHub) ghResourceID(ctx context.Context, token, itemURL string) (string, error) {
	var out struct {
		Resource struct {
			ID string `json:"id"`
		} `json:"resource"`
	}
	q := fmt.Sprintf(`query { resource(url:%q) { ... on Issue { id } ... on PullRequest { id } } }`, itemURL)
	if err := c.ghGraphQL(ctx, token, q, nil, &out); err != nil {
		return "", err
	}
	if out.Resource.ID == "" {
		return "", fmt.Errorf("github: resource not found for url %q", itemURL)
	}
	return out.Resource.ID, nil
}

func (c *GitHub) ghProjectItemID(ctx context.Context, token, itemURL, projectID string) (string, error) {
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
	q := fmt.Sprintf(`query { resource(url:%q) { ... on Issue { projectItems(first:20) { nodes { id project { id } } } } ... on PullRequest { projectItems(first:20) { nodes { id project { id } } } } } }`, itemURL)
	if err := c.ghGraphQL(ctx, token, q, nil, &out); err != nil {
		return "", err
	}
	for _, n := range out.Resource.ProjectItems.Nodes {
		if n.Project.ID == projectID {
			return n.ID, nil
		}
	}
	return "", fmt.Errorf("github: item for %q not on project %q", itemURL, projectID)
}
