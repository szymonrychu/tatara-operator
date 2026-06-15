package scm

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestGitHubGraphQLParameterized verifies that GraphQL mutations/queries send
// values via the variables map rather than interpolated into the query string.
// This is the TDD test for the parameterized-query fix (audit finding #1).
func TestGitHubGraphQLParameterized(t *testing.T) {
	type gqlReq struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables"`
	}

	capture := func(srv *httptest.Server) func() []gqlReq {
		var reqs []gqlReq
		srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			var gr gqlReq
			_ = json.Unmarshal(b, &gr)
			reqs = append(reqs, gr)
			// Return a permissive success response for any query shape.
			// user.projectV2 is null so organization wins; this avoids the
			// SetBoardColumn case where user.projectV2.id is set but has no
			// field options, causing "column not found".
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
				"user":                          map[string]any{"projectV2": nil},
				"organization":                  map[string]any{"projectV2": map[string]any{"id": "PVT_O", "field": map[string]any{"id": "F1", "options": []any{map[string]any{"id": "OPT1", "name": "Done"}}}}},
				"resource":                      map[string]any{"id": "RES1", "projectItems": map[string]any{"nodes": []any{map[string]any{"id": "ITEM1", "project": map[string]any{"id": "PVT_O"}}}}},
				"addProjectV2ItemById":          map[string]any{"item": map[string]any{"id": "ITEM1"}},
				"updateProjectV2ItemFieldValue": map[string]any{"clientMutationId": ""},
			}})
		})
		return func() []gqlReq { return reqs }
	}

	t.Run("AddBoardItem uses variables not interpolation", func(t *testing.T) {
		srv := httptest.NewUnstartedServer(nil)
		srv.Start()
		getReqs := capture(srv)
		defer srv.Close()

		c := &GitHub{apiBase: "unused", graphQLBase: srv.URL}
		board := BoardRef{Provider: "github", Owner: "acme", GitHubProjectNumber: 3, StatusField: "Status"}
		if err := c.AddBoardItem(context.Background(), "tok", board, "https://github.com/acme/r/issues/9"); err != nil {
			t.Fatalf("AddBoardItem: %v", err)
		}

		reqs := getReqs()
		// Find the mutation request.
		for _, gr := range reqs {
			if strings.Contains(gr.Query, "addProjectV2ItemById") {
				// Query must not contain the literal node IDs.
				if strings.Contains(gr.Query, "PVT") || strings.Contains(gr.Query, "RES") {
					t.Errorf("mutation query interpolates IDs into string: %q", gr.Query)
				}
				// Variables must carry the IDs.
				if gr.Variables["projectId"] == nil && gr.Variables["projectID"] == nil {
					t.Errorf("mutation missing projectId in variables; vars=%v", gr.Variables)
				}
				if gr.Variables["contentId"] == nil && gr.Variables["contentID"] == nil {
					t.Errorf("mutation missing contentId in variables; vars=%v", gr.Variables)
				}
			}
			if strings.Contains(gr.Query, "projectV2") && !strings.Contains(gr.Query, "addProjectV2") {
				// The project-lookup query must not interpolate the owner name.
				if strings.Contains(gr.Query, `"acme"`) {
					t.Errorf("project-lookup query interpolates owner into string: %q", gr.Query)
				}
			}
		}
	})

	t.Run("SetBoardColumn uses variables not interpolation", func(t *testing.T) {
		srv := httptest.NewUnstartedServer(nil)
		srv.Start()
		getReqs := capture(srv)
		defer srv.Close()

		c := &GitHub{apiBase: "unused", graphQLBase: srv.URL}
		board := BoardRef{Provider: "github", Owner: "acme", GitHubProjectNumber: 3, StatusField: "Status"}
		if err := c.SetBoardColumn(context.Background(), "tok", board, "https://github.com/acme/r/issues/9", "Done"); err != nil {
			t.Fatalf("SetBoardColumn: %v", err)
		}

		for _, gr := range getReqs() {
			if strings.Contains(gr.Query, `"acme"`) {
				t.Errorf("query interpolates owner literal %q", gr.Query)
			}
			if strings.Contains(gr.Query, "updateProjectV2ItemFieldValue") {
				if gr.Variables["projectId"] == nil && gr.Variables["projectID"] == nil {
					t.Errorf("update mutation missing projectId in variables; vars=%v", gr.Variables)
				}
			}
		}
	})
}

func TestGitHubBoardManagerUserOwnedProject(t *testing.T) {
	// The board owner is a USER (szymonrychu), not an organization. The
	// project id must resolve from the user-aliased root while organization
	// stays null.
	t.Run("AddBoardItem resolves user-owned project", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			var req struct {
				Query     string         `json:"query"`
				Variables map[string]any `json:"variables"`
			}
			_ = json.Unmarshal(b, &req)
			switch {
			case strings.Contains(req.Query, "projectV2") && !strings.Contains(req.Query, "addProjectV2"):
				// user root non-null, organization null
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
					"user":         map[string]any{"projectV2": map[string]any{"id": "PVT_USER"}},
					"organization": nil,
				}})
			case strings.Contains(req.Query, "resource(url"):
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"resource": map[string]any{"id": "I_1"}}})
			case strings.Contains(req.Query, "addProjectV2ItemById"):
				// With parameterized queries the project id is in variables, not the query string.
				if req.Variables["projectId"] != "PVT_USER" {
					t.Fatalf("add item did not use user-owned project id: variables=%v", req.Variables)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"addProjectV2ItemById": map[string]any{"item": map[string]any{"id": "ITEM_1"}}}})
			default:
				t.Fatalf("unexpected query %q", req.Query)
			}
		}))
		defer srv.Close()
		c := &GitHub{apiBase: "unused", graphQLBase: srv.URL}
		board := BoardRef{Provider: "github", Owner: "szymonrychu", GitHubProjectNumber: 3, StatusField: "Status"}
		if err := c.AddBoardItem(context.Background(), "tok", board, "https://github.com/szymonrychu/r/issues/9"); err != nil {
			t.Fatalf("AddBoardItem: %v", err)
		}
	})
}

func TestGitHubBoardManager(t *testing.T) {
	t.Run("AddBoardItem", func(t *testing.T) {
		var queries []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			var req struct {
				Query string `json:"query"`
			}
			_ = json.Unmarshal(b, &req)
			queries = append(queries, req.Query)
			switch {
			case strings.Contains(req.Query, "organization"):
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"organization": map[string]any{"projectV2": map[string]any{"id": "PVT_1"}}}})
			case strings.Contains(req.Query, "resource(url"):
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"resource": map[string]any{"id": "I_1"}}})
			case strings.Contains(req.Query, "addProjectV2ItemById"):
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"addProjectV2ItemById": map[string]any{"item": map[string]any{"id": "ITEM_1"}}}})
			default:
				t.Fatalf("unexpected query %q", req.Query)
			}
		}))
		defer srv.Close()
		c := &GitHub{apiBase: "unused", graphQLBase: srv.URL}
		board := BoardRef{Provider: "github", Owner: "acme", GitHubProjectNumber: 3, StatusField: "Status"}
		if err := c.AddBoardItem(context.Background(), "tok", board, "https://github.com/acme/r/issues/9"); err != nil {
			t.Fatalf("AddBoardItem: %v", err)
		}
		if len(queries) != 3 {
			t.Fatalf("expected 3 graphql calls, got %d", len(queries))
		}
	})
	t.Run("SetBoardColumn", func(t *testing.T) {
		var sawUpdate bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			var req struct {
				Query     string         `json:"query"`
				Variables map[string]any `json:"variables"`
			}
			_ = json.Unmarshal(b, &req)
			switch {
			case strings.Contains(req.Query, "organization"):
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
					"user": map[string]any{"projectV2": nil},
					"organization": map[string]any{"projectV2": map[string]any{
						"id": "PVT_1",
						"field": map[string]any{
							"id": "FIELD_1",
							"options": []any{
								map[string]any{"id": "OPT_PROPOSED", "name": "Proposed"},
								map[string]any{"id": "OPT_DONE", "name": "Done"},
							},
						},
					}}}})
			case strings.Contains(req.Query, "resource(url"):
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"resource": map[string]any{"id": "I_1", "projectItems": map[string]any{"nodes": []any{map[string]any{"id": "ITEM_1", "project": map[string]any{"id": "PVT_1"}}}}}}})
			case strings.Contains(req.Query, "updateProjectV2ItemFieldValue"):
				sawUpdate = true
				// With parameterized queries the option id is in variables, not the query string.
				if req.Variables["optionId"] != "OPT_PROPOSED" {
					t.Fatalf("update did not select Proposed option: variables=%v", req.Variables)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"updateProjectV2ItemFieldValue": map[string]any{"clientMutationId": ""}}})
			default:
				t.Fatalf("unexpected query %q", req.Query)
			}
		}))
		defer srv.Close()
		c := &GitHub{apiBase: "unused", graphQLBase: srv.URL}
		board := BoardRef{Provider: "github", Owner: "acme", GitHubProjectNumber: 3, StatusField: "Status"}
		if err := c.SetBoardColumn(context.Background(), "tok", board, "https://github.com/acme/r/issues/9", "Proposed"); err != nil {
			t.Fatalf("SetBoardColumn: %v", err)
		}
		if !sawUpdate {
			t.Fatalf("updateProjectV2ItemFieldValue not called")
		}
	})
}
