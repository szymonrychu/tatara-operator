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

func TestGitHubBoardManagerUserOwnedProject(t *testing.T) {
	// The board owner is a USER (szymonrychu), not an organization. The
	// project id must resolve from the user-aliased root while organization
	// stays null.
	t.Run("AddBoardItem resolves user-owned project", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			var req struct {
				Query string `json:"query"`
			}
			_ = json.Unmarshal(b, &req)
			switch {
			case strings.Contains(req.Query, "projectV2"):
				// user root non-null, organization null
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
					"user":         map[string]any{"projectV2": map[string]any{"id": "PVT_USER"}},
					"organization": nil,
				}})
			case strings.Contains(req.Query, "resource(url"):
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"resource": map[string]any{"id": "I_1"}}})
			case strings.Contains(req.Query, "addProjectV2ItemById"):
				if !strings.Contains(req.Query, "PVT_USER") {
					t.Fatalf("add item did not use user-owned project id: %q", req.Query)
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
				Query string `json:"query"`
			}
			_ = json.Unmarshal(b, &req)
			switch {
			case strings.Contains(req.Query, "organization"):
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"organization": map[string]any{"projectV2": map[string]any{
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
				if !strings.Contains(req.Query, "OPT_PROPOSED") {
					t.Fatalf("update did not select Proposed option: %q", req.Query)
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
