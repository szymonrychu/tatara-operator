package scm

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// LatestWorkflowRun returns the most recent run of workflowFile on branch.
// GitHub returns runs newest-first, so per_page=1 yields the latest.
func (c *GitHub) LatestWorkflowRun(ctx context.Context, owner, repo, workflowFile, branch string) (WorkflowRun, bool, error) {
	var out struct {
		WorkflowRuns []struct {
			HeadSHA    string    `json:"head_sha"`
			Status     string    `json:"status"`
			Conclusion string    `json:"conclusion"`
			HTMLURL    string    `json:"html_url"`
			CreatedAt  time.Time `json:"created_at"`
		} `json:"workflow_runs"`
	}
	path := fmt.Sprintf("/repos/%s/%s/actions/workflows/%s/runs?branch=%s&per_page=1",
		owner, repo, workflowFile, branch)
	if err := ghDo(ctx, c.base(), http.MethodGet, path, c.token, nil, &out); err != nil {
		return WorkflowRun{}, false, err
	}
	if len(out.WorkflowRuns) == 0 {
		return WorkflowRun{}, false, nil
	}
	r := out.WorkflowRuns[0]
	return WorkflowRun{
		HeadSHA:    r.HeadSHA,
		Status:     r.Status,
		Conclusion: r.Conclusion,
		HTMLURL:    r.HTMLURL,
		CreatedAt:  r.CreatedAt,
	}, true, nil
}

// GetFileContent reads path at ref via the contents API. A 404 (file absent at
// ref) returns ("", nil) so callers can probe candidate pin files without
// erroring on the misses.
func (c *GitHub) GetFileContent(ctx context.Context, owner, repo, path, ref string) (string, error) {
	var out struct {
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
	}
	apiPath := fmt.Sprintf("/repos/%s/%s/contents/%s?ref=%s", owner, repo, path, ref)
	if err := ghDo(ctx, c.base(), http.MethodGet, apiPath, c.token, nil, &out); err != nil {
		var he *HTTPError
		if errors.As(err, &he) && he.Status == http.StatusNotFound {
			return "", nil
		}
		return "", err
	}
	if out.Encoding == "base64" {
		// GitHub wraps the base64 payload at 60 columns with newlines.
		raw, derr := base64.StdEncoding.DecodeString(strings.ReplaceAll(out.Content, "\n", ""))
		if derr != nil {
			return "", fmt.Errorf("github: decode file content %s@%s: %w", path, ref, derr)
		}
		return string(raw), nil
	}
	return out.Content, nil
}

// LatestSemverTag returns the highest vX.Y.Z (or X.Y.Z) tag on the repo.
func (c *GitHub) LatestSemverTag(ctx context.Context, owner, repo string) (string, bool, error) {
	var tags []struct {
		Name string `json:"name"`
	}
	path := fmt.Sprintf("/repos/%s/%s/tags?per_page=100", owner, repo)
	if err := ghDo(ctx, c.base(), http.MethodGet, path, c.token, nil, &tags); err != nil {
		return "", false, err
	}
	best := ""
	var bestT semverTriple
	found := false
	for _, t := range tags {
		tr, ok := parseSemverTag(t.Name)
		if !ok {
			continue
		}
		if !found || tr.greater(bestT) {
			best, bestT, found = t.Name, tr, true
		}
	}
	return best, found, nil
}

type semverTriple struct{ major, minor, patch int }

func (a semverTriple) greater(b semverTriple) bool {
	if a.major != b.major {
		return a.major > b.major
	}
	if a.minor != b.minor {
		return a.minor > b.minor
	}
	return a.patch > b.patch
}

// parseSemverTag parses a vX.Y.Z or X.Y.Z tag (a leading 'v' is optional;
// pre-release/build suffixes are ignored). ok=false for non-semver tags.
func parseSemverTag(name string) (semverTriple, bool) {
	s := strings.TrimPrefix(name, "v")
	// Drop any pre-release/build metadata so v1.2.3-rc1 still sorts on the core.
	if i := strings.IndexAny(s, "-+"); i >= 0 {
		s = s[:i]
	}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return semverTriple{}, false
	}
	var tr semverTriple
	var err error
	if tr.major, err = strconv.Atoi(parts[0]); err != nil {
		return semverTriple{}, false
	}
	if tr.minor, err = strconv.Atoi(parts[1]); err != nil {
		return semverTriple{}, false
	}
	if tr.patch, err = strconv.Atoi(parts[2]); err != nil {
		return semverTriple{}, false
	}
	return tr, true
}
