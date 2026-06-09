package scm

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type glLabel struct {
	Title string `json:"title"`
}

type glPayload struct {
	ObjectKind string `json:"object_kind"`
	Ref        string `json:"ref"`
	Project    struct {
		GitHTTPURL        string `json:"git_http_url"`
		PathWithNamespace string `json:"path_with_namespace"`
	} `json:"project"`
	ObjectAttributes struct {
		IID         int    `json:"iid"`
		Title       string `json:"title"`
		Description string `json:"description"`
		URL         string `json:"url"`
	} `json:"object_attributes"`
	Labels []glLabel `json:"labels"`
}

// DetectAndVerify verifies the X-Gitlab-Token and parses the payload.
func (*GitLab) DetectAndVerify(h http.Header, payload []byte, secret string) (WebhookEvent, error) {
	token := h.Get("X-Gitlab-Token")
	if token == "" {
		return WebhookEvent{}, errors.New("gitlab: missing X-Gitlab-Token")
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(secret)) != 1 {
		return WebhookEvent{}, errors.New("gitlab: token mismatch")
	}
	var p glPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return WebhookEvent{}, fmt.Errorf("gitlab: parse payload: %w", err)
	}
	switch h.Get("X-Gitlab-Event") {
	case "Push Hook":
		return WebhookEvent{Kind: "push", Repo: p.Project.GitHTTPURL, Branch: trimGitLabRef(p.Ref)}, nil
	case "Issue Hook":
		return glWorkItemEvent("issue", p), nil
	case "Merge Request Hook":
		return glWorkItemEvent("mr", p), nil
	default:
		return WebhookEvent{Kind: "other"}, nil
	}
}

func trimGitLabRef(ref string) string {
	const prefix = "refs/heads/"
	if len(ref) > len(prefix) && ref[:len(prefix)] == prefix {
		return ref[len(prefix):]
	}
	return ref
}

func glWorkItemEvent(kind string, p glPayload) WebhookEvent {
	labels := make([]string, 0, len(p.Labels))
	for _, l := range p.Labels {
		labels = append(labels, l.Title)
	}
	return WebhookEvent{
		Kind:     kind,
		Repo:     p.Project.GitHTTPURL,
		Labels:   labels,
		Title:    p.ObjectAttributes.Title,
		Body:     p.ObjectAttributes.Description,
		IssueRef: fmt.Sprintf("%s!%d", p.Project.PathWithNamespace, p.ObjectAttributes.IID),
		URL:      p.ObjectAttributes.URL,
	}
}

func (c *GitLab) base() string {
	if c.apiBase != "" {
		return c.apiBase
	}
	return "https://gitlab.com/api/v4"
}

// OpenChange creates a merge request and returns its web_url.
func (c *GitLab) OpenChange(ctx context.Context, repoURL, token, sourceBranch, targetBranch, title, body string) (string, error) {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return "", err
	}
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests"
	reqBody := map[string]string{
		"source_branch": sourceBranch,
		"target_branch": targetBranch,
		"title":         title,
		"description":   body,
	}
	var out struct {
		WebURL string `json:"web_url"`
	}
	if err := glDo(ctx, c.base(), http.MethodPost, path, token, reqBody, &out); err != nil {
		return "", err
	}
	return out.WebURL, nil
}

// Comment posts a note on an issue identified by group/proj!iid.
func (c *GitLab) Comment(ctx context.Context, token, issueRef, body string) error {
	proj, iid, err := glIssueRef(issueRef)
	if err != nil {
		return err
	}
	path := "/projects/" + url.PathEscape(proj) + "/issues/" + strconv.Itoa(iid) + "/notes"
	return glDo(ctx, c.base(), http.MethodPost, path, token, map[string]string{"body": body}, nil)
}

func glProjectPath(repoURL string) (string, error) {
	u, err := url.Parse(repoURL)
	if err != nil {
		return "", fmt.Errorf("gitlab: parse repo url %q: %w", repoURL, err)
	}
	p := strings.TrimSuffix(strings.Trim(u.Path, "/"), ".git")
	if p == "" {
		return "", fmt.Errorf("gitlab: cannot derive project path from %q", repoURL)
	}
	return p, nil
}

func glIssueRef(ref string) (string, int, error) {
	at := strings.LastIndex(ref, "!")
	if at < 0 {
		return "", 0, fmt.Errorf("gitlab: malformed issue ref %q", ref)
	}
	proj, iidStr := ref[:at], ref[at+1:]
	if proj == "" {
		return "", 0, fmt.Errorf("gitlab: malformed issue ref %q", ref)
	}
	iid, err := strconv.Atoi(iidStr)
	if err != nil {
		return "", 0, fmt.Errorf("gitlab: malformed iid in %q: %w", ref, err)
	}
	return proj, iid, nil
}

func glDo(ctx context.Context, base, method, path, token string, in, out any) error {
	var rdr io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("gitlab: encode body: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, rdr)
	if err != nil {
		return fmt.Errorf("gitlab: build request: %w", err)
	}
	req.Header.Set("PRIVATE-TOKEN", token)
	req.Header.Set("Accept", "application/json")
	if rdr != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("gitlab: do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		buf, _ := io.ReadAll(resp.Body)
		return &HTTPError{Status: resp.StatusCode, Body: string(buf), Path: path}
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("gitlab: decode response: %w", err)
	}
	return nil
}

// SCMWriter stubs -- replaced by real implementations in subsequent tasks.

func (c *GitLab) CreateIssue(_ context.Context, _, _ string, _ IssueReq) (IssueRef, error) {
	return IssueRef{}, fmt.Errorf("gitlab: CreateIssue: not implemented")
}
func (c *GitLab) AddLabel(_ context.Context, _, _, _ string) error {
	return fmt.Errorf("gitlab: AddLabel: not implemented")
}
func (c *GitLab) RemoveLabel(_ context.Context, _, _, _ string) error {
	return fmt.Errorf("gitlab: RemoveLabel: not implemented")
}
func (c *GitLab) GetPRState(_ context.Context, _, _ string, _ int) (PRState, error) {
	return PRState{}, fmt.Errorf("gitlab: GetPRState: not implemented")
}
func (c *GitLab) Approve(_ context.Context, _, _ string, _ int, _ string) error {
	return fmt.Errorf("gitlab: Approve: not implemented")
}
func (c *GitLab) RequestChanges(_ context.Context, _, _ string, _ int, _ string) error {
	return fmt.Errorf("gitlab: RequestChanges: not implemented")
}
func (c *GitLab) Suggest(_ context.Context, _, _ string, _ int, _ []Suggestion) error {
	return fmt.Errorf("gitlab: Suggest: not implemented")
}
func (c *GitLab) Merge(_ context.Context, _, _ string, _ int, _ string) error {
	return fmt.Errorf("gitlab: Merge: not implemented")
}
func (c *GitLab) ClosePR(_ context.Context, _, _ string, _ int, _ string) error {
	return fmt.Errorf("gitlab: ClosePR: not implemented")
}
func (c *GitLab) AddBoardItem(_ context.Context, _ string, _ BoardRef, _ string) error {
	return fmt.Errorf("gitlab: AddBoardItem: not implemented")
}
func (c *GitLab) SetBoardColumn(_ context.Context, _ string, _ BoardRef, _, _ string) error {
	return fmt.Errorf("gitlab: SetBoardColumn: not implemented")
}
