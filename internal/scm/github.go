package scm

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type ghLabel struct {
	Name string `json:"name"`
}

type ghWorkItem struct {
	Number  int       `json:"number"`
	Title   string    `json:"title"`
	Body    string    `json:"body"`
	Labels  []ghLabel `json:"labels"`
	HTMLURL string    `json:"html_url"`
}

type ghPayload struct {
	Ref        string `json:"ref"`
	After      string `json:"after"`
	Repository struct {
		CloneURL string `json:"clone_url"`
		FullName string `json:"full_name"`
	} `json:"repository"`
	Issue       *ghWorkItem `json:"issue"`
	PullRequest *ghWorkItem `json:"pull_request"`
}

// DetectAndVerify verifies the X-Hub-Signature-256 HMAC and parses the payload.
func (*GitHub) DetectAndVerify(h http.Header, payload []byte, secret string) (WebhookEvent, error) {
	if err := verifyGitHubSig(h.Get("X-Hub-Signature-256"), payload, secret); err != nil {
		return WebhookEvent{}, err
	}
	event := h.Get("X-GitHub-Event")
	var p ghPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return WebhookEvent{}, fmt.Errorf("github: parse payload: %w", err)
	}
	switch event {
	case "push":
		return WebhookEvent{Kind: "push", Repo: p.Repository.CloneURL, Branch: strings.TrimPrefix(p.Ref, "refs/heads/")}, nil
	case "issues":
		return ghWorkItemEvent("issue", p.Repository.FullName, p.Repository.CloneURL, p.Issue), nil
	case "pull_request":
		return ghWorkItemEvent("mr", p.Repository.FullName, p.Repository.CloneURL, p.PullRequest), nil
	default:
		return WebhookEvent{Kind: "other"}, nil
	}
}

func ghWorkItemEvent(kind, fullName, cloneURL string, wi *ghWorkItem) WebhookEvent {
	if wi == nil {
		return WebhookEvent{Kind: "other"}
	}
	labels := make([]string, 0, len(wi.Labels))
	for _, l := range wi.Labels {
		labels = append(labels, l.Name)
	}
	return WebhookEvent{
		Kind:     kind,
		Repo:     cloneURL,
		Labels:   labels,
		Title:    wi.Title,
		Body:     wi.Body,
		IssueRef: fmt.Sprintf("%s#%d", fullName, wi.Number),
		URL:      wi.HTMLURL,
	}
}

func verifyGitHubSig(header string, payload []byte, secret string) error {
	if header == "" {
		return errors.New("github: missing X-Hub-Signature-256")
	}
	want, ok := strings.CutPrefix(header, "sha256=")
	if !ok {
		return errors.New("github: malformed signature")
	}
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(payload)
	got := hex.EncodeToString(m.Sum(nil))
	if !hmac.Equal([]byte(got), []byte(want)) {
		return errors.New("github: signature mismatch")
	}
	return nil
}

func (c *GitHub) base() string {
	if c.apiBase != "" {
		return c.apiBase
	}
	return "https://api.github.com"
}

// OpenChange creates a pull request and returns its html_url.
func (c *GitHub) OpenChange(ctx context.Context, repoURL, token, sourceBranch, targetBranch, title, body string) (string, error) {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return "", err
	}
	path := fmt.Sprintf("/repos/%s/%s/pulls", owner, repo)
	reqBody := map[string]string{"title": title, "head": sourceBranch, "base": targetBranch, "body": body}
	var out struct {
		HTMLURL string `json:"html_url"`
	}
	if err := ghDo(ctx, c.base(), http.MethodPost, path, token, reqBody, &out); err != nil {
		return "", err
	}
	return out.HTMLURL, nil
}

// Comment posts a comment on an issue or PR identified by owner/repo#number.
func (c *GitHub) Comment(ctx context.Context, token, issueRef, body string) error {
	owner, repo, number, err := ghIssueRef(issueRef)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/repos/%s/%s/issues/%d/comments", owner, repo, number)
	return ghDo(ctx, c.base(), http.MethodPost, path, token, map[string]string{"body": body}, nil)
}

func ghOwnerRepo(repoURL string) (string, string, error) {
	u, err := url.Parse(repoURL)
	if err != nil {
		return "", "", fmt.Errorf("github: parse repo url %q: %w", repoURL, err)
	}
	p := strings.TrimSuffix(strings.Trim(u.Path, "/"), ".git")
	parts := strings.Split(p, "/")
	if len(parts) < 2 || parts[len(parts)-1] == "" || parts[len(parts)-2] == "" {
		return "", "", fmt.Errorf("github: cannot derive owner/repo from %q", repoURL)
	}
	return parts[len(parts)-2], parts[len(parts)-1], nil
}

func ghIssueRef(ref string) (string, string, int, error) {
	at := strings.LastIndex(ref, "#")
	if at < 0 {
		return "", "", 0, fmt.Errorf("github: malformed issue ref %q", ref)
	}
	or, numStr := ref[:at], ref[at+1:]
	oParts := strings.Split(or, "/")
	if len(oParts) != 2 || oParts[0] == "" || oParts[1] == "" {
		return "", "", 0, fmt.Errorf("github: malformed issue ref %q", ref)
	}
	n, err := strconv.Atoi(numStr)
	if err != nil {
		return "", "", 0, fmt.Errorf("github: malformed issue number in %q: %w", ref, err)
	}
	return oParts[0], oParts[1], n, nil
}

func ghDo(ctx context.Context, base, method, path, token string, in, out any) error {
	var rdr io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("github: encode body: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, rdr)
	if err != nil {
		return fmt.Errorf("github: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	if rdr != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("github: do request: %w", err)
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
		return fmt.Errorf("github: decode response: %w", err)
	}
	return nil
}

// SCMWriter stubs -- replaced by real implementations in subsequent tasks.

func (c *GitHub) CreateIssue(_ context.Context, _, _ string, _ IssueReq) (IssueRef, error) {
	return IssueRef{}, fmt.Errorf("github: CreateIssue: not implemented")
}
func (c *GitHub) AddLabel(_ context.Context, _, _, _ string) error {
	return fmt.Errorf("github: AddLabel: not implemented")
}
func (c *GitHub) RemoveLabel(_ context.Context, _, _, _ string) error {
	return fmt.Errorf("github: RemoveLabel: not implemented")
}
func (c *GitHub) GetPRState(_ context.Context, _, _ string, _ int) (PRState, error) {
	return PRState{}, fmt.Errorf("github: GetPRState: not implemented")
}
func (c *GitHub) Approve(_ context.Context, _, _ string, _ int, _ string) error {
	return fmt.Errorf("github: Approve: not implemented")
}
func (c *GitHub) RequestChanges(_ context.Context, _, _ string, _ int, _ string) error {
	return fmt.Errorf("github: RequestChanges: not implemented")
}
func (c *GitHub) Suggest(_ context.Context, _, _ string, _ int, _ []Suggestion) error {
	return fmt.Errorf("github: Suggest: not implemented")
}
func (c *GitHub) Merge(_ context.Context, _, _ string, _ int, _ string) error {
	return fmt.Errorf("github: Merge: not implemented")
}
func (c *GitHub) ClosePR(_ context.Context, _, _ string, _ int, _ string) error {
	return fmt.Errorf("github: ClosePR: not implemented")
}
func (c *GitHub) AddBoardItem(_ context.Context, _ string, _ BoardRef, _ string) error {
	return fmt.Errorf("github: AddBoardItem: not implemented")
}
func (c *GitHub) SetBoardColumn(_ context.Context, _ string, _ BoardRef, _, _ string) error {
	return fmt.Errorf("github: SetBoardColumn: not implemented")
}
