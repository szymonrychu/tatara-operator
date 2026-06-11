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
	Number int    `json:"number"`
	Title  string `json:"title"`
	Body   string `json:"body"`
	User   struct {
		Login string `json:"login"`
	} `json:"user"`
	Labels  []ghLabel `json:"labels"`
	HTMLURL string    `json:"html_url"`
	Head    struct {
		SHA string `json:"sha"`
		Ref string `json:"ref"`
	} `json:"head"`
	PullRequest *struct {
		URL string `json:"url"`
	} `json:"pull_request"`
}

type ghPayload struct {
	Action     string `json:"action"`
	Ref        string `json:"ref"`
	After      string `json:"after"`
	Repository struct {
		CloneURL string `json:"clone_url"`
		FullName string `json:"full_name"`
	} `json:"repository"`
	Sender struct {
		Login string `json:"login"`
	} `json:"sender"`
	Label struct {
		Name string `json:"name"`
	} `json:"label"`
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
		return ghWorkItemEvent("issue", false, p, p.Issue), nil
	case "issue_comment":
		isPR := p.Issue != nil && p.Issue.PullRequest != nil
		ev := ghWorkItemEvent("issue", isPR, p, p.Issue)
		if isPR {
			ev.Kind = "mr"
		}
		return ev, nil
	case "pull_request":
		return ghWorkItemEvent("mr", true, p, p.PullRequest), nil
	case "pull_request_review":
		return ghWorkItemEvent("mr", true, p, p.PullRequest), nil
	default:
		return WebhookEvent{Kind: "other"}, nil
	}
}

func ghWorkItemEvent(kind string, isPR bool, p ghPayload, wi *ghWorkItem) WebhookEvent {
	if wi == nil {
		return WebhookEvent{Kind: "other"}
	}
	labels := make([]string, 0, len(wi.Labels))
	for _, l := range wi.Labels {
		labels = append(labels, l.Name)
	}
	return WebhookEvent{
		Kind:         kind,
		Repo:         p.Repository.CloneURL,
		Labels:       labels,
		Title:        wi.Title,
		Body:         wi.Body,
		IssueRef:     fmt.Sprintf("%s#%d", p.Repository.FullName, wi.Number),
		URL:          wi.HTMLURL,
		AuthorLogin:  wi.User.Login,
		ActorLogin:   p.Sender.Login,
		Action:       p.Action,
		Number:       wi.Number,
		IsPR:         isPR,
		HeadSHA:      wi.Head.SHA,
		HeadBranch:   wi.Head.Ref,
		ChangedLabel: p.Label.Name,
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

// OwnerRepo parses a GitHub clone/repo URL into owner and repo name.
func OwnerRepo(repoURL string) (string, string, error) { return ghOwnerRepo(repoURL) }

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

// CreateIssue opens an issue and returns its ref + url.
func (c *GitHub) CreateIssue(ctx context.Context, repoURL, token string, req IssueReq) (CreatedIssue, error) {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return CreatedIssue{}, err
	}
	path := fmt.Sprintf("/repos/%s/%s/issues", owner, repo)
	in := map[string]any{"title": req.Title, "body": req.Body}
	if len(req.Labels) > 0 {
		in["labels"] = req.Labels
	}
	var out struct {
		Number  int    `json:"number"`
		HTMLURL string `json:"html_url"`
	}
	if err := ghDo(ctx, c.base(), http.MethodPost, path, token, in, &out); err != nil {
		return CreatedIssue{}, err
	}
	return CreatedIssue{Ref: fmt.Sprintf("%s/%s#%d", owner, repo, out.Number), URL: out.HTMLURL}, nil
}

// AddLabel adds a single label to an issue/PR identified by owner/repo#number.
func (c *GitHub) AddLabel(ctx context.Context, token, issueRef, label string) error {
	owner, repo, number, err := ghIssueRef(issueRef)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/repos/%s/%s/issues/%d/labels", owner, repo, number)
	return ghDo(ctx, c.base(), http.MethodPost, path, token, map[string][]string{"labels": {label}}, nil)
}

// RemoveLabel removes a single label from an issue/PR.
func (c *GitHub) RemoveLabel(ctx context.Context, token, issueRef, label string) error {
	owner, repo, number, err := ghIssueRef(issueRef)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/repos/%s/%s/issues/%d/labels/%s", owner, repo, number, label)
	return ghDo(ctx, c.base(), http.MethodDelete, path, token, nil, nil)
}

// GetPRState reads a PR plus its head check-runs, deriving CIStatus.
func (c *GitHub) GetPRState(ctx context.Context, repoURL, token string, number int) (PRState, error) {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return PRState{}, err
	}
	var pr struct {
		User struct {
			Login string `json:"login"`
		} `json:"user"`
		Mergeable bool `json:"mergeable"`
		Head      struct {
			SHA string `json:"sha"`
			Ref string `json:"ref"`
		} `json:"head"`
	}
	if err := ghDo(ctx, c.base(), http.MethodGet, fmt.Sprintf("/repos/%s/%s/pulls/%d", owner, repo, number), token, nil, &pr); err != nil {
		return PRState{}, err
	}
	st := PRState{Author: pr.User.Login, HeadSHA: pr.Head.SHA, HeadBranch: pr.Head.Ref, Mergeable: pr.Mergeable}
	var checks struct {
		CheckRuns []struct {
			Status     string `json:"status"`
			Conclusion string `json:"conclusion"`
		} `json:"check_runs"`
	}
	if err := ghDo(ctx, c.base(), http.MethodGet, fmt.Sprintf("/repos/%s/%s/commits/%s/check-runs", owner, repo, pr.Head.SHA), token, nil, &checks); err != nil {
		return PRState{}, err
	}
	st.CIStatus = deriveGHCIStatus(checks.CheckRuns)
	return st, nil
}

func deriveGHCIStatus(runs []struct {
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}) string {
	if len(runs) == 0 {
		return ""
	}
	failure, pending := false, false
	for _, run := range runs {
		if run.Status != "completed" {
			pending = true
			continue
		}
		if run.Conclusion != "success" && run.Conclusion != "neutral" && run.Conclusion != "skipped" {
			failure = true
		}
	}
	switch {
	case failure:
		return "failure"
	case pending:
		return "pending"
	default:
		return "success"
	}
}

func (c *GitHub) review(ctx context.Context, repoURL, token string, number int, event, body string, comments []map[string]any) error {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return err
	}
	in := map[string]any{"event": event}
	if body != "" {
		in["body"] = body
	}
	if comments != nil {
		in["comments"] = comments
	}
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/reviews", owner, repo, number)
	return ghDo(ctx, c.base(), http.MethodPost, path, token, in, nil)
}

// Approve posts an APPROVE review.
func (c *GitHub) Approve(ctx context.Context, repoURL, token string, number int, body string) error {
	return c.review(ctx, repoURL, token, number, "APPROVE", body, nil)
}

// RequestChanges posts a REQUEST_CHANGES review.
func (c *GitHub) RequestChanges(ctx context.Context, repoURL, token string, number int, body string) error {
	return c.review(ctx, repoURL, token, number, "REQUEST_CHANGES", body, nil)
}

// Suggest posts inline review comments with ```suggestion bodies.
func (c *GitHub) Suggest(ctx context.Context, repoURL, token string, number int, sugg []Suggestion) error {
	comments := make([]map[string]any, 0, len(sugg))
	for _, s := range sugg {
		comments = append(comments, map[string]any{
			"path": s.Path,
			"line": s.Line,
			"body": "```suggestion\n" + s.Body + "\n```",
		})
	}
	return c.review(ctx, repoURL, token, number, "COMMENT", "", comments)
}

// Merge merges a PR with the given method (squash|merge|rebase).
func (c *GitHub) Merge(ctx context.Context, repoURL, token string, number int, method string) error {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/merge", owner, repo, number)
	return ghDo(ctx, c.base(), http.MethodPut, path, token, map[string]string{"merge_method": method}, nil)
}

// ClosePR closes a PR (state=closed) and posts a comment with the reason.
func (c *GitHub) ClosePR(ctx context.Context, repoURL, token string, number int, body string) error {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d", owner, repo, number)
	if err := ghDo(ctx, c.base(), http.MethodPatch, path, token, map[string]string{"state": "closed"}, nil); err != nil {
		return err
	}
	if body == "" {
		return nil
	}
	return c.Comment(ctx, token, fmt.Sprintf("%s/%s#%d", owner, repo, number), body)
}
