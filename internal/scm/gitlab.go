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
	User       struct {
		Username string `json:"username"`
	} `json:"user"`
	Project struct {
		GitHTTPURL        string `json:"git_http_url"`
		PathWithNamespace string `json:"path_with_namespace"`
	} `json:"project"`
	ObjectAttributes struct {
		IID          int    `json:"iid"`
		Title        string `json:"title"`
		Description  string `json:"description"`
		URL          string `json:"url"`
		Action       string `json:"action"`
		SourceBranch string `json:"source_branch"`
		LastCommit   struct {
			ID string `json:"id"`
		} `json:"last_commit"`
	} `json:"object_attributes"`
	Issue struct {
		IID int `json:"iid"`
	} `json:"issue"`
	MergeRequest struct {
		IID          int    `json:"iid"`
		SourceBranch string `json:"source_branch"`
	} `json:"merge_request"`
	Changes struct {
		Labels struct {
			Previous []glLabel `json:"previous"`
			Current  []glLabel `json:"current"`
		} `json:"labels"`
	} `json:"changes"`
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
		return glWorkItemEvent("issue", false, p), nil
	case "Merge Request Hook":
		return glWorkItemEvent("mr", true, p), nil
	case "Note Hook":
		return glNoteEvent(p), nil
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

func glWorkItemEvent(kind string, isPR bool, p glPayload) WebhookEvent {
	labels := make([]string, 0, len(p.Labels))
	for _, l := range p.Labels {
		labels = append(labels, l.Title)
	}
	sep := "!"
	if kind == "issue" {
		sep = "#"
	}
	action, changed := glActionAndLabel(p)
	// GitLab webhook payloads carry the event actor (p.User) but not a
	// distinct resource-author field on the issue/MR object, so AuthorLogin
	// falls back to the actor. This is only a hint; the authoritative
	// authorship gate lives in the controller (which calls GetPRState).
	return WebhookEvent{
		Kind:         kind,
		Repo:         p.Project.GitHTTPURL,
		Labels:       labels,
		Title:        p.ObjectAttributes.Title,
		Body:         p.ObjectAttributes.Description,
		IssueRef:     fmt.Sprintf("%s%s%d", p.Project.PathWithNamespace, sep, p.ObjectAttributes.IID),
		URL:          p.ObjectAttributes.URL,
		AuthorLogin:  p.User.Username,
		ActorLogin:   p.User.Username,
		Action:       action,
		Number:       p.ObjectAttributes.IID,
		IsPR:         isPR,
		HeadSHA:      p.ObjectAttributes.LastCommit.ID,
		HeadBranch:   p.ObjectAttributes.SourceBranch,
		ChangedLabel: changed,
	}
}

// glNoteEvent builds a WebhookEvent for a Note Hook (comment). The note's own
// object_attributes.iid is the note id, not the work item; the work item iid
// comes from whichever nested object (issue or merge_request) is populated.
func glNoteEvent(p glPayload) WebhookEvent {
	labels := make([]string, 0, len(p.Labels))
	for _, l := range p.Labels {
		labels = append(labels, l.Title)
	}
	kind, sep, isPR := "issue", "#", false
	number := p.Issue.IID
	headBranch := ""
	if p.MergeRequest.IID != 0 {
		kind, sep, isPR = "mr", "!", true
		number = p.MergeRequest.IID
		headBranch = p.MergeRequest.SourceBranch
	}
	// GitLab Note payloads carry the comment author (p.User) but not a
	// distinct work-item author; AuthorLogin falls back to the actor. The
	// authoritative authorship gate lives in the controller (GetPRState).
	return WebhookEvent{
		Kind:        kind,
		Repo:        p.Project.GitHTTPURL,
		Labels:      labels,
		Title:       p.ObjectAttributes.Title,
		Body:        p.ObjectAttributes.Description,
		IssueRef:    fmt.Sprintf("%s%s%d", p.Project.PathWithNamespace, sep, number),
		URL:         p.ObjectAttributes.URL,
		AuthorLogin: p.User.Username,
		ActorLogin:  p.User.Username,
		Action:      "created",
		Number:      number,
		IsPR:        isPR,
		HeadBranch:  headBranch,
	}
}

// glActionAndLabel normalizes the GitLab action and derives labeled/unlabeled
// plus the single changed label from object_attributes.action + changes.labels.
func glActionAndLabel(p glPayload) (string, string) {
	prev := labelSet(p.Changes.Labels.Previous)
	cur := labelSet(p.Changes.Labels.Current)
	for name := range cur {
		if !prev[name] {
			return "labeled", name
		}
	}
	for name := range prev {
		if !cur[name] {
			return "unlabeled", name
		}
	}
	switch p.ObjectAttributes.Action {
	case "open", "reopen":
		return "opened", ""
	case "close":
		return "closed", ""
	case "update":
		return "synchronize", ""
	case "approved":
		return "submitted", ""
	case "":
		return "other", ""
	default:
		return p.ObjectAttributes.Action, ""
	}
}

func labelSet(ls []glLabel) map[string]bool {
	out := make(map[string]bool, len(ls))
	for _, l := range ls {
		out[l.Title] = true
	}
	return out
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

// Comment posts a note on an issue identified by group/proj#iid.
func (c *GitLab) Comment(ctx context.Context, token, issueRef, body string) error {
	proj, iid, err := glHashRef(issueRef)
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

// GitLabProjectPath parses a GitLab repo URL into its project path.
func GitLabProjectPath(repoURL string) (string, error) { return glProjectPath(repoURL) }

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

// CreateIssue opens an issue and returns its ref + url.
func (c *GitLab) CreateIssue(ctx context.Context, repoURL, token string, req IssueReq) (CreatedIssue, error) {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return CreatedIssue{}, err
	}
	in := map[string]string{"title": req.Title, "description": req.Body}
	if len(req.Labels) > 0 {
		in["labels"] = strings.Join(req.Labels, ",")
	}
	var out struct {
		IID    int    `json:"iid"`
		WebURL string `json:"web_url"`
	}
	path := "/projects/" + url.PathEscape(proj) + "/issues"
	if err := glDo(ctx, c.base(), http.MethodPost, path, token, in, &out); err != nil {
		return CreatedIssue{}, err
	}
	return CreatedIssue{Ref: fmt.Sprintf("%s#%d", proj, out.IID), URL: out.WebURL}, nil
}

// AddLabel adds a label to an issue identified by group/proj#iid.
func (c *GitLab) AddLabel(ctx context.Context, token, issueRef, label string) error {
	proj, iid, err := glHashRef(issueRef)
	if err != nil {
		return err
	}
	path := "/projects/" + url.PathEscape(proj) + "/issues/" + strconv.Itoa(iid)
	return glDo(ctx, c.base(), http.MethodPut, path, token, map[string]string{"add_labels": label}, nil)
}

// RemoveLabel removes a label from an issue identified by group/proj#iid.
func (c *GitLab) RemoveLabel(ctx context.Context, token, issueRef, label string) error {
	proj, iid, err := glHashRef(issueRef)
	if err != nil {
		return err
	}
	path := "/projects/" + url.PathEscape(proj) + "/issues/" + strconv.Itoa(iid)
	return glDo(ctx, c.base(), http.MethodPut, path, token, map[string]string{"remove_labels": label}, nil)
}

// GetPRState reads an MR and its head pipeline status.
func (c *GitLab) GetPRState(ctx context.Context, repoURL, token string, number int) (PRState, error) {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return PRState{}, err
	}
	var mr struct {
		Author struct {
			Username string `json:"username"`
		} `json:"author"`
		SHA          string `json:"sha"`
		SourceBranch string `json:"source_branch"`
		MergeStatus  string `json:"merge_status"`
		HeadPipeline struct {
			Status string `json:"status"`
		} `json:"head_pipeline"`
	}
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number)
	if err := glDo(ctx, c.base(), http.MethodGet, path, token, nil, &mr); err != nil {
		return PRState{}, err
	}
	return PRState{
		Author:     mr.Author.Username,
		HeadSHA:    mr.SHA,
		HeadBranch: mr.SourceBranch,
		Mergeable:  mr.MergeStatus == "can_be_merged",
		CIStatus:   glCIStatus(mr.HeadPipeline.Status),
	}, nil
}

func glCIStatus(s string) string {
	switch s {
	case "":
		return ""
	case "success":
		return "success"
	case "failed", "canceled":
		return "failure"
	default:
		return "pending"
	}
}

// Approve approves an MR.
func (c *GitLab) Approve(ctx context.Context, repoURL, token string, number int, body string) error {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return err
	}
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number) + "/approve"
	if err := glDo(ctx, c.base(), http.MethodPost, path, token, nil, nil); err != nil {
		return err
	}
	if body == "" {
		return nil
	}
	return c.mrNote(ctx, c.base(), proj, number, token, body)
}

// RequestChanges unapproves, awards thumbsdown, and posts a note.
func (c *GitLab) RequestChanges(ctx context.Context, repoURL, token string, number int, body string) error {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return err
	}
	base := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number)
	if err := glDo(ctx, c.base(), http.MethodPost, base+"/unapprove", token, nil, nil); err != nil {
		return err
	}
	if err := glDo(ctx, c.base(), http.MethodPost, base+"/award_emoji", token, map[string]string{"name": "thumbsdown"}, nil); err != nil {
		return err
	}
	if body == "" {
		body = "Requesting changes."
	}
	return c.mrNote(ctx, c.base(), proj, number, token, body)
}

// Suggest posts inline ```suggestion notes on the MR.
func (c *GitLab) Suggest(ctx context.Context, repoURL, token string, number int, sugg []Suggestion) error {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return err
	}
	for _, s := range sugg {
		note := fmt.Sprintf("`%s:%d`\n```suggestion\n%s\n```", s.Path, s.Line, s.Body)
		if err := c.mrNote(ctx, c.base(), proj, number, token, note); err != nil {
			return err
		}
	}
	return nil
}

// Merge merges an MR.
func (c *GitLab) Merge(ctx context.Context, repoURL, token string, number int, method string) error {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return err
	}
	in := map[string]bool{"squash": method == "squash"}
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number) + "/merge"
	return glDo(ctx, c.base(), http.MethodPut, path, token, in, nil)
}

// ClosePR closes an MR (state_event=close) and posts the reason as a note.
func (c *GitLab) ClosePR(ctx context.Context, repoURL, token string, number int, body string) error {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return err
	}
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number)
	if err := glDo(ctx, c.base(), http.MethodPut, path, token, map[string]string{"state_event": "close"}, nil); err != nil {
		return err
	}
	if body == "" {
		return nil
	}
	return c.mrNote(ctx, c.base(), proj, number, token, body)
}

func (c *GitLab) mrNote(ctx context.Context, base, proj string, number int, token, body string) error {
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number) + "/notes"
	return glDo(ctx, base, http.MethodPost, path, token, map[string]string{"body": body}, nil)
}

// glHashRef parses group/proj#iid into project path + iid (issue refs use '#').
func glHashRef(ref string) (string, int, error) {
	at := strings.LastIndex(ref, "#")
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

// glIssueURLRef parses a GitLab issue web URL (.../g/p/-/issues/4) into a
// project path + iid for board label updates.
func glIssueURLRef(itemURL string) (string, int, error) {
	u, err := url.Parse(itemURL)
	if err != nil {
		return "", 0, fmt.Errorf("gitlab: parse item url %q: %w", itemURL, err)
	}
	p := strings.Trim(u.Path, "/")
	idx := strings.Index(p, "/-/issues/")
	if idx < 0 {
		return "", 0, fmt.Errorf("gitlab: not an issue url %q", itemURL)
	}
	proj := p[:idx]
	iid, err := strconv.Atoi(p[idx+len("/-/issues/"):])
	if err != nil {
		return "", 0, fmt.Errorf("gitlab: bad iid in %q: %w", itemURL, err)
	}
	return proj, iid, nil
}

// AddBoardItem ensures the issue carries the board's default list label so it
// appears on the GitLab issue board. No-op semantics beyond the label.
func (c *GitLab) AddBoardItem(ctx context.Context, token string, board BoardRef, itemURL string) error {
	return c.setBoardLabel(ctx, token, itemURL, "Open")
}

// SetBoardColumn swaps the issue's board::<col> scoped label.
func (c *GitLab) SetBoardColumn(ctx context.Context, token string, board BoardRef, itemURL, column string) error {
	return c.setBoardLabel(ctx, token, itemURL, column)
}

func (c *GitLab) setBoardLabel(ctx context.Context, token, itemURL, column string) error {
	proj, iid, err := glIssueURLRef(itemURL)
	if err != nil {
		return err
	}
	path := "/projects/" + url.PathEscape(proj) + "/issues/" + strconv.Itoa(iid)
	return glDo(ctx, c.base(), http.MethodPut, path, token, map[string]string{"add_labels": "board::" + column}, nil)
}

// GetCommitCIStatus returns the CI status for a commit sha by reading its
// statuses. owner is the project path (group/project). Returns "" (none) |
// "pending" | "success" | "failure".
func (c *GitLab) GetCommitCIStatus(ctx context.Context, owner, _ /*repo*/, sha string) (string, error) {
	var statuses []struct {
		Status string `json:"status"`
	}
	path := "/projects/" + url.PathEscape(owner) + "/repository/commits/" + sha + "/statuses"
	if err := glDo(ctx, c.base(), http.MethodGet, path, c.token, nil, &statuses); err != nil {
		return "", err
	}
	if len(statuses) == 0 {
		return "", nil
	}
	// Aggregate: failure > pending > success.
	failure, pending := false, false
	for _, s := range statuses {
		switch glCIStatus(s.Status) {
		case "failure":
			failure = true
		case "pending":
			pending = true
		}
	}
	switch {
	case failure:
		return "failure", nil
	case pending:
		return "pending", nil
	default:
		return "success", nil
	}
}
