package scm

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	Before     string `json:"before"`
	After      string `json:"after"`
	User       struct {
		Username string `json:"username"`
	} `json:"user"`
	Project struct {
		GitHTTPURL        string `json:"git_http_url"`
		PathWithNamespace string `json:"path_with_namespace"`
	} `json:"project"`
	ObjectAttributes struct {
		IID          int    `json:"iid"`
		NoteID       int    `json:"id"` // note comment id (Note Hook only; distinct from iid)
		Title        string `json:"title"`
		Description  string `json:"description"`
		Note         string `json:"note"`
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
	if secret == "" {
		return WebhookEvent{}, errors.New("gitlab: empty webhook secret")
	}
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
		return WebhookEvent{Kind: "push", Repo: p.Project.GitHTTPURL, Branch: trimGitLabRef(p.Ref), HeadSHA: p.After, BaseSHA: p.Before}, nil
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
		CommentBody: p.ObjectAttributes.Note,
		CommentID:   p.ObjectAttributes.NoteID,
		IsComment:   true,
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
// Single-event limitation: when a GitLab webhook atomically adds and removes
// labels in one update, only the first added label is reported as "labeled";
// the removed label's "unlabeled" event is not surfaced. Tatara's phase-label
// dedup keys on label presence (not events), so this has no functional impact.
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
		// Return "other" rather than the raw action to keep the metric label a closed set.
		return "other", ""
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

// Comment posts a note on the work item identified by the ref. A '!' ref
// (group/proj!iid) targets a merge request; a '#' ref (group/proj#iid) targets
// an issue. The ref is the single source of truth: GitLab issues and MRs have
// distinct note endpoints, unlike GitHub where both share /issues/{n}/comments.
// Routing uses LastIndex to match glBangRef/glHashRef semantics so project paths
// containing '!' are not misrouted.
func (c *GitLab) Comment(ctx context.Context, token, issueRef, body string) error {
	bangAt := strings.LastIndex(issueRef, "!")
	hashAt := strings.LastIndex(issueRef, "#")
	if bangAt > hashAt {
		proj, iid, err := glBangRef(issueRef)
		if err != nil {
			return err
		}
		return c.mrNote(ctx, c.base(), proj, iid, token, body)
	}
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

// glDoPaged performs paginated GET requests following the X-Next-Page header.
// It decodes each page into a []T and returns all pages concatenated.
// A non-advancing page number or more than maxPaginationPages pages is treated
// as an error to prevent infinite loops.
func glDoPaged[T any](ctx context.Context, base, path, token string) ([]T, error) {
	var all []T
	nextPath := path
	prevPage := ""
	for pageNum := 0; nextPath != "" && pageNum < maxPaginationPages; pageNum++ {
		var page []T
		nextPage, err := glDoWithHeaders(ctx, base, nextPath, token, &page)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		if nextPage == "" {
			break
		}
		if nextPage == prevPage {
			return nil, fmt.Errorf("gitlab: pagination stuck: X-Next-Page %q did not advance", nextPage)
		}
		prevPage = nextPage
		// Build the next path preserving existing query params and adding/updating page.
		u, err := url.Parse(nextPath)
		if err != nil {
			return nil, fmt.Errorf("gitlab: build next page url %q: %w", nextPath, err)
		}
		q := u.Query()
		q.Set("page", nextPage)
		u.RawQuery = q.Encode()
		nextPath = u.String()
	}
	return all, nil
}

// glDoWithHeaders performs a single GET and returns (X-Next-Page header, error).
func glDoWithHeaders(ctx context.Context, base, path, token string, out any) (nextPage string, err error) {
	return doPagedGET(ctx, base+path, "gitlab", path, map[string]string{
		"PRIVATE-TOKEN": token,
		"Accept":        "application/json",
	}, "X-Next-Page", out)
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
	if werr := waitEgress(ctx, "gitlab", path); werr != nil {
		return werr
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
	resp, err := scmHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("gitlab: do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		buf, _ := io.ReadAll(resp.Body)
		if ghIsRateLimited(resp, string(buf)) {
			recordRateLimited("gitlab", path, resp, string(buf))
			return rateLimitedError(resp.StatusCode, string(buf), path)
		}
		return &HTTPError{Status: resp.StatusCode, Body: string(buf), Path: path}
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil && err != io.EOF {
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
	ref := fmt.Sprintf("%s#%d", proj, out.IID)
	return CreatedIssue{Ref: ref, URL: out.WebURL}, nil
}

// AddLabel adds a label to the work item identified by ref. A '!' ref
// (group/proj!iid) targets a merge request; a '#' ref (group/proj#iid) targets an
// issue (glLabelRef routes it). Issue #301: MR label writes used to fail
// "malformed issue ref" because this path only understood '#'.
func (c *GitLab) AddLabel(ctx context.Context, token, issueRef, label string) error {
	proj, iid, resource, err := glLabelRef(issueRef)
	if err != nil {
		return err
	}
	path := "/projects/" + url.PathEscape(proj) + "/" + resource + "/" + strconv.Itoa(iid)
	return glDo(ctx, c.base(), http.MethodPut, path, token, map[string]string{"add_labels": label}, nil)
}

// EnsureLabel creates the project label with the given color, or updates its
// color if it already exists (POST -> 409 conflict -> PUT color). color is 6
// hex digits without '#'; GitLab wants the leading '#', added here.
func (c *GitLab) EnsureLabel(ctx context.Context, repoURL, token, name, color string) error {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return err
	}
	hexColor := "#" + color
	return createOrUpdateOnConflict(
		func() error {
			return glDo(ctx, c.base(), http.MethodPost,
				"/projects/"+url.PathEscape(proj)+"/labels", token,
				map[string]string{"name": name, "color": hexColor}, nil)
		},
		http.StatusConflict,
		func() error {
			return glDo(ctx, c.base(), http.MethodPut,
				"/projects/"+url.PathEscape(proj)+"/labels/"+url.PathEscape(name), token,
				map[string]string{"color": hexColor}, nil)
		},
	)
}

// RemoveLabel removes a label from the work item identified by ref. Routing
// mirrors AddLabel: a '!' ref targets a merge request, a '#' ref an issue.
func (c *GitLab) RemoveLabel(ctx context.Context, token, issueRef, label string) error {
	proj, iid, resource, err := glLabelRef(issueRef)
	if err != nil {
		return err
	}
	path := "/projects/" + url.PathEscape(proj) + "/" + resource + "/" + strconv.Itoa(iid)
	return glDo(ctx, c.base(), http.MethodPut, path, token, map[string]string{"remove_labels": label}, nil)
}

// GetPRState reads an MR and its head pipeline status.
// When head_pipeline is null (pipeline not yet attached) or its SHA does not
// match the MR's current SHA (stale pipeline), the status falls back to the
// commit-statuses endpoint so external CI reporters are visible and the merge
// gate does not act on a stale success.
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
		State        string `json:"state"`
		HeadPipeline *struct {
			SHA    string `json:"sha"`
			Status string `json:"status"`
		} `json:"head_pipeline"`
	}
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number)
	if err := glDo(ctx, c.base(), http.MethodGet, path, token, nil, &mr); err != nil {
		return PRState{}, err
	}

	ciStatus := ""
	switch {
	case mr.HeadPipeline == nil:
		// No pipeline attached yet; fall back to commit statuses so external CI
		// reporters (commit-status-only) are visible through the merge gate. Use
		// the per-call token, not c.token: the writer client from ByProvider has
		// an empty c.token, so GetCommitCIStatus (which reads c.token) would issue
		// an unauthenticated read and 401 on private projects.
		ciStatus, err = c.commitCIStatus(ctx, proj, mr.SHA, token)
		if err != nil {
			return PRState{}, err
		}
	case mr.HeadPipeline.SHA != "" && mr.HeadPipeline.SHA != mr.SHA:
		// Pipeline is present but belongs to a previous commit (lag window after
		// a push). Treat as pending so the merge gate waits for the new pipeline
		// rather than acting on a stale success.
		ciStatus = "pending"
	default:
		// Pipeline is for the current SHA (or SHA field not reported by the API).
		ciStatus = glCIStatus(mr.HeadPipeline.Status)
	}

	return PRState{
		Author:     mr.Author.Username,
		HeadSHA:    mr.SHA,
		HeadBranch: mr.SourceBranch,
		CIStatus:   ciStatus,
		Merged:     mr.State == "merged",
		Closed:     mr.State == "closed",
	}, nil
}

// GetIssueState resolves an issue's author and closed state using the
// per-call token (not c.token), matching GetPRState's writer-client contract.
func (c *GitLab) GetIssueState(ctx context.Context, repoURL, token string, number int) (IssueState, error) {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return IssueState{}, err
	}
	var raw struct {
		Author struct {
			Username string `json:"username"`
		} `json:"author"`
		State string `json:"state"`
	}
	path := "/projects/" + url.PathEscape(proj) + "/issues/" + strconv.Itoa(number)
	if err := glDo(ctx, c.base(), http.MethodGet, path, token, nil, &raw); err != nil {
		return IssueState{}, err
	}
	return IssueState{Author: raw.Author.Username, Closed: raw.State == "closed"}, nil
}

func glCIStatus(s string) string {
	switch s {
	case "":
		return ""
	case "success":
		return "success"
	case "failed", "canceled":
		return "failure"
	// skipped pipelines are effectively neutral (GitHub treats skipped/neutral as success).
	// manual/scheduled/waiting_for_resource/preparing pipelines have not started yet; map to pending.
	case "skipped":
		return "success"
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
		// GitLab returns 401 from /approve when the caller has ALREADY approved the
		// MR (idempotency-via-error). For the review bot the approval already
		// stands, so this is benign: mirror RequestChanges tolerating the 404 from
		// /unapprove and fall through to the optional note. Any other status aborts
		// (a genuine auth failure also breaks reads/comments and surfaces there).
		var he *HTTPError
		if !errors.As(err, &he) || he.Status != http.StatusUnauthorized {
			return err
		}
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
		// GitLab returns 404 from unapprove when the caller never approved the MR
		// (the common case for the review bot). Nothing to unapprove is benign;
		// proceed to the thumbsdown + note. Other failures still abort.
		var he *HTTPError
		if !errors.As(err, &he) || he.Status != http.StatusNotFound {
			return err
		}
	}
	if err := glDo(ctx, c.base(), http.MethodPost, base+"/award_emoji", token, map[string]string{"name": "thumbsdown"}, nil); err != nil {
		// GitLab returns 404 ("Award Emoji Name has already been taken") when the
		// thumbsdown was already awarded on a prior request-changes pass. Benign
		// idempotency (mirrors the unapprove 404 above): proceed to the note.
		// Other failures still abort.
		var he *HTTPError
		if !errors.As(err, &he) || he.Status != http.StatusNotFound {
			return err
		}
	}
	if body == "" {
		body = "Requesting changes."
	}
	return c.mrNote(ctx, c.base(), proj, number, token, body)
}

// Suggest posts inline ```suggestion notes on the MR. Notes are posted
// one-by-one (GitLab has no batch review API). On the first error the
// remaining suggestions are abandoned and the partial-post count is logged
// so callers can reason about retries (retrying re-posts already-posted
// suggestions as duplicates since there is no idempotency key).
func (c *GitLab) Suggest(ctx context.Context, repoURL, token string, number int, sugg []Suggestion) error {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return err
	}
	for i, s := range sugg {
		note := fmt.Sprintf("`%s:%d`\n```suggestion\n%s\n```", s.Path, s.Line, s.Body)
		if err := c.mrNote(ctx, c.base(), proj, number, token, note); err != nil {
			slog.WarnContext(ctx, "suggest partial failure", "provider", "gitlab", "resource_id", fmt.Sprintf("%s!%d", proj, number), "posted", i, "total", len(sugg), "error", err.Error())
			return err
		}
	}
	return nil
}

// Merge merges an MR. Returns the merge commit SHA on success. Returns
// ErrMergeConflict when GitLab signals the MR is not mergeable (405/406/409).
// method must be "squash" or "merge"; GitLab does not support "rebase" via the
// merge endpoint and treating it silently as a non-squash merge would be wrong.
// expectedHeadSHA, when non-empty, is sent as `sha`: GitLab refuses the merge with
// a 409 ("SHA does not match HEAD of source branch") if the head moved between the
// review and the merge, and that 409 maps to ErrHeadMoved rather than
// ErrMergeConflict so the caller re-reviews the new head. This closes the TOCTOU
// on the forge that owns tatara-helmfile, whose deploy runner is cluster-admin.
// An empty expectedHeadSHA means "no pin" and preserves the pre-pin behaviour.
func (c *GitLab) Merge(ctx context.Context, repoURL, token string, number int, method, expectedHeadSHA string) (string, error) {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return "", err
	}
	switch method {
	case "squash", "merge", "":
		// supported
	default:
		return "", fmt.Errorf("gitlab: unsupported merge method %q (use \"squash\" or \"merge\")", method)
	}
	in := map[string]any{"squash": method == "squash"}
	if expectedHeadSHA != "" {
		in["sha"] = expectedHeadSHA
	}
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number) + "/merge"
	var resp struct {
		MergeCommitSHA string `json:"merge_commit_sha"`
	}
	if err := glDo(ctx, c.base(), http.MethodPut, path, token, in, &resp); err != nil {
		var he *HTTPError
		if errors.As(err, &he) && !errors.Is(err, ErrRateLimited) {
			if he.Status == http.StatusConflict && expectedHeadSHA != "" {
				return "", fmt.Errorf("%w: %w", ErrHeadMoved, err)
			}
			if he.Status == http.StatusMethodNotAllowed || he.Status == http.StatusNotAcceptable || he.Status == http.StatusConflict {
				return "", ErrMergeConflict
			}
		}
		return "", err
	}
	return resp.MergeCommitSHA, nil
}

// EnableAutoMerge sets merge-when-pipeline-succeeds on the MR at mrURL, so GitLab
// merges it once its pipeline passes. Best-effort: the endpoint can 405 when no
// pipeline exists yet, which callers treat as non-fatal.
func (c *GitLab) EnableAutoMerge(ctx context.Context, repoURL, token, mrURL, method string) error {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return err
	}
	iid, err := glIIDFromURL(mrURL)
	if err != nil {
		return err
	}
	in := map[string]bool{"merge_when_pipeline_succeeds": true, "squash": method == "squash"}
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(iid) + "/merge"
	return glDo(ctx, c.base(), http.MethodPut, path, token, in, nil)
}

// DisableAutoMerge cancels merge-when-pipeline-succeeds on the MR at mrURL, so
// an MR the operator has decided must not ship cannot merge itself once its
// pipeline passes. Best-effort: the endpoint 406s when the MR has no pending
// auto-merge, which callers treat as non-fatal.
func (c *GitLab) DisableAutoMerge(ctx context.Context, repoURL, token, mrURL string) error {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return err
	}
	iid, err := glIIDFromURL(mrURL)
	if err != nil {
		return err
	}
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(iid) + "/cancel_merge_when_pipeline_succeeds"
	return glDo(ctx, c.base(), http.MethodPost, path, token, nil, nil)
}

// glIIDFromURL parses the trailing iid out of a GitLab MR web URL
// (.../merge_requests/5, with optional trailing slash or ?#fragment).
func glIIDFromURL(mrURL string) (int, error) {
	s := mrURL
	if i := strings.IndexAny(s, "?#"); i >= 0 {
		s = s[:i]
	}
	s = strings.TrimRight(s, "/")
	i := strings.LastIndex(s, "/")
	if i < 0 || i+1 >= len(s) {
		return 0, fmt.Errorf("gitlab: cannot parse iid from %q", mrURL)
	}
	return strconv.Atoi(s[i+1:])
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

// GetMergeState reads the MR's merge_status + has_conflicts and maps them to a
// provider-neutral MergeState. cannot_be_merged with conflicts is a real merge
// conflict (dirty); without conflicts it is mergeable-blocked; unchecked and
// checking are not yet computed.
func (c *GitLab) GetMergeState(ctx context.Context, repoURL, token string, number int) (MergeState, error) {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return MergeStateUnknown, fmt.Errorf("gitlab: get merge state: %w", err)
	}
	var mr struct {
		MergeStatus  string `json:"merge_status"`
		HasConflicts bool   `json:"has_conflicts"`
	}
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number)
	if err := glDo(ctx, c.base(), http.MethodGet, path, token, nil, &mr); err != nil {
		return MergeStateUnknown, fmt.Errorf("gitlab: get merge state: %w", err)
	}
	return glMergeState(mr.MergeStatus, mr.HasConflicts), nil
}

func glMergeState(status string, hasConflicts bool) MergeState {
	switch status {
	case "can_be_merged":
		return MergeStateClean
	case "cannot_be_merged", "cannot_be_merged_recheck":
		if hasConflicts {
			return MergeStateDirty
		}
		return MergeStateBlocked
	default: // unchecked, checking, "" - not yet computed
		return MergeStateUnknown
	}
}

func (c *GitLab) mrNote(ctx context.Context, base, proj string, number int, token, body string) error {
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number) + "/notes"
	return glDo(ctx, base, http.MethodPost, path, token, map[string]string{"body": body}, nil)
}

// glParseRef parses group/proj<sep>iid into project path + iid; kind names
// the ref kind ("issue" or "mr") for the error text.
func glParseRef(ref string, sep byte, kind string) (string, int, error) {
	at := strings.LastIndexByte(ref, sep)
	if at < 0 {
		return "", 0, fmt.Errorf("gitlab: malformed %s ref %q", kind, ref)
	}
	proj, iidStr := ref[:at], ref[at+1:]
	if proj == "" {
		return "", 0, fmt.Errorf("gitlab: malformed %s ref %q", kind, ref)
	}
	iid, err := strconv.Atoi(iidStr)
	if err != nil {
		return "", 0, fmt.Errorf("gitlab: malformed iid in %q: %w", ref, err)
	}
	return proj, iid, nil
}

// glHashRef parses group/proj#iid into project path + iid (issue refs use '#').
func glHashRef(ref string) (string, int, error) { return glParseRef(ref, '#', "issue") }

// glBangRef parses group/proj!iid into project path + iid (MR refs use '!').
func glBangRef(ref string) (string, int, error) { return glParseRef(ref, '!', "mr") }

// glLabelRef parses a work-item ref into the project path, iid, and GitLab REST
// resource segment ("merge_requests" or "issues") for a label write. A '!' ref
// (group/proj!iid) targets a merge request; a '#' ref (group/proj#iid) an issue.
// Issues and MRs have separate iid spaces and endpoints, so routing an MR ref to
// /issues mislabels or 404s. Routing mirrors Comment (LastIndex so project paths
// containing the sigil are not misrouted).
func glLabelRef(ref string) (proj string, iid int, resource string, err error) {
	if strings.LastIndex(ref, "!") > strings.LastIndex(ref, "#") {
		proj, iid, err = glBangRef(ref)
		return proj, iid, "merge_requests", err
	}
	proj, iid, err = glHashRef(ref)
	return proj, iid, "issues", err
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
// All pages are fetched (per_page=100) so a failing status beyond the
// default 20-item first page is not missed.
func (c *GitLab) GetCommitCIStatus(ctx context.Context, owner, _ /*repo*/, sha string) (string, error) {
	return c.commitCIStatus(ctx, owner, sha, c.token)
}

// commitCIStatus is the token-explicit core of GetCommitCIStatus, shared by
// GetCommitCIStatus (reader path, token=c.token) and GetPRState (writer path,
// per-call token). Keeping the token explicit prevents the empty-c.token writer
// client (from ByProvider) issuing an unauthenticated statuses read - which
// would 401 on private projects and break the merge gate when an MR has no head
// pipeline yet.
func (c *GitLab) commitCIStatus(ctx context.Context, owner, sha, token string) (string, error) {
	type statusItem struct {
		Status string `json:"status"`
	}
	path := "/projects/" + url.PathEscape(owner) + "/repository/commits/" + url.PathEscape(sha) + "/statuses?per_page=100"
	statuses, err := glDoPaged[statusItem](ctx, c.base(), path, token)
	if err != nil {
		return "", err
	}
	if len(statuses) == 0 {
		return "", nil
	}
	// Aggregate through the shared failure > pending > success > "" reducer.
	mapped := make([]string, len(statuses))
	for i, s := range statuses {
		mapped[i] = glCIStatus(s.Status)
	}
	return foldCIStatuses(mapped...), nil
}
