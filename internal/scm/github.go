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
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// scmHTTPClient is a shared http.Client with a sane timeout for all SCM REST
// calls. http.DefaultClient has no timeout and can hang indefinitely.
var scmHTTPClient = &http.Client{Timeout: 30 * time.Second}

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
	Comment struct {
		ID   int    `json:"id"`
		Body string `json:"body"`
	} `json:"comment"`
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
		ev.CommentBody = p.Comment.Body
		ev.CommentID = p.Comment.ID
		ev.IsComment = true
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
		Action:       ghNormalizeAction(p.Action),
		Number:       wi.Number,
		IsPR:         isPR,
		HeadSHA:      wi.Head.SHA,
		HeadBranch:   wi.Head.Ref,
		ChangedLabel: p.Label.Name,
	}
}

// ghNormalizeAction maps a raw GitHub action to the same closed label set that
// the GitLab path produces (see glActionAndLabel in gitlab.go). Any action not
// in the known set collapses to "other" so the Prometheus label cardinality is
// bounded regardless of which webhook event types are enabled on the repository.
func ghNormalizeAction(action string) string {
	switch action {
	case "opened", "reopened", "labeled", "unlabeled", "closed",
		"synchronize", "submitted", "created":
		return action
	default:
		return "other"
	}
}

func verifyGitHubSig(header string, payload []byte, secret string) error {
	if secret == "" {
		return errors.New("github: empty webhook secret")
	}
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
	// GitHub repo URLs are exactly owner/repo; deeper paths (enterprise, teams,
	// etc.) must be rejected rather than silently truncated to the last two segments.
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("github: cannot derive owner/repo from %q", repoURL)
	}
	return parts[0], parts[1], nil
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

// ghLinkNext parses the RFC5988 Link header and returns the URL for rel="next", or "".
var ghLinkNextRE = regexp.MustCompile(`<([^>]+)>;\s*rel="next"`)

func ghLinkNext(header string) string {
	m := ghLinkNextRE.FindStringSubmatch(header)
	if m == nil {
		return ""
	}
	return m[1]
}

// maxPaginationPages is an upper bound on the number of pages ghDoPaged and
// glDoPaged will follow.  A legitimate scan across hundreds of open PRs/issues
// should never exceed a few pages at per_page=100; 500 pages is a safety net
// against a self-referential or non-advancing next link.
const maxPaginationPages = 500

// ghDoPaged performs paginated GET requests following Link rel="next" headers.
// It decodes each page into a []T and appends to a single slice returned to the caller.
// in must be nil (GET only). out must be a *[]T.
// A self-referential next URL or more than maxPaginationPages pages is treated
// as an error to prevent infinite loops.
func ghDoPaged[T any](ctx context.Context, base, path, token string) ([]T, error) {
	var all []T
	nextURL := base + path
	for pageNum := 0; pageNum < maxPaginationPages; pageNum++ {
		prevURL := nextURL
		var page []T
		link, err := ghDoWithHeaders(ctx, nextURL, token, &page)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		raw := ghLinkNext(link)
		if raw == "" {
			return all, nil
		}
		if raw == prevURL {
			return nil, fmt.Errorf("github: pagination stuck: next URL equals current URL %q", raw)
		}
		// The next URL from the Link header is absolute and is passed to ghDoWithHeaders as the full request URL.
		nextURL = raw
	}
	return nil, fmt.Errorf("github: pagination exceeded %d pages", maxPaginationPages)
}

// ghDoWithHeaders performs a single GET, decodes JSON body into out, and returns the Link header.
func ghDoWithHeaders(ctx context.Context, fullURL, token string, out any) (linkHeader string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return "", fmt.Errorf("github: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := scmHTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("github: do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	link := resp.Header.Get("Link")
	if resp.StatusCode >= 400 {
		buf, _ := io.ReadAll(resp.Body)
		return "", &HTTPError{Status: resp.StatusCode, Body: string(buf), Path: fullURL}
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil && err != io.EOF {
			return "", fmt.Errorf("github: decode response: %w", err)
		}
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	return link, nil
}

// ghMaxRetries bounds in-process retries of a rate-limited write request.
// ghMaxBackoff caps how long a single retry will wait; if GitHub asks for a
// longer wait we fail fast and let the reconcile requeue (controller-runtime
// backs off) rather than blocking a worker goroutine.
const (
	ghMaxRetries = 3
	ghMaxBackoff = 16 * time.Second
)

// ghRetrySleep waits for d or until ctx is done. It is a package var so tests
// can stub the wait instead of sleeping for real.
var ghRetrySleep = func(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// ghIsRateLimited reports whether a >=400 response is a GitHub rate-limit
// rejection (primary or secondary). GitHub answers rate limits with 429 or 403;
// a plain 403 (permission denied) carries none of the rate-limit signals and is
// NOT treated as one, so genuine auth failures still surface immediately.
func ghIsRateLimited(resp *http.Response, body string) bool {
	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		return true
	case http.StatusForbidden:
		if resp.Header.Get("Retry-After") != "" {
			return true
		}
		if resp.Header.Get("X-RateLimit-Remaining") == "0" {
			return true
		}
		return strings.Contains(strings.ToLower(body), "rate limit")
	default:
		return false
	}
}

// ghRateLimitDelay decides whether a >=400 response is a retryable rate-limit
// rejection and, if so, how long to wait before retrying. Only rate-limit
// responses are retryable: GitHub rejects them unprocessed, so re-sending is
// side-effect-free even for non-idempotent writes (a 5xx, by contrast, may have
// been applied server-side and is never retried here). The wait honors
// Retry-After (seconds), else X-RateLimit-Reset (epoch) when remaining is 0,
// else exponential backoff (1s<<attempt) with jitter to de-correlate a burst.
func ghRateLimitDelay(resp *http.Response, body string, attempt int) (time.Duration, bool) {
	if !ghIsRateLimited(resp, body) {
		return 0, false
	}
	if ra := strings.TrimSpace(resp.Header.Get("Retry-After")); ra != "" {
		if secs, err := strconv.Atoi(ra); err == nil && secs >= 0 {
			return time.Duration(secs) * time.Second, true
		}
	}
	if resp.Header.Get("X-RateLimit-Remaining") == "0" {
		if reset := strings.TrimSpace(resp.Header.Get("X-RateLimit-Reset")); reset != "" {
			if epoch, err := strconv.ParseInt(reset, 10, 64); err == nil {
				if d := time.Until(time.Unix(epoch, 0)); d > 0 {
					return d, true
				}
			}
		}
	}
	base := time.Second << attempt
	return base + ghJitter(base), true
}

// ghJitter returns a random duration in [0, base/2] so concurrent retries
// scatter instead of re-colliding in lockstep.
func ghJitter(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	return rand.N(base/2 + 1)
}

func ghDo(ctx context.Context, base, method, path, token string, in, out any) error {
	var body []byte
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("github: encode body: %w", err)
		}
		body = b
	}
	for attempt := 0; ; attempt++ {
		var rdr io.Reader
		if body != nil {
			rdr = bytes.NewReader(body)
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
		resp, err := scmHTTPClient.Do(req)
		if err != nil {
			return fmt.Errorf("github: do request: %w", err)
		}
		if resp.StatusCode >= 400 {
			buf, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if delay, retry := ghRateLimitDelay(resp, string(buf), attempt); retry && attempt < ghMaxRetries && delay <= ghMaxBackoff {
				slog.WarnContext(ctx, "github: rate-limited, backing off before retry",
					"provider", "github", "method", method, "path", path,
					"status", resp.StatusCode, "attempt", attempt+1, "delay_ms", delay.Milliseconds())
				if serr := ghRetrySleep(ctx, delay); serr != nil {
					return serr
				}
				continue
			}
			return &HTTPError{Status: resp.StatusCode, Body: string(buf), Path: path}
		}
		if out == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return nil
		}
		derr := json.NewDecoder(resp.Body).Decode(out)
		_ = resp.Body.Close()
		if derr != nil && derr != io.EOF {
			return fmt.Errorf("github: decode response: %w", derr)
		}
		return nil
	}
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
	ref := fmt.Sprintf("%s/%s#%d", owner, repo, out.Number)
	return CreatedIssue{Ref: ref, URL: out.HTMLURL}, nil
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
// The label name is URL-encoded so slashes in tatara/* phase labels hit the
// correct DELETE /repos/{owner}/{repo}/issues/{n}/labels/{name} route.
// Removing a label that is not on the issue is a no-op: GitHub answers 404
// "Label does not exist". We swallow that 404 so best-effort sibling-label
// cleanup stays idempotent and benign 404s are not counted as SCM write
// errors (mirrors GitLab.RemoveLabel's idempotent PUT remove_labels=). Real
// failures (401/403/5xx/network) still surface AND are logged at WARN: this is
// best-effort sibling cleanup whose error the caller (setLifecycleLabel)
// tolerates, so without a log here the only trace of a failure is the
// operator_scm_writes_total error increment - which is what made the #161
// remove_label burst undiagnosable. WARN (not ERROR) keeps it out of the
// level="ERROR" log-burst alert while staying queryable in Loki.
func (c *GitHub) RemoveLabel(ctx context.Context, token, issueRef, label string) error {
	owner, repo, number, err := ghIssueRef(issueRef)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/repos/%s/%s/issues/%d/labels/%s", owner, repo, number, url.PathEscape(label))
	if err := ghDo(ctx, c.base(), http.MethodDelete, path, token, nil, nil); err != nil {
		var he *HTTPError
		if errors.As(err, &he) && he.Status == http.StatusNotFound {
			// "Label does not exist": the label was already absent, so the
			// DELETE is a benign no-op. Log at DEBUG so an investigation has a
			// signal without inflating WARN/ERROR or the write-failure metric.
			slog.DebugContext(ctx, "remove label no-op: label not present",
				"provider", "github", "issue_ref", issueRef, "label", label)
			return nil
		}
		slog.WarnContext(ctx, "github: remove label failed",
			"provider", "github", "issue_ref", issueRef, "label", label,
			"status", ErrorStatus(err), "err", err.Error())
		return err
	}
	return nil
}

// EnsureLabel creates the repo label with the given color, or updates its color
// if it already exists (POST -> 422 already-exists -> PATCH color). color is 6
// hex digits without a leading '#'.
func (c *GitHub) EnsureLabel(ctx context.Context, repoURL, token, name, color string) error {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return err
	}
	err = ghDo(ctx, c.base(), http.MethodPost,
		fmt.Sprintf("/repos/%s/%s/labels", owner, repo), token,
		map[string]string{"name": name, "color": color}, nil)
	if err == nil {
		return nil
	}
	var he *HTTPError
	if errors.As(err, &he) && he.Status == http.StatusUnprocessableEntity {
		return ghDo(ctx, c.base(), http.MethodPatch,
			fmt.Sprintf("/repos/%s/%s/labels/%s", owner, repo, url.PathEscape(name)), token,
			map[string]string{"color": color}, nil)
	}
	return err
}

// ghCheckRun is the decoded shape of one GitHub check-run entry.
type ghCheckRun struct {
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}

// GetPRState reads a PR and derives CIStatus by delegating to GetCommitCIStatus,
// which folds both check-runs and the legacy combined-status API. This keeps the
// merge gate and the scan loop consistent across all CI reporting styles.
// GitHub returns mergeable:null while it computes mergeability; the field is
// advisory-only and not part of PRState (see ErrMergeConflict for the merge gate).
func (c *GitHub) GetPRState(ctx context.Context, repoURL, token string, number int) (PRState, error) {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return PRState{}, err
	}
	var pr struct {
		User struct {
			Login string `json:"login"`
		} `json:"user"`
		Head struct {
			SHA string `json:"sha"`
			Ref string `json:"ref"`
		} `json:"head"`
		Merged bool   `json:"merged"`
		State  string `json:"state"` // "open" | "closed"
	}
	if err := ghDo(ctx, c.base(), http.MethodGet, fmt.Sprintf("/repos/%s/%s/pulls/%d", owner, repo, number), token, nil, &pr); err != nil {
		return PRState{}, err
	}
	// Derive CI by folding check-runs and the legacy combined-status endpoint
	// (CI systems that report via Commit Statuses are otherwise invisible from
	// check-runs alone). Use the per-call token, not c.token: the writer client
	// from ByProvider has an empty c.token and supplies the real token per call,
	// so delegating to GetCommitCIStatus (which reads c.token) would issue an
	// unauthenticated request and fail on private repos.
	ciStatus, err := c.commitCIStatus(ctx, owner, repo, pr.Head.SHA, token)
	if err != nil {
		return PRState{}, err
	}
	return PRState{
		Author: pr.User.Login, HeadSHA: pr.Head.SHA, HeadBranch: pr.Head.Ref,
		CIStatus: ciStatus, Merged: pr.Merged, Closed: pr.State == "closed",
	}, nil
}

func deriveGHCIStatus(runs []ghCheckRun) string {
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
// GitHub requires a non-empty body for REQUEST_CHANGES (422 otherwise); default it to match GitLab parity.
func (c *GitHub) RequestChanges(ctx context.Context, repoURL, token string, number int, body string) error {
	if body == "" {
		body = "Requesting changes."
	}
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
// Returns the merge commit SHA on success. Returns ErrMergeConflict when
// GitHub signals the PR is not mergeable (405 Method Not Allowed) or a
// conflict/head mismatch occurred (409 Conflict). The poll model means CI
// must be green before this is called; use pollRequeue to avoid rate-limit
// storms while waiting for CI rather than enabling platform auto-merge.
func (c *GitHub) Merge(ctx context.Context, repoURL, token string, number int, method string) (string, error) {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return "", err
	}
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/merge", owner, repo, number)
	var resp struct {
		SHA string `json:"sha"`
	}
	if err := ghDo(ctx, c.base(), http.MethodPut, path, token, map[string]string{"merge_method": method}, &resp); err != nil {
		var he *HTTPError
		if errors.As(err, &he) && (he.Status == 405 || he.Status == 409) {
			return "", ErrMergeConflict
		}
		return "", err
	}
	return resp.SHA, nil
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

// GetCommitCIStatus returns the CI status for the given commit sha by reading
// both check-runs and the legacy combined-status API and folding the results:
//   - failure if either source reports failure
//   - pending if either source reports pending (and neither reports failure)
//   - success if all present signals report success
//   - "" if neither source has any data
//
// Returns "" (none) | "pending" | "success" | "failure".
func (c *GitHub) GetCommitCIStatus(ctx context.Context, owner, repo, sha string) (string, error) {
	return c.commitCIStatus(ctx, owner, repo, sha, c.token)
}

// commitCIStatus is the token-parameterized implementation behind both
// GetCommitCIStatus (reader path, token=c.token) and GetPRState (writer path,
// per-call token). Keeping the token explicit prevents the empty-c.token writer
// client from issuing unauthenticated CI requests.
func (c *GitHub) commitCIStatus(ctx context.Context, owner, repo, sha, token string) (string, error) {
	var checks struct {
		CheckRuns []ghCheckRun `json:"check_runs"`
	}
	checkPath := fmt.Sprintf("/repos/%s/%s/commits/%s/check-runs", owner, repo, sha)
	if err := ghDo(ctx, c.base(), http.MethodGet, checkPath, token, nil, &checks); err != nil {
		return "", err
	}
	checkStatus := deriveGHCIStatus(checks.CheckRuns)

	// Also read the legacy combined-status endpoint so CI systems that report via
	// Commit Statuses (not Check Runs) are visible.
	var combined struct {
		State      string `json:"state"`
		TotalCount int    `json:"total_count"`
	}
	statusPath := fmt.Sprintf("/repos/%s/%s/commits/%s/status", owner, repo, sha)
	if err := ghDo(ctx, c.base(), http.MethodGet, statusPath, token, nil, &combined); err != nil {
		return "", err
	}
	// combined.State is always present (even "pending" when TotalCount==0).
	// Treat TotalCount==0 as "no data" (same as "" from check-runs).
	combinedStatus := ""
	if combined.TotalCount > 0 {
		switch combined.State {
		case "success":
			combinedStatus = "success"
		case "pending":
			combinedStatus = "pending"
		case "failure", "error":
			combinedStatus = "failure"
		default:
			// Unknown state with statuses present: fail-safe to "pending" so an
			// unrecognised GitHub state never silently becomes no-data on the
			// merge gate (unlike deriveGHCIStatus which fails closed).
			combinedStatus = "pending"
		}
	}

	return foldCIStatuses(checkStatus, combinedStatus), nil
}

// foldCIStatuses merges two CI status strings with the precedence rule:
// failure > pending > success > "" (none).
func foldCIStatuses(a, b string) string {
	if a == "failure" || b == "failure" {
		return "failure"
	}
	if a == "pending" || b == "pending" {
		return "pending"
	}
	if a == "success" || b == "success" {
		return "success"
	}
	return ""
}
