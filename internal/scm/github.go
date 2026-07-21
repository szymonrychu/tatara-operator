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
	Merged      bool `json:"merged"` // pull_request.merged: true when a PR-close delivery is a merge
	PullRequest *struct {
		URL string `json:"url"`
	} `json:"pull_request"`
}

type ghPayload struct {
	Action     string `json:"action"`
	Ref        string `json:"ref"`
	Before     string `json:"before"`
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
	Review      struct {
		ID       int64  `json:"id"`
		State    string `json:"state"`
		CommitID string `json:"commit_id"`
		Body     string `json:"body"`
	} `json:"review"`
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
		return WebhookEvent{Kind: "push", Repo: p.Repository.CloneURL, Branch: strings.TrimPrefix(p.Ref, "refs/heads/"), HeadSHA: p.After, BaseSHA: p.Before}, nil
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
	case "pull_request_review_comment":
		// WS3-I1: a human reply on an inline review thread. It is a plain MR
		// comment (mirror + pending event, NEVER a verdict), distinct from
		// pull_request_review. Only action=created acts; edited/deleted are ignored.
		if p.Action != "created" || p.PullRequest == nil {
			return WebhookEvent{Kind: "other"}, nil
		}
		ev := ghWorkItemEvent("mr", true, p, p.PullRequest)
		ev.CommentBody = p.Comment.Body
		ev.CommentID = p.Comment.ID
		ev.IsComment = true
		return ev, nil
	case "pull_request_review":
		ev := ghWorkItemEvent("mr", true, p, p.PullRequest)
		ev.IsReview = true
		ev.ReviewState = p.Review.State // github vocab already: approved|changes_requested|commented|dismissed
		ev.ReviewID = strconv.FormatInt(p.Review.ID, 10)
		ev.ReviewCommitSHA = p.Review.CommitID
		ev.CommentBody = p.Review.Body // carries the review text through the folded pending-event path
		return ev, nil
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
		Merged:       wi.Merged,
	}
}

// ghNormalizeAction maps a raw GitHub action to the same closed label set that
// the GitLab path produces (see glActionAndLabel in gitlab.go). Any action not
// in the known set collapses to "other" so the Prometheus label cardinality is
// bounded regardless of which webhook event types are enabled on the repository.
func ghNormalizeAction(action string) string {
	switch action {
	case "opened", "reopened", "labeled", "unlabeled", "closed",
		"synchronize", "submitted", "created", "edited":
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

// AddSubIssue makes childNumber a sub-issue of parentRef via GitHub's sub-issues
// API. It resolves the child's numeric id (the API body param is sub_issue_id,
// NOT the number) and pre-checks the parent's child count (<100). Any 4xx (cap,
// 403 cross-repo/org, unique-parent conflict) surfaces so the caller can fall
// back to a cross-reference comment.
func (c *GitHub) AddSubIssue(ctx context.Context, token, parentRef string, childNumber int) error {
	owner, repo, parent, err := ghIssueRef(parentRef)
	if err != nil {
		return err
	}
	var child struct {
		ID int64 `json:"id"`
	}
	childPath := fmt.Sprintf("/repos/%s/%s/issues/%d", owner, repo, childNumber)
	if err := ghDo(ctx, c.base(), http.MethodGet, childPath, token, nil, &child); err != nil {
		return fmt.Errorf("github: resolve sub-issue child %s/%s#%d id: %w", owner, repo, childNumber, err)
	}
	if child.ID == 0 {
		return fmt.Errorf("github: sub-issue child %s/%s#%d returned no id", owner, repo, childNumber)
	}
	var summary struct {
		Total int `json:"total"`
	}
	sumPath := fmt.Sprintf("/repos/%s/%s/issues/%d/sub_issues_summary", owner, repo, parent)
	if err := ghDo(ctx, c.base(), http.MethodGet, sumPath, token, nil, &summary); err != nil {
		return fmt.Errorf("github: read parent %s/%s#%d sub-issues summary: %w", owner, repo, parent, err)
	}
	if summary.Total >= 100 {
		return fmt.Errorf("github: parent %s/%s#%d already holds %d sub-issues (max 100)", owner, repo, parent, summary.Total)
	}
	path := fmt.Sprintf("/repos/%s/%s/issues/%d/sub_issues", owner, repo, parent)
	return ghDo(ctx, c.base(), http.MethodPost, path, token, map[string]int64{"sub_issue_id": child.ID}, nil)
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
	return doPagedGET(ctx, fullURL, "github", fullURL, map[string]string{
		"Authorization": "Bearer " + token,
		"Accept":        "application/vnd.github+json",
	}, "Link", out)
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
		if werr := waitEgress(ctx, "github", path); werr != nil {
			return werr
		}
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
			if !ghIsRateLimited(resp, string(buf)) {
				return &HTTPError{Status: resp.StatusCode, Body: string(buf), Path: path}
			}
			recordRateLimited("github", path, resp, string(buf))
			delay, _ := ghRateLimitDelay(resp, string(buf), attempt)
			if attempt < ghMaxRetries && delay <= ghMaxBackoff {
				slog.WarnContext(ctx, "github: rate-limited, backing off before retry",
					"provider", "github", "method", method, "path", path,
					"status", resp.StatusCode, "limit_type", limitType(resp, string(buf)),
					"attempt", attempt+1, "delay_ms", delay.Milliseconds())
				if serr := ghRetrySleep(ctx, delay); serr != nil {
					return serr
				}
				continue
			}
			return rateLimitedError(resp.StatusCode, string(buf), path)
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
	return createOrUpdateOnConflict(
		func() error {
			return ghDo(ctx, c.base(), http.MethodPost,
				fmt.Sprintf("/repos/%s/%s/labels", owner, repo), token,
				map[string]string{"name": name, "color": color}, nil)
		},
		http.StatusUnprocessableEntity,
		func() error {
			return ghDo(ctx, c.base(), http.MethodPatch,
				fmt.Sprintf("/repos/%s/%s/labels/%s", owner, repo, url.PathEscape(name)), token,
				map[string]string{"color": color}, nil)
		},
	)
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

// GetIssueState resolves an issue's author and closed state using the
// per-call token (not c.token), matching GetPRState's writer-client contract.
func (c *GitHub) GetIssueState(ctx context.Context, repoURL, token string, number int) (IssueState, error) {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return IssueState{}, err
	}
	var raw ghIssueItem
	path := fmt.Sprintf("/repos/%s/%s/issues/%d", owner, repo, number)
	if err := ghDo(ctx, c.base(), http.MethodGet, path, token, nil, &raw); err != nil {
		return IssueState{}, err
	}
	return IssueState{Author: raw.User.Login, Closed: raw.State == "closed"}, nil
}

// ghRunCIStatus maps one GitHub check-run to the shared "" | pending |
// success | failure vocabulary: any non-completed run is pending regardless
// of conclusion; a completed run is a failure unless its conclusion is
// success/neutral/skipped.
func ghRunCIStatus(run ghCheckRun) string {
	if run.Status != "completed" {
		return "pending"
	}
	if run.Conclusion != "success" && run.Conclusion != "neutral" && run.Conclusion != "skipped" {
		return "failure"
	}
	return "success"
}

func deriveGHCIStatus(runs []ghCheckRun) string {
	if len(runs) == 0 {
		return ""
	}
	statuses := make([]string, len(runs))
	for i, run := range runs {
		statuses[i] = ghRunCIStatus(run)
	}
	return foldCIStatuses(statuses...)
}

// review POSTs a review and returns the review id GitHub assigned it. The
// response body used to be thrown away (ghDo(..., nil)), which is why no SCM
// method returned an id at all; PostReview needs one, so it is decoded here and
// the id-free callers simply ignore it.
func (c *GitHub) review(ctx context.Context, repoURL, token string, number int, event, body string, comments []map[string]any) (string, error) {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return "", err
	}
	in := map[string]any{"event": event}
	if body != "" {
		in["body"] = body
	}
	if comments != nil {
		in["comments"] = comments
	}
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/reviews", owner, repo, number)
	var out struct {
		ID int64 `json:"id"`
	}
	if err := ghDo(ctx, c.base(), http.MethodPost, path, token, in, &out); err != nil {
		return "", err
	}
	return strconv.FormatInt(out.ID, 10), nil
}

// Approve posts an APPROVE review.
//
// GitHub forbids approving your own pull request: a bot-authored PR returns a
// deterministic 422 ("Can not approve your own pull request"). That is a terminal
// client error, not a transient one, so retrying it is pointless and floods the
// SCM write-failure alert forever (issue #301). Tolerate that specific 422 as a
// benign no-op - mirroring GitLab.Approve tolerating the already-approved 401. A
// bot PR does not need a native GitHub approval to merge: the tatara-approved
// managed label gates the deploy supervisor and native auto-merge, so the caller
// proceeds to label it. Any other error (including an unrelated 422) still surfaces.
func (c *GitHub) Approve(ctx context.Context, repoURL, token string, number int, body string) error {
	_, err := c.review(ctx, repoURL, token, number, "APPROVE", body, nil)
	var he *HTTPError
	if errors.As(err, &he) && he.Status == http.StatusUnprocessableEntity &&
		strings.Contains(he.Body, "Can not approve your own pull request") {
		return nil
	}
	return err
}

// RequestChanges posts a REQUEST_CHANGES review.
// GitHub requires a non-empty body for REQUEST_CHANGES (422 otherwise); default it to match GitLab parity.
func (c *GitHub) RequestChanges(ctx context.Context, repoURL, token string, number int, body string) error {
	if body == "" {
		body = "Requesting changes."
	}
	_, err := c.review(ctx, repoURL, token, number, "REQUEST_CHANGES", body, nil)
	return err
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
	_, err := c.review(ctx, repoURL, token, number, reviewEventComment, "", comments)
	return err
}

// GetPRHead reads the LIVE head sha of a PR.
func (c *GitHub) GetPRHead(ctx context.Context, repoURL, token string, number int) (string, error) {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return "", err
	}
	var pr struct {
		Head struct {
			SHA string `json:"sha"`
		} `json:"head"`
	}
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d", owner, repo, number)
	if err := ghDo(ctx, c.base(), http.MethodGet, path, token, nil, &pr); err != nil {
		return "", err
	}
	if pr.Head.SHA == "" {
		return "", fmt.Errorf("github: pr %s/%s#%d returned no head sha", owner, repo, number)
	}
	return pr.Head.SHA, nil
}

// ListReviews returns the PR's reviews. This is the forge-side idempotency check
// behind the review post: a body already carrying the round marker means the
// post landed, even if the process died before recording that it had.
func (c *GitHub) ListReviews(ctx context.Context, repoURL, token string, number int) ([]Review, error) {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return nil, err
	}
	type ghReview struct {
		ID          int64     `json:"id"`
		Body        string    `json:"body"`
		State       string    `json:"state"`
		CommitID    string    `json:"commit_id"`
		SubmittedAt time.Time `json:"submitted_at"`
		User        struct {
			Login string `json:"login"`
		} `json:"user"`
	}
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/reviews?per_page=100", owner, repo, number)
	raw, err := ghDoPaged[ghReview](ctx, c.base(), path, token)
	if err != nil {
		return nil, err
	}
	out := make([]Review, 0, len(raw))
	for _, r := range raw {
		out = append(out, Review{
			ID:        strconv.FormatInt(r.ID, 10),
			Body:      r.Body,
			State:     r.State,
			CommitID:  r.CommitID,
			Author:    r.User.Login,
			CreatedAt: r.SubmittedAt,
		})
	}
	return out, nil
}

// PostReview posts the review body plus its inline findings in ONE atomic call
// and returns the review id. The event is always COMMENT (reviewEventComment):
// with one bot identity GitHub 422s both APPROVE and REQUEST_CHANGES on a
// self-authored PR, so those events are not in the enum at all and no code path
// can produce them.
//
// Because the create-review call is atomic, one round marker in the body is a
// truthful "everything for this round landed" flag - which is what makes the
// GitHub post crash-safe with a single marker, and why GitLab (N+1 calls) cannot
// use the same scheme.
//
// The response is the REVIEW object; it does NOT carry the created inline
// comments. Their ids come from ListReviewComments, a second, separate read.
func (c *GitHub) PostReview(ctx context.Context, repoURL, token string, number int, body string, findings []ReviewFinding) (string, error) {
	comments := make([]map[string]any, 0, len(findings))
	for _, f := range findings {
		if f.Line <= 0 {
			// A finding with no line (a file-level finding, #398; the CR->scm bridge
			// lowers a nil *int to 0) can NOT be a line-anchored review comment - GitHub
			// 422s a line=0 comment. Post it as a file-level comment instead.
			comments = append(comments, map[string]any{
				"path":         f.Path,
				"subject_type": "file",
				"body":         f.Body,
			})
			continue
		}
		comments = append(comments, map[string]any{
			"path": f.Path,
			"line": f.Line,
			"body": f.Body,
		})
	}
	if len(comments) == 0 {
		comments = nil
	}
	id, err := c.review(ctx, repoURL, token, number, reviewEventComment, body, comments)
	if err != nil {
		return "", classifyReviewPostError(err)
	}
	return id, nil
}

// ListReviewComments is the SECOND read: GitHub's create-review response does
// not carry the inline comments it created, so their ids, paths and lines are
// fetched here. reviewID is the id PostReview returned (or the id of the review
// the forge-side dedup check found already present).
func (c *GitHub) ListReviewComments(ctx context.Context, repoURL, token string, number int, reviewID string) ([]PostedComment, error) {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return nil, err
	}
	if reviewID == "" {
		return nil, errors.New("github: list review comments: empty review id")
	}
	type ghReviewComment struct {
		ID           int64     `json:"id"`
		Path         string    `json:"path"`
		Line         int       `json:"line"`
		OriginalLine int       `json:"original_line"`
		InReplyToID  int64     `json:"in_reply_to_id"`
		Body         string    `json:"body"`
		CreatedAt    time.Time `json:"created_at"`
	}
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/reviews/%s/comments?per_page=100",
		owner, repo, number, url.PathEscape(reviewID))
	raw, err := ghDoPaged[ghReviewComment](ctx, c.base(), path, token)
	if err != nil {
		return nil, err
	}
	out := make([]PostedComment, 0, len(raw))
	for _, rc := range raw {
		line := rc.Line
		if line == 0 {
			// GitHub nulls `line` for a comment on an outdated diff position and
			// keeps the anchor in original_line. A zero line would mirror as
			// "file-level", losing the anchor the agent needs to reply.
			line = rc.OriginalLine
		}
		pc := PostedComment{
			ExternalID: strconv.FormatInt(rc.ID, 10),
			Path:       rc.Path,
			Line:       line,
			Body:       rc.Body,
			CreatedAt:  rc.CreatedAt,
		}
		if rc.InReplyToID != 0 {
			pc.InReplyTo = strconv.FormatInt(rc.InReplyToID, 10)
		}
		out = append(out, pc)
	}
	return out, nil
}

// classifyReviewPostError maps a structural 4xx from the review POST to the
// TERMINAL ErrReviewRefused. 422 ("Can not approve your own pull request"), 401,
// 403 and 400 (GitLab's deterministic "line_code can't be blank" for a position
// it cannot anchor, #394) cannot be fixed by retrying, and hot-requeueing them -
// which is what writeback_review.go does today with any Approve error - spins
// forever. The caller parks at review-post-refused instead. Any other error stays
// retryable.
func classifyReviewPostError(err error) error {
	var he *HTTPError
	if !errors.As(err, &he) {
		return err
	}
	switch he.Status {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity:
		// A rate-limit 403 is transient, not structural: it must stay requeueable.
		if errors.Is(err, ErrRateLimited) {
			return err
		}
		return fmt.Errorf("%w: %w", ErrReviewRefused, err)
	default:
		return err
	}
}

// Merge merges a PR with the given method (squash|merge|rebase).
// Returns the merge commit SHA on success. Returns ErrMergeConflict when
// GitHub signals the PR is not mergeable (405 Method Not Allowed) or a
// conflict/head mismatch occurred (409 Conflict). The poll model means CI
// must be green before this is called; use pollRequeue to avoid rate-limit
// storms while waiting for CI rather than enabling platform auto-merge.
// expectedHeadSHA, when non-empty, is sent as `sha` so GitHub refuses the merge
// with a 409 if the head moved between the review and the merge. That 409 maps to
// ErrHeadMoved (NOT ErrMergeConflict): the caller re-reviews the new head rather
// than treating a moved head as an unmergeable PR. An empty expectedHeadSHA means
// "no pin" and preserves the pre-pin behaviour exactly.
func (c *GitHub) Merge(ctx context.Context, repoURL, token string, number int, method, expectedHeadSHA string) (string, error) {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return "", err
	}
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/merge", owner, repo, number)
	in := map[string]string{"merge_method": method}
	if expectedHeadSHA != "" {
		in["sha"] = expectedHeadSHA
	}
	var resp struct {
		SHA string `json:"sha"`
	}
	if err := ghDo(ctx, c.base(), http.MethodPut, path, token, in, &resp); err != nil {
		var he *HTTPError
		if errors.As(err, &he) && !errors.Is(err, ErrRateLimited) {
			if he.Status == http.StatusUnauthorized || he.Status == http.StatusForbidden {
				return "", fmt.Errorf("%w: %w", ErrAuthFailed, err)
			}
			if he.Status == http.StatusConflict && expectedHeadSHA != "" {
				return "", fmt.Errorf("%w: %w", ErrHeadMoved, err)
			}
			if he.Status == http.StatusMethodNotAllowed || he.Status == http.StatusConflict {
				return "", ErrMergeConflict
			}
		}
		return "", err
	}
	return resp.SHA, nil
}

// EnableAutoMerge turns on GitHub native auto-merge for the PR at prURL, so the
// forge merges it once the branch's required status checks pass. Requires the
// repo to allow auto-merge and main to have a branch-protection rule with at
// least one required check; otherwise GitHub returns an error (callers treat it
// as non-fatal). repoURL is unused for GitHub (the PR node id is resolved from
// prURL) but kept for interface symmetry with GitLab.
func (c *GitHub) EnableAutoMerge(ctx context.Context, _, token, prURL, method string) error {
	prID, err := c.ghResourceID(ctx, token, prURL)
	if err != nil {
		return fmt.Errorf("github: resolve pr node id: %w", err)
	}
	q := fmt.Sprintf(`mutation { enablePullRequestAutoMerge(input:{pullRequestId:%q, mergeMethod: %s}) { clientMutationId } }`,
		prID, ghMergeMethod(method))
	return c.ghGraphQL(ctx, token, q, nil, nil)
}

// DisableAutoMerge turns GitHub native auto-merge back off for the PR at prURL,
// so a PR the operator has decided must not ship cannot merge itself once its
// required checks pass. GitHub errors when auto-merge was never enabled;
// callers treat that as non-fatal. repoURL is unused (the PR node id is
// resolved from prURL), kept for interface symmetry with GitLab.
func (c *GitHub) DisableAutoMerge(ctx context.Context, _, token, prURL string) error {
	prID, err := c.ghResourceID(ctx, token, prURL)
	if err != nil {
		return fmt.Errorf("github: resolve pr node id: %w", err)
	}
	q := fmt.Sprintf(`mutation { disablePullRequestAutoMerge(input:{pullRequestId:%q}) { clientMutationId } }`, prID)
	return c.ghGraphQL(ctx, token, q, nil, nil)
}

func ghMergeMethod(method string) string {
	switch method {
	case "merge":
		return "MERGE"
	case "rebase":
		return "REBASE"
	default:
		return "SQUASH"
	}
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

// mergeStateRecomputeDelay is how long to wait before re-fetching a PR when
// GitHub returns mergeable:null / mergeable_state:"unknown" (lazy recompute).
// GitHub typically resolves mergeability within 2-5 seconds. It is a package
// var so tests can stub it to 0 instead of sleeping for real.
var mergeStateRecomputeDelay = 2 * time.Second

// GetMergeState reads the PR's mergeability via REST. GitHub computes it lazily
// (mergeable:null / mergeable_state:"unknown" on the first read after a push),
// so when the first read is unresolved this polls once more after a short delay
// before mapping to a provider-neutral MergeState.
func (c *GitHub) GetMergeState(ctx context.Context, repoURL, token string, number int) (MergeState, error) {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return MergeStateUnknown, fmt.Errorf("github: get merge state: %w", err)
	}
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d", owner, repo, number)
	var pr struct {
		Mergeable      *bool  `json:"mergeable"`
		MergeableState string `json:"mergeable_state"`
	}
	if err := ghDo(ctx, c.base(), http.MethodGet, path, token, nil, &pr); err != nil {
		return MergeStateUnknown, fmt.Errorf("github: get merge state: %w", err)
	}
	if pr.Mergeable == nil || pr.MergeableState == "" || pr.MergeableState == "unknown" {
		select {
		case <-ctx.Done():
			return MergeStateUnknown, fmt.Errorf("github: get merge state: %w", ctx.Err())
		case <-time.After(mergeStateRecomputeDelay):
		}
		if err := ghDo(ctx, c.base(), http.MethodGet, path, token, nil, &pr); err != nil {
			return MergeStateUnknown, fmt.Errorf("github: get merge state (recompute): %w", err)
		}
	}
	return ghMergeState(pr.MergeableState), nil
}

// ghMergeState maps GitHub's mergeable_state to a provider-neutral MergeState.
// unstable/has_hooks mean mergeable-but-noisy (non-required checks) - not a
// conflict - so they map to clean for the conflict-sweep decision.
func ghMergeState(state string) MergeState {
	switch state {
	case "clean", "has_hooks", "unstable":
		return MergeStateClean
	case "dirty":
		return MergeStateDirty
	case "behind":
		return MergeStateBehind
	case "blocked", "draft":
		return MergeStateBlocked
	default:
		return MergeStateUnknown
	}
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

// foldCIStatuses merges any number of CI status strings with the precedence
// rule: failure > pending > success > "" (none). Shared by all three CI
// aggregation sites (GitHub check-runs, GitHub check-runs+combined-status,
// GitLab commit-statuses) so the priority rule is defined exactly once.
func foldCIStatuses(statuses ...string) string {
	failure, pending, success := false, false, false
	for _, s := range statuses {
		switch s {
		case "failure":
			failure = true
		case "pending":
			pending = true
		case "success":
			success = true
		}
	}
	switch {
	case failure:
		return "failure"
	case pending:
		return "pending"
	case success:
		return "success"
	default:
		return ""
	}
}
