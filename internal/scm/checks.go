package scm

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"unicode/utf8"
)

// CICheck is one CI check on a PR/MR head.
type CICheck struct {
	Name       string `json:"name"`
	Status     string `json:"status"`               // queued|in_progress|completed
	Conclusion string `json:"conclusion,omitempty"` // success|failure|timed_out|cancelled|skipped|neutral|action_required|"" (empty until completed)
	URL        string `json:"url,omitempty"`
	// JobID is the provider job id the log fetch needs. Empty when the provider
	// exposes no fetchable log for this check (e.g. a plain GitHub commit status).
	JobID string `json:"-"`
}

// CIResult is the live CI view of one PR/MR head.
type CIResult struct {
	HeadSHA   string
	Status    string // none|pending|running|green|red
	Mergeable bool
	Checks    []CICheck
}

// CIReader is the live-CI capability behind GET /projects/{p}/scm/ci. It is an
// OPTIONAL SCMWriter capability (type-asserted by the caller), not a member of
// SCMReader/SCMWriter: adding a method to those interfaces would break every
// existing fake in the test suite.
type CIReader interface {
	// PRChecks reads the LIVE head SHA, mergeability and per-check state of a PR/MR.
	PRChecks(ctx context.Context, repoURL, token string, number int) (CIResult, error)
	// JobLogTail returns at most maxBytes of the TAIL of a job's log, cut on a
	// UTF-8 rune boundary. It is called ONLY for a check whose conclusion is
	// failure|timed_out|cancelled: a green run's logs are never fetched.
	JobLogTail(ctx context.Context, repoURL, token, jobID string, maxBytes int) (string, error)
}

var _ CIReader = (*GitHub)(nil)
var _ CIReader = (*GitLab)(nil)

// deriveCIStatus folds a set of checks into the closed vocabulary
// none|pending|running|green|red:
//   - zero checks -> none
//   - any check whose conclusion is a failing one -> red
//   - else any check not yet completed -> running (an in_progress one present) or pending
//   - else -> green
func deriveCIStatus(checks []CICheck) string {
	if len(checks) == 0 {
		return "none"
	}
	anyInProgress := false
	allCompleted := true
	for _, c := range checks {
		switch c.Conclusion {
		case "failure", "timed_out", "cancelled", "action_required":
			return "red"
		}
		if c.Status != "completed" {
			allCompleted = false
			if c.Status == "in_progress" {
				anyInProgress = true
			}
		}
	}
	if allCompleted {
		return "green"
	}
	if anyInProgress {
		return "running"
	}
	return "pending"
}

// maxLogFetchBytes caps the raw log body read before it is tailed, so a
// pathologically huge job log is never read fully into memory.
const maxLogFetchBytes = 8 << 20

// fetchRawLog performs a GET expecting a plain-text (non-JSON) body, shared by
// GitHub's job-log and GitLab's job-trace fetches. A 404 means "log expired /
// not fetchable" and is not an error: it returns ("", nil). Any other non-2xx
// becomes an *HTTPError.
func fetchRawLog(ctx context.Context, fullURL, path string, headers map[string]string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return "", fmt.Errorf("scm: build log request: %w", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := scmHTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("scm: fetch log: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return "", nil
	}
	buf, err := io.ReadAll(io.LimitReader(resp.Body, maxLogFetchBytes))
	if err != nil {
		return "", fmt.Errorf("scm: read log body: %w", err)
	}
	if resp.StatusCode >= 400 {
		return "", &HTTPError{Status: resp.StatusCode, Body: string(buf), Path: path}
	}
	return string(buf), nil
}

// tailUTF8 returns at most maxBytes of the tail of s, trimming forward from a
// mid-rune cut byte-by-byte until the result is valid UTF-8.
func tailUTF8(s string, maxBytes int) string {
	if maxBytes <= 0 || s == "" {
		return ""
	}
	if len(s) <= maxBytes {
		return s
	}
	tail := s[len(s)-maxBytes:]
	for len(tail) > 0 && !utf8.ValidString(tail) {
		tail = tail[1:]
	}
	return tail
}

// ghJobIDFromURLRE extracts the numeric Actions job id from a check-run's
// html_url (".../actions/runs/123/job/456").
var ghJobIDFromURLRE = regexp.MustCompile(`/job/(\d+)`)

// PRChecks reads the live head SHA, mergeability and check-runs of a GitHub PR.
func (c *GitHub) PRChecks(ctx context.Context, repoURL, token string, number int) (CIResult, error) {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return CIResult{}, err
	}
	var pr struct {
		Head struct {
			SHA string `json:"sha"`
		} `json:"head"`
		Mergeable *bool `json:"mergeable"`
	}
	prPath := fmt.Sprintf("/repos/%s/%s/pulls/%d", owner, repo, number)
	if err := ghDo(ctx, c.base(), http.MethodGet, prPath, token, nil, &pr); err != nil {
		return CIResult{}, fmt.Errorf("github: pr checks: get pr: %w", err)
	}
	var runsResp struct {
		CheckRuns []struct {
			ID         int64  `json:"id"`
			Name       string `json:"name"`
			Status     string `json:"status"`
			Conclusion string `json:"conclusion"`
			HTMLURL    string `json:"html_url"`
		} `json:"check_runs"`
	}
	checkPath := fmt.Sprintf("/repos/%s/%s/commits/%s/check-runs", owner, repo, pr.Head.SHA)
	if err := ghDo(ctx, c.base(), http.MethodGet, checkPath, token, nil, &runsResp); err != nil {
		return CIResult{}, fmt.Errorf("github: pr checks: list check-runs: %w", err)
	}
	checks := make([]CICheck, 0, len(runsResp.CheckRuns))
	for _, run := range runsResp.CheckRuns {
		jobID := ghJobIDFromHTMLURL(run.HTMLURL)
		if jobID == "" && run.ID != 0 {
			jobID = strconv.FormatInt(run.ID, 10)
		}
		checks = append(checks, CICheck{
			Name:       run.Name,
			Status:     run.Status,
			Conclusion: run.Conclusion,
			URL:        run.HTMLURL,
			JobID:      jobID,
		})
	}
	return CIResult{
		HeadSHA:   pr.Head.SHA,
		Status:    deriveCIStatus(checks),
		Mergeable: pr.Mergeable != nil && *pr.Mergeable,
		Checks:    checks,
	}, nil
}

func ghJobIDFromHTMLURL(htmlURL string) string {
	m := ghJobIDFromURLRE.FindStringSubmatch(htmlURL)
	if m == nil {
		return ""
	}
	return m[1]
}

// JobLogTail returns the tail of a GitHub Actions job's log. The logs endpoint
// 302s to a presigned blob URL; the default http.Client follows the redirect
// and correctly strips the Authorization header cross-host.
func (c *GitHub) JobLogTail(ctx context.Context, repoURL, token, jobID string, maxBytes int) (string, error) {
	owner, repo, err := ghOwnerRepo(repoURL)
	if err != nil {
		return "", err
	}
	path := fmt.Sprintf("/repos/%s/%s/actions/jobs/%s/logs", owner, repo, jobID)
	body, err := fetchRawLog(ctx, c.base()+path, path, map[string]string{
		"Authorization": "Bearer " + token,
		"Accept":        "application/vnd.github+json",
	})
	if err != nil {
		return "", fmt.Errorf("github: job log tail: %w", err)
	}
	return tailUTF8(body, maxBytes), nil
}

// PRChecks reads the live head SHA, mergeability and pipeline jobs of a
// GitLab MR. No head pipeline yet means zero checks.
func (c *GitLab) PRChecks(ctx context.Context, repoURL, token string, number int) (CIResult, error) {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return CIResult{}, err
	}
	var mr struct {
		SHA          string `json:"sha"`
		MergeStatus  string `json:"merge_status"`
		HeadPipeline *struct {
			ID int64 `json:"id"`
		} `json:"head_pipeline"`
	}
	mrPath := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number)
	if err := glDo(ctx, c.base(), http.MethodGet, mrPath, token, nil, &mr); err != nil {
		return CIResult{}, fmt.Errorf("gitlab: pr checks: get mr: %w", err)
	}
	var checks []CICheck
	if mr.HeadPipeline != nil {
		var jobs []struct {
			ID     int64  `json:"id"`
			Name   string `json:"name"`
			Status string `json:"status"`
			WebURL string `json:"web_url"`
		}
		jobsPath := "/projects/" + url.PathEscape(proj) + "/pipelines/" + strconv.FormatInt(mr.HeadPipeline.ID, 10) + "/jobs"
		if err := glDo(ctx, c.base(), http.MethodGet, jobsPath, token, nil, &jobs); err != nil {
			return CIResult{}, fmt.Errorf("gitlab: pr checks: list jobs: %w", err)
		}
		checks = make([]CICheck, 0, len(jobs))
		for _, j := range jobs {
			status, conclusion := glJobCIStatus(j.Status)
			checks = append(checks, CICheck{
				Name:       j.Name,
				Status:     status,
				Conclusion: conclusion,
				URL:        j.WebURL,
				JobID:      strconv.FormatInt(j.ID, 10),
			})
		}
	}
	return CIResult{
		HeadSHA:   mr.SHA,
		Status:    deriveCIStatus(checks),
		Mergeable: mr.MergeStatus == "can_be_merged",
		Checks:    checks,
	}, nil
}

// glJobCIStatus maps a GitLab job status to the GitHub-shaped (Status,
// Conclusion) pair so both providers produce the same CICheck vocabulary.
func glJobCIStatus(status string) (string, string) {
	switch status {
	case "created", "pending", "manual", "scheduled", "waiting_for_resource", "preparing":
		return "queued", ""
	case "running":
		return "in_progress", ""
	case "success":
		return "completed", "success"
	case "failed":
		return "completed", "failure"
	case "canceled":
		return "completed", "cancelled"
	case "skipped":
		return "completed", "skipped"
	default:
		return "queued", ""
	}
}

// JobLogTail returns the tail of a GitLab job's trace log.
func (c *GitLab) JobLogTail(ctx context.Context, repoURL, token, jobID string, maxBytes int) (string, error) {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return "", err
	}
	path := "/projects/" + url.PathEscape(proj) + "/jobs/" + jobID + "/trace"
	body, err := fetchRawLog(ctx, c.base()+path, path, map[string]string{
		"PRIVATE-TOKEN": token,
	})
	if err != nil {
		return "", fmt.Errorf("gitlab: job log tail: %w", err)
	}
	return tailUTF8(body, maxBytes), nil
}
