// Package memclient is the operator's HTTP client for tatara-memory's REST
// API. It satisfies internal/objbudget.Spiller structurally (this package
// does not import objbudget - objbudget defines the interface, memclient only
// implements it) and additionally exposes Fetch, the rehydrate path
// task_context(notes=all) needs to read spilled notes back.
//
// Wire shape (verified against code/tatara-memory/internal/httpapi/router.go
// and internal/memory/{types,service}.go - there is NO dedicated spill
// endpoint; the byte-guard spill rides the ordinary memory surface):
//
//	POST /memories  {"id":"","text":<payload JSON>,"metadata":{...}}  -> 201 + Memory
//	GET  /memories/{id}                                               -> 200 + Memory
//
// mountV1 mounts those routes at the router root, so there is no "/v1" path
// prefix. The evicted batch is marshalled to JSON and carried in Memory.Text;
// kind and name ride in Memory.Metadata under the Metadata*Key constants
// below, which is how a future GC sweep finds spill records. tatara-memory
// decodes POST /memories with DisallowUnknownFields, so the request body must
// be EXACTLY a memory.Memory - any extra key is a 400.
//
// The created Memory's ID IS the LightRAG track_id (memory/service.go:
// "CreateMemory submits m to LightRAG and returns it with track_id as ID"), so
// Spill returns the response's id, and Fetch GETs by it.
//
// Re-spilling identical text is naturally idempotent: LightRAG answers a
// repeat insert with status "duplicated" and the existing, reusable track_id,
// which tatara-memory treats as success (memory/service.go CreateMemory). That
// is load-bearing for the A.7 retry path - a spill that succeeded upstream but
// whose response was lost can be retried without creating a second record or a
// second track_id.
package memclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	requestTimeout  = 15 * time.Second
	maxResponseBody = 8 << 20 // 8 MiB: a spill batch is bounded by ObjectByteBudget (800 KB); generous headroom for rehydrate.
	errBodyLimit    = 200
)

// Metadata keys stamped on every spill record, so a spilled batch is
// attributable to the object it was evicted from and a future GC sweep can
// find spill records among ordinary memories.
const (
	// MetadataKindKey holds the source object's kind (Issue/MergeRequest/Task).
	MetadataKindKey = "tatara_kind"
	// MetadataNameKey holds the source object's name.
	MetadataNameKey = "tatara_name"
	// MetadataSpillKey is the marker, always "1", identifying a byte-guard
	// spill record as opposed to an agent-written memory.
	MetadataSpillKey = "tatara_spill"
)

// ErrRetryable marks a memclient error as transient: a 5xx/429 response, a
// timeout, or a transport failure. A caller should requeue.
var ErrRetryable = errors.New("memclient: retryable error")

// ErrTerminal marks a memclient error as permanent: a 4xx response other than
// 429. A caller should give up, not requeue forever.
var ErrTerminal = errors.New("memclient: terminal error")

// RetryableError is returned for a transient failure: 5xx/429 responses and
// transport-level errors (connection refused, timeout, DNS).
type RetryableError struct {
	Status int // 0 for a transport-level failure with no HTTP response.
	Body   string
	Path   string
	Err    error
	// RetryAfter is the server's own requested wait, parsed from a
	// Retry-After response header (delta-seconds or an HTTP-date) and capped
	// at maxRetryAfter. Zero when the response carried no Retry-After header
	// or it was empty - a transport-level failure never has one either, since
	// there is no response to read a header from.
	RetryAfter time.Duration
}

func (e *RetryableError) Error() string {
	if e.Status == 0 {
		return fmt.Sprintf("memclient: %s: %v", e.Path, e.Err)
	}
	return fmt.Sprintf("memclient: %s -> %d: %s", e.Path, e.Status, e.Body)
}

func (e *RetryableError) Unwrap() error { return e.Err }

func (e *RetryableError) Is(target error) bool { return target == ErrRetryable }

// TerminalError is returned for a permanent failure: any 4xx response other
// than 429 Too Many Requests.
type TerminalError struct {
	Status int
	Body   string
	Path   string
}

func (e *TerminalError) Error() string {
	return fmt.Sprintf("memclient: %s -> %d: %s", e.Path, e.Status, e.Body)
}

func (e *TerminalError) Is(target error) bool { return target == ErrTerminal }

// classify turns an HTTP status plus a truncated body into a RetryableError
// (5xx, 429) or a TerminalError (any other 4xx). Only call it for non-2xx.
// retryAfterHeader is the raw Retry-After header value (may be empty); it is
// parsed and attached to a RetryableError only, since a TerminalError is
// never retried and the value would be meaningless there.
func classify(status int, body []byte, path string, retryAfterHeader string) error {
	b := string(body)
	if len(b) > errBodyLimit {
		b = b[:errBodyLimit] + "...[truncated]"
	}
	if status >= 500 || status == http.StatusTooManyRequests {
		return &RetryableError{Status: status, Body: b, Path: path, RetryAfter: parseRetryAfter(retryAfterHeader)}
	}
	return &TerminalError{Status: status, Body: b, Path: path}
}

// parseRetryAfter parses an HTTP Retry-After header value into a wait
// duration, capped at maxRetryAfter. Returns 0 for an empty, unparseable, or
// non-positive value - the caller then falls back to the fixed retryBackoff
// schedule exactly as if no header had been sent.
//
// Retry-After (RFC 9110 section 10.2.3) is either delta-seconds ("120") or an
// HTTP-date ("Wed, 21 Oct 2026 07:28:00 GMT"); both forms are accepted.
func parseRetryAfter(header string) time.Duration {
	if header == "" {
		return 0
	}
	if secs, err := strconv.Atoi(header); err == nil {
		if secs <= 0 {
			return 0
		}
		return capRetryAfter(time.Duration(secs) * time.Second)
	}
	if when, err := http.ParseTime(header); err == nil {
		if d := time.Until(when); d > 0 {
			return capRetryAfter(d)
		}
	}
	return 0
}

func capRetryAfter(d time.Duration) time.Duration {
	if d > maxRetryAfter {
		return maxRetryAfter
	}
	return d
}

// memoryObject is the operator-side mirror of tatara-memory's memory.Memory.
// Source of truth: code/tatara-memory/internal/memory/types.go. tatara-memory
// is a separate Go module and is deliberately not imported; keep this struct
// field-for-field identical to it, because POST /memories decodes strictly
// (DisallowUnknownFields) and any drift is a hard 400.
//
// CreatedAt is a pointer purely so it can be omitted on the request: the
// server stamps its own creation time, and a zero time.Time would otherwise
// serialise as "0001-01-01T00:00:00Z".
type memoryObject struct {
	ID        string            `json:"id"`
	Text      string            `json:"text"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	CreatedAt *time.Time        `json:"created_at,omitempty"`
}

// Client is the operator's HTTP client for tatara-memory. The zero value is
// not usable; construct with New.
type Client struct {
	baseURL string
	token   func(ctx context.Context) (string, error)
	http    *http.Client
}

// New returns a Client targeting baseURL (a Project's
// status.memory.endpoint). tokenSource mints the bearer token tatara-memory
// verifies (aud names tatara-memory); pass an
// (*internal/auth.TokenSource).Token method value. httpClient may be nil, in
// which case a client with requestTimeout is used.
func New(baseURL string, tokenSource func(ctx context.Context) (string, error), httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: requestTimeout}
	}
	return &Client{baseURL: baseURL, token: tokenSource, http: httpClient}
}

// Spill sends one eviction batch to tatara-memory and returns the durable
// track_id the caller must record. Satisfies objbudget.Spiller structurally.
//
// The batch is marshalled to JSON into Memory.Text; kind and name ride in
// Memory.Metadata. The response's id IS the track_id.
//
// A 201 carrying an empty id is returned as an error, never as a silent ""
// success - objbudget appends the returned string verbatim to Spilled*Refs, so
// an empty track_id would be indistinguishable from a real one and would
// orphan the batch forever (contract fix M19).
func (c *Client) Spill(ctx context.Context, kind, name string, payload any) (string, error) {
	const path = "/memories"

	text, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("memclient: marshal spill payload for %s %s: %w", kind, name, err)
	}

	body, err := json.Marshal(memoryObject{
		Text: string(text),
		Metadata: map[string]string{
			MetadataKindKey:  kind,
			MetadataNameKey:  name,
			MetadataSpillKey: "1",
		},
	})
	if err != nil {
		return "", fmt.Errorf("memclient: marshal spill request for %s %s: %w", kind, name, err)
	}

	respBody, err := c.do(ctx, http.MethodPost, path, body)
	if err != nil {
		return "", err
	}

	var created memoryObject
	if err := json.Unmarshal(respBody, &created); err != nil {
		return "", fmt.Errorf("memclient: decode spill response for %s %s: %w", kind, name, err)
	}
	if created.ID == "" {
		return "", fmt.Errorf("memclient: spill response for %s %s carried no id (track_id)", kind, name)
	}

	slog.InfoContext(ctx, "memclient: spilled batch",
		"kind", kind, "name", name, "track_id", created.ID, "bytes", len(text))
	return created.ID, nil
}

// Fetch rehydrates the payload stored under trackID - the read path
// task_context(notes=all) uses once per entry in
// Task.status.stats.notesSpilledRefs. It returns the JSON that Spill
// marshalled into Memory.Text, so a caller can unmarshal it back into the type
// it evicted.
func (c *Client) Fetch(ctx context.Context, trackID string) (json.RawMessage, error) {
	path := "/memories/" + url.PathEscape(trackID)

	respBody, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	var m memoryObject
	if err := json.Unmarshal(respBody, &m); err != nil {
		return nil, fmt.Errorf("memclient: decode fetch response for track_id %s: %w", trackID, err)
	}
	if m.Text == "" {
		return nil, fmt.Errorf("memclient: memory %s carried no text to rehydrate", trackID)
	}

	slog.InfoContext(ctx, "memclient: fetched batch",
		"kind", m.Metadata[MetadataKindKey], "name", m.Metadata[MetadataNameKey],
		"track_id", trackID, "bytes", len(m.Text))
	return json.RawMessage(m.Text), nil
}

// retryBackoff is the delay before each retry attempt inside do: ~200ms
// before the first retry, ~1s before the second, so at most 2 retries (3
// attempts total) add ~1.2s worst case. A package-level var, not a const, so
// a test can shrink it and keep the retry-path suite fast.
//
// This is only the FALLBACK schedule. When a retryable response carries a
// Retry-After header, do waits that long instead (issue #466 part b):
// tatara-memory answers a shed 503/429 with an explicit Retry-After, and a
// hardcoded ~200ms/1s budget against a server asking for several seconds
// converts recoverable backpressure into a hard failure. Retry-After takes
// priority per attempt; retryBackoff still bounds how many attempts happen
// (len(retryBackoff) retries) regardless of which wait was used.
var retryBackoff = []time.Duration{200 * time.Millisecond, time.Second}

// maxRetryAfter caps how long do will honour a server-supplied Retry-After
// value. Without a cap, a misconfigured or hostile server could park a
// reconcile goroutine indefinitely; 30s comfortably covers the observed
// real-world value (tatara-memory's Retry-After: 5) with headroom. A
// package-level var, not a const, so a test can shrink it.
var maxRetryAfter = 30 * time.Second

// do issues an HTTP request against the memory service, attaching the bearer
// token and reading a bounded response body. It returns the raw response body
// on 2xx, or a *RetryableError/*TerminalError otherwise.
//
// A retryable failure (5xx/429, or a transport-level error) is retried up to
// len(retryBackoff) times; a terminal failure (any other 4xx) and a
// token-mint or request-build failure are returned immediately, never
// retried - retrying a permanent rejection or a client misconfiguration
// cannot change the outcome. Each attempt rebuilds the request from body
// (doOnce takes the []byte, not a shared reader), because an *http.Request's
// body reader is single-use and cannot be replayed across attempts. Retrying
// a POST /memories is safe even though it is not naturally idempotent HTTP:
// the package doc comment above records that LightRAG answers a duplicate
// insert with status "duplicated" and the same track_id, so a retried Spill
// cannot create a second record.
//
// http.Client.Timeout (requestTimeout, 15s by default) bounds each attempt
// independently, not the call as a whole: with no deadline on ctx, worst-case
// wall time is 3 attempts * 15s plus up to ~1.2s of backoff between them.
func (c *Client) do(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	tok, err := c.token(ctx)
	if err != nil {
		return nil, fmt.Errorf("memclient: mint token for %s: %w", path, err)
	}

	for attempt := 0; ; attempt++ {
		respBody, err := c.doOnce(ctx, method, path, body, tok)
		if err == nil {
			return respBody, nil
		}
		if !errors.Is(err, ErrRetryable) || attempt >= len(retryBackoff) {
			return nil, err
		}

		wait := retryBackoff[attempt]
		var re *RetryableError
		if errors.As(err, &re) && re.RetryAfter > 0 {
			wait = re.RetryAfter
		}

		slog.WarnContext(ctx, "memclient: retrying after transient error",
			"attempt", attempt+1, "path", path, "status", retryableStatus(err), "wait", wait, "error", err)

		select {
		case <-ctx.Done():
			// ctx.Err() is more actionable than the transient HTTP error that
			// triggered the wait: it tells the caller WHY the retry loop
			// stopped (deadline exceeded / cancelled) instead of replaying a
			// 503 that is no longer the reason the request failed.
			return nil, fmt.Errorf("memclient: %s: %w", path, ctx.Err())
		case <-time.After(wait):
		}
	}
}

// doOnce performs a single HTTP attempt. body is passed as a []byte, not a
// reader, so do can call this once per attempt and get a fresh, unconsumed
// bytes.Reader every time.
func (c *Client) doOnce(ctx context.Context, method, path string, body []byte, tok string) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return nil, fmt.Errorf("memclient: build request for %s: %w", path, err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &RetryableError{Path: path, Err: err}
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		return nil, &RetryableError{Path: path, Err: fmt.Errorf("read response body: %w", err)}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, classify(resp.StatusCode, respBody, path, resp.Header.Get("Retry-After"))
	}
	return respBody, nil
}

// retryableStatus extracts the HTTP status from a retryable error for
// logging, or 0 when the failure was transport-level (no response at all).
func retryableStatus(err error) int {
	var re *RetryableError
	if errors.As(err, &re) {
		return re.Status
	}
	return 0
}
