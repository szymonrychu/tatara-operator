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
func classify(status int, body []byte, path string) error {
	b := string(body)
	if len(b) > errBodyLimit {
		b = b[:errBodyLimit] + "...[truncated]"
	}
	if status >= 500 || status == http.StatusTooManyRequests {
		return &RetryableError{Status: status, Body: b, Path: path}
	}
	return &TerminalError{Status: status, Body: b, Path: path}
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

// do issues an HTTP request against the memory service, attaching the bearer
// token and reading a bounded response body. It returns the raw response body
// on 2xx, or a *RetryableError/*TerminalError otherwise.
func (c *Client) do(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	tok, err := c.token(ctx)
	if err != nil {
		return nil, fmt.Errorf("memclient: mint token for %s: %w", path, err)
	}

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
		return nil, classify(resp.StatusCode, respBody, path)
	}
	return respBody, nil
}
