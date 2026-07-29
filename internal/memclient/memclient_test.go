// Package memclient (internal test package, not memclient_test) so retry
// tests can shrink the unexported retryBackoff var directly - it is the
// package's own knob, not part of the public API, so an external test
// package has no way to reach it.
package memclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/objbudget"
)

// Compile-time check: *Client satisfies objbudget.Spiller structurally
// (memclient's non-test code must not import objbudget - objbudget defines
// the interface, memclient only implements it; a _test.go file importing it
// does not invert that dependency, since test files are excluded from the
// production build).
var _ objbudget.Spiller = (*Client)(nil)

// serverMemory mirrors tatara-memory's memory.Memory
// (code/tatara-memory/internal/memory/types.go) EXACTLY, so decoding a request
// body into it with DisallowUnknownFields reproduces the real server's strict
// decode: any field memclient invents (kind, name, payload, ...) is a 400
// there and must be a test failure here.
type serverMemory struct {
	ID        string            `json:"id"`
	Text      string            `json:"text"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
}

// decodeStrictOrReject mirrors tatara-memory's httpapi.decodeStrict: an
// unknown field is a 400, not a silent accept.
func decodeStrictOrReject(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("invalid body: " + err.Error()))
		return false
	}
	return true
}

func staticToken(tok string) func(context.Context) (string, error) {
	return func(context.Context) (string, error) { return tok, nil }
}

// shrinkRetryBackoff replaces the package's retry backoff with near-zero
// delays for the duration of the test and restores the original on cleanup,
// so a test that drives the retry loop in (*Client).do does not spend real
// wall time sleeping (200ms + 1s per exhausted attempt otherwise).
func shrinkRetryBackoff(t *testing.T) {
	t.Helper()
	orig := retryBackoff
	retryBackoff = []time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() { retryBackoff = orig })
}

func TestSpill_Success(t *testing.T) {
	var gotAuth, gotPath, gotMethod string
	var gotMem serverMemory
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotMethod = r.Method
		if !decodeStrictOrReject(w, r, &gotMem) {
			return
		}
		created := gotMem
		created.ID = "trk-123"
		created.CreatedAt = time.Now().UTC()
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(created)
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken("secret-tok"), nil)
	trackID, err := c.Spill(context.Background(), "Issue", "issue-42", []string{"a", "b"})
	if err != nil {
		t.Fatalf("Spill: unexpected error: %v", err)
	}
	if trackID != "trk-123" {
		t.Fatalf("Spill: trackID = %q, want %q (the created Memory's id IS the track_id)", trackID, "trk-123")
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %q, want POST", gotMethod)
	}
	if gotAuth != "Bearer secret-tok" {
		t.Fatalf("Authorization header = %q, want %q", gotAuth, "Bearer secret-tok")
	}
	if gotPath != "/memories" {
		t.Fatalf("path = %q, want /memories", gotPath)
	}
	if gotMem.Metadata[MetadataKindKey] != "Issue" {
		t.Fatalf("metadata[%s] = %q, want Issue", MetadataKindKey, gotMem.Metadata[MetadataKindKey])
	}
	if gotMem.Metadata[MetadataNameKey] != "issue-42" {
		t.Fatalf("metadata[%s] = %q, want issue-42", MetadataNameKey, gotMem.Metadata[MetadataNameKey])
	}
	if gotMem.Metadata[MetadataSpillKey] != "1" {
		t.Fatalf("metadata[%s] = %q, want 1", MetadataSpillKey, gotMem.Metadata[MetadataSpillKey])
	}
	var back []string
	if err := json.Unmarshal([]byte(gotMem.Text), &back); err != nil {
		t.Fatalf("Text is not the marshalled payload: %v (text=%q)", err, gotMem.Text)
	}
	if len(back) != 2 || back[0] != "a" || back[1] != "b" {
		t.Fatalf("payload round-trip = %v, want [a b]", back)
	}
}

// TestSpill_BodyIsStrictlyAMemory is the regression guard for the wire-shape
// bug this package was first built with: tatara-memory decodes POST /memories
// with DisallowUnknownFields, so a body carrying any field outside
// memory.Memory (id/text/metadata/created_at) is a hard 400 in production.
func TestSpill_BodyIsStrictlyAMemory(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m serverMemory
		if !decodeStrictOrReject(w, r, &m) {
			return
		}
		m.ID = "trk-ok"
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(m)
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken("tok"), nil)
	if _, err := c.Spill(context.Background(), "Task", "task-7", map[string]any{"notes": []string{"n1"}}); err != nil {
		t.Fatalf("Spill: strict memory.Memory decode rejected the body: %v", err)
	}
}

func TestSpill_ServerError_IsRetryable(t *testing.T) {
	shrinkRetryBackoff(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("lightrag unavailable"))
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken("tok"), nil)
	_, err := c.Spill(context.Background(), "Issue", "issue-42", "payload")
	if err == nil {
		t.Fatal("Spill: want error on 500, got nil")
	}
	if !errors.Is(err, ErrRetryable) {
		t.Fatalf("Spill: 500 error = %v, want errors.Is(err, ErrRetryable)", err)
	}
	if errors.Is(err, ErrTerminal) {
		t.Fatalf("Spill: 500 error must NOT be terminal: %v", err)
	}
}

func TestSpill_ClientError_IsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("string_too_short"))
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken("tok"), nil)
	_, err := c.Spill(context.Background(), "Issue", "issue-42", "payload")
	if err == nil {
		t.Fatal("Spill: want error on 400, got nil")
	}
	if !errors.Is(err, ErrTerminal) {
		t.Fatalf("Spill: 400 error = %v, want errors.Is(err, ErrTerminal)", err)
	}
	if errors.Is(err, ErrRetryable) {
		t.Fatalf("Spill: 400 error must NOT be retryable: %v", err)
	}
}

// TestSpill_MissingTrackID_IsError is the M19 guard: a 201 response with no id
// (= no track_id) must NEVER be treated as a silent "" success - that empty
// string gets appended to spilledCommentsRefs and orphans the batch forever
// (contract A.1/A.7).
func TestSpill_MissingTrackID_IsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"","text":"x","created_at":"2026-07-13T00:00:00Z"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken("tok"), nil)
	trackID, err := c.Spill(context.Background(), "Issue", "issue-42", "payload")
	if err == nil {
		t.Fatalf("Spill: want error when response carries no id, got trackID=%q", trackID)
	}
	if trackID != "" {
		t.Fatalf("Spill: trackID must be empty on error, got %q", trackID)
	}
}

func TestFetch_RoundTrip(t *testing.T) {
	payload := json.RawMessage(`{"comments":[{"body":"hi"}]}`)
	var gotAuth, gotPath, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotMethod = r.Method
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(serverMemory{
			ID:        "trk-123",
			Text:      string(payload),
			Metadata:  map[string]string{MetadataKindKey: "Task"},
			CreatedAt: time.Now().UTC(),
		})
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken("secret-tok"), nil)
	got, err := c.Fetch(context.Background(), "trk-123")
	if err != nil {
		t.Fatalf("Fetch: unexpected error: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("Fetch: payload = %s, want %s", got, payload)
	}
	if gotMethod != http.MethodGet {
		t.Fatalf("method = %q, want GET", gotMethod)
	}
	if gotAuth != "Bearer secret-tok" {
		t.Fatalf("Authorization header = %q, want Bearer secret-tok", gotAuth)
	}
	if gotPath != "/memories/trk-123" {
		t.Fatalf("path = %q, want /memories/trk-123", gotPath)
	}
}

// TestSpillFetch_RoundTrip drives both halves against one in-memory store:
// what Spill marshals into Text is exactly what Fetch hands back, so a caller
// can unmarshal it into the same type it evicted.
func TestSpillFetch_RoundTrip(t *testing.T) {
	store := map[string]serverMemory{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var m serverMemory
			if !decodeStrictOrReject(w, r, &m) {
				return
			}
			m.ID = "trk-rt"
			m.CreatedAt = time.Now().UTC()
			store[m.ID] = m
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(m)
			return
		}
		m, ok := store["trk-rt"]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(m)
	}))
	defer srv.Close()

	type note struct {
		Kind string `json:"kind"`
		Body string `json:"body"`
	}
	want := []note{{Kind: "handoff", Body: "first"}, {Kind: "finding", Body: "second"}}

	c := New(srv.URL, staticToken("tok"), nil)
	trackID, err := c.Spill(context.Background(), "Task", "task-7", want)
	if err != nil {
		t.Fatalf("Spill: %v", err)
	}
	raw, err := c.Fetch(context.Background(), trackID)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	var got []note
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Fetch payload does not unmarshal into the spilled type: %v", err)
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("round-trip = %+v, want %+v", got, want)
	}
}

// TestFetch_EmptyText_IsError: a Memory with no text cannot be rehydrated into
// notes, and an empty RawMessage would render as a silently missing note batch
// in task_context(notes=all).
func TestFetch_EmptyText_IsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"trk-123","text":"","created_at":"2026-07-13T00:00:00Z"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken("tok"), nil)
	got, err := c.Fetch(context.Background(), "trk-123")
	if err == nil {
		t.Fatalf("Fetch: want error on empty text, got payload=%q", got)
	}
}

func TestFetch_NotFound_IsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("no such track"))
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken("tok"), nil)
	_, err := c.Fetch(context.Background(), "does-not-exist")
	if err == nil {
		t.Fatal("Fetch: want error on 404, got nil")
	}
	if !errors.Is(err, ErrTerminal) {
		t.Fatalf("Fetch: 404 error = %v, want errors.Is(err, ErrTerminal)", err)
	}
}

func TestFetch_ServiceUnavailable_IsRetryable(t *testing.T) {
	shrinkRetryBackoff(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken("tok"), nil)
	_, err := c.Fetch(context.Background(), "trk-123")
	if !errors.Is(err, ErrRetryable) {
		t.Fatalf("Fetch: 503 error = %v, want errors.Is(err, ErrRetryable)", err)
	}
}

// TestSpill_TransportFailure_IsRetryable covers connection-level failures
// (server unreachable, timeout) - distinct from an HTTP status response -
// which must also be retryable, never terminal and never a silent success.
func TestSpill_TransportFailure_IsRetryable(t *testing.T) {
	shrinkRetryBackoff(t)
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	srv.Close() // closed before use: connection refused

	c := New(srv.URL, staticToken("tok"), nil)
	_, err := c.Spill(context.Background(), "Issue", "issue-42", "payload")
	if err == nil {
		t.Fatal("Spill: want error on unreachable server, got nil")
	}
	if !errors.Is(err, ErrRetryable) {
		t.Fatalf("Spill: transport error = %v, want errors.Is(err, ErrRetryable)", err)
	}
	if errors.Is(err, ErrTerminal) {
		t.Fatalf("Spill: transport error must NOT be terminal: %v", err)
	}
}

func TestSpill_TokenSourceError_IsReturned(t *testing.T) {
	wantErr := errors.New("token mint failed")
	c := New("http://unused.invalid", func(context.Context) (string, error) {
		return "", wantErr
	}, nil)
	_, err := c.Spill(context.Background(), "Issue", "issue-42", "payload")
	if !errors.Is(err, wantErr) {
		t.Fatalf("Spill: err = %v, want wrapping %v", err, wantErr)
	}
}

// --- retry loop coverage (A.7 / do's bounded in-client retries) ---

// TestSpill_RetrySucceedsOnSecondAttempt is required coverage 1: a 503 that
// succeeds on the 2nd attempt returns the track_id, and the handler saw
// exactly 2 requests (1 failure + 1 success, no third attempt wasted).
func TestSpill_RetrySucceedsOnSecondAttempt(t *testing.T) {
	shrinkRetryBackoff(t)
	var mu sync.Mutex
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		count++
		n := count
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("lightrag unavailable"))
			return
		}
		var m serverMemory
		if !decodeStrictOrReject(w, r, &m) {
			return
		}
		m.ID = "trk-retry-ok"
		m.CreatedAt = time.Now().UTC()
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(m)
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken("tok"), nil)
	trackID, err := c.Spill(context.Background(), "Issue", "issue-1", "payload")
	if err != nil {
		t.Fatalf("Spill: unexpected error after retry: %v", err)
	}
	if trackID != "trk-retry-ok" {
		t.Fatalf("Spill: trackID = %q, want trk-retry-ok", trackID)
	}
	mu.Lock()
	got := count
	mu.Unlock()
	if got != 2 {
		t.Fatalf("handler saw %d requests, want exactly 2", got)
	}
}

// TestDo_RetryClassification is required coverage 2 (persistent 503
// exhausts all 3 attempts and returns ErrRetryable) and coverage 3+4 (a 400
// is terminal and stops after exactly 1 request; a 429 is classified
// retryable just like a 503, and is retried).
func TestDo_RetryClassification(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		wantRequests  int
		wantRetryable bool
		wantTerminal  bool
	}{
		{
			name:          "persistent 503 exhausts all 3 attempts and stays retryable",
			status:        http.StatusServiceUnavailable,
			wantRequests:  3,
			wantRetryable: true,
		},
		{
			name:         "400 is terminal and is never retried",
			status:       http.StatusBadRequest,
			wantRequests: 1,
			wantTerminal: true,
		},
		{
			name:          "429 classifies retryable and is retried like a 503",
			status:        http.StatusTooManyRequests,
			wantRequests:  3,
			wantRetryable: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shrinkRetryBackoff(t)
			var mu sync.Mutex
			count := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				mu.Lock()
				count++
				mu.Unlock()
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte("err"))
			}))
			defer srv.Close()

			c := New(srv.URL, staticToken("tok"), nil)
			_, err := c.Spill(context.Background(), "Issue", "issue-1", "payload")
			if err == nil {
				t.Fatalf("Spill: want error for status %d, got nil", tt.status)
			}
			if tt.wantRetryable && !errors.Is(err, ErrRetryable) {
				t.Fatalf("err = %v, want errors.Is(err, ErrRetryable)", err)
			}
			if tt.wantTerminal && !errors.Is(err, ErrTerminal) {
				t.Fatalf("err = %v, want errors.Is(err, ErrTerminal)", err)
			}
			if tt.wantTerminal && errors.Is(err, ErrRetryable) {
				t.Fatalf("err = %v, must NOT also be retryable", err)
			}
			mu.Lock()
			got := count
			mu.Unlock()
			if got != tt.wantRequests {
				t.Fatalf("handler saw %d requests, want exactly %d", got, tt.wantRequests)
			}
		})
	}
}

// TestFetch_RetrySucceedsOnSecondAttempt is required coverage 5: the retry
// loop lives in do, shared by Spill and Fetch, so Fetch must retry too.
func TestFetch_RetrySucceedsOnSecondAttempt(t *testing.T) {
	shrinkRetryBackoff(t)
	var mu sync.Mutex
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		count++
		n := count
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(serverMemory{
			ID:        "trk-123",
			Text:      `{"ok":true}`,
			CreatedAt: time.Now().UTC(),
		})
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken("tok"), nil)
	got, err := c.Fetch(context.Background(), "trk-123")
	if err != nil {
		t.Fatalf("Fetch: unexpected error after retry: %v", err)
	}
	if string(got) != `{"ok":true}` {
		t.Fatalf("Fetch: payload = %s, want {\"ok\":true}", got)
	}
	mu.Lock()
	reqs := count
	mu.Unlock()
	if reqs != 2 {
		t.Fatalf("handler saw %d requests, want exactly 2", reqs)
	}
}

// TestSpill_RetryReplaysBodyIntact is required coverage 6: a consumed
// io.Reader cannot be replayed, so do must rebuild the request body fresh on
// each attempt. Asserts the handler read the same non-empty JSON body twice.
func TestSpill_RetryReplaysBodyIntact(t *testing.T) {
	shrinkRetryBackoff(t)
	var mu sync.Mutex
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		mu.Lock()
		bodies = append(bodies, string(raw))
		n := len(bodies)
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var m serverMemory
		if err := json.Unmarshal(raw, &m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		m.ID = "trk-replay-ok"
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(m)
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken("tok"), nil)
	if _, err := c.Spill(context.Background(), "Issue", "issue-1", []string{"a", "b", "c"}); err != nil {
		t.Fatalf("Spill: unexpected error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 {
		t.Fatalf("handler saw %d requests, want exactly 2", len(bodies))
	}
	if bodies[0] == "" || bodies[1] == "" {
		t.Fatalf("request body must not be empty: attempt1=%q attempt2=%q", bodies[0], bodies[1])
	}
	if bodies[0] != bodies[1] {
		t.Fatalf("retry body differs from first attempt:\nattempt1=%s\nattempt2=%s", bodies[0], bodies[1])
	}
}

// --- Retry-After coverage (issue #466 part b): the server's own Retry-After
// answer must win over the fixed retryBackoff schedule, for both 503 and 429. ---

// TestDo_HonoursRetryAfterHeader_503 is the regression guard for the reported
// defect: tatara-memory answers a 503 with "Retry-After: 5", but the client
// used to wait a fixed ~200ms/1s regardless. retryBackoff is inflated to 5s
// here so an unfixed client - which ignores the header - blows well past the
// 3s context deadline and the call fails; a fixed client waits ~1s (the
// header's value) and succeeds well inside the deadline.
func TestDo_HonoursRetryAfterHeader_503(t *testing.T) {
	orig := retryBackoff
	retryBackoff = []time.Duration{5 * time.Second, 5 * time.Second}
	t.Cleanup(func() { retryBackoff = orig })

	var mu sync.Mutex
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		count++
		n := count
		mu.Unlock()
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("upstream temporarily unavailable"))
			return
		}
		var m serverMemory
		if !decodeStrictOrReject(w, r, &m) {
			return
		}
		m.ID = "trk-retry-after-503-ok"
		m.CreatedAt = time.Now().UTC()
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(m)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	c := New(srv.URL, staticToken("tok"), nil)
	start := time.Now()
	trackID, err := c.Spill(ctx, "Issue", "issue-1", "payload")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Spill: want the server's Retry-After: 1 honoured (~1s wait), got error after %s: %v", elapsed, err)
	}
	if trackID != "trk-retry-after-503-ok" {
		t.Fatalf("trackID = %q, want trk-retry-after-503-ok", trackID)
	}
	if elapsed >= 3*time.Second {
		t.Fatalf("elapsed = %s, want < 3s (Retry-After: 1 must win over the 5s fallback backoff)", elapsed)
	}
	if elapsed < 900*time.Millisecond {
		t.Fatalf("elapsed = %s, want >= ~1s (Retry-After: 1 must actually be honoured, not skipped)", elapsed)
	}
}

// TestDo_HonoursRetryAfterHeader_429 mirrors the 503 case for 429: the shed
// signal tatara-memory's bulk admission control is expected to use going
// forward (see tatara-memory branch fix/bulk-admission-control) must be
// honoured exactly like a 503's.
func TestDo_HonoursRetryAfterHeader_429(t *testing.T) {
	orig := retryBackoff
	retryBackoff = []time.Duration{5 * time.Second, 5 * time.Second}
	t.Cleanup(func() { retryBackoff = orig })

	var mu sync.Mutex
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		count++
		n := count
		mu.Unlock()
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("shed"))
			return
		}
		var m serverMemory
		if !decodeStrictOrReject(w, r, &m) {
			return
		}
		m.ID = "trk-retry-after-429-ok"
		m.CreatedAt = time.Now().UTC()
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(m)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	c := New(srv.URL, staticToken("tok"), nil)
	start := time.Now()
	trackID, err := c.Spill(ctx, "Issue", "issue-1", "payload")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Spill: want the server's Retry-After: 1 honoured (~1s wait), got error after %s: %v", elapsed, err)
	}
	if trackID != "trk-retry-after-429-ok" {
		t.Fatalf("trackID = %q, want trk-retry-after-429-ok", trackID)
	}
	if elapsed >= 3*time.Second {
		t.Fatalf("elapsed = %s, want < 3s (Retry-After: 1 must win over the 5s fallback backoff)", elapsed)
	}
}

// TestDo_RetryAfterHeader_Capped guards against a misbehaving or hostile
// server parking a reconcile goroutine indefinitely: an absurd Retry-After is
// capped at maxRetryAfter rather than honoured verbatim. maxRetryAfter is
// shrunk for the test the same way retryBackoff is.
func TestDo_RetryAfterHeader_Capped(t *testing.T) {
	origBackoff := retryBackoff
	retryBackoff = []time.Duration{5 * time.Second, 5 * time.Second}
	t.Cleanup(func() { retryBackoff = origBackoff })

	origCap := maxRetryAfter
	maxRetryAfter = 200 * time.Millisecond
	t.Cleanup(func() { maxRetryAfter = origCap })

	var mu sync.Mutex
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		count++
		n := count
		mu.Unlock()
		if n == 1 {
			w.Header().Set("Retry-After", "3600")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var m serverMemory
		if !decodeStrictOrReject(w, r, &m) {
			return
		}
		m.ID = "trk-capped-ok"
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(m)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	c := New(srv.URL, staticToken("tok"), nil)
	start := time.Now()
	_, err := c.Spill(ctx, "Issue", "issue-1", "payload")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Spill: want the 1-hour Retry-After capped to maxRetryAfter, got error after %s: %v", elapsed, err)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("elapsed = %s, want < 2s (a 3600s Retry-After must be capped, not honoured verbatim)", elapsed)
	}
}

// TestDo_MissingRetryAfter_UsesFallbackBackoff is the compatibility guard: a
// 503/429 with no Retry-After header must fall back to the existing fixed
// retryBackoff schedule exactly as before this change.
func TestDo_MissingRetryAfter_UsesFallbackBackoff(t *testing.T) {
	shrinkRetryBackoff(t)
	var mu sync.Mutex
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		count++
		n := count
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var m serverMemory
		if !decodeStrictOrReject(w, r, &m) {
			return
		}
		m.ID = "trk-no-header-ok"
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(m)
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken("tok"), nil)
	trackID, err := c.Spill(context.Background(), "Issue", "issue-1", "payload")
	if err != nil {
		t.Fatalf("Spill: unexpected error: %v", err)
	}
	if trackID != "trk-no-header-ok" {
		t.Fatalf("trackID = %q, want trk-no-header-ok", trackID)
	}
}
