package memclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/memclient"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
)

// Compile-time check: *memclient.Client satisfies objbudget.Spiller
// structurally (memclient must not import objbudget from its non-test code -
// that would invert the dependency).
var _ objbudget.Spiller = (*memclient.Client)(nil)

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

	c := memclient.New(srv.URL, staticToken("secret-tok"), nil)
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
	if gotMem.Metadata[memclient.MetadataKindKey] != "Issue" {
		t.Fatalf("metadata[%s] = %q, want Issue", memclient.MetadataKindKey, gotMem.Metadata[memclient.MetadataKindKey])
	}
	if gotMem.Metadata[memclient.MetadataNameKey] != "issue-42" {
		t.Fatalf("metadata[%s] = %q, want issue-42", memclient.MetadataNameKey, gotMem.Metadata[memclient.MetadataNameKey])
	}
	if gotMem.Metadata[memclient.MetadataSpillKey] != "1" {
		t.Fatalf("metadata[%s] = %q, want 1", memclient.MetadataSpillKey, gotMem.Metadata[memclient.MetadataSpillKey])
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

	c := memclient.New(srv.URL, staticToken("tok"), nil)
	if _, err := c.Spill(context.Background(), "Task", "task-7", map[string]any{"notes": []string{"n1"}}); err != nil {
		t.Fatalf("Spill: strict memory.Memory decode rejected the body: %v", err)
	}
}

func TestSpill_ServerError_IsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("lightrag unavailable"))
	}))
	defer srv.Close()

	c := memclient.New(srv.URL, staticToken("tok"), nil)
	_, err := c.Spill(context.Background(), "Issue", "issue-42", "payload")
	if err == nil {
		t.Fatal("Spill: want error on 500, got nil")
	}
	if !errors.Is(err, memclient.ErrRetryable) {
		t.Fatalf("Spill: 500 error = %v, want errors.Is(err, ErrRetryable)", err)
	}
	if errors.Is(err, memclient.ErrTerminal) {
		t.Fatalf("Spill: 500 error must NOT be terminal: %v", err)
	}
}

func TestSpill_ClientError_IsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("string_too_short"))
	}))
	defer srv.Close()

	c := memclient.New(srv.URL, staticToken("tok"), nil)
	_, err := c.Spill(context.Background(), "Issue", "issue-42", "payload")
	if err == nil {
		t.Fatal("Spill: want error on 400, got nil")
	}
	if !errors.Is(err, memclient.ErrTerminal) {
		t.Fatalf("Spill: 400 error = %v, want errors.Is(err, ErrTerminal)", err)
	}
	if errors.Is(err, memclient.ErrRetryable) {
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

	c := memclient.New(srv.URL, staticToken("tok"), nil)
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
			Metadata:  map[string]string{memclient.MetadataKindKey: "Task"},
			CreatedAt: time.Now().UTC(),
		})
	}))
	defer srv.Close()

	c := memclient.New(srv.URL, staticToken("secret-tok"), nil)
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

	c := memclient.New(srv.URL, staticToken("tok"), nil)
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

	c := memclient.New(srv.URL, staticToken("tok"), nil)
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

	c := memclient.New(srv.URL, staticToken("tok"), nil)
	_, err := c.Fetch(context.Background(), "does-not-exist")
	if err == nil {
		t.Fatal("Fetch: want error on 404, got nil")
	}
	if !errors.Is(err, memclient.ErrTerminal) {
		t.Fatalf("Fetch: 404 error = %v, want errors.Is(err, ErrTerminal)", err)
	}
}

func TestFetch_ServiceUnavailable_IsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := memclient.New(srv.URL, staticToken("tok"), nil)
	_, err := c.Fetch(context.Background(), "trk-123")
	if !errors.Is(err, memclient.ErrRetryable) {
		t.Fatalf("Fetch: 503 error = %v, want errors.Is(err, ErrRetryable)", err)
	}
}

// TestSpill_TransportFailure_IsRetryable covers connection-level failures
// (server unreachable, timeout) - distinct from an HTTP status response -
// which must also be retryable, never terminal and never a silent success.
func TestSpill_TransportFailure_IsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	srv.Close() // closed before use: connection refused

	c := memclient.New(srv.URL, staticToken("tok"), nil)
	_, err := c.Spill(context.Background(), "Issue", "issue-42", "payload")
	if err == nil {
		t.Fatal("Spill: want error on unreachable server, got nil")
	}
	if !errors.Is(err, memclient.ErrRetryable) {
		t.Fatalf("Spill: transport error = %v, want errors.Is(err, ErrRetryable)", err)
	}
	if errors.Is(err, memclient.ErrTerminal) {
		t.Fatalf("Spill: transport error must NOT be terminal: %v", err)
	}
}

func TestSpill_TokenSourceError_IsReturned(t *testing.T) {
	wantErr := errors.New("token mint failed")
	c := memclient.New("http://unused.invalid", func(context.Context) (string, error) {
		return "", wantErr
	}, nil)
	_, err := c.Spill(context.Background(), "Issue", "issue-42", "payload")
	if !errors.Is(err, wantErr) {
		t.Fatalf("Spill: err = %v, want wrapping %v", err, wantErr)
	}
}
