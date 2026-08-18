package memclient

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestSpill_SchemelessBaseURLFailsOnceAndIsNotRetryable is #616's tail. A
// Project with no memory endpoint used to yield a Client with baseURL "", so
// every call became `Post "/memories": unsupported protocol scheme ""` - a
// transport error, which doOnce classified RETRYABLE without exception. A
// malformed URL is a client misconfiguration: no amount of retrying repairs
// it, and the full backoff schedule was burned on every single call.
func TestSpill_SchemelessBaseURLFailsOnceAndIsNotRetryable(t *testing.T) {
	tokens := 0
	c := New("", func(context.Context) (string, error) {
		tokens++
		return "tok", nil
	}, nil)

	_, err := c.Spill(context.Background(), "Task", "t1", []string{"a"})
	if err == nil {
		t.Fatal("Spill: want error on an empty base URL, got nil")
	}
	if errors.Is(err, ErrRetryable) {
		t.Fatalf("Spill err = %v, want a NON-retryable error: a URL with no scheme or host cannot be fixed by waiting", err)
	}
	if !strings.Contains(err.Error(), "/memories") {
		t.Fatalf("Spill err = %v, want the path named", err)
	}
	if tokens != 1 {
		t.Fatalf("token minted %d times, want 1 (no retry loop)", tokens)
	}
}
