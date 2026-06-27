package objstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
)

// timeoutErr is a net.Error reporting a timeout, used to exercise the dial
// i/o-timeout branch of IsUnavailable.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestIsUnavailable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"connection refused", fmt.Errorf("dial tcp 10.0.0.1:80: connect: %w", syscall.ECONNREFUSED), true},
		{"connection reset", fmt.Errorf("read tcp: %w", syscall.ECONNRESET), true},
		{"host unreachable", fmt.Errorf("dial tcp: %w", syscall.EHOSTUNREACH), true},
		{"retry quota exceeded", fmt.Errorf("failed to get rate limit token, %w",
			ratelimit.QuotaExceededError{Available: 0, Requested: 5}), true},
		{"dns no such host", fmt.Errorf("lookup rgw: %w",
			&net.DNSError{Err: "no such host", Name: "rgw", IsNotFound: true}), true},
		{"dial i/o timeout", fmt.Errorf("dial tcp: %w", timeoutErr{}), true},
		// Genuine per-object failures must NOT be treated as store-wide.
		{"object not found", &s3types.NotFound{}, false},
		{"access denied", &smithy.GenericAPIError{Code: "AccessDenied", Message: "no"}, false},
		{"generic object error", errors.New("objstore exists k: object-level failure"), false},
	}
	for _, tc := range cases {
		if got := IsUnavailable(tc.err); got != tc.want {
			t.Errorf("%s: IsUnavailable=%v want %v", tc.name, got, tc.want)
		}
	}
}

func TestConfigEnabled(t *testing.T) {
	if (Config{}).Enabled() {
		t.Error("empty config must be disabled")
	}
	if !(Config{Bucket: "b"}).Enabled() {
		t.Error("config with a bucket must be enabled")
	}
}

func TestNew_RequiresBucket(t *testing.T) {
	if _, err := New(context.Background(), Config{}); err == nil {
		t.Error("New must error without a bucket")
	}
}

func TestNew_BuildsClient(t *testing.T) {
	c, err := New(context.Background(), Config{
		Bucket: "conv", Region: "us-east-1",
		Endpoint: "http://rook-ceph-rgw.tatara.svc", ForcePathStyle: true,
		KeyPrefix: "conversations",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.bucket != "conv" || c.prefix != "conversations" || c.s3 == nil {
		t.Errorf("client not constructed: %+v", c)
	}
}

func TestFullKey(t *testing.T) {
	cases := []struct{ prefix, key, want string }{
		{"", "issue-1.jsonl", "issue-1.jsonl"},
		{"conversations", "tatara/r/issue-1.jsonl", "conversations/tatara/r/issue-1.jsonl"},
		{"/conv/", "/k", "conv/k"},
	}
	for _, tc := range cases {
		c := &Client{prefix: tc.prefix}
		if got := c.fullKey(tc.key); got != tc.want {
			t.Errorf("fullKey(%q,%q)=%q want %q", tc.prefix, tc.key, got, tc.want)
		}
	}
}
