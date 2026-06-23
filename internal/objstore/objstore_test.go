package objstore

import (
	"context"
	"testing"
)

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
