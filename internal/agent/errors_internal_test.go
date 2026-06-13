package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
)

type fakeTimeoutErr struct{}

func (fakeTimeoutErr) Error() string   { return "i/o timeout" }
func (fakeTimeoutErr) Timeout() bool   { return true }
func (fakeTimeoutErr) Temporary() bool { return true }

func TestIsUnreachable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"conn refused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, true},
		{"conn refused wrapped", fmt.Errorf("Post: %w", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}), true},
		{"conn reset", &net.OpError{Op: "read", Err: syscall.ECONNRESET}, true},
		{"dns not found", &net.DNSError{IsNotFound: true}, true},
		{"context canceled", context.Canceled, false},
		{"context deadline", context.DeadlineExceeded, false},
		{"net timeout", fakeTimeoutErr{}, false},
		{"net timeout wrapped", fmt.Errorf("Post: %w", fakeTimeoutErr{}), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isUnreachable(tc.err); got != tc.want {
				t.Errorf("isUnreachable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestUnreachableError_NilErr(t *testing.T) {
	e := &UnreachableError{}
	_ = e.Error() // must not panic
	if !errors.As(error(e), new(*UnreachableError)) {
		t.Fatal("errors.As must match *UnreachableError")
	}
}
