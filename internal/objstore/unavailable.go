package objstore

import (
	"errors"
	"net"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
)

// IsUnavailable reports whether err indicates the object store as a whole is
// unreachable - a connection-level or store-wide failure rather than a problem
// with one specific object. gcConversations (issue #149) uses this to
// short-circuit the conversation-GC pass: when the S3/RGW endpoint refuses
// connections, times out, cannot be resolved, or the SDK's adaptive-retry rate
// limiter is exhausted, every per-key HeadObject fails identically, so probing
// the rest of the backlog only emits a burst of duplicate ERROR lines. Genuine
// per-object failures (NotFound, AccessDenied, a single 5xx) are NOT classified
// as unavailable, so they still surface individually.
func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	// SDK adaptive-retry rate limiter exhausted: the store has been failing so
	// fast that the retry token bucket drained ("failed to get rate limit
	// token, retry quota exceeded, N available, M requested").
	var quota ratelimit.QuotaExceededError
	if errors.As(err, &quota) {
		return true
	}
	// DNS resolution failure for the endpoint host (e.g. the RGW Service has no
	// endpoints): the store is unreachable, not a single missing object.
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	// TCP dial failures: refused / reset / host or network unreachable.
	for _, errno := range []syscall.Errno{
		syscall.ECONNREFUSED, syscall.ECONNRESET,
		syscall.EHOSTUNREACH, syscall.ENETUNREACH,
	} {
		if errors.Is(err, errno) {
			return true
		}
	}
	// Any net-level timeout (e.g. dial i/o timeout): treat a stalled endpoint as
	// unavailable and retry on the next reaper cycle rather than per key.
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return true
	}
	return false
}
