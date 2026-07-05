// Package slug provides the DNS-1123 label sanitisation shared by pod naming,
// the per-project deploy ledger, and the per-project queue sequence counter.
// A dependency-free leaf package (stdlib only) so all three call sites can
// import it without risking an import cycle.
package slug

import "strings"

// SanitizeDNS1123 lowercases s, collapses every run of non-[a-z0-9] into a
// single '-', trims leading/trailing '-', and caps the result at maxLen
// (trimmed again so a cut never leaves a trailing '-'). Callers pick maxLen to
// fit their own naming constraints (e.g. a fixed prefix within the DNS-1123
// 63-char label limit).
func SanitizeDNS1123(s string, maxLen int) string {
	s = strings.ToLower(s)
	var b strings.Builder
	prevDash := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevDash = false
		} else if !prevDash {
			b.WriteByte('-')
			prevDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > maxLen {
		out = strings.Trim(out[:maxLen], "-")
	}
	return out
}
