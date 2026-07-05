package scm

import "testing"

// TestFoldCIStatuses pins the shared variadic reducer's truth table
// (failure > pending > success > "") in isolation, ahead of S24 wiring it
// into deriveGHCIStatus, GitHub's check-run+combined-status fold, and
// GitLab's commit-statuses aggregate.
func TestFoldCIStatuses(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want string
	}{
		{"empty", nil, ""},
		{"lone blank", []string{""}, ""},
		{"all blank", []string{"", ""}, ""},
		{"lone success", []string{"success"}, "success"},
		{"lone pending", []string{"pending"}, "pending"},
		{"lone failure", []string{"failure"}, "failure"},
		{"blank mixed with success", []string{"", "success"}, "success"},
		{"blank mixed with pending", []string{"", "pending"}, "pending"},
		{"blank mixed with failure", []string{"", "failure"}, "failure"},
		{"pending beats success", []string{"success", "pending"}, "pending"},
		{"failure beats pending", []string{"pending", "failure"}, "failure"},
		{"failure beats success", []string{"success", "failure"}, "failure"},
		{"three-way failure wins", []string{"success", "pending", "failure"}, "failure"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := foldCIStatuses(tc.in...); got != tc.want {
				t.Fatalf("foldCIStatuses(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
