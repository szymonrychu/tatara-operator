package controller

import (
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/scm"
)

func TestOpenChangeSkipReason(t *testing.T) {
	cases := []struct {
		name string
		he   *scm.HTTPError
		want string
	}{
		{"no commits 422", &scm.HTTPError{Status: 422, Body: "No commits between main and tatara/task-x", Path: "/repos/o/r/pulls"}, "no-change"},
		{"already exists 422", &scm.HTTPError{Status: 422, Body: "A pull request already exists for o:tatara/task-x", Path: "/repos/o/r/pulls"}, "already-exists"},
		{"branch missing 404", &scm.HTTPError{Status: 404, Body: "Not Found", Path: "/repos/o/r/pulls"}, "skip-4xx"},
		{"other 422", &scm.HTTPError{Status: 422, Body: "Validation Failed", Path: "/repos/o/r/pulls"}, "skip-4xx"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := openChangeSkipReason(tc.he); got != tc.want {
				t.Errorf("openChangeSkipReason(%q) = %q, want %q", tc.he.Body, got, tc.want)
			}
		})
	}
}
