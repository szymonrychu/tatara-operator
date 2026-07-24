package controller

import "testing"

// TestCommentRef is tatara-operator#426's SECONDARY fix: GitLab sigils on '!'
// for merge requests and '#' for issues (same iid, different resource) - a
// '#' ref on an MR resolves the wrong (or absent) issue and 404s. GitHub
// routes both through the same issues/<n>/comments endpoint regardless.
func TestCommentRef(t *testing.T) {
	cases := []struct {
		name     string
		slug     string
		provider string
		number   int
		isPR     bool
		want     string
	}{
		{"gitlab MR uses bang", "g/p", "gitlab", 1311, true, "g/p!1311"},
		{"gitlab issue uses hash", "g/p", "gitlab", 12, false, "g/p#12"},
		{"github PR uses hash", "o/r", "github", 5, true, "o/r#5"},
		{"github issue uses hash", "o/r", "github", 5, false, "o/r#5"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := commentRef(c.slug, c.provider, c.number, c.isPR); got != c.want {
				t.Fatalf("commentRef(%q,%q,%d,%v) = %q, want %q", c.slug, c.provider, c.number, c.isPR, got, c.want)
			}
		})
	}
}
