package v1alpha1

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

// TruncateRunes is the RUNE twin of TruncateUTF8 and the two are not
// interchangeable. Every case here that uses multi-byte text is a case
// TruncateUTF8 gets wrong for a forge title, because the forge counts
// characters and TruncateUTF8 counts bytes.
func TestTruncateRunes(t *testing.T) {
	cases := []struct {
		name     string
		s        string
		maxRunes int
		want     string
	}{
		{"zero max yields nothing", "hello", 0, ""},
		{"negative max yields nothing", "hello", -1, ""},
		{"empty input", "", 10, ""},
		{"under the limit is untouched", "hello", 10, "hello"},
		{"exactly at the limit is untouched", "hello", 5, "hello"},
		{"ascii over by one", "hello", 4, "hell"},
		{"multi-byte under the limit is untouched", "こんにちは", 5, "こんにちは"},
		{"multi-byte cuts on a rune boundary", "こんにちは", 3, "こんに"},
		{"mixed width cuts on a rune boundary", "a界b", 2, "a界"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := TruncateRunes(tc.s, tc.maxRunes)
			require.Equal(t, tc.want, got)
			require.True(t, utf8.ValidString(got), "a cut that lands mid-rune produces invalid UTF-8")
			require.True(t, strings.HasPrefix(tc.s, got), "the result must be a prefix of the input")
		})
	}
}

// A title that already fits and carries no surrounding whitespace comes back
// BYTE-IDENTICAL. Callers compare stored values (the Issue CR mirror is matched
// against what the forge holds), so a gratuitous rewrite would not be free. The
// one rewrite that does happen is the trim, and that one earns its keep - see
// TestClampIssueTitle_TrimsPaddingOffTitlesThatAlreadyFit.
func TestClampIssueTitle_TitlesThatFitAreUntouched(t *testing.T) {
	cases := []struct {
		name  string
		title string
	}{
		{"empty", ""},
		{"ordinary", "fix: clamp the issue title before the forge call"},
		{"exactly the cap in ascii", strings.Repeat("a", IssueTitleMaxChars)},
		{"well under the cap in multi-byte text", strings.Repeat("界", 100)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.title, ClampIssueTitle(tc.title))
		})
	}
}

// THE BYTE-VERSUS-CHARACTER TRAP, and the reason this helper exists at all.
// GitLab rejects a title over 255 CHARACTERS. 255 CJK characters is 765 bytes,
// so the byte-based TruncateUTF8 would mutilate a title the forge accepts
// happily. Reusing it here would have traded a 400 for silent data loss.
func TestClampIssueTitle_CountsCharactersNotBytes(t *testing.T) {
	title := strings.Repeat("界", IssueTitleMaxChars) // 255 chars, 765 bytes

	require.Equal(t, title, ClampIssueTitle(title), "255 characters is at the cap, whatever it weighs in bytes")
	require.NotEqual(t, title, TruncateUTF8(title, IssueTitleMaxChars),
		"TruncateUTF8 cuts this to 255 BYTES: it is not a drop-in for a character cap")
}

func TestClampIssueTitle_OverlongTitlesAreCutAndVisiblyMarked(t *testing.T) {
	cases := []struct {
		name  string
		title string
	}{
		{"ascii over by one", strings.Repeat("a", IssueTitleMaxChars+1)},
		{"multi-byte well over", strings.Repeat("界", 300)},
		{
			// The shape that actually fired: an escalation-template title that
			// reads like a sentence and runs to several hundred characters.
			"the incident's own title shape",
			"`submit_outcome(file_issue)` never clamps `issue.title`: a >255-char title 400s on " +
				"GitLab-backed projects and the accepted outcome is dropped with no retry and no " +
				"persisted state - every body field has a cap, no title cap exists, so an agent " +
				"that honours its once-only contract loses the whole investigation it just wrote",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Greater(t, utf8.RuneCountInString(tc.title), IssueTitleMaxChars, "test input must actually be over the cap")

			got := ClampIssueTitle(tc.title)

			require.LessOrEqual(t, utf8.RuneCountInString(got), IssueTitleMaxChars,
				"the clamped title must fit the strictest forge cap, marker included")
			require.True(t, utf8.ValidString(got))
			require.True(t, strings.HasSuffix(got, titleTruncatedMarker),
				"a truncated title must READ as truncated, not as a title that happens to stop mid-word")
			require.True(t, strings.HasPrefix(tc.title, strings.TrimSuffix(got, titleTruncatedMarker)),
				"everything before the marker must be the original title, verbatim")
		})
	}
}

// A title that is only over the cap because of the whitespace around it is not
// really over it: the forge strips a title before it length-validates it, so
// cutting real words out of a 250-character title padded with 20 spaces would
// lose content the forge would have accepted whole.
func TestClampIssueTitle_WhitespaceDoesNotSpendTheBudget(t *testing.T) {
	t.Run("padding alone never triggers a cut", func(t *testing.T) {
		title := strings.Repeat("a", 250)

		require.Equal(t, title, ClampIssueTitle("   "+title+"                  "))
	})

	// The pathological case: leading whitespace long enough to consume the whole
	// cut on its own, which would otherwise clamp to nothing but the marker and
	// throw away every word of the title.
	t.Run("leading padding does not swallow the title", func(t *testing.T) {
		title := strings.Repeat(" ", 300) + "the real title starts here " + strings.Repeat("b", 300)

		got := ClampIssueTitle(title)

		require.True(t, strings.HasPrefix(got, "the real title starts here"),
			"the clamp must spend its budget on the title, not on the padding in front of it")
		require.LessOrEqual(t, utf8.RuneCountInString(got), IssueTitleMaxChars)
	})
}

// A title that is nothing but whitespace is UNDER the cap, so a clamp that
// length-checks before it trims hands it back byte-identical and the forge sees
// a blank title. Both forges reject that, and on the deferred edit path the
// rejection is unrecoverable: EditIssue 400s on every replay, the drain returns
// on that error before removePendingComments, and the intent requeues forever
// with every intent queued behind it stuck too. Trimming has to happen BEFORE
// the length check, not after it.
func TestClampIssueTitle_WhitespaceOnlyTitlesCollapseToEmpty(t *testing.T) {
	cases := []struct {
		name  string
		title string
	}{
		{"spaces", "   "},
		{"tabs", "\t\t"},
		{"newlines", "\n\n"},
		{"mixed ascii whitespace", " \t \n "},
		{"unicode non-breaking space", "  "},
		{"ideographic space", "　"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, "", ClampIssueTitle(tc.title),
				"a whitespace-only title must collapse to empty so callers can drop it, "+
					"not reach the forge as a blank title it will reject")
		})
	}
}

// Trimming moved above the length check, so a title that fits but carries
// padding is no longer returned byte-identical. That is deliberate: the forge
// strips before it stores, so the trimmed value is what the mirror will hold
// anyway and matching it here stops a spurious Status.Title diff on every
// refresh.
func TestClampIssueTitle_TrimsPaddingOffTitlesThatAlreadyFit(t *testing.T) {
	require.Equal(t, "a short title", ClampIssueTitle("  a short title\n"))
}

// Cutting at a fixed offset routinely lands inside the run of spaces between two
// words, which would render as "... foo   ...(truncated)".
func TestClampIssueTitle_TrimsWhitespaceOffTheCutBeforeMarking(t *testing.T) {
	// The cut lands at IssueTitleMaxChars-len(marker), which this input places
	// squarely inside a run of spaces.
	cut := IssueTitleMaxChars - utf8.RuneCountInString(titleTruncatedMarker)
	title := strings.Repeat("a", cut-3) + strings.Repeat(" ", 10) + strings.Repeat("b", 60)

	got := ClampIssueTitle(title)

	require.Equal(t, strings.Repeat("a", cut-3)+titleTruncatedMarker, got)
}

// ClampIssueTitle feeds a line-oriented wire format ("title: <t>\n<body>").
// An interior line break in a title decodes back as the issue BODY and
// overwrites the real one on the forge, so every interior line break must be
// flattened to a single space BEFORE the trim and before the length check.
// A title is a single line everywhere it is used, and an interior line break in
// one is not merely untidy: the deferred edit intent carries the title in a
// LINE-ORIENTED encoding whose decoder cuts at the first "\n", so an unflattened
// break let the tail of a title come back as the issue BODY and overwrite the
// real one on the forge. The trim never reached it - it only touches the ends.
//
// The last two cases are pins, not new behaviour: flattening must not defeat the
// trim it runs in front of.
func TestClampIssueTitle_FlattensLineBreaks(t *testing.T) {
	cases := []struct {
		name  string
		title string
		want  string
	}{
		{"LF", "Fix crash\nSTATUS: done", "Fix crash STATUS: done"},
		{"CRLF collapses to one space", "Fix crash\r\nSTATUS: done", "Fix crash STATUS: done"},
		{"CR", "a\rb", "a b"},
		{"VT and FF", "a\vb\fc", "a b c"},
		{"unicode line separators", "a\u2028b\u2029c\u0085d", "a b c d"},
		{"leading and trailing LF is a no-op post-trim", "\nreal title\n", "real title"},
		{"whitespace-only still collapses", "  \n  ", ""},
	}
	lineBreakRunes := []rune{'\n', '\r', '\v', '\f', 0x2028, 0x2029, 0x0085}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClampIssueTitle(tc.title)
			require.Equal(t, tc.want, got)
			for _, r := range lineBreakRunes {
				require.NotContains(t, got, string(r))
			}
		})
	}
}

// Flattening runs BEFORE the length check and the cut, so a break cannot survive
// by sitting on either side of the cut point.
func TestClampIssueTitle_FlattensBeforeCutting(t *testing.T) {
	t.Run("newline past the cut point", func(t *testing.T) {
		title := strings.Repeat("a", 300) + "\n" + strings.Repeat("b", 300)

		got := ClampIssueTitle(title)

		require.Equal(t, IssueTitleMaxChars, utf8.RuneCountInString(got))
		require.True(t, strings.HasSuffix(got, titleTruncatedMarker))
		require.NotContains(t, got, "\n")
	})

	t.Run("newline before the cut point", func(t *testing.T) {
		title := "a\nb" + strings.Repeat("c", 300)

		got := ClampIssueTitle(title)

		require.NotContains(t, got, "\n")
		require.Equal(t, IssueTitleMaxChars, utf8.RuneCountInString(got))
	})
}
