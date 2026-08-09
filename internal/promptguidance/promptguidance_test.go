package promptguidance

import (
	"strings"
	"testing"
)

// instructsReport models what an instruction-following agent actually does
// with a guidance block: it walks each sentence (split on '.') and, for any
// sentence mentioning report_internal_issue, checks whether a standalone
// negation word ("not" or an "n't" contraction) appears earlier as a whole
// token in that sentence. A sentence that names the tool without a preceding
// negation token reads as an instruction to call it; a negated sentence ("do
// NOT call report_internal_issue") reads as an instruction NOT to. Matching
// whole tokens (not a raw substring) matters: "you cannot reach" contains the
// substring "not " inside "cannot" without being a negation of anything after
// it. This is a coarse model of agent-instruction-following, not a full NLP
// parse, but it distinguishes "call this" from "do not call this" - a bare
// substring grep for "report_internal_issue" cannot, since both phrasings
// contain it.
func instructsReport(guidance string) bool {
	isNegationToken := func(tok string) bool {
		tok = strings.ToLower(strings.Trim(tok, "`'\"(),:;-"))
		return tok == "not" || strings.HasSuffix(tok, "n't")
	}
	for _, sentence := range strings.FieldsFunc(guidance, func(r rune) bool { return r == '.' || r == '\n' }) {
		idx := strings.Index(sentence, "report_internal_issue")
		if idx == -1 {
			continue
		}
		negatedBefore := false
		for _, tok := range strings.Fields(sentence[:idx]) {
			if isNegationToken(tok) {
				negatedBefore = true
				break
			}
		}
		if negatedBefore {
			continue
		}
		return true
	}
	return false
}

func TestInstructsReport_Sanity(t *testing.T) {
	// Positive control: PlatformProblemGuidance is the baseline "do report a
	// platform problem" text - it must read as instructing a report.
	if !instructsReport(PlatformProblemGuidance) {
		t.Fatal("sanity check failed: PlatformProblemGuidance should instruct a report_internal_issue call")
	}
	// Regression baseline: MemoryDisabledGuidance was written specifically to
	// NOT instruct a report (a disabled project isn't a fault). If this ever
	// flips, the detector itself is broken, not just the guidance text.
	if instructsReport(MemoryDisabledGuidance) {
		t.Fatal("sanity check failed: MemoryDisabledGuidance must not instruct a report_internal_issue call")
	}
}

// tatara-operator#523: MemoryDegradedGuidance declared the degraded condition
// "EXPECTED" and "already alerted on" while ALSO instructing
// report_internal_issue - so every turn on a degraded (enabled but not
// stably-ready) project manufactured one bogus platform-problem alert. The
// text must not instruct a report for a condition it declares already-tracked.
func TestMemoryDegradedGuidanceDoesNotInstructReport(t *testing.T) {
	if instructsReport(MemoryDegradedGuidance) {
		t.Fatal("MemoryDegradedGuidance instructs report_internal_issue for a condition it declares " +
			"already EXPECTED and already alerted on (tatara-operator#523)")
	}
	// The guidance must still say the condition is tracked, or this test would
	// pass vacuously if the "already alerted on" framing were simply deleted
	// rather than fixed.
	if !strings.Contains(MemoryDegradedGuidance, "already alerted") {
		t.Fatal("MemoryDegradedGuidance should still state the degraded condition is already alerted on")
	}
}
