package cicontract

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
)

// The release fan-out is FOUR HARD-CODED STEPS in release.yml, one per entry in
// LiveConsumers, and nothing but this file ties the two together.
//
// That is the hole #640 is about, one edit away: add a fifth consumer and
// `audit` reds daily forever, `plan` writes bump-<repo>=true, and no step
// consumes it - a fan-out that silently drops a repo. The four-explicit-steps
// shape was chosen over a matrix because a dynamic
// `steps.plan.outputs[format(...)]` miss skips the bump SILENTLY; that argument
// only holds while something asserts the static list is complete.
//
// EVERY ASSERTION HERE IS PER STEP. An earlier version of this file asserted
// the three properties independently over the whole job - the multiset of
// `parent_repo:`, that every `if:` carried both gate halves, and that each
// bump key appeared SOMEWHERE in the job text - and never joined them. Swapping
// two steps' `if:` lines passed all three while a release that made only
// tatara-memory stale opened a bump PR against tatara-cli and none against
// tatara-memory. `strings.Contains(job, key)` was satisfiable by a COMMENT,
// too, since the job text is read line-wise and not as YAML.
//
// Offline: it reads this repo's own working tree, the same way
// merge_gate_test.go does. It is a contract test, not a fleet check - a green
// run here still says nothing about which revision a consumer executes, which
// is what cmd/ci-shared-pins is for.
const releaseWorkflowPath = "../../.github/workflows/release.yml"

const fanoutJobName = "fanout-ci-shared"

// pinSpec is one entry of a cd-release `pins:` JSON array.
type pinSpec struct {
	File          string `json:"file"`
	Pattern       string `json:"pattern"`
	ValueTemplate string `json:"value_template"`
}

// bumpStep is one `uses: cd-release` step, with the three fields that have to
// agree with each other READ FROM THE SAME STEP.
type bumpStep struct {
	name       string
	line       int
	cond       string
	parentRepo string
	pins       []pinSpec
}

// jobBlock returns the lines of one top-level job, from `  <name>:` up to the
// next key at the same indent.
func jobBlock(t *testing.T, workflow, name string) []string {
	t.Helper()
	var (
		out []string
		in  bool
	)
	topKey := regexp.MustCompile(`^  \S+:\s*$`)
	for _, line := range strings.Split(workflow, "\n") {
		switch {
		case line == "  "+name+":":
			in = true
		case in && topKey.MatchString(line):
			return out
		}
		if in {
			out = append(out, line)
		}
	}
	if !in {
		t.Fatalf("%s declares no %q job", releaseWorkflowPath, name)
	}
	return out
}

func readFanoutJob(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile(releaseWorkflowPath)
	if err != nil {
		t.Fatalf("read %s: %v", releaseWorkflowPath, err)
	}
	return jobBlock(t, string(b), fanoutJobName)
}

var (
	stepStartRe = regexp.MustCompile(`^      - name:\s*(.+?)\s*$`)
	condRe      = regexp.MustCompile(`^\s*if:\s*(.+?)\s*$`)
	parentRe    = regexp.MustCompile(`^\s*parent_repo:\s*(\S+)\s*$`)
	commentRe   = regexp.MustCompile(`^\s*#`)
)

// bumpSteps splits the job into its `cd-release` steps and reads each step's
// own `if:`, `parent_repo:` and `pins:`. Comment lines are dropped first, so a
// property can never be satisfied by prose that gates nothing.
func bumpSteps(t *testing.T, job []string) []bumpStep {
	t.Helper()

	var steps []bumpStep
	var cur *bumpStep
	flush := func() {
		if cur != nil && cur.parentRepo != "" {
			steps = append(steps, *cur)
		}
		cur = nil
	}

	for i := 0; i < len(job); i++ {
		line := job[i]
		if commentRe.MatchString(line) {
			continue
		}
		if m := stepStartRe.FindStringSubmatch(line); m != nil {
			flush()
			cur = &bumpStep{name: m[1], line: i}
			continue
		}
		if cur == nil {
			continue
		}
		switch {
		case condRe.MatchString(line):
			cur.cond = condRe.FindStringSubmatch(line)[1]
		case parentRe.MatchString(line):
			cur.parentRepo = parentRe.FindStringSubmatch(line)[1]
		case strings.TrimSpace(line) == "pins: |":
			var raw []string
			for j := i + 1; j < len(job); j++ {
				l := job[j]
				if strings.TrimSpace(l) == "" || !strings.HasPrefix(l, strings.Repeat(" ", 12)) {
					break
				}
				raw = append(raw, strings.TrimSpace(l))
			}
			if err := json.Unmarshal([]byte(strings.Join(raw, "\n")), &cur.pins); err != nil {
				t.Fatalf("step %q: pins block is not valid JSON: %v\n%s", cur.name, err, strings.Join(raw, "\n"))
			}
		}
	}
	flush()
	return steps
}

// TestReleaseFanout_HasOneBumpStepPerLiveConsumer is the assertion that would
// have caught a consumer added to LiveConsumers and forgotten in release.yml.
func TestReleaseFanout_HasOneBumpStepPerLiveConsumer(t *testing.T) {
	steps := bumpSteps(t, readFanoutJob(t))

	seen := map[string]int{}
	for _, s := range steps {
		seen[s.parentRepo]++
	}
	if len(steps) != len(LiveConsumers) {
		t.Fatalf("%s has %d cd-release steps, want one per LiveConsumers entry (%d): %v",
			fanoutJobName, len(steps), len(LiveConsumers), seen)
	}
	for _, want := range LiveConsumers {
		switch seen[want] {
		case 1:
		case 0:
			t.Errorf("%s has no step with parent_repo: %s. That consumer is in LiveConsumers, so the "+
				"currency check reds on it daily while nothing ever fixes it", fanoutJobName, want)
		default:
			t.Errorf("%s bumps %s %d times; two steps rewriting one pin race each other",
				fanoutJobName, want, seen[want])
		}
	}
}

// TestReleaseFanout_EveryStepGatesOnItsOwnConsumer is the cross-wiring
// assertion. The gate key is derived from THE SAME STEP's parent_repo, so
// swapping two steps' `if:` lines fails here - previously it passed, and a
// release that made only tatara-memory stale opened a PR against tatara-cli.
//
// It also covers two independent ways the gating goes wrong on its own:
//
//  1. A step gated on a key `plan` never writes never runs.
//  2. `plan` writes $GITHUB_OUTPUT BEFORE it returns 1 on an unreadable
//     consumer, so the outputs are present and non-empty on the exact failure
//     the design is built around. A step carrying a status-check function
//     (`!cancelled()`) loses the implicit `success() &&`, so without an explicit
//     `steps.plan.outcome == 'success'` it bumps off a plan the code just
//     refused to vouch for - while a step WITHOUT one is skipped. That
//     asymmetry drops one repo and fans the other three out anyway.
func TestReleaseFanout_EveryStepGatesOnItsOwnConsumer(t *testing.T) {
	for _, s := range bumpSteps(t, readFanoutJob(t)) {
		if s.cond == "" {
			t.Errorf("step %q (line %d) bumps %s with no `if:` at all, so every release opens a PR against it",
				s.name, s.line, s.parentRepo)
			continue
		}
		key := fmt.Sprintf("steps.plan.outputs.%s == 'true'", BumpOutputKey(s.parentRepo))
		if !strings.Contains(s.cond, key) {
			t.Errorf("step %q bumps %s but gates on %q, which does not contain %q. "+
				"The gate and the target are read from the same step here precisely so a cross-wired "+
				"pair cannot pass: this step would bump the wrong repo, and the right one never",
				s.name, s.parentRepo, s.cond, key)
		}
		if !strings.Contains(s.cond, "steps.plan.outcome == 'success'") {
			t.Errorf("step %q (%s) does not require the plan to have succeeded; "+
				"`plan` writes its outputs before it fails, so this bumps off a plan "+
				"that could not read every consumer", s.name, s.cond)
		}
		if !strings.Contains(s.cond, "!cancelled()") {
			t.Errorf("step %q (%s) has no `!cancelled()`, so it inherits the implicit "+
				"`success() &&` and one consumer's bump failure strands the rest", s.name, s.cond)
		}
	}
}

// TestReleaseFanout_PinPatternAgreesWithTheReader is the other half of the same
// class: fleetpins.go READS the pin with pinRe and cd-release's apply-pins.py
// WRITES it with a regex re-typed as a JSON literal in release.yml, four times.
// Two independently-maintained patterns for one line is how a reader and a
// writer end up disagreeing about what a pin looks like.
func TestReleaseFanout_PinPatternAgreesWithTheReader(t *testing.T) {
	steps := bumpSteps(t, readFanoutJob(t))

	// The reader must accept every spelling the writer leaves behind, including
	// the trailing-comment form: apply-pins.py's `...@).*$` rewrites the ref and
	// keeps whatever follows it, so a consumer that annotates its pin stays
	// writable. A reader that rejected it would turn that one-line edit in a
	// sibling repo into a red release HERE.
	written := []string{
		"    uses: " + ProducerRepo + "/" + CISharedPath + "@v3.10.0",
		"    uses: " + ProducerRepo + "/" + CISharedPath + "@v3.10.0  # pinned by cd",
	}

	for _, s := range steps {
		if len(s.pins) != 1 {
			t.Fatalf("step %q has %d pins entries, want exactly the ci.yml pin", s.name, len(s.pins))
		}
		spec := s.pins[0]
		if spec.File != ConsumerWorkflowPath {
			t.Errorf("step %q rewrites %q, but the reader reads %q", s.name, spec.File, ConsumerWorkflowPath)
		}
		if spec.Pattern != steps[0].pins[0].Pattern {
			t.Errorf("step %q's pattern differs from the first step's; four hand-copied regexes "+
				"for one line is a drift waiting to happen:\n  %s\n  %s",
				s.name, steps[0].pins[0].Pattern, spec.Pattern)
		}
		re, err := regexp.Compile(spec.Pattern)
		if err != nil {
			t.Fatalf("step %q's pattern does not compile: %v", s.name, err)
		}
		for _, line := range written {
			if !re.MatchString(line) {
				t.Errorf("step %q's writer pattern does not match a line the reader accepts:\n  %s\n  %s",
					s.name, spec.Pattern, line)
			}
			if _, err := PinRef([]byte(line + "\n")); err != nil {
				t.Errorf("PinRef rejects a line cd-release writes (%q): %v", line, err)
			}
		}
	}
}
