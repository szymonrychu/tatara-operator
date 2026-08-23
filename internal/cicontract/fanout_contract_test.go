package cicontract

import (
	"encoding/json"
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

// TestReleaseFanout_HasOneBumpStepPerLiveConsumer is the assertion that would
// have caught a consumer added to LiveConsumers and forgotten in release.yml.
func TestReleaseFanout_HasOneBumpStepPerLiveConsumer(t *testing.T) {
	job := strings.Join(readFanoutJob(t), "\n")

	parentRe := regexp.MustCompile(`(?m)^\s*parent_repo:\s*(\S+)\s*$`)
	var got []string
	for _, m := range parentRe.FindAllStringSubmatch(job, -1) {
		got = append(got, m[1])
	}
	if len(got) != len(LiveConsumers) {
		t.Fatalf("%s has %d cd-release steps (%v), want one per LiveConsumers entry (%v)",
			fanoutJobName, len(got), got, LiveConsumers)
	}
	for _, want := range LiveConsumers {
		if !slicesContains(got, want) {
			t.Errorf("%s bumps %v and never %s: that consumer is in LiveConsumers, so the "+
				"currency check reds on it daily while nothing fixes it", fanoutJobName, got, want)
		}
	}
}

// TestReleaseFanout_EveryBumpIsGatedOnItsOwnPlanKeyAndOnThePlanSucceeding
// covers two independent ways the gating goes wrong.
//
//  1. A step gated on a key `plan` never writes never runs.
//  2. `plan` writes $GITHUB_OUTPUT BEFORE it returns 1 on an unreadable
//     consumer, so the outputs are present and non-empty on the exact failure
//     the design is built around. A step carrying a status-check function
//     (`!cancelled()`) loses the implicit `success() &&`, so without an explicit
//     `steps.plan.outcome == 'success'` it bumps off a plan the code just
//     refused to vouch for - while a step WITHOUT one is skipped. That
//     asymmetry drops one repo and fans the other three out anyway.
func TestReleaseFanout_EveryBumpIsGatedOnItsOwnPlanKeyAndOnThePlanSucceeding(t *testing.T) {
	ifRe := regexp.MustCompile(`(?m)^\s*if:\s*(.+?)\s*$`)
	job := strings.Join(readFanoutJob(t), "\n")

	conds := ifRe.FindAllStringSubmatch(job, -1)
	if len(conds) != len(LiveConsumers) {
		t.Fatalf("%s has %d `if:` conditions, want one per consumer (%d)",
			fanoutJobName, len(conds), len(LiveConsumers))
	}

	for i, c := range conds {
		cond := c[1]
		if !strings.Contains(cond, "steps.plan.outcome == 'success'") {
			t.Errorf("condition %d (%s) does not require the plan to have succeeded; "+
				"`plan` writes its outputs before it fails, so this bumps off a plan "+
				"that could not read every consumer", i, cond)
		}
		if !strings.Contains(cond, "!cancelled()") {
			t.Errorf("condition %d (%s) has no `!cancelled()`, so it inherits the implicit "+
				"`success() &&` and one consumer's bump failure strands the rest", i, cond)
		}
	}

	for _, repo := range LiveConsumers {
		key := "steps.plan.outputs.bump-" + ShortName(repo) + " == 'true'"
		if !strings.Contains(job, key) {
			t.Errorf("no step is gated on %q; PlanBumps emits that key and nothing reads it", key)
		}
	}
}

// TestReleaseFanout_PinPatternAgreesWithTheReader is the other half of the same
// class: fleetpins.go READS the pin with pinRe and cd-release's apply-pins.py
// WRITES it with a regex re-typed as a JSON literal in release.yml, four times.
// Two independently-maintained patterns for one line is how a reader and a
// writer end up disagreeing about what a pin looks like.
func TestReleaseFanout_PinPatternAgreesWithTheReader(t *testing.T) {
	job := readFanoutJob(t)

	var specs [][]pinSpec
	for i := 0; i < len(job); i++ {
		if strings.TrimSpace(job[i]) != "pins: |" {
			continue
		}
		var raw []string
		for j := i + 1; j < len(job); j++ {
			line := job[j]
			if strings.TrimSpace(line) == "" {
				break
			}
			if !strings.HasPrefix(line, strings.Repeat(" ", 12)) {
				break
			}
			raw = append(raw, strings.TrimSpace(line))
		}
		var parsed []pinSpec
		if err := json.Unmarshal([]byte(strings.Join(raw, "\n")), &parsed); err != nil {
			t.Fatalf("pins block at line %d is not valid JSON: %v\n%s", i, err, strings.Join(raw, "\n"))
		}
		specs = append(specs, parsed)
	}

	if len(specs) != len(LiveConsumers) {
		t.Fatalf("found %d `pins:` blocks, want one per consumer (%d)", len(specs), len(LiveConsumers))
	}

	canonical := "    uses: " + ProducerRepo + "/" + CISharedPath + "@v3.10.0"
	for i, block := range specs {
		if len(block) != 1 {
			t.Fatalf("pins block %d has %d entries, want exactly the ci.yml pin", i, len(block))
		}
		spec := block[0]
		if spec.File != ConsumerWorkflowPath {
			t.Errorf("pins block %d rewrites %q, but the reader reads %q",
				i, spec.File, ConsumerWorkflowPath)
		}
		if spec.Pattern != specs[0][0].Pattern {
			t.Errorf("pins block %d's pattern differs from block 0's; four hand-copied regexes "+
				"for one line is a drift waiting to happen:\n  %s\n  %s", i, specs[0][0].Pattern, spec.Pattern)
		}
		re, err := regexp.Compile(spec.Pattern)
		if err != nil {
			t.Fatalf("pins block %d's pattern does not compile: %v", i, err)
		}
		if !re.MatchString(canonical) {
			t.Errorf("pins block %d's writer pattern does not match the line the reader accepts:\n  %s\n  %s",
				i, spec.Pattern, canonical)
		}
	}

	// And the reader must accept the line the writer produces.
	if _, err := PinRef([]byte(canonical + "\n")); err != nil {
		t.Errorf("PinRef rejects the line cd-release writes: %v", err)
	}
}

func slicesContains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
