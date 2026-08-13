// Package upgrade renders the goal text for a cron-minted upgrade Task.
//
// It renders the RESOLVED POLICY and nothing else. Every judgement the policy
// describes - which release is mandatory, whether a hop is safe, what the blast
// radius is - belongs to the agent, made against release notes no operator can
// read. See tatara-agent-skills' tatara-upgrade-workflow for the procedure.
package upgrade

import (
	"fmt"
	"strings"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// GoalProject builds the turn-0 goal for an upgrade Task over repoSlugs.
//
// A nil policy renders the DEFAULT-OFF shape: engine none, nextHopOnly, no
// minimum release age. It has to be resolved here rather than read back off the
// CR, because a kubebuilder default declared inside a nil struct pointer is
// never applied - the API server has no object to default into.
func GoalProject(repoSlugs []string, p *v1alpha1.UpgradePolicySpec) string {
	engine, strategy := "none", "nextHopOnly"
	if p != nil {
		if p.Engine != "" {
			engine = p.Engine
		}
		if p.MajorStrategy != "" {
			strategy = p.MajorStrategy
		}
	}
	age := "major 0d, minor 0d, patch 0d (bleeding edge)"
	if p != nil && p.MinimumReleaseAge != nil {
		a := p.MinimumReleaseAge
		age = fmt.Sprintf("major %dd, minor %dd, patch %dd", a.Major, a.Minor, a.Patch)
		if a.Major == 0 && a.Minor == 0 && a.Patch == 0 {
			age += " (bleeding edge)"
		}
	}
	return fmt.Sprintf(`Invoke the `+"`tatara-upgrade-workflow`"+` skill FIRST and follow its phases in order.

You are the upgrade agent for the following repositories: %s.

Take EXACTLY ONE upgrade unit this Task. An upgrade unit is one thing being upgraded, across however many repos it touches. Call `+"`task_context(index=true)`"+` before you pick and skip any unit a live sibling Task already claims - that index read plus a read of each sibling's merge request is the only dedup mechanism there is.

## Resolved policy

- engine: %s
- majorStrategy: %s
- minimumReleaseAge: %s

## Non-negotiable

Read the release notes for EVERY release between the current pin and your target, not just the target. There is no machine-readable signal that a hop is mandatory: artifacthub's changes annotation has no breaking kind, and no dependency engine on this platform reads it. Prose-reading is the job.

Run the repo's REAL test suite, not just a build. A build that succeeds while the tests are red is a failed upgrade.

Declare the merge order whenever you change more than one repo, in publish-dependency order. There is no default: getting it backwards ships a chart against an image tag that never published.

End with `+"`submit_outcome(action=submitted, ...)`"+` or `+"`submit_outcome(action=declined, decline_reason=...)`"+`. declined is a correct and common answer.`,
		strings.Join(repoSlugs, ", "), engine, strategy, age)
}
