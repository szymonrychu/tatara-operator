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

// GoalAdopted builds the goal for an upgrade Task ADOPTED onto a third-party
// dependency merge request that already exists.
//
// TWO AGENTS READ THIS TEXT. Spec.Goal goes into the turn-0 bundle for whatever
// pod the Task's CURRENT state spawns, and an adopted Task spawns a REVIEW pod
// at awaiting-review and an UPGRADE pod at under-implementation. So it is
// written as a shared preamble plus two clearly labelled halves, and each half
// says what that agent's verdict actually DOES - which is the thing neither
// agent can infer, because an adopted merge request is a shape neither has seen
// before: it is a third party's, and yet approving it MERGES it.
//
// It is a different job from GoalProject and deliberately shares none of its
// text. There is nothing to discover: the unit is whatever this merge request
// bumps, the pin change is already committed on its branch, and the changelog is
// already in the merge request BODY - which is the only place it exists at all,
// because a Renovate dry run never fetches release notes.
func GoalAdopted(repoSlug, headBranch, mrTitle string, number int, p *v1alpha1.UpgradePolicySpec) string {
	strategy := "nextHopOnly"
	if p != nil && p.MajorStrategy != "" {
		strategy = p.MajorStrategy
	}
	return fmt.Sprintf(`A dependency merge request is already open in %s: !%d, %q, on branch %s. A third-party dependency bot opened it; the platform adopted it into this Task. That branch is your TASK_BRANCH. Do NOT open a new merge request and do NOT create a branch.

The merge request BODY carries the changelog and release notes for this bump. That text exists nowhere else - the discovery engine does not fetch release notes at all - so the body is the primary source and the upstream release pages are the confirmation. A body elided for size is MARKED: the bundle renders it as `+"`<body truncated=\"true\">`"+`, so an empty body carrying that marker was cut under byte pressure and an empty body without it is genuinely empty. Either way, re-read it with `+"`scm_read(kind=\"mr\", repo=..., number=...)`"+` before concluding the changelog says nothing.

The question, for both of you, is one question: does the changelog name a breaking change, a renamed or removed configuration key, a required migration, a raised minimum version, or a mandatory intermediate release?

## IF YOU ARE THE REVIEW AGENT

Invoke `+"`tatara-review-checklist`"+` and read its ADOPTED MERGE REQUEST row before you pick a verdict. Your verdict here does NOT park, which is what it does on an ordinary third-party merge request:

- `+"`approve`"+` means the changelog obliges nothing beyond the pin and the change is ready: approving MERGES it. That is the COMMON case and it is correct - most of these merge requests need nothing from us, and routing a trivial bump through an implement turn is the cost this design exists to avoid.
- `+"`request_changes`"+` means the changelog obliges complementary work. It hands the merge request to the upgrade agent, which pushes onto this same branch. Your findings ARE its work order, so name the key, the migration or the manifest, not just "check the release notes".

Read the diff as well as the body. It is usually one line; if it is not - a vendored manifest, a lockfile, a generated file - that is a signal, not noise.

## IF YOU ARE THE UPGRADE AGENT

Invoke the `+"`tatara-upgrade-workflow`"+` skill FIRST and follow its ADOPTED MERGE REQUEST path (section 2a). The discovery and claim phases do not apply to you: the unit was decided when the merge request was opened.

You are here because a review round requested changes on this merge request. Read those findings first - they are in your bundle - then make the complementary change on this SAME branch, so one merge request carries the bump and the code that makes it work. If the findings are wrong, say why and decline rather than making a change you do not believe in.

If the changelog says this hop is not safe to take directly - a mandatory intermediate release stands between the current pin and this target - do NOT retarget the branch. Say so in a merge request comment and decline. The bot chose the target and the title names it.

## Scope, for both of you

ONE REPO. This Task is bound to one repo, %s, and its merge order is that repo alone. If the changelog obliges a change in another repo, decline and name it: the scheduled discovery path handles cross-repo units, this one does not.

## Resolved policy

- majorStrategy: %s

## Non-negotiable

Run the repo's REAL test suite, not just a build. A build that succeeds while the tests are red is a failed upgrade.

Never force-push. Both `+"`--force`"+` and `+"`--force-with-lease`"+` are hard-denied in this pod, and this branch belongs to a third-party bot.

Never rebase this branch and never ask anyone else to. A rebase request discards every commit on it that is not the bot's own.`,
		repoSlug, number, mrTitle, headBranch, repoSlug, strategy)
}
