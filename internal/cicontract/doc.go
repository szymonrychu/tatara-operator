// Package cicontract holds STATIC TEXT ASSERTIONS about this repo's CI
// definition. Read the limits below before trusting anything in it.
//
// It exists because `.github/workflows/ci-shared.yml` is not this repo's CI
// alone: four live sibling repos consume it by tag - LiveConsumers in
// fleetpins.go; tatara-chat was a fifth and is decommissioned - so a hole in
// the shared job graph is a hole in every one of them at once, and nothing else
// in the tree would notice. #556 is what that costs - the merge gate never
// compiled the Dockerfile, three repos merged a Go bump green, and their
// release train was dead for ten days on a one-line pin the gate structurally
// could not see.
//
// WHAT THESE TESTS CANNOT DO, and it is not a small caveat.
//
// They parse a YAML file and match strings in it. They do not run a workflow,
// do not reach buildkitd, and do not execute one line of shell. A job can be
// RED IN CI WHILE EVERY TEST HERE IS GREEN, and that is not hypothetical: the
// first cut of #556 asserted that the literal `type=cacheonly` appeared in the
// workflow, went green, and shipped a job that failed on its own PR with
// `exporter "cacheonly" could not be found` - buildkit v0.18.2's daemon has no
// such exporter. The assertion was true. The job was broken. A test whose
// green light survives a red gate is worse than no test, because it launders
// the failure.
//
// So every test here is named TestWorkflowText_*, to make the reader ask what
// it actually proved. THE REAL SIGNAL IS THE `image-verify` JOB GOING GREEN ON
// A PULL REQUEST. These tests only stop a specific, known set of regressions
// from being introduced silently by an editor who never runs the workflow:
// re-gating the Dockerfile build behind `push`, adding a publishing output or
// registry credentials to the PR path, or drifting the PR build away from the
// remote-context shape the post-merge push build uses.
//
// Anything about what the DAEMON accepts has to be probed against the daemon.
// There is no way to learn it from here.
//
// THE LIMITATION THAT WAS NOT ON THAT LIST, and it is the one that bit (#640).
//
// merge_gate_test.go reads `../../.github/workflows/ci-shared.yml`: THIS repo's
// working tree. It says nothing whatever about which revision of that file any
// consumer executes, and for the four that matter the answer was `@v1.36.1`,
// tagged nine days before #556 was filed. Every test above was green on main
// for fourteen days while the `image-verify` job they guard had never once run
// in any of the four repos #556 measured. The gate's SHAPE was correct and its
// REACH was zero, and nothing in this package could tell the difference.
//
// AND THE PIN IS ONLY HALF OF IT. A consumer can run the newest ci-shared.yml
// with the gate switched off - `enable-image-verify: false` in its own caller -
// and a content comparison cannot see that either, because ci-shared.yml at
// that pin is byte-identical either way. Same shape, same zero reach, one line
// further out. So Audit also reports a consumer that opts out unless
// ImageVerifyExempt names it and says why, which is what makes an opt-out a
// diff against THIS repo instead of a line in a caller nobody reads.
//
// One repo is exempt today and its reason is the general one: tatara-claude-
// code-wrapper's Dockerfile pulls a private Harbor base image, image-verify
// passes no `--opt target=` so the whole graph is always solved, and this job
// holds no registry credentials. That last part is the CONSTRAINT, not an
// oversight - it is on the forbidden list above, the only Harbor credentials in
// this workflow are the PUSH ones, and on pull_request the CALLER is taken from
// the PR head, so a PR could edit the job that handles them. A gate is not
// worth a push credential on a PR-editable path. Closing it needs a pull-only
// credential or an anonymously pullable base image: a registry decision, not a
// workflow edit.
//
// fleetpins.go is the half that can: it resolves each consumer's pin and
// compares the ci-shared.yml at that ref against a reference BY CONTENT. It
// takes a Fetcher rather than doing I/O, so `make test` stays offline; the
// network half is cmd/ci-shared-pins, driven by two callers that must stay
// separate:
//
//   - release.yml's `fanout-ci-shared` job WRITES the pin, so a ci-shared
//     change reaches the fleet on the release that publishes it. That is what
//     was missing - the pin was correct in shape and nothing advanced it.
//   - ci-shared-currency.yml READS it on a schedule and files an issue. It is
//     deliberately not a merge gate and not in lint: staleness is a fleet
//     condition, never a property of the PR in front of you, and a gate that is
//     legitimately red while a release is in flight gets weakened to a warning
//     and then deleted.
package cicontract
