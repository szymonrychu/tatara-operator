// Package cicontract holds STATIC TEXT ASSERTIONS about this repo's CI
// definition. Read the limits below before trusting anything in it.
//
// It exists because `.github/workflows/ci-shared.yml` is not this repo's CI
// alone: five sibling repos consume it by tag, so a hole in the shared job
// graph is a hole in every one of them at once, and nothing else in the tree
// would notice. #556 is what that costs - the merge gate never compiled the
// Dockerfile, three repos merged a Go bump green, and their release train was
// dead for ten days on a one-line pin the gate structurally could not see.
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
package cicontract
