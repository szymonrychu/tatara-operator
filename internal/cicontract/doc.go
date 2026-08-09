// Package cicontract holds tests that assert properties of this repo's CI
// definition itself, rather than of any Go code.
//
// It exists because `.github/workflows/ci-shared.yml` is not this repo's CI
// alone: five sibling repos consume it by tag, so a hole in the shared job
// graph is a hole in every one of them at once, and nothing else in the tree
// would notice. #556 is what that costs - the merge gate never compiled the
// Dockerfile, three repos merged a Go bump green, and their release train was
// dead for ten days on a one-line pin the gate structurally could not see.
package cicontract
