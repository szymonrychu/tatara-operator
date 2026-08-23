package cicontract

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// ProducerRepo owns ci-shared.yml. Every consumer's pin resolves against it.
const ProducerRepo = "szymonrychu/tatara-operator"

// CISharedPath is the shared workflow inside ProducerRepo, and
// ConsumerWorkflowPath is the caller that pins it. Both are repo-relative, so
// they name a path on the FORGE - unlike merge_gate_test.go's ciSharedPath,
// which is a working-tree path and is exactly the reason #640 was invisible.
const (
	CISharedPath         = ".github/workflows/ci-shared.yml"
	ConsumerWorkflowPath = ".github/workflows/ci.yml"
)

// consumerRef is the branch a consumer's pin is read from. A consumer's default
// branch is what its PRs merge into and therefore what its CI runs.
const consumerRef = "main"

// LiveConsumers are the repos whose ci.yml calls ci-shared.yml by tag.
//
// tatara-chat was the fifth (doc.go still said "five sibling repos" when #640
// was filed) and is decommissioned: installed: false in tatara-helmfile's
// helmfile.yaml.gotmpl. It is deliberately EXEMPT rather than silently dropped
// - a completeness check that reds forever on a repo nobody intends to fix is
// a check that gets muted, and then deleted.
var LiveConsumers = []string{
	"szymonrychu/tatara-cli",
	"szymonrychu/tatara-memory",
	"szymonrychu/tatara-memory-repo-ingester",
	"szymonrychu/tatara-claude-code-wrapper",
}

// Fetcher reads a public repo from the forge. It is an interface so Audit and
// ReferenceAtNewestTag stay offline in `make test`: internal/promptguidance/
// toolnames.go records why this repo refuses a network-dependent test, and a
// fetch that soft-warns on failure is a check that can silently stop checking.
type Fetcher interface {
	File(ctx context.Context, repo, ref, path string) ([]byte, error)
	Tags(ctx context.Context, repo string) ([]string, error)
}

// FindingKind separates the two states the callers must handle differently: a
// stale consumer is what the release fan-out is about to fix, an unreadable one
// is a check that could not check.
type FindingKind string

const (
	FindingStale      FindingKind = "stale"
	FindingUnreadable FindingKind = "unreadable"
)

// Finding is one problem with one consumer. Consumer is empty for a fleet-wide
// one (nothing was checked at all).
type Finding struct {
	Consumer string
	Kind     FindingKind
	Detail   string
}

func (f Finding) String() string {
	if f.Consumer == "" {
		return fmt.Sprintf("[%s] %s", f.Kind, f.Detail)
	}
	return fmt.Sprintf("[%s] %s: %s", f.Kind, f.Consumer, f.Detail)
}

// pinRe matches the caller line. Anchored on the full workflow path so a
// consumer that also calls some other reusable workflow from this repo is not
// mistaken for a ci-shared pin.
var pinRe = regexp.MustCompile(
	`(?m)^\s*uses:\s*` + regexp.QuoteMeta(ProducerRepo+"/"+CISharedPath) + `@(\S+)\s*$`)

// PinRef reports the ref a consumer's ci.yml pins ci-shared.yml at.
//
// Several matches naming the SAME ref collapse; several naming different refs
// are an error rather than a majority vote, because a repo running two
// revisions of the shared graph has no single answer to "what CI does it run".
func PinRef(ciYAML []byte) (string, error) {
	seen := map[string]bool{}
	var refs []string
	for _, m := range pinRe.FindAllSubmatch(ciYAML, -1) {
		ref := string(m[1])
		if !seen[ref] {
			seen[ref] = true
			refs = append(refs, ref)
		}
	}
	switch len(refs) {
	case 1:
		return refs[0], nil
	case 0:
		return "", fmt.Errorf("no `uses: %s/%s@<ref>` pin found", ProducerRepo, CISharedPath)
	default:
		return "", fmt.Errorf("several different pins found (%s); a repo cannot run two revisions of the shared job graph",
			strings.Join(refs, ", "))
	}
}

// Audit reports every consumer that is not running `reference`.
//
// CONTENT IDENTITY, NOT TAG DISTANCE. A consumer sitting on an older tag whose
// ci-shared.yml is byte-identical to the reference runs the same job graph, so
// it is current. That is the invariant #640 is actually about - the four
// consumers were not stale because v1.36.1 was old, they were stale because the
// image-verify job was not in it - and it removes the lag fudge factor
// check_skills_currency.py needs (a release train legitimately in flight is
// content-identical here, so there is nothing to tolerate).
//
// FAIL-CLOSED throughout: an unreadable ci.yml, a missing or ambiguous pin, a
// pin naming a ref the producer does not serve, an empty consumer set and an
// empty reference are each a Finding.
func Audit(ctx context.Context, f Fetcher, consumers []string, reference []byte, referenceRef string) []Finding {
	if len(reference) == 0 {
		return []Finding{{
			Kind: FindingUnreadable,
			Detail: fmt.Sprintf("the reference %s at %s is empty, so there is nothing to compare the fleet against; "+
				"that is a broken check, not a current fleet", CISharedPath, referenceRef),
		}}
	}
	if len(consumers) == 0 {
		return []Finding{{
			Kind: FindingUnreadable,
			Detail: "no consumer repos were given, so nothing was checked at all. " +
				"Either LiveConsumers was emptied or this ran with an explicit empty set",
		}}
	}

	var findings []Finding
	for _, repo := range consumers {
		ciYAML, err := f.File(ctx, repo, consumerRef, ConsumerWorkflowPath)
		if err != nil {
			findings = append(findings, Finding{repo, FindingUnreadable,
				fmt.Sprintf("could not read %s@%s:%s: %v", repo, consumerRef, ConsumerWorkflowPath, err)})
			continue
		}
		pin, err := PinRef(ciYAML)
		if err != nil {
			findings = append(findings, Finding{repo, FindingUnreadable,
				fmt.Sprintf("could not read its ci-shared pin from %s: %v", ConsumerWorkflowPath, err)})
			continue
		}
		pinned, err := f.File(ctx, ProducerRepo, pin, CISharedPath)
		if err != nil {
			findings = append(findings, Finding{repo, FindingUnreadable,
				fmt.Sprintf("pins ci-shared.yml@%s, which %s does not serve: %v. "+
					"Either the tag was deleted or the pin was hand-written; every run of this repo's CI resolves it",
					pin, ProducerRepo, err)})
			continue
		}
		if !bytes.Equal(pinned, reference) {
			findings = append(findings, Finding{repo, FindingStale,
				fmt.Sprintf("pins ci-shared.yml@%s, whose content differs from the reference at %s. "+
					"It is running a different job graph from the one this repo's internal/cicontract tests assert on",
					pin, referenceRef)})
		}
	}
	return findings
}

// ShortName is the repo half of an owner/name slug.
func ShortName(repo string) string {
	if i := strings.LastIndex(repo, "/"); i >= 0 {
		return repo[i+1:]
	}
	return repo
}

// PlanBumps turns an Audit into the release fan-out's decision: which consumers
// to open a pin-bump PR against, keyed by short repo name, plus the findings
// that must fail the caller instead.
//
// EVERY consumer gets an entry, including the false ones. release.yml gates
// each cd-release step on its own key, and a missing key evaluates to the empty
// string there - indistinguishable from a deliberate false, so the map is
// always complete rather than sparse.
//
// A stale consumer is NOT an error: it is exactly what the caller is about to
// fix. An unreadable one is, because a fan-out that skips a repo it could not
// read is the silence #640 is about.
func PlanBumps(consumers []string, findings []Finding) (map[string]bool, []Finding) {
	bump := make(map[string]bool, len(consumers))
	for _, repo := range consumers {
		bump[ShortName(repo)] = false
	}
	var unreadable []Finding
	for _, f := range findings {
		switch f.Kind {
		case FindingStale:
			bump[ShortName(f.Consumer)] = true
		default:
			unreadable = append(unreadable, f)
		}
	}
	return bump, unreadable
}

// ReferenceAtNewestTag resolves the reference ci-shared.yml: the copy at the
// producer's newest published semver tag.
//
// NOT main. main being ahead of the newest tag means "a release is due", which
// is a different condition from "the fleet is stale" - comparing against main
// would red this for the whole window between a ci-shared merge and the release
// that publishes it, i.e. during normal operation.
func ReferenceAtNewestTag(ctx context.Context, f Fetcher, repo string) ([]byte, string, error) {
	tags, err := f.Tags(ctx, repo)
	if err != nil {
		return nil, "", fmt.Errorf("could not read the tag list of %s: %w", repo, err)
	}
	newest, err := newestSemverTag(tags)
	if err != nil {
		return nil, "", fmt.Errorf("%s: %w", repo, err)
	}
	content, err := f.File(ctx, repo, newest, CISharedPath)
	if err != nil {
		return nil, "", fmt.Errorf("could not read %s@%s:%s: %w", repo, newest, CISharedPath, err)
	}
	if len(content) == 0 {
		return nil, "", fmt.Errorf("%s@%s:%s is empty", repo, newest, CISharedPath)
	}
	return content, newest, nil
}

var semverTagRe = regexp.MustCompile(`^v(\d+)\.(\d+)\.(\d+)$`)

// newestSemverTag picks the highest vX.Y.Z. Non-semver refs and the `^{}`
// peeled entries `git ls-remote` emits for annotated tags are dropped: a ref
// this pin could never legitimately hold is not a yardstick for it.
func newestSemverTag(tags []string) (string, error) {
	type parsed struct {
		tag string
		n   [3]int
	}
	var found []parsed
	for _, t := range tags {
		m := semverTagRe.FindStringSubmatch(t)
		if m == nil {
			continue
		}
		var p parsed
		p.tag = t
		for i := range p.n {
			p.n[i], _ = strconv.Atoi(m[i+1])
		}
		found = append(found, p)
	}
	if len(found) == 0 {
		return "", fmt.Errorf("published no vX.Y.Z tags, so the currency check has nothing to compare against; that is a broken check, not a clean fleet")
	}
	sort.Slice(found, func(i, j int) bool { return lessVer(found[i].n, found[j].n) })
	return found[len(found)-1].tag, nil
}

func lessVer(a, b [3]int) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}
