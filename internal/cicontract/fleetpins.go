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
	FindingGateDark   FindingKind = "gate-dark"
	// FindingExemptionUnused is the removal direction: a consumer named in
	// ImageVerifyExempt that is in fact running the gate. Nothing else would
	// ever say so, because the exemption is only consulted when the gate is
	// off, and what goes stale is not one line - it is a rationale here, a
	// paragraph in doc.go, a bullet in ci-shared.yml and twenty lines in the
	// consumer's caller.
	FindingExemptionUnused FindingKind = "exemption-unused"
)

// ImageVerifyExempt are the consumers allowed to run with the pull_request
// Dockerfile compile switched off, each mapped to why. Anything not in here
// that sets `enable-image-verify: false` is a Finding.
//
// This map is the forcing function, and it is here rather than in the caller on
// purpose. A pin that reaches the fleet is only half of #640; the other half is
// a consumer running the current job graph with the gate off, which Audit's
// content comparison cannot see, because ci-shared.yml at that pin is
// byte-identical either way. An opt-out written only as `false` in a caller
// nobody reads is #640's shape - correct shape, zero reach - one line further
// out. Written here, it is next to the fleet contract, it must carry a reason,
// and adding a second one is a diff against this repo.
var ImageVerifyExempt = map[string]string{
	// Dockerfile:57 is `FROM harbor.szymonrichert.pl/containers/tatara-cli:
	// ${TATARA_CLI_VERSION}`, a private registry, and image-verify passes no
	// `--opt target=`, so it builds the FINAL stage and everything that stage
	// transitively needs - and the final stage has `COPY --from=tatara-cli`,
	// so this one is always reached. (Not "the whole graph": a stage nothing
	// copies from is never built, which is why release.yml's build.sh names
	// the test-guard stage explicitly.) Probed on PR #186 at v3.10.0:
	// `401 Unauthorized` on the manifest HEAD. No project on that Harbor
	// serves anonymous pull.
	//
	// THE OBVIOUS FIX IS THE FORBIDDEN ONE. Handing image-verify the
	// HARBOR_USERNAME/HARBOR_PASSWORD the push `image` job uses is item two on
	// doc.go's list of regressions this package exists to stop, and
	// merge_gate_test.go fails on a step-level HARBOR_* env in any job that
	// builds the Dockerfile and is not push-only.
	// Those are the PUSH credentials, and on pull_request the CALLER is taken
	// from the PR head, so the shared job would be writing a push-capable
	// credential onto a runner whose job graph a PR can edit. The gate is worth
	// less than that.
	//
	// So this is not "the gate is inconvenient here". It is: under the no-
	// credentials-on-the-PR-path contract, the shared job structurally cannot
	// compile this Dockerfile. Closing it needs a pull-only credential or an
	// anonymously pullable base image - a deliberate registry decision, not a
	// workflow edit - and until then this repo's Dockerfile is first compiled
	// in release.yml. Tracked on #640.
	"szymonrychu/tatara-claude-code-wrapper": "Dockerfile:57 pulls a private Harbor base image; " +
		"the shared job must not carry registry credentials on the pull_request path (doc.go)",
}

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
//
// THE TRAILING COMMENT IS DELIBERATE, and the asymmetry it closes is the point.
// The WRITER is apply-pins.py's `^(\s*uses: ...@).*$` -> `\1{{version}}`, which
// happily rewrites `...@v3.10.0  # pinned` and leaves the comment in place. A
// reader ending `@(\S+)\s*$` did not match that line, so PinRef errored ->
// FindingUnreadable -> `plan` exit 1 -> fanout-ci-shared red -> THIS REPO'S
// RELEASE went red, caused by a one-line cosmetic edit in a sibling repo, after
// tag/publish/bump/verify-pin had all succeeded. A reader stricter than its own
// writer turns someone else's formatting into your outage.
// fanout_contract_test.go asserts both spellings against both patterns.
var pinRe = regexp.MustCompile(
	`(?m)^\s*uses:\s*` + regexp.QuoteMeta(ProducerRepo+"/"+CISharedPath) + `@([^\s#]+)\s*(?:#.*)?$`)

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

// imageVerifyInput is the ci-shared.yml input this reads out of a consumer.
// merge_gate_test.go binds it to the input's declaration AND to the `if:` that
// actually gates the job, so a rename cannot leave this reading a name nothing
// uses any more and reporting every gate as ON forever.
const imageVerifyInput = "enable-image-verify"

// imageVerifyRe matches the caller's `enable-image-verify:` input. Anchored on
// the colon so `enable-image: false` - which every one of the four sets, and
// which is a different input entirely - can never match it.
//
// Two things the obvious pattern got wrong, both fail-OPEN, which is the only
// direction that matters here:
//
//   - `\S+` does not match `${{ inputs.x }}`, so an expression matched NOTHING,
//     read as absent, and defaulted to enabled. The value is therefore captured
//     to end-of-line, minus a trailing `# comment`.
//   - stopping at end-of-line missed `with: {a: b, enable-image-verify: false}`,
//     a flow mapping Actions accepts. `,` and `}` terminate the value too.
//
// NOT for ci-shared.yml itself: against the producer this matches the input's
// own DEFINITION block and captures the following `description:` line. It reads
// a CALLER.
var imageVerifyRe = regexp.MustCompile(
	`(?m)(?:^|[{,])[ \t]*` + imageVerifyInput + `:[ \t]*([^#,}\n]*[^#,}\s])`)

// ImageVerifyEnabled reports whether a consumer's ci.yml leaves the
// pull_request Dockerfile compile on. Absent is TRUE: that is the input's
// default and it is the state the whole fleet should be in.
//
// A value that resolves to neither true nor false is an ERROR, not the default.
// An expression or a typo is a caller this cannot read, and "I could not tell"
// has to fail closed, or the check quietly starts reporting the gate as on.
func ImageVerifyEnabled(ciYAML []byte) (bool, error) {
	seen := map[string]bool{}
	var values []string
	for _, m := range imageVerifyRe.FindAllSubmatch(ciYAML, -1) {
		v := string(m[1])
		if !seen[v] {
			seen[v] = true
			values = append(values, v)
		}
	}
	switch len(values) {
	case 0:
		return true, nil
	case 1:
		// Quoted and capitalised spellings are the same boolean to Actions.
		// Reading `False` as unparseable would still fail closed, but the
		// finding would tell the reader the wrong thing about their repo.
		switch strings.ToLower(strings.Trim(values[0], `'"`)) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		default:
			return false, fmt.Errorf("%s is %q, which is neither true nor false", imageVerifyInput, values[0])
		}
	default:
		return false, fmt.Errorf("%s is set several times with different values (%s)",
			imageVerifyInput, strings.Join(values, ", "))
	}
}

// Audit reports every consumer that is not running `reference`.
//
// CONTENT IDENTITY, NOT TAG DISTANCE. A consumer sitting on an older tag whose
// ci-shared.yml is byte-identical to the reference runs the same job graph, so
// it is current. That is the invariant #640 is actually about - the four
// consumers were not stale because v1.36.1 was old, they were stale because the
// image-verify job was not in it.
//
// That is NOT the same as saying there is no transient window. There is one,
// and it is exactly the fan-out's own: a release that changed ci-shared.yml
// makes every consumer content-divergent until its bump PR merges, so a cron
// landing inside that window reds legitimately. The window is bounded by the
// consumers' own CI (which is the point - each bump PR runs image-verify,
// EXCEPT in a repo named in ImageVerifyExempt, where it runs the rest of the
// graph and not that job) and the finding self-closes on the next green run.
// What content identity DOES
// remove is check_skills_currency.py's MAX_LAG: a consumer several tags behind
// on a run of releases that never touched ci-shared.yml is genuinely current
// and is never reported, so no tag-distance tolerance has to be invented for it.
//
// It also reports a consumer running the reference with the gate SWITCHED OFF,
// unless ImageVerifyExempt says why. Content identity cannot see that: the
// pinned ci-shared.yml is byte-identical whether or not the caller passes
// enable-image-verify: false, so a consumer that opts out reads as CURRENT
// forever while its Dockerfile is never compiled on a PR.
//
// FAIL-CLOSED throughout: an unreadable ci.yml, a missing or ambiguous pin, a
// pin naming a ref the producer does not serve, an unreadable
// enable-image-verify value, an empty consumer set and an empty reference are
// each a Finding.
func Audit(ctx context.Context, f Fetcher, consumers []string, reference []byte, referenceRef string) []Finding {
	return audit(ctx, f, consumers, reference, referenceRef, ImageVerifyExempt)
}

// audit is Audit with an injectable exemption set, so a test never mutates the
// package-level one.
func audit(ctx context.Context, f Fetcher, consumers []string, reference []byte, referenceRef string,
	exempt map[string]string) []Finding {
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

	// Every consumer normally pins the same ref, so resolving it once keeps a
	// four-consumer audit to one producer fetch. It also stops one transient
	// 5xx from raw.githubusercontent getting four independent chances to land.
	producer := map[string][]byte{}
	fetchProducer := func(ref string) ([]byte, error) {
		if b, ok := producer[ref]; ok {
			return b, nil
		}
		b, err := f.File(ctx, ProducerRepo, ref, CISharedPath)
		if err != nil {
			return nil, err
		}
		producer[ref] = b
		return b, nil
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
		// A MOVING REF IS NOT A PIN. `@main` or `@<sha>` resolves to something
		// this check cannot reason about: main is normally content-identical to
		// the newest tag, so a consumer that regressed to it reads as CURRENT
		// forever and the fan-out computes bump=false and never restores it.
		// The pin's contract is a published vX.Y.Z tag - that is exactly what
		// cd-release writes - so anything else has left the CD-managed pin.
		if !semverTagRe.MatchString(pin) {
			findings = append(findings, Finding{repo, FindingUnreadable,
				fmt.Sprintf("pins ci-shared.yml@%s, which is not a published vX.Y.Z tag. "+
					"A branch or SHA resolves to a revision this check cannot hold still, so it would "+
					"read as current forever while the release fan-out never rewrites it", pin)})
			continue
		}
		pinned, err := fetchProducer(pin)
		if err != nil {
			findings = append(findings, Finding{repo, FindingUnreadable,
				fmt.Sprintf("pins ci-shared.yml@%s and %s did not serve it: %v. "+
					"Every run of that repo's CI resolves this ref, so if the fetch itself is sound "+
					"the tag is gone and its CI is already broken", pin, ProducerRepo, err)})
			continue
		}
		if !bytes.Equal(pinned, reference) {
			findings = append(findings, Finding{repo, FindingStale,
				fmt.Sprintf("pins ci-shared.yml@%s, whose content differs from the reference at %s. "+
					"It is running a different job graph from the one this repo's internal/cicontract tests assert on",
					pin, referenceRef)})
			continue
		}
		// Reached only for a consumer on the reference: a stale one's gate
		// state is a question about a job graph it is not running yet, and
		// reporting both would file two findings for one fix.
		enabled, err := ImageVerifyEnabled(ciYAML)
		_, isExempt := exempt[repo]
		switch {
		case err != nil:
			// NOT FindingUnreadable, and the kind is the whole point.
			// `unreadable` is fatal to the release fan-out because it means a
			// repo the fan-out would silently skip. This is not that: the
			// bytes.Equal above already passed, so the consumer is known to be
			// running the reference and the only open question is one boolean.
			// Calling it fatal would let one typo in one caller stop the pin
			// being written to all four - #640 restored by the check added to
			// prevent it. Fail closed on the gate, not on the fan-out.
			findings = append(findings, Finding{repo, FindingGateDark,
				fmt.Sprintf("runs ci-shared.yml@%s and its %s could not be read, so the gate must be "+
					"assumed off: %v. Resolve it to a literal true or false", pin, ConsumerWorkflowPath, err)})
		case !enabled && !isExempt:
			findings = append(findings, Finding{repo, FindingGateDark,
				fmt.Sprintf("runs ci-shared.yml@%s but sets %s: false, so nothing compiles its "+
					"Dockerfile on a pull request. That is the #556 state the pin bump was supposed to end. "+
					"If the shared job genuinely cannot build this repo's image, add it to "+
					"cicontract.ImageVerifyExempt with the reason", pin, imageVerifyInput)})
		case enabled && isExempt:
			findings = append(findings, Finding{repo, FindingExemptionUnused,
				fmt.Sprintf("runs the gate, but cicontract.ImageVerifyExempt still exempts it: %q. "+
					"Delete that entry and the rationale around it - an exemption is only ever consulted "+
					"when the gate is off, so nothing else would report this", exempt[repo])})
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

// BumpOutputKey is the $GITHUB_OUTPUT key `plan` writes for one consumer and
// release.yml's cd-release step for that consumer gates on.
//
// It exists so the `bump-` prefix is ONE literal. It used to be two - a format
// string in cmd/ci-shared-pins and a hand-rebuilt string in
// fanout_contract_test.go - bound to each other by nothing, and renaming either
// left the build and every test green. At runtime that ships as: `plan` exits 0,
// all four `steps.plan.outputs.bump-<repo>` gates evaluate ” == 'true' (Actions
// resolves a missing output to the empty string, it does not error), every step
// skips, and `fanout-ci-shared` reports SUCCESS having written no pin anywhere.
// A silently dead fan-out is precisely what release.yml's four-explicit-steps
// shape was chosen over a matrix to prevent, so leaving the key itself unbound
// reopened the hole one layer up.
//
// Accepts either an owner/name slug or a bare short name.
func BumpOutputKey(repo string) string { return "bump-" + ShortName(repo) }

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
//
// A gate-dark one is neither. It is a fleet condition for the scheduled audit
// to file an issue about, and failing the plan on it would skip the ENTIRE
// fan-out on every release for as long as one consumer had image-verify off -
// so the pin would stop being written, which is #640 restored by the check
// added to prevent it. The `default` arm stays fail-closed for any kind added
// later; this one is listed because it was reasoned about.
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
		case FindingGateDark, FindingExemptionUnused:
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
