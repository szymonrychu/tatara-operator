// Command ci-shared-pins reports which of ci-shared.yml's consumer repos are
// not running the shared job graph this repo publishes.
//
// It is the network half of internal/cicontract, kept OUT of that package's
// tests on purpose: `make test` runs offline (see internal/promptguidance/
// toolnames.go), and a fetch that soft-warns on failure is a check that can
// silently stop checking. All the logic lives in the package and is exercised
// against a fake Fetcher; this file is transport and exit codes.
//
//	audit           the scheduled currency check (ci-shared-currency.yml).
//	                Reference is ci-shared.yml at the newest published tag.
//	                Exit 1 on ANY finding.
//	plan <file>     the release fan-out (release.yml). Reference is the local
//	                checkout. Writes bump-<repo>=true|false to $GITHUB_OUTPUT.
//	                Exit 1 only when a consumer could not be READ - a stale one
//	                is what the caller is about to fix.
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/cicontract"
)

const fetchTimeout = 3 * time.Minute

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: ci-shared-pins audit | ci-shared-pins plan <ci-shared.yml>")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
	defer cancel()

	switch os.Args[1] {
	case "audit":
		os.Exit(audit(ctx))
	case "plan":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: ci-shared-pins plan <ci-shared.yml>")
			os.Exit(2)
		}
		os.Exit(plan(ctx, os.Args[2]))
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(2)
	}
}

func audit(ctx context.Context) int {
	f := forgeFetcher{}
	reference, ref, err := cicontract.ReferenceAtNewestTag(ctx, f, cicontract.ProducerRepo)
	if err != nil {
		// The reference itself is unreadable, so no consumer can be judged.
		// Loud and non-zero: an unknown fleet is not a current fleet.
		fmt.Fprintf(os.Stderr, "::error::ci-shared currency check could not resolve its reference: %v\n", err)
		return 1
	}

	findings := cicontract.Audit(ctx, f, cicontract.LiveConsumers, reference, ref)
	if len(findings) == 0 {
		fmt.Printf("every live consumer runs ci-shared.yml as published at %s.\n", ref)
		return 0
	}
	fmt.Fprintf(os.Stderr, "::error::ci-shared.yml pin currency check failed against %s; "+
		"the fleet is not running the job graph internal/cicontract asserts on.\n", ref)
	for _, fd := range findings {
		fmt.Fprintf(os.Stderr, "  - %s\n", fd)
	}
	fmt.Fprintln(os.Stderr, "  Do NOT close this by hand-editing the `uses:` line in a consumer.")
	fmt.Fprintln(os.Stderr, "  That pin is CD-managed by this repo's release.yml `fanout-ci-shared` job;")
	fmt.Fprintln(os.Stderr, "  a hand bump hides the real fault until the next ci-shared change.")
	return 1
}

func plan(ctx context.Context, referencePath string) int {
	reference, err := os.ReadFile(referencePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "::error::could not read the local reference %s: %v\n", referencePath, err)
		return 1
	}

	findings := cicontract.Audit(ctx, forgeFetcher{}, cicontract.LiveConsumers, reference, referencePath)
	bump, unreadable := cicontract.PlanBumps(cicontract.LiveConsumers, findings)

	if err := writeOutputs(bump); err != nil {
		fmt.Fprintf(os.Stderr, "::error::could not write the bump plan: %v\n", err)
		return 1
	}
	for _, fd := range findings {
		if fd.Kind == cicontract.FindingStale {
			fmt.Printf("bump: %s\n", fd)
		}
	}
	if len(unreadable) == 0 {
		return 0
	}
	// A consumer this could not read is a consumer the fan-out would silently
	// skip, which is #640 with a new trigger. Fail the job instead.
	fmt.Fprintln(os.Stderr, "::error::the ci-shared fan-out could not read every consumer, so it cannot vouch for the ones it skipped:")
	for _, fd := range unreadable {
		fmt.Fprintf(os.Stderr, "  - %s\n", fd)
	}
	return 1
}

// writeOutputs emits the per-consumer booleans to $GITHUB_OUTPUT, or to stdout
// when run outside Actions.
func writeOutputs(bump map[string]bool) error {
	keys := make([]string, 0, len(bump))
	for k := range bump {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&sb, "bump-%s=%t\n", k, bump[k])
	}

	path := os.Getenv("GITHUB_OUTPUT")
	if path == "" {
		fmt.Print(sb.String())
		return nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck // the write below is the one that matters
	if _, err := f.WriteString(sb.String()); err != nil {
		return err
	}
	return f.Close()
}

// forgeFetcher reads public repos. Every tatara repo is public, so this needs
// no token - same posture as tatara-helmfile's check_skills_currency.py.
type forgeFetcher struct{}

func (forgeFetcher) File(ctx context.Context, repo, ref, path string) ([]byte, error) {
	url := fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s", repo, ref, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck // read-only response
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

func (forgeFetcher) Tags(ctx context.Context, repo string) ([]string, error) {
	cmd := exec.CommandContext(ctx, "git", "ls-remote", "--tags",
		"https://github.com/"+repo+".git")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git ls-remote --tags %s: %w", repo, err)
	}
	// `<sha>\trefs/tags/<name>`, with a `^{}`-suffixed second entry per
	// annotated tag. newestSemverTag drops anything that is not vX.Y.Z, so the
	// peeled entries need no special case here.
	var tags []string
	for _, line := range strings.Split(string(out), "\n") {
		_, ref, ok := strings.Cut(line, "refs/tags/")
		if !ok {
			continue
		}
		tags = append(tags, strings.TrimSpace(ref))
	}
	return tags, nil
}
