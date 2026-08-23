package cicontract

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// The two shapes #640 is about. `stale` is what v1.36.1 held: no image-verify
// job and no enable-image-verify input. `current` is what main holds.
const (
	staleCIShared = `name: ci-shared
on:
  workflow_call:
    inputs:
      runner-label:
        type: string
        required: true
jobs:
  build:
    runs-on: ${{ inputs.runner-label }}
`
	currentCIShared = `name: ci-shared
on:
  workflow_call:
    inputs:
      runner-label:
        type: string
        required: true
      enable-image-verify:
        type: boolean
        default: true
jobs:
  build:
    runs-on: ${{ inputs.runner-label }}
  image-verify:
    needs: [build]
    runs-on: ${{ inputs.runner-label }}
`
)

func consumerCI(pin string) string {
	return `name: ci
jobs:
  ci:
    uses: szymonrychu/tatara-operator/.github/workflows/ci-shared.yml@` + pin + `
    with:
      runner-label: tatara-cli
`
}

// fakeFetcher serves a fixed forge. Keys are "repo@ref:path", so a ref this
// map does not carry reads as a deleted tag - which is a Finding, not a pass.
type fakeFetcher struct {
	files   map[string]string
	tags    []string
	tagsErr error
}

func (f fakeFetcher) File(_ context.Context, repo, ref, path string) ([]byte, error) {
	v, ok := f.files[repo+"@"+ref+":"+path]
	if !ok {
		return nil, errors.New("404 not found")
	}
	return []byte(v), nil
}

func (f fakeFetcher) Tags(_ context.Context, _ string) ([]string, error) {
	if f.tagsErr != nil {
		return nil, f.tagsErr
	}
	return f.tags, nil
}

// forge builds a fetcher where tatara-cli pins `pin` and the producer serves
// `served` at that pin.
func forge(pin, served string) fakeFetcher {
	return fakeFetcher{
		files: map[string]string{
			"szymonrychu/tatara-cli@main:" + ConsumerWorkflowPath:     consumerCI(pin),
			"szymonrychu/tatara-operator@" + pin + ":" + CISharedPath: served,
			"szymonrychu/tatara-operator@v3.10.0:" + CISharedPath:     currentCIShared,
		},
		tags: []string{"v1.36.1", "v3.10.0"},
	}
}

func onlyCLI() []string { return []string{"szymonrychu/tatara-cli"} }

func TestPinRef(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		want    string
		wantErr bool
	}{
		{name: "single pin", yaml: consumerCI("v1.36.1"), want: "v1.36.1"},
		{
			name: "repeated identical pins collapse",
			yaml: consumerCI("v3.10.0") + consumerCI("v3.10.0"),
			want: "v3.10.0",
		},
		{
			name:    "two different pins is unreadable, not a majority vote",
			yaml:    consumerCI("v1.36.1") + consumerCI("v3.10.0"),
			wantErr: true,
		},
		{
			name:    "no pin at all",
			yaml:    "name: ci\njobs:\n  ci:\n    runs-on: ubuntu-latest\n",
			wantErr: true,
		},
		{
			name:    "a different reusable workflow is not this pin",
			yaml:    "    uses: szymonrychu/tatara-operator/.github/workflows/release.yml@v1.0.0\n",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PinRef([]byte(tt.yaml))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("PinRef() = %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("PinRef() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("PinRef() = %q, want %q", got, tt.want)
			}
		})
	}
}

// #640's literal state is the regression fixture: four repos pinned at a
// ci-shared.yml with no image-verify job while this repo's own copy has one.
func TestAudit_ConsumerPinnedBeforeTheImageVerifyJobIsStale(t *testing.T) {
	got := Audit(context.Background(), forge("v1.36.1", staleCIShared),
		onlyCLI(), []byte(currentCIShared), "v3.10.0")
	if len(got) != 1 {
		t.Fatalf("Audit() = %v, want exactly one finding", got)
	}
	if got[0].Kind != FindingStale {
		t.Errorf("Kind = %q, want %q", got[0].Kind, FindingStale)
	}
	if got[0].Consumer != "szymonrychu/tatara-cli" {
		t.Errorf("Consumer = %q", got[0].Consumer)
	}
	for _, want := range []string{"v1.36.1", "v3.10.0"} {
		if !strings.Contains(got[0].Detail, want) {
			t.Errorf("Detail = %q, want it to name %q", got[0].Detail, want)
		}
	}
}

// Content identity, not tag distance. A consumer sitting on an older tag whose
// ci-shared.yml is byte-identical to the reference runs the same CI, so it is
// current - which is the real invariant and needs no lag fudge factor.
func TestAudit_OlderTagWithIdenticalContentIsCurrent(t *testing.T) {
	got := Audit(context.Background(), forge("v2.4.0", currentCIShared),
		onlyCLI(), []byte(currentCIShared), "v3.10.0")
	if len(got) != 0 {
		t.Fatalf("Audit() = %v, want no findings", got)
	}
}

// FAIL-CLOSED. Every one of these is a Finding: a currency check that silently
// stops checking is worse than no currency check.
func TestAudit_FailsClosed(t *testing.T) {
	current := []byte(currentCIShared)

	tests := []struct {
		name      string
		fetcher   fakeFetcher
		consumers []string
		reference []byte
		wantKind  FindingKind
		wantIn    string
	}{
		{
			name:      "consumer ci.yml unreadable",
			fetcher:   fakeFetcher{files: map[string]string{}},
			consumers: onlyCLI(),
			reference: current,
			wantKind:  FindingUnreadable,
			wantIn:    ConsumerWorkflowPath,
		},
		{
			name: "consumer ci.yml carries no pin",
			fetcher: fakeFetcher{files: map[string]string{
				"szymonrychu/tatara-cli@main:" + ConsumerWorkflowPath: "name: ci\njobs: {}\n",
			}},
			consumers: onlyCLI(),
			reference: current,
			wantKind:  FindingUnreadable,
			wantIn:    "pin",
		},
		{
			name: "pin names a ref the producer does not serve",
			fetcher: fakeFetcher{files: map[string]string{
				"szymonrychu/tatara-cli@main:" + ConsumerWorkflowPath: consumerCI("v9.9.9"),
			}},
			consumers: onlyCLI(),
			reference: current,
			wantKind:  FindingUnreadable,
			wantIn:    "v9.9.9",
		},
		{
			name:      "empty consumer set checked nothing",
			fetcher:   forge("v3.10.0", currentCIShared),
			consumers: nil,
			reference: current,
			wantKind:  FindingUnreadable,
			wantIn:    "no consumer",
		},
		{
			name:      "empty reference has nothing to compare against",
			fetcher:   forge("v3.10.0", currentCIShared),
			consumers: onlyCLI(),
			reference: nil,
			wantKind:  FindingUnreadable,
			wantIn:    "reference",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Audit(context.Background(), tt.fetcher, tt.consumers, tt.reference, "v3.10.0")
			if len(got) != 1 {
				t.Fatalf("Audit() = %v, want exactly one finding", got)
			}
			if got[0].Kind != tt.wantKind {
				t.Errorf("Kind = %q, want %q", got[0].Kind, tt.wantKind)
			}
			if !strings.Contains(got[0].Detail, tt.wantIn) {
				t.Errorf("Detail = %q, want it to mention %q", got[0].Detail, tt.wantIn)
			}
		})
	}
}

func TestAudit_EveryConsumerIsReportedIndependently(t *testing.T) {
	f := fakeFetcher{files: map[string]string{
		"szymonrychu/tatara-cli@main:" + ConsumerWorkflowPath:    consumerCI("v1.36.1"),
		"szymonrychu/tatara-memory@main:" + ConsumerWorkflowPath: consumerCI("v3.10.0"),
		"szymonrychu/tatara-operator@v1.36.1:" + CISharedPath:    staleCIShared,
		"szymonrychu/tatara-operator@v3.10.0:" + CISharedPath:    currentCIShared,
	}}
	got := Audit(context.Background(), f,
		[]string{"szymonrychu/tatara-cli", "szymonrychu/tatara-memory"},
		[]byte(currentCIShared), "v3.10.0")
	if len(got) != 1 || got[0].Consumer != "szymonrychu/tatara-cli" {
		t.Fatalf("Audit() = %v, want only tatara-cli reported", got)
	}
}

// A consumer that regressed to a branch or a SHA is the same blindness with a
// different trigger: `@main` resolves to the producer's main, main is normally
// content-identical to the newest tag, so the check is GREEN forever and the
// fan-out computes bump=false and never restores a real pin. The pin's contract
// is a published vX.Y.Z tag - that is what cd-release writes - so anything else
// is a consumer that has silently left the CD-managed pin.
func TestAudit_ConsumerOnAMovingRefIsReported(t *testing.T) {
	for _, ref := range []string{"main", "d22349a", "v3.10", "3.10.0"} {
		t.Run(ref, func(t *testing.T) {
			f := fakeFetcher{files: map[string]string{
				"szymonrychu/tatara-cli@main:" + ConsumerWorkflowPath: consumerCI(ref),
				// Content-identical, so a content-only check would pass it.
				"szymonrychu/tatara-operator@" + ref + ":" + CISharedPath: currentCIShared,
			}}
			got := Audit(context.Background(), f, onlyCLI(), []byte(currentCIShared), "v3.10.0")
			if len(got) != 1 || got[0].Kind != FindingUnreadable {
				t.Fatalf("Audit() = %v, want one unreadable finding for the %q pin", got, ref)
			}
		})
	}
}

// The four live consumers, and tatara-chat deliberately not among them.
func TestLiveConsumers(t *testing.T) {
	want := []string{
		"szymonrychu/tatara-cli",
		"szymonrychu/tatara-memory",
		"szymonrychu/tatara-memory-repo-ingester",
		"szymonrychu/tatara-claude-code-wrapper",
	}
	if len(LiveConsumers) != len(want) {
		t.Fatalf("LiveConsumers = %v, want %v", LiveConsumers, want)
	}
	for i, w := range want {
		if LiveConsumers[i] != w {
			t.Errorf("LiveConsumers[%d] = %q, want %q", i, LiveConsumers[i], w)
		}
	}
	for _, c := range LiveConsumers {
		if strings.Contains(c, "tatara-chat") {
			t.Errorf("tatara-chat is decommissioned and must stay out of the fan-out")
		}
	}
}

// The release fan-out must bump only what a release actually made stale, and
// must never treat "I could not read this consumer" as "nothing to do".
func TestPlanBumps(t *testing.T) {
	consumers := []string{"szymonrychu/tatara-cli", "szymonrychu/tatara-memory"}

	tests := []struct {
		name           string
		findings       []Finding
		wantBump       map[string]bool
		wantUnreadable int
	}{
		{
			name:     "a current fleet bumps nothing",
			findings: nil,
			wantBump: map[string]bool{"tatara-cli": false, "tatara-memory": false},
		},
		{
			name:     "only the stale consumer is bumped",
			findings: []Finding{{"szymonrychu/tatara-cli", FindingStale, "stale"}},
			wantBump: map[string]bool{"tatara-cli": true, "tatara-memory": false},
		},
		{
			name:           "an unreadable consumer is not a silent false",
			findings:       []Finding{{"szymonrychu/tatara-memory", FindingUnreadable, "404"}},
			wantBump:       map[string]bool{"tatara-cli": false, "tatara-memory": false},
			wantUnreadable: 1,
		},
		{
			name:           "a fleet-wide finding fails the plan outright",
			findings:       []Finding{{"", FindingUnreadable, "no consumer repos were given"}},
			wantBump:       map[string]bool{"tatara-cli": false, "tatara-memory": false},
			wantUnreadable: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bump, unreadable := PlanBumps(consumers, tt.findings)
			if len(unreadable) != tt.wantUnreadable {
				t.Errorf("unreadable = %v, want %d", unreadable, tt.wantUnreadable)
			}
			if len(bump) != len(tt.wantBump) {
				t.Fatalf("bump = %v, want %v", bump, tt.wantBump)
			}
			for k, want := range tt.wantBump {
				if bump[k] != want {
					t.Errorf("bump[%q] = %v, want %v", k, bump[k], want)
				}
			}
		})
	}
}

func TestReferenceAtNewestTag(t *testing.T) {
	f := fakeFetcher{
		files: map[string]string{
			"szymonrychu/tatara-operator@v3.10.0:" + CISharedPath: currentCIShared,
		},
		// Deliberately unsorted, with a peeled entry and a non-semver tag: the
		// newest tag is a semver comparison, not the last line of ls-remote.
		tags: []string{"v3.10.0", "v1.36.1", "v3.9.12", "nightly", "v3.10.0^{}"},
	}
	content, ref, err := ReferenceAtNewestTag(context.Background(), f, ProducerRepo)
	if err != nil {
		t.Fatalf("ReferenceAtNewestTag() error = %v", err)
	}
	if ref != "v3.10.0" {
		t.Errorf("ref = %q, want v3.10.0", ref)
	}
	if string(content) != currentCIShared {
		t.Errorf("content = %q", content)
	}
}

func TestReferenceAtNewestTag_FailsClosed(t *testing.T) {
	tests := []struct {
		name    string
		fetcher fakeFetcher
	}{
		{
			name:    "no published semver tags is a broken check, not a clean fleet",
			fetcher: fakeFetcher{tags: []string{"nightly"}},
		},
		{
			name:    "tag list unreadable",
			fetcher: fakeFetcher{tagsErr: errors.New("ls-remote exited 128")},
		},
		{
			name:    "newest tag serves no ci-shared.yml",
			fetcher: fakeFetcher{tags: []string{"v3.10.0"}, files: map[string]string{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := ReferenceAtNewestTag(context.Background(), tt.fetcher, ProducerRepo); err == nil {
				t.Fatal("ReferenceAtNewestTag() = nil error, want one")
			}
		})
	}
}
