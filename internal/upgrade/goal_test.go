package upgrade

import (
	"strings"
	"testing"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/promptguidance"
)

func TestGoalProject_RendersTheResolvedPolicy(t *testing.T) {
	g := GoalProject([]string{"szymonrychu/containers", "szymonrychu/charts"},
		&v1alpha1.UpgradePolicySpec{
			Engine:            "renovate",
			MajorStrategy:     "nextHopOnly",
			MinimumReleaseAge: &v1alpha1.ReleaseAgeSpec{Major: 0, Minor: 0, Patch: 0},
		})
	for _, want := range []string{
		"tatara-upgrade-workflow", "szymonrychu/containers", "szymonrychu/charts",
		"renovate", "nextHopOnly", "EXACTLY ONE",
	} {
		if !strings.Contains(g, want) {
			t.Errorf("goal missing %q", want)
		}
	}
}

// A nil policy is the DEFAULT-OFF shape: engine none, nextHopOnly, no minimum
// release age. A kubebuilder default inside a nil struct pointer is never
// applied, so this function - not the API server - is what resolves it.
func TestGoalProject_NilPolicyRendersEngineNone(t *testing.T) {
	g := GoalProject([]string{"szymonrychu/mtg-decks"}, nil)
	if strings.Contains(g, "renovate") {
		t.Fatal("a nil policy must not advertise the renovate engine")
	}
	if !strings.Contains(g, "engine: none") {
		t.Fatal("a nil policy must render engine none explicitly")
	}
	if !strings.Contains(g, "nextHopOnly") {
		t.Fatal("a nil policy still resolves the nextHopOnly default")
	}
}

// A policy present but half-filled still resolves every field: an empty Engine
// is not "no engine line", it is the default.
func TestGoalProject_PartialPolicyResolvesEachFieldIndependently(t *testing.T) {
	g := GoalProject([]string{"o/a"}, &v1alpha1.UpgradePolicySpec{MajorStrategy: "latest"})
	if !strings.Contains(g, "engine: none") {
		t.Error("an empty Engine resolves to none")
	}
	if !strings.Contains(g, "majorStrategy: latest") {
		t.Error("an explicit MajorStrategy wins")
	}
	if !strings.Contains(g, "bleeding edge") {
		t.Error("a nil MinimumReleaseAge is bleeding edge, and must say so")
	}
}

func TestGoalProject_RendersNonZeroReleaseAges(t *testing.T) {
	g := GoalProject([]string{"o/a"}, &v1alpha1.UpgradePolicySpec{
		MinimumReleaseAge: &v1alpha1.ReleaseAgeSpec{Major: 14, Minor: 7, Patch: 3},
	})
	if !strings.Contains(g, "major 14d, minor 7d, patch 3d") {
		t.Fatalf("release ages not rendered: %s", g)
	}
	if strings.Contains(g, "bleeding edge") {
		t.Fatal("a non-zero minimum release age is not bleeding edge")
	}
}

// Every tool the goal names in backticks must exist. See
// internal/promptguidance/toolnames.go for the registry and the extraction rule.
func TestUpgradeGoalNamesOnlyRealTools(t *testing.T) {
	for _, g := range []string{
		GoalProject([]string{"o/a", "o/b"}, &v1alpha1.UpgradePolicySpec{Engine: "renovate"}),
		GoalProject([]string{"o/a"}, nil),
	} {
		if bad := promptguidance.UnknownToolNames(g); len(bad) > 0 {
			t.Fatalf("upgrade.GoalProject names tools that do not exist: %v", bad)
		}
	}
}
