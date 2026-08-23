// Copyright 2026 tatara authors.

package v1alpha1

import "testing"

// TestSignificanceRank_IsTheOneTable pins the ordering the review-escalation
// clause and the auto-approve ceiling now SHARE. restapi held a private copy of
// this map; two tables that must agree and can drift is exactly the shape this
// export removes.
func TestSignificanceRank_IsTheOneTable(t *testing.T) {
	if len(SignificanceRank) != 3 {
		t.Fatalf("SignificanceRank must hold exactly the three semver levels, got %v", SignificanceRank)
	}
	if SignificanceRank["patch"] >= SignificanceRank["minor"] || SignificanceRank["minor"] >= SignificanceRank["major"] {
		t.Fatalf("patch < minor < major violated: %v", SignificanceRank)
	}
}

// TestAutoApproveCeiling_EmptyReadsOff is the fail-closed default. A Project CR
// created before this field existed, or one whose value the apiserver pruned,
// carries the empty string; it must read as off, never as major.
func TestAutoApproveCeiling_EmptyReadsOff(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  string
		want string
	}{
		{"unset", "", AutoApproveOff},
		{"off", AutoApproveOff, AutoApproveOff},
		{"patch", "patch", "patch"},
		{"minor", "minor", "minor"},
		{"major", "major", "major"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &Project{Spec: ProjectSpec{AutoApproveMaxSignificance: tc.set}}
			if got := AutoApproveCeiling(p); got != tc.want {
				t.Fatalf("AutoApproveCeiling = %q, want %q", got, tc.want)
			}
		})
	}
	if got := AutoApproveCeiling(nil); got != AutoApproveOff {
		t.Fatalf("AutoApproveCeiling(nil) = %q, want off", got)
	}
}

// TestAutoApproveCeilingRank_OffSitsBelowPatch is what makes ceiling=off refuse
// every declared level: off must rank strictly below the lowest real one.
func TestAutoApproveCeilingRank_OffSitsBelowPatch(t *testing.T) {
	if AutoApproveCeilingRank(AutoApproveOff) >= SignificanceRank["patch"] {
		t.Fatalf("off must rank below patch, got off=%d patch=%d",
			AutoApproveCeilingRank(AutoApproveOff), SignificanceRank["patch"])
	}
	if AutoApproveCeilingRank("") != AutoApproveCeilingRank(AutoApproveOff) {
		t.Fatal("the empty ceiling must rank as off")
	}
}

// TestAutoApproveOverCeiling is the whole severity rule in one table. The empty
// significance is NOT over any ceiling: mr_write(action=open) runs before the
// agent has declared one, and refusing there would gate the open on a value the
// wire cannot yet carry.
func TestAutoApproveOverCeiling(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ceiling string
		sig     string
		want    bool
	}{
		{"off refuses patch", AutoApproveOff, "patch", true},
		{"off refuses major", AutoApproveOff, "major", true},
		{"unset ceiling refuses patch", "", "patch", true},
		{"patch passes patch", "patch", "patch", false},
		{"patch refuses minor", "patch", "minor", true},
		{"minor passes patch", "minor", "patch", false},
		{"minor passes minor", "minor", "minor", false},
		{"minor refuses major", "minor", "major", true},
		{"major passes major", "major", "major", false},
		{"no declared significance is never over", "off", "", false},
		{"an unknown level fails closed", "major", "catastrophic", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &Project{Spec: ProjectSpec{AutoApproveMaxSignificance: tc.ceiling}}
			if got := AutoApproveOverCeiling(p, tc.sig); got != tc.want {
				t.Fatalf("AutoApproveOverCeiling(%q, %q) = %v, want %v", tc.ceiling, tc.sig, got, tc.want)
			}
		})
	}
}
