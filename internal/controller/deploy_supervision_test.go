package controller

import "testing"

// deployPinFiles and releaseArtifact are two mirrors of the same tatara-helmfile
// pin list (see their doc comments). project-mtg was omitted from both once
// already; this test pins every tatara-project release into both maps so the
// next project that is added the same way fails here instead of silently
// drifting.
func TestDeployPinFilesCoversEveryTataraProject(t *testing.T) {
	want := []string{
		"values/project-tatara/common.yaml",
		"values/project-infrastructure/common.yaml",
		"values/project-mtg/common.yaml",
	}
	for _, f := range want {
		t.Run(f, func(t *testing.T) {
			for _, got := range deployPinFiles {
				if got == f {
					return
				}
			}
			t.Fatalf("deployPinFiles missing %q", f)
		})
	}
}

// releaseArtifact attributes a bare chart-version pin line to the artifact of
// its enclosing helmfile release block. Every tatara-project release pins the
// tatara-operator chart, so a chart-version pin for each must resolve to
// "tatara-operator" through pinCarriesArtifactVersion (the attribution rule
// artifactPinLines/pinAtOrPastArtifactVersion both build on).
func TestReleaseArtifactAttributesEveryTataraProject(t *testing.T) {
	cases := []struct {
		release  string
		artifact string
	}{
		{"tatara-operator", "tatara-operator"},
		{"project-tatara", "tatara-operator"},
		{"project-infrastructure", "tatara-operator"},
		{"project-mtg", "tatara-operator"},
	}
	for _, tc := range cases {
		t.Run(tc.release, func(t *testing.T) {
			if got := releaseArtifact[tc.release]; got != tc.artifact {
				t.Fatalf("releaseArtifact[%q] = %q, want %q", tc.release, got, tc.artifact)
			}
			pin := mdPin(tc.release, "1.4.0")
			if !pinCarriesArtifactVersion(pin, tc.artifact, "v1.4.0") {
				t.Fatalf("chart-version pin for release %q not attributed to artifact %q", tc.release, tc.artifact)
			}
		})
	}
}
