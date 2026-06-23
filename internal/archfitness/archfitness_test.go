package archfitness_test

import (
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/archfitness"
)

// Vendor SCM SDK import prefixes that must NEVER appear outside the designated
// adapter package internal/scm.
var bannedPrefixes = []string{
	"github.com/google/go-github",
	"github.com/xanzy/go-gitlab",
	"gitlab.com/gitlab-org/api",
	"github.com/shurcooL/githubv4",
}

const adapterPkg = "github.com/szymonrychu/tatara-operator/internal/scm"

// TestCheck_FailsOnBannedImportOutsideAdapter proves the checker correctly
// identifies a violation when a non-adapter package imports a banned SDK.
// This is the "deliberately-bad fixture" that proves the check fails.
func TestCheck_FailsOnBannedImportOutsideAdapter(t *testing.T) {
	graph := map[string][]string{
		"github.com/szymonrychu/tatara-operator/internal/controller": {
			"github.com/google/go-github/v60/github",
			"context",
			"fmt",
		},
	}
	violations := archfitness.CheckImports(graph, adapterPkg, bannedPrefixes)
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation, got %d: %v", len(violations), violations)
	}
	if violations[0].Package != "github.com/szymonrychu/tatara-operator/internal/controller" {
		t.Errorf("violation package wrong: %q", violations[0].Package)
	}
	if violations[0].BannedImport != "github.com/google/go-github/v60/github" {
		t.Errorf("violation bannedImport wrong: %q", violations[0].BannedImport)
	}
}

// TestCheck_PassesWhenAdapterImportsBanned verifies the adapter package itself
// is allowed to import any banned SDK (it is the sanctioned isolation boundary).
func TestCheck_PassesWhenAdapterImportsBanned(t *testing.T) {
	graph := map[string][]string{
		adapterPkg: {
			"github.com/google/go-github/v60/github",
		},
	}
	violations := archfitness.CheckImports(graph, adapterPkg, bannedPrefixes)
	if len(violations) != 0 {
		t.Fatalf("expected 0 violations for adapter package, got %d: %v", len(violations), violations)
	}
}

// TestCheck_PassesWhenNoBannedImports verifies a clean import graph produces
// no violations.
func TestCheck_PassesWhenNoBannedImports(t *testing.T) {
	graph := map[string][]string{
		"github.com/szymonrychu/tatara-operator/internal/controller": {
			"context",
			"fmt",
			"strings",
		},
		"github.com/szymonrychu/tatara-operator/internal/restapi": {
			"net/http",
		},
	}
	violations := archfitness.CheckImports(graph, adapterPkg, bannedPrefixes)
	if len(violations) != 0 {
		t.Fatalf("expected 0 violations, got %d: %v", len(violations), violations)
	}
}

// TestCheck_AdapterSubpackagesAreExempt verifies that sub-packages of the
// adapter (e.g. internal/scm/github) are also exempt from the ban.
func TestCheck_AdapterSubpackagesAreExempt(t *testing.T) {
	subpkg := adapterPkg + "/github"
	graph := map[string][]string{
		subpkg: {
			"github.com/google/go-github/v60/github",
		},
	}
	violations := archfitness.CheckImports(graph, adapterPkg, bannedPrefixes)
	if len(violations) != 0 {
		t.Fatalf("expected 0 violations for adapter sub-package %q, got %d: %v", subpkg, len(violations), violations)
	}
}

// TestCheck_MultipleBannedImportsSinglePackage verifies that each distinct
// banned import in a package generates a separate Violation.
func TestCheck_MultipleBannedImportsSinglePackage(t *testing.T) {
	graph := map[string][]string{
		"github.com/szymonrychu/tatara-operator/internal/bad": {
			"github.com/google/go-github/v60/github",
			"github.com/shurcooL/githubv4",
		},
	}
	violations := archfitness.CheckImports(graph, adapterPkg, bannedPrefixes)
	if len(violations) != 2 {
		t.Fatalf("expected 2 violations, got %d: %v", len(violations), violations)
	}
}

// TestSCMVendorSDKIsolated is the live evolutionary-architecture guardrail that
// runs against the real module. It loads the actual package graph and asserts
// that no package outside internal/scm imports a vendor SCM client SDK.
//
// This test PASSES today (the baseline is already clean - zero vendor SCM SDK
// imports exist in the operator). It will FAIL as soon as an accidental coupling
// is introduced, catching it before the branch is merged.
func TestSCMVendorSDKIsolated(t *testing.T) {
	graph, err := archfitness.LoadModuleGraph(".")
	if err != nil {
		t.Fatalf("LoadModuleGraph: %v", err)
	}
	violations := archfitness.CheckImports(graph, adapterPkg, bannedPrefixes)
	if len(violations) > 0 {
		t.Errorf("SCM vendor SDK isolation violated (%d violation(s)); fix by moving SCM I/O into %s:", len(violations), adapterPkg)
		for _, v := range violations {
			t.Errorf("  package %s imports %s", v.Package, v.BannedImport)
		}
		t.FailNow()
	}
}
