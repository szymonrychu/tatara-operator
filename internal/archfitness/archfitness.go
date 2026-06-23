// Package archfitness provides evolutionary-architecture fitness functions for
// the tatara-operator module. Its primary check enforces that vendor SCM client
// SDKs (go-github, go-gitlab, githubv4, etc.) are imported ONLY inside the
// designated adapter package internal/scm and never leak into other packages.
//
// Usage in tests:
//
//	graph, err := archfitness.LoadModuleGraph(".")
//	violations := archfitness.CheckImports(graph, adapterPkg, bannedPrefixes)
package archfitness

import (
	"fmt"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Violation describes a single architectural-isolation violation: a package
// outside the allowed adapter that imports a banned SDK prefix.
type Violation struct {
	// Package is the fully-qualified import path of the offending package.
	Package string
	// BannedImport is the exact import path that triggered the ban.
	BannedImport string
}

func (v Violation) String() string {
	return fmt.Sprintf("%s imports %s", v.Package, v.BannedImport)
}

// CheckImports walks the given import graph and returns a Violation for every
// package (other than adapterPkg and its sub-packages) whose import list
// contains an import path matching any of the bannedPrefixes.
//
// graph is a map from package import path to its direct imports.
// adapterPkg is the import path of the sanctioned adapter package; it and any
// package with adapterPkg+"/" as a prefix are exempt from the ban.
// bannedPrefixes is the list of import-path prefixes that are prohibited outside
// the adapter.
func CheckImports(graph map[string][]string, adapterPkg string, bannedPrefixes []string) []Violation {
	var violations []Violation
	for pkg, imports := range graph {
		if pkg == adapterPkg || strings.HasPrefix(pkg, adapterPkg+"/") {
			// Adapter and its sub-packages are the sanctioned home for SCM SDKs.
			continue
		}
		for _, imp := range imports {
			for _, banned := range bannedPrefixes {
				if imp == banned || strings.HasPrefix(imp, banned+"/") || strings.HasPrefix(imp, banned+"@") {
					violations = append(violations, Violation{Package: pkg, BannedImport: imp})
				}
			}
		}
	}
	return violations
}

// LoadModuleGraph loads the import graph for all packages in the module rooted
// at moduleRoot (a directory path, typically ".") using go/packages. It returns
// a map from each package's import path to its direct imports.
//
// The load is intentionally shallow (NeedName|NeedImports only) to keep it fast
// even on large module trees. It does not compile packages; it only resolves
// metadata. Any package-level load errors are aggregated and returned as a
// single error so the caller can surface them clearly.
func LoadModuleGraph(moduleRoot string) (map[string][]string, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedImports,
		Dir:  moduleRoot,
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		return nil, fmt.Errorf("archfitness: packages.Load: %w", err)
	}

	// Collect any per-package errors.
	var errs []string
	for _, p := range pkgs {
		for _, e := range p.Errors {
			errs = append(errs, e.Error())
		}
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("archfitness: package load errors: %s", strings.Join(errs, "; "))
	}

	graph := make(map[string][]string, len(pkgs))
	for _, p := range pkgs {
		imports := make([]string, 0, len(p.Imports))
		for imp := range p.Imports {
			imports = append(imports, imp)
		}
		graph[p.PkgPath] = imports
	}
	return graph, nil
}
