//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package license

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const licensePkgPath = "github.com/weaviate/weaviate/usecases/license"

// wlImporters are the only non-test files outside wl/ that may import wl/, each calling it only in FeatureLicensed.
var wlImporters = []string{
	"adapters/handlers/rest/configure_api.go",
	"adapters/handlers/rest/db_user_expiration.go",
	"adapters/handlers/rest/namespace_qualifier.go",
}

// wlGates are the gate functions whose FuncLit arguments may use wl/; each runs them in FeatureLicensed alone.
var wlGates = map[string]bool{
	"selfRecoveryFor":                true,
	"setupSelfRecoveryDebugHandlers": true,
	"dedupePlannerFor":               true,
	"setupNamespaceHandlers":         true,
	"setupDBUserExpirationHandlers":  true,
}

// gateModeArgs maps each function that takes a feature's mode to the index of that argument; the argument must be a variable or a ModeFor call.
var gateModeArgs = map[string]int{
	"selfRecoveryFor":                0,
	"setupSelfRecoveryDebugHandlers": 1,
	"dedupePlannerFor":               0,
	"setupNamespaceHandlers":         1,
	"namespaceQualifier":             0,
	"setupDBUserExpirationHandlers":  1,
	"dbUserExpiryResolver":           0,
}

type goFile struct {
	rel  string
	fset *token.FileSet
	file *ast.File
}

func repoGoFiles(t *testing.T) []goFile {
	t.Helper()
	root := repoRoot(t)
	fset := token.NewFileSet()
	var files []goFile
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "vendor" || name == "testdata" || name == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		files = append(files, goFile{rel: filepath.ToSlash(rel), fset: fset, file: f})
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, files)
	return files
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "no go.mod above the test")
		dir = parent
	}
}

func inWLTree(rel string) bool { return strings.HasPrefix(rel, "wl/") }

func importsOf(f *ast.File) []string {
	paths := make([]string, 0, len(f.Imports))
	for _, imp := range f.Imports {
		if p, err := strconv.Unquote(imp.Path.Value); err == nil {
			paths = append(paths, p)
		}
	}
	return paths
}

func callsAFunction(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		switch n.(type) {
		case *ast.FuncLit:
			return false
		case *ast.CallExpr:
			found = true
			return false
		}
		return true
	})
	return found
}

// wlUseViolations returns "file:line: pkg.Name" for every use of a wl/ import, type positions included, outside a FuncLit passed to a gate or a case license.FeatureLicensed clause, plus licensedModeViolations.
func wlUseViolations(fset *token.FileSet, rel string, f *ast.File, gates map[string]bool) []string {
	var violations []string
	wlNames := map[string]bool{}
	for _, imp := range f.Imports {
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil || !inWL(p) {
			continue
		}
		name := p[strings.LastIndex(p, "/")+1:]
		if imp.Name != nil {
			name = imp.Name.Name
		}
		if name == "_" || name == "." {
			violations = append(violations, fmt.Sprintf("%s:%d: %s import of %s", rel, fset.Position(imp.Pos()).Line, name, p))
			continue
		}
		wlNames[name] = true
	}
	licenseNames := licenseImportNames(f)
	var stack []ast.Node
	ast.Inspect(f, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		if sel, ok := n.(*ast.SelectorExpr); ok {
			if id, ok := sel.X.(*ast.Ident); ok && wlNames[id.Name] && !wlUseAllowed(stack, gates, licenseNames) {
				violations = append(violations, fmt.Sprintf("%s:%d: %s.%s", rel, fset.Position(sel.Pos()).Line, id.Name, sel.Sel.Name))
			}
		}
		stack = append(stack, n)
		return true
	})
	return append(violations, licensedModeViolations(fset, rel, f)...)
}

func wlUseAllowed(stack []ast.Node, gates, licenseNames map[string]bool) bool {
	for i := len(stack) - 1; i >= 0; i-- {
		switch n := stack[i].(type) {
		case *ast.FuncLit:
			if i > 0 && isGateArgument(n, stack[i-1], gates) {
				return true
			}
		case *ast.CaseClause:
			if isLicensedCase(n, licenseNames) {
				return true
			}
		}
	}
	return false
}

// licensedModeViolations returns a violation for every license.FeatureLicensed that is not a case label, every license.Mode conversion and every gate mode argument that is neither a variable nor a ModeFor call, so no code can hard-code the licensed mode past ModeFor.
func licensedModeViolations(fset *token.FileSet, rel string, f *ast.File) []string {
	var violations []string
	for _, imp := range f.Imports {
		if p, err := strconv.Unquote(imp.Path.Value); err == nil && p == licensePkgPath && imp.Name != nil && imp.Name.Name == "." {
			violations = append(violations, fmt.Sprintf("%s:%d: . import of %s", rel, fset.Position(imp.Pos()).Line, licensePkgPath))
		}
	}
	names := licenseImportNames(f)
	isLicense := func(e ast.Expr, sym string) bool { return isLicenseSelector(e, sym, names) }
	var stack []ast.Node
	ast.Inspect(f, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		switch n := n.(type) {
		case *ast.CallExpr:
			if gate, ok := ast.Unparen(n.Fun).(*ast.Ident); ok {
				if i, ok := gateModeArgs[gate.Name]; ok && (i >= len(n.Args) || !isModeArgument(n.Args[i])) {
					violations = append(violations, fmt.Sprintf("%s:%d: %s mode argument", rel, fset.Position(n.Pos()).Line, gate.Name))
				}
			}
			if isLicense(n.Fun, "Mode") {
				violations = append(violations, fmt.Sprintf("%s:%d: license.Mode", rel, fset.Position(n.Pos()).Line))
			}
		case *ast.ValueSpec:
			if isLicense(n.Type, "Mode") && slices.ContainsFunc(n.Values, func(v ast.Expr) bool { return !isModeForCall(v) }) {
				violations = append(violations, fmt.Sprintf("%s:%d: license.Mode declaration", rel, fset.Position(n.Pos()).Line))
			}
		case *ast.SelectorExpr:
			if isLicense(n, "FeatureLicensed") {
				cc, ok := stack[len(stack)-1].(*ast.CaseClause)
				if !ok || !slices.Contains(cc.List, ast.Expr(n)) {
					violations = append(violations, fmt.Sprintf("%s:%d: license.FeatureLicensed", rel, fset.Position(n.Pos()).Line))
				}
			}
		}
		stack = append(stack, n)
		return true
	})
	return violations
}

func isModeArgument(arg ast.Expr) bool {
	_, ok := arg.(*ast.Ident)
	return ok || isModeForCall(arg)
}

func isModeForCall(e ast.Expr) bool {
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return strings.HasSuffix(fn.Name, "ModeFor")
	case *ast.SelectorExpr:
		return strings.HasSuffix(fn.Sel.Name, "ModeFor")
	}
	return false
}

// licenseImportNames returns the names f imports usecases/license under, blank and dot imports excluded.
func licenseImportNames(f *ast.File) map[string]bool {
	names := map[string]bool{}
	for _, imp := range f.Imports {
		if p, err := strconv.Unquote(imp.Path.Value); err != nil || p != licensePkgPath {
			continue
		}
		name := "license"
		if imp.Name != nil {
			name = imp.Name.Name
		}
		if name != "_" && name != "." {
			names[name] = true
		}
	}
	return names
}

func isLicenseSelector(e ast.Expr, sym string, names map[string]bool) bool {
	sel, ok := ast.Unparen(e).(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != sym {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && names[id.Name]
}

func isGateArgument(lit *ast.FuncLit, parent ast.Node, gates map[string]bool) bool {
	call, ok := parent.(*ast.CallExpr)
	if !ok {
		return false
	}
	fn, ok := call.Fun.(*ast.Ident)
	if !ok || !gates[fn.Name] {
		return false
	}
	return slices.ContainsFunc(call.Args, func(arg ast.Expr) bool { return arg == lit })
}

func isLicensedCase(cc *ast.CaseClause, licenseNames map[string]bool) bool {
	return len(cc.List) == 1 && isLicenseSelector(cc.List[0], "FeatureLicensed", licenseNames)
}

func TestWLGuards(t *testing.T) {
	files := repoGoFiles(t)
	var importers, unlicensedUses, hardCodedModes, licenseImports, inits, calledVars []string
	for _, gf := range files {
		isTest := strings.HasSuffix(gf.rel, "_test.go")
		if !inWLTree(gf.rel) {
			switch {
			case isTest || strings.HasPrefix(gf.rel, "usecases/license/"):
			case slices.ContainsFunc(importsOf(gf.file), inWL):
				importers = append(importers, gf.rel)
				unlicensedUses = append(unlicensedUses, wlUseViolations(gf.fset, gf.rel, gf.file, wlGates)...)
			default:
				hardCodedModes = append(hardCodedModes, licensedModeViolations(gf.fset, gf.rel, gf.file)...)
			}
			continue
		}
		if slices.Contains(importsOf(gf.file), licensePkgPath) {
			licenseImports = append(licenseImports, gf.rel)
		}
		if isTest {
			continue
		}
		for _, decl := range gf.file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.Name == "init" {
					inits = append(inits, gf.rel)
				}
			case *ast.GenDecl:
				if d.Tok != token.VAR {
					continue
				}
				for _, spec := range d.Specs {
					if slices.ContainsFunc(spec.(*ast.ValueSpec).Values, callsAFunction) {
						calledVars = append(calledVars, gf.rel)
					}
				}
			}
		}
	}

	t.Run("only allowlisted files outside wl import wl", func(t *testing.T) {
		require.ElementsMatch(t, wlImporters, importers)
	})
	t.Run("wl is used outside wl only on the licensed path", func(t *testing.T) {
		require.Empty(t, unlicensedUses)
	})
	t.Run("license.FeatureLicensed is only a case label and no code converts to license.Mode", func(t *testing.T) {
		require.Empty(t, hardCodedModes)
	})
	t.Run("every gate has a mode argument index", func(t *testing.T) {
		for gate := range wlGates {
			require.Contains(t, gateModeArgs, gate)
		}
	})
	t.Run("no wl package imports usecases/license", func(t *testing.T) {
		require.Empty(t, licenseImports)
	})
	t.Run("no wl package declares init", func(t *testing.T) {
		require.Empty(t, inits)
	})
	t.Run("no wl package-level var calls a function", func(t *testing.T) {
		require.Empty(t, calledVars)
	})
}

func TestCallsAFunction(t *testing.T) {
	cases := []struct {
		src  string
		want bool
	}{
		{src: `errors.New("x")`, want: true},
		{src: `&T{A: f()}`, want: true},
		{src: `T(1)`, want: true},
		{src: `"x"`},
		{src: `T{A: 1}`},
		{src: `func() int { return f() }`},
	}
	for _, tc := range cases {
		t.Run(tc.src, func(t *testing.T) {
			expr, err := parser.ParseExpr(tc.src)
			require.NoError(t, err)
			require.Equal(t, tc.want, callsAFunction(expr))
		})
	}
}

func TestWLUseViolations(t *testing.T) {
	const header = `package rest

import (
	"github.com/weaviate/weaviate/usecases/license"
	"github.com/weaviate/weaviate/wl/selfrecovery"
	srhandlers "github.com/weaviate/weaviate/wl/selfrecovery/handlers"
)
`
	cases := []struct {
		name string
		body string
		want []string
	}{
		{
			name: "call inside a gate closure",
			body: `func f(mode license.Mode) { selfRecoveryFor(mode, nil, func() any { return selfrecovery.New(selfrecovery.Config{}) }) }`,
		},
		{
			name: "nested closure inside a gate closure",
			body: `func f(mode license.Mode) { setupSelfRecoveryDebugHandlers(nil, mode, func(m any) { func() { srhandlers.SetupHandlers(m, nil, nil) }() }) }`,
		},
		{
			name: "call in a licensed case",
			body: `func f(mode license.Mode) any {
	switch mode {
	case license.FeatureLicensed:
		return selfrecovery.New(selfrecovery.Config{})
	case license.FeatureOff, license.FeatureUnlicensed:
	}
	return nil
}`,
		},
		{
			name: "type assertion inside a gate closure",
			body: `func f(mode license.Mode, v any) {
	setupSelfRecoveryDebugHandlers(nil, mode, func(m any) {
		o, ok := v.(*selfrecovery.Orchestrator)
		if ok {
			srhandlers.SetupHandlers(m, nil, o)
		}
	})
}`,
		},
		{
			name: "type positions outside a gate",
			body: `type T struct{ o *selfrecovery.Orchestrator }
type S = []selfrecovery.ShardRef
var v *selfrecovery.Orchestrator
func g(o any, refs ...selfrecovery.ShardRef) (m map[string]selfrecovery.ShardRef) { return o.(map[string]selfrecovery.ShardRef) }`,
			want: []string{
				"fixture.go:9: selfrecovery.Orchestrator",
				"fixture.go:10: selfrecovery.ShardRef",
				"fixture.go:11: selfrecovery.Orchestrator",
				"fixture.go:12: selfrecovery.ShardRef",
				"fixture.go:12: selfrecovery.ShardRef",
				"fixture.go:12: selfrecovery.ShardRef",
			},
		},
		{
			name: "method call on a wl-typed parameter",
			body: `func f(o *selfrecovery.Orchestrator) { o.Close(nil) }`,
			want: []string{"fixture.go:9: selfrecovery.Orchestrator"},
		},
		{
			name: "method call on a wl-typed var",
			body: `var o *selfrecovery.Orchestrator
func f() { o.Close(nil) }`,
			want: []string{"fixture.go:9: selfrecovery.Orchestrator"},
		},
		{
			name: "embedded wl type promotes its methods",
			body: `type T struct{ *selfrecovery.Orchestrator }
func f(t T) { t.Close(nil) }`,
			want: []string{"fixture.go:9: selfrecovery.Orchestrator"},
		},
		{
			name: "alias of a wl type",
			body: `type X = selfrecovery.Orchestrator
func f(x *X) { x.Close(nil) }`,
			want: []string{"fixture.go:9: selfrecovery.Orchestrator"},
		},
		{
			name: "hard-coded licensed mode passed to a gate",
			body: `func f() { selfRecoveryFor(license.FeatureLicensed, nil, func() any { return selfrecovery.New(selfrecovery.Config{}) }) }`,
			want: []string{"fixture.go:9: selfRecoveryFor mode argument", "fixture.go:9: license.FeatureLicensed"},
		},
		{
			name: "switch on the licensed mode",
			body: `func f() any {
	switch license.FeatureLicensed {
	case license.FeatureLicensed:
		return selfrecovery.New(selfrecovery.Config{})
	case license.FeatureOff, license.FeatureUnlicensed:
	}
	return nil
}`,
			want: []string{"fixture.go:10: license.FeatureLicensed"},
		},
		{
			name: "call in a licensed case under an aliased license import",
			body: `import lic "github.com/weaviate/weaviate/usecases/license"
func f(mode lic.Mode) any {
	switch mode {
	case lic.FeatureLicensed:
		return selfrecovery.New(selfrecovery.Config{})
	case lic.FeatureOff, lic.FeatureUnlicensed:
	}
	return nil
}`,
		},
		{
			name: "mutation: New called outside the gate closure",
			body: `func f(mode license.Mode) {
	o := selfrecovery.New(selfrecovery.Config{})
	selfRecoveryFor(mode, nil, func() any { return o })
}`,
			want: []string{"fixture.go:10: selfrecovery.New", "fixture.go:10: selfrecovery.Config"},
		},
		{
			name: "closure passed to a function that is not a gate",
			body: `func f() { run(func() { srhandlers.SetupHandlers(nil, nil, nil) }) }`,
			want: []string{"fixture.go:9: srhandlers.SetupHandlers"},
		},
		{
			name: "closure stored before reaching the gate",
			body: `func f(mode license.Mode) {
	build := func() any { return selfrecovery.New(selfrecovery.Config{}) }
	selfRecoveryFor(mode, nil, build)
}`,
			want: []string{"fixture.go:10: selfrecovery.New", "fixture.go:10: selfrecovery.Config"},
		},
		{
			name: "case shared with another mode",
			body: `func f(mode license.Mode) any {
	switch mode {
	case license.FeatureLicensed, license.FeatureUnlicensed:
		return selfrecovery.New(selfrecovery.Config{})
	}
	return nil
}`,
			want: []string{"fixture.go:12: selfrecovery.New", "fixture.go:12: selfrecovery.Config"},
		},
		{
			name: "value use of a constant",
			body: `var p = srhandlers.RestartPath`,
			want: []string{"fixture.go:9: srhandlers.RestartPath"},
		},
		{
			name: "method value taken at package level",
			body: `var n = selfrecovery.New`,
			want: []string{"fixture.go:9: selfrecovery.New"},
		},
		{
			name: "array length",
			body: `var a [selfrecovery.TriggerLen]int`,
			want: []string{"fixture.go:9: selfrecovery.TriggerLen"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "fixture.go", header+"\n"+tc.body+"\n", parser.SkipObjectResolution)
			require.NoError(t, err)

			require.Equal(t, tc.want, wlUseViolations(fset, "fixture.go", f, wlGates))
		})
	}
}

func TestLicensedModeViolations(t *testing.T) {
	cases := []struct {
		name string
		imp  string
		body string
		want []string
	}{
		{
			name: "sole and shared case labels",
			body: `func f(mode license.Mode) {
	switch mode {
	case license.FeatureLicensed:
	case license.FeatureOff, license.FeatureLicensed:
	}
}`,
		},
		{name: "call argument", body: `func f() { selfRecoveryFor(license.FeatureLicensed, nil, nil) }`, want: []string{"fixture.go:5: selfRecoveryFor mode argument", "fixture.go:5: license.FeatureLicensed"}},
		{name: "assignment", body: `var m = license.FeatureLicensed`, want: []string{"fixture.go:5: license.FeatureLicensed"}},
		{name: "switch tag", body: `func f() { switch license.FeatureLicensed { case license.FeatureLicensed: } }`, want: []string{"fixture.go:5: license.FeatureLicensed"}},
		{name: "comparison", body: `func f(mode license.Mode) bool { return mode == license.FeatureLicensed }`, want: []string{"fixture.go:5: license.FeatureLicensed"}},
		{name: "comparison in a case", body: `func f(mode license.Mode) { switch { case mode == license.FeatureLicensed: } }`, want: []string{"fixture.go:5: license.FeatureLicensed"}},
		{name: "mode conversion", body: `var m = license.Mode(2)`, want: []string{"fixture.go:5: license.Mode"}},
		{name: "typed var from a literal", body: `var m license.Mode = 2`, want: []string{"fixture.go:5: license.Mode declaration"}},
		{name: "typed const from a literal", body: `const m license.Mode = 2`, want: []string{"fixture.go:5: license.Mode declaration"}},
		{name: "typed local var from a literal", body: `func f() { var m license.Mode = 2; selfRecoveryFor(m, nil, nil) }`, want: []string{"fixture.go:5: license.Mode declaration"}},
		{name: "typed var from a conversion", body: `var m license.Mode = license.Mode(2)`, want: []string{"fixture.go:5: license.Mode declaration", "fixture.go:5: license.Mode"}},
		{name: "typed var from a variable", body: `var m license.Mode = other`, want: []string{"fixture.go:5: license.Mode declaration"}},
		{name: "typed var from a ModeFor call", body: `var m license.Mode = selfRecoveryModeFor(cfg)`},
		{name: "typed var, field and parameter without a value", body: `var m license.Mode
type T struct{ mode license.Mode }
func f(mode license.Mode) { selfRecoveryFor(mode, nil, nil) }`},
		{name: "typed var from a literal under an aliased import", imp: `lic "github.com/weaviate/weaviate/usecases/license"`, body: `var m lic.Mode = 2`, want: []string{"fixture.go:5: license.Mode declaration"}},
		{name: "parenthesised mode conversion", body: `var m = (license.Mode)(2)`, want: []string{"fixture.go:5: license.Mode"}},
		{
			name: "gate modes from variables and ModeFor calls",
			body: `func f(mode license.Mode) {
	selfRecoveryFor(mode, nil, nil)
	setupSelfRecoveryDebugHandlers(nil, mode, nil)
	dedupePlannerFor(dedupeModeFor(cfg), nil)
	setupNamespaceHandlers(nil, namespaceModeFor(cfg), nil)
	namespaceQualifier(license.ModeFor(true, false))
}`,
		},
		{name: "gate mode from a literal", body: `func f() { selfRecoveryFor(2, nil, nil) }`, want: []string{"fixture.go:5: selfRecoveryFor mode argument"}},
		{name: "gate mode from a conversion", body: `func f() { dedupePlannerFor(license.Mode(2), nil) }`, want: []string{"fixture.go:5: dedupePlannerFor mode argument", "fixture.go:5: license.Mode"}},
		{name: "gate mode from another expression", body: `func f(m []license.Mode) { setupNamespaceHandlers(nil, m[0], nil) }`, want: []string{"fixture.go:5: setupNamespaceHandlers mode argument"}},
		{name: "gate mode from a call that is not ModeFor", body: `func f() { setupSelfRecoveryDebugHandlers(nil, pickMode(), nil) }`, want: []string{"fixture.go:5: setupSelfRecoveryDebugHandlers mode argument"}},
		{name: "qualifier mode from a literal", body: `func f() { namespaceQualifier(2) }`, want: []string{"fixture.go:5: namespaceQualifier mode argument"}},
		{name: "expiration gate mode from a literal", body: `func f() { setupDBUserExpirationHandlers(nil, 2, nil) }`, want: []string{"fixture.go:5: setupDBUserExpirationHandlers mode argument"}},
		{name: "expiry resolver mode from a literal", body: `func f() { dbUserExpiryResolver(2) }`, want: []string{"fixture.go:5: dbUserExpiryResolver mode argument"}},
		{name: "parenthesised gate", body: `func f() { (selfRecoveryFor)(2, nil, nil) }`, want: []string{"fixture.go:5: selfRecoveryFor mode argument"}},
		{name: "gate mode from a literal without a license import", imp: `"github.com/weaviate/weaviate/usecases/other"`, body: `func f() { selfRecoveryFor(2, nil, nil) }`, want: []string{"fixture.go:5: selfRecoveryFor mode argument"}},
		{name: "typed var from another mode", body: `var m license.Mode = license.FeatureOff`, want: []string{"fixture.go:5: license.Mode declaration"}},
		{name: "other modes outside a declaration", body: `func f() any { return license.FeatureOff }`},
		{name: "aliased import", imp: `lic "github.com/weaviate/weaviate/usecases/license"`, body: `var m = lic.FeatureLicensed`, want: []string{"fixture.go:5: license.FeatureLicensed"}},
		{name: "dot import", imp: `. "github.com/weaviate/weaviate/usecases/license"`, body: `var m = FeatureLicensed`, want: []string{"fixture.go:3: . import of github.com/weaviate/weaviate/usecases/license"}},
		{name: "no license import", imp: `"github.com/weaviate/weaviate/usecases/other"`, body: `var m = license.FeatureLicensed`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			imp := tc.imp
			if imp == "" {
				imp = strconv.Quote(licensePkgPath)
			}
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "fixture.go", "package rest\n\nimport "+imp+"\n\n"+tc.body+"\n", parser.SkipObjectResolution)
			require.NoError(t, err)

			require.Equal(t, tc.want, licensedModeViolations(fset, "fixture.go", f))
		})
	}
}

func TestWLUseViolationsImportForms(t *testing.T) {
	cases := []struct {
		name string
		imp  string
		want []string
	}{
		{name: "blank import", imp: `_ "github.com/weaviate/weaviate/wl/selfrecovery"`, want: []string{"fixture.go:3: _ import of github.com/weaviate/weaviate/wl/selfrecovery"}},
		{name: "dot import", imp: `. "github.com/weaviate/weaviate/wl/selfrecovery"`, want: []string{"fixture.go:3: . import of github.com/weaviate/weaviate/wl/selfrecovery"}},
		{name: "non-wl import", imp: `"github.com/weaviate/weaviate/wlx"`},
		{
			name: "blank wl import beside a hard-coded licensed mode",
			imp:  "(\n\t_ \"github.com/weaviate/weaviate/wl/selfrecovery\"\n\t\"github.com/weaviate/weaviate/usecases/license\"\n)\n\nvar m = license.FeatureLicensed",
			want: []string{"fixture.go:4: _ import of github.com/weaviate/weaviate/wl/selfrecovery", "fixture.go:8: license.FeatureLicensed"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "fixture.go", "package rest\n\nimport "+tc.imp+"\n", parser.SkipObjectResolution)
			require.NoError(t, err)

			require.Equal(t, tc.want, wlUseViolations(fset, "fixture.go", f, wlGates))
		})
	}
}
