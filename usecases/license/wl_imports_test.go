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
	"adapters/handlers/rest/namespace_qualifier.go",
}

// wlGates are the gate functions whose FuncLit arguments may use wl/; each runs them in FeatureLicensed alone.
var wlGates = map[string]bool{
	"selfRecoveryFor":                true,
	"setupSelfRecoveryDebugHandlers": true,
	"dedupePlannerFor":               true,
	"setupNamespaceHandlers":         true,
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

// wlUseViolations returns "file:line: pkg.Name" for every use of a wl/ import outside a type position, a FuncLit passed to a gate, or a case license.FeatureLicensed clause.
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
	if len(wlNames) == 0 {
		return violations
	}
	var stack []ast.Node
	ast.Inspect(f, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		if sel, ok := n.(*ast.SelectorExpr); ok {
			if id, ok := sel.X.(*ast.Ident); ok && wlNames[id.Name] && !wlUseAllowed(sel, stack, gates) {
				violations = append(violations, fmt.Sprintf("%s:%d: %s.%s", rel, fset.Position(sel.Pos()).Line, id.Name, sel.Sel.Name))
			}
		}
		stack = append(stack, n)
		return true
	})
	return violations
}

func wlUseAllowed(sel *ast.SelectorExpr, stack []ast.Node, gates map[string]bool) bool {
	if inTypePosition(sel, stack) {
		return true
	}
	for i := len(stack) - 1; i >= 0; i-- {
		switch n := stack[i].(type) {
		case *ast.FuncLit:
			if i > 0 && isGateArgument(n, stack[i-1], gates) {
				return true
			}
		case *ast.CaseClause:
			if isLicensedCase(n) {
				return true
			}
		}
	}
	return false
}

func inTypePosition(sel *ast.SelectorExpr, stack []ast.Node) bool {
	var child ast.Node = sel
	for i := len(stack) - 1; i >= 0; i-- {
		switch p := stack[i].(type) {
		case *ast.StarExpr, *ast.MapType, *ast.ChanType, *ast.Ellipsis:
			child = p
			continue
		case *ast.ArrayType:
			if p.Elt != child {
				return false
			}
			child = p
			continue
		case *ast.ValueSpec:
			return p.Type == child
		case *ast.Field:
			return p.Type == child
		case *ast.CompositeLit:
			return p.Type == child
		case *ast.TypeSpec:
			return p.Type == child
		case *ast.TypeAssertExpr:
			return p.Type == child
		}
		return false
	}
	return false
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

func isLicensedCase(cc *ast.CaseClause) bool {
	if len(cc.List) != 1 {
		return false
	}
	sel, ok := cc.List[0].(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "license" && sel.Sel.Name == "FeatureLicensed"
}

func TestWLGuards(t *testing.T) {
	files := repoGoFiles(t)
	var importers, unlicensedUses, licenseImports, inits, calledVars []string
	for _, gf := range files {
		isTest := strings.HasSuffix(gf.rel, "_test.go")
		if !inWLTree(gf.rel) {
			if !isTest && slices.ContainsFunc(importsOf(gf.file), inWL) {
				importers = append(importers, gf.rel)
				unlicensedUses = append(unlicensedUses, wlUseViolations(gf.fset, gf.rel, gf.file, wlGates)...)
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
			name: "type positions",
			body: `type T struct{ o *selfrecovery.Orchestrator }
type S = []selfrecovery.ShardRef
var v *selfrecovery.Orchestrator
func g(o *selfrecovery.Orchestrator, refs ...selfrecovery.ShardRef) (m map[string]selfrecovery.ShardRef) { _ = o.(*selfrecovery.Orchestrator); return nil }`,
		},
		{
			name: "mutation: New called outside the gate closure",
			body: `func f(mode license.Mode) {
	o := selfrecovery.New(selfrecovery.Config{})
	selfRecoveryFor(mode, nil, func() any { return o })
}`,
			want: []string{"fixture.go:10: selfrecovery.New"},
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
			want: []string{"fixture.go:10: selfrecovery.New"},
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
			want: []string{"fixture.go:12: selfrecovery.New"},
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

func TestWLUseViolationsImportForms(t *testing.T) {
	cases := []struct {
		name string
		imp  string
		want []string
	}{
		{name: "blank import", imp: `_ "github.com/weaviate/weaviate/wl/selfrecovery"`, want: []string{"fixture.go:3: _ import of github.com/weaviate/weaviate/wl/selfrecovery"}},
		{name: "dot import", imp: `. "github.com/weaviate/weaviate/wl/selfrecovery"`, want: []string{"fixture.go:3: . import of github.com/weaviate/weaviate/wl/selfrecovery"}},
		{name: "non-wl import", imp: `"github.com/weaviate/weaviate/wlx"`},
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
