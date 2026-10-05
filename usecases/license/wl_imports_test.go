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

type goFile struct {
	rel  string
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
		files = append(files, goFile{rel: filepath.ToSlash(rel), file: f})
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

func TestWLGuards(t *testing.T) {
	files := repoGoFiles(t)
	var importers, licenseImports, inits, calledVars []string
	for _, gf := range files {
		isTest := strings.HasSuffix(gf.rel, "_test.go")
		if !inWLTree(gf.rel) {
			if !isTest && slices.ContainsFunc(importsOf(gf.file), inWL) {
				importers = append(importers, gf.rel)
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
