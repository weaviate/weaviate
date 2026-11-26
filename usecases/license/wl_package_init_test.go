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
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWLHasNoPackageInitialisation fails on a package-level var or an init
// function in a non-test file under wl/. Go initialises both when the binary
// starts, and the binary links wl/ on every node, licensed or not.
func TestWLHasNoPackageInitialisation(t *testing.T) {
	fset := token.NewFileSet()
	files := 0
	var found []string
	err := filepath.WalkDir("../../wl", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		files++
		for _, decl := range f.Decls {
			switch decl := decl.(type) {
			case *ast.GenDecl:
				if decl.Tok == token.VAR {
					found = append(found, fset.Position(decl.Pos()).String()+": package-level var")
				}
			case *ast.FuncDecl:
				if decl.Recv == nil && decl.Name.Name == "init" {
					found = append(found, fset.Position(decl.Pos()).String()+": init function")
				}
			}
		}
		return nil
	})

	require.NoError(t, err)
	require.NotZero(t, files, "no non-test Go file under ../../wl")
	require.Empty(t, found)
}
