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

package db

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Every rename in the migration machinery publishes something a durable record
// already vouches for, so a bare os.Rename is a crash window no test can
// observe: there is no fault-injection filesystem in this repo, and a
// container kill keeps the page cache. This guard is the pin instead.
//
// Test files are out of scope — a fixture that renames the records directory
// aside is planting a fault, not publishing one.
func TestMigrationRenamesGoThroughTheDurableHelper(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	scanned := 0

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if !matchesAny(name, "inverted_reindex_*.go", "reindex_*.go") {
			continue
		}

		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		scanned++

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Rename" {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != "os" {
				return true
			}
			require.Failf(t, "os.Rename in the migration machinery",
				"%s: use diskio.RenameAndSync — a rename a durable record vouches for "+
					"must be synced, or a crash publishes a record for a name the "+
					"filesystem never kept", fset.Position(call.Pos()))
			return false
		})
	}

	require.Greater(t, scanned, 20, "the guard has to be reading the migration files")
}

func matchesAny(name string, patterns ...string) bool {
	for _, pattern := range patterns {
		if ok, err := filepath.Match(pattern, name); err == nil && ok {
			return true
		}
	}
	return false
}
