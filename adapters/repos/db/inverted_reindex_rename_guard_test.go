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

// Every read of payload.mig happens on a path a RAFT apply can reach, and a
// payload names every targeted tenant, so a large multi-tenant migration puts
// megabytes there. Reading one unbounded holds the FSM loop cluster-wide.
//
// A guard rather than a behavioral test: refusing costs a stat and reading
// costs a read, and nothing in the outcome of either distinguishes them.
func TestEveryPayloadReadIsBounded(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	checked := 0

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			names := identsIn(fn.Body)
			if !names["reindexRecoveryPayloadFile"] || !names["ReadFile"] {
				continue
			}
			checked++
			require.Truef(t, names["refuseOversizedRecoveryPayload"],
				"%s: %s reads %s without bounding it first — every reader of this file is on a "+
					"path a RAFT apply can reach, so none of them may read an unbounded one",
				fset.Position(fn.Pos()), fn.Name.Name, "payload.mig")
		}
	}

	require.GreaterOrEqual(t, checked, 3, "the guard has to be finding the readers of this file")
}

// identsIn collects every identifier named in n, which is all this guard needs:
// it asks whether a function mentions a helper, not where.
func identsIn(n ast.Node) map[string]bool {
	found := map[string]bool{}
	ast.Inspect(n, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok {
			found[ident.Name] = true
		}
		return true
	})
	return found
}
