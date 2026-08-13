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

// Pins that [migrationDirScope.match] never reaches
// [migrationDirScope.matchByName] through any call path, so the
// unloaded-shard gate's fail-open answer can't take the name shortcut
// (the preserve set does take it; see [TestWidenedMatchesAgreesWithTheNarrowGate]).
func TestMatchNeverReachesMatchByName(t *testing.T) {
	callees := packageCallGraph(t, ".")
	require.Contains(t, callees, "match", "the call graph must have found match itself")
	require.Contains(t, callees["matches"], "matchByName",
		"matches is the caller the shortcut belongs to; without it this test proves nothing")

	seen := map[string]bool{"match": true}
	queue := []string{"match"}
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		for callee := range callees[fn] {
			require.NotEqual(t, "matchByName", callee,
				"match reaches matchByName through %q", fn)
			if !seen[callee] {
				seen[callee] = true
				queue = append(queue, callee)
			}
		}
	}
}

// packageCallGraph maps every function declared in dir to the names it calls.
// Receivers are dropped, so two methods sharing a name share a node: the graph
// only ever gains edges, which is the safe direction for proving one function
// cannot reach another.
func packageCallGraph(t *testing.T, dir string) map[string]map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	fset := token.NewFileSet()
	callees := map[string]map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		require.NoError(t, err)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if callees[fn.Name.Name] == nil {
				callees[fn.Name.Name] = map[string]bool{}
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch f := call.Fun.(type) {
				case *ast.Ident:
					callees[fn.Name.Name][f.Name] = true
				case *ast.SelectorExpr:
					callees[fn.Name.Name][f.Sel.Name] = true
				}
				return true
			})
		}
	}
	return callees
}
