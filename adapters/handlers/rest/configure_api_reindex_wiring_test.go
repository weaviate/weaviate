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

package rest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

// The two cleanup lookups read this node's own provider and need nothing from
// RAFT or DTM, and the gates that consult them are reachable over HTTP as soon
// as the server serves. Installing them inside MakeAppState's post-bootstrap
// goroutine leaves both nil through a RAFT replay plus a wait of up to 60s for
// DTM — a window in which a submission that is deleting sidecars right now is
// invisible to a concurrent backup and to a concurrent restore.
//
// Only reachable as a source-level check: the goroutine's own wiring is what is
// under test, and MakeAppState cannot be run without a real cluster.
func TestCleanupLookupsAreInstalledBeforeTheBootstrapWait(t *testing.T) {
	installs := []string{
		"SetReindexCleanupInProgressLookup",
		"SetAnyCleanupInProgressLookup",
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "configure_api.go", nil, 0)
	require.NoError(t, err)

	var makeAppState *ast.FuncDecl
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "MakeAppState" {
			makeAppState = fn
		}
	}
	require.NotNil(t, makeAppState, "MakeAppState is where the gate lookups are wired")

	// found holds the installs MakeAppState runs itself; deferred holds the
	// ones that only run when some function literal is later scheduled.
	found := map[string]bool{}
	deferred := map[string]bool{}
	ast.Inspect(makeAppState.Body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.FuncLit:
			ast.Inspect(n.Body, func(inner ast.Node) bool {
				if sel, ok := inner.(*ast.SelectorExpr); ok {
					for _, name := range installs {
						if sel.Sel.Name == name {
							deferred[name] = true
						}
					}
				}
				return true
			})
			return false
		case *ast.SelectorExpr:
			for _, name := range installs {
				if n.Sel.Name == name {
					found[name] = true
				}
			}
		}
		return true
	})

	for _, name := range installs {
		require.Truef(t, found[name], "%s must be called directly in MakeAppState", name)
		require.Falsef(t, deferred[name],
			"%s is installed inside a function literal, so the gate it feeds stays unwired "+
				"while HTTP already serves", name)
	}
}
