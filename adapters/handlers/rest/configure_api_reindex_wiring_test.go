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

// Pins: the two node-local cleanup lookups must install before
// MakeAppState's post-bootstrap goroutine, not inside it, or a submission
// deleting sidecars is invisible to concurrent backups/restores for up to
// 60s. Source-level check since MakeAppState needs a real cluster to run.
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

	// found: install position by name. deferred: installs only reachable from
	// a scheduled function literal. bootstrapWait: position of the goroutine
	// that blocks on RAFT replay.
	found := map[string]token.Pos{}
	deferred := map[string]bool{}
	var bootstrapWait token.Pos
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
		case *ast.CallExpr:
			if sel, ok := n.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "GoWrapper" && n.Pos() > bootstrapWait {
				bootstrapWait = n.Pos()
			}
		case *ast.SelectorExpr:
			for _, name := range installs {
				if n.Sel.Name == name {
					found[name] = n.Pos()
				}
			}
		}
		return true
	})
	require.NotZero(t, bootstrapWait, "MakeAppState launches the post-bootstrap goroutine with GoWrapper")

	for _, name := range installs {
		require.Containsf(t, found, name, "%s must be called directly in MakeAppState", name)
		require.Falsef(t, deferred[name],
			"%s is installed inside a function literal, so the gate it feeds stays unwired "+
				"while HTTP already serves", name)
		require.Lessf(t, found[name], bootstrapWait,
			"%s is installed after the goroutine that waits on RAFT replay, so the gate it feeds "+
				"stays unwired for as long as that wait lasts", name)
	}
}
