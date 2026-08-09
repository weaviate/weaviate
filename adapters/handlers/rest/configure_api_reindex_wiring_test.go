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
	"go/types"
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

// Pins the opposite half of the same wiring: the reindex audit deps must
// land only AFTER the metastore is ready, because opening the metastore is
// what replays RAFT and loads the existing indices. Every shard that
// initializes in that window resolves task liveness as Unknown, which
// decides Leave rather than Refuse — the contract documented on
// [db.mergedPromotionDecision] and in docs/runtime-reindex.md.
//
// Moving the install above the wait would silently flip eager shards onto
// the refusal arm, so the order is what this asserts, not merely that both
// calls exist. Source-level check since MakeAppState needs a real cluster
// to run.
func TestReindexAuditDepsAreInstalledAfterTheMetaStoreWait(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "configure_api.go", nil, 0)
	require.NoError(t, err)

	var makeAppState *ast.FuncDecl
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "MakeAppState" {
			makeAppState = fn
		}
	}
	require.NotNil(t, makeAppState, "MakeAppState is where the audit deps are wired")

	// Positions of the two calls within the one function literal that
	// holds both. Zero means "not seen yet".
	var waitPos, installPos token.Pos
	ast.Inspect(makeAppState.Body, func(node ast.Node) bool {
		lit, ok := node.(*ast.FuncLit)
		if !ok {
			return true
		}
		var wait, install token.Pos
		ast.Inspect(lit.Body, func(inner ast.Node) bool {
			sel, ok := inner.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "waitForMetaStore":
				if wait == 0 {
					wait = sel.Pos()
				}
			case "SetReindexAuditDeps":
				if install == 0 {
					install = sel.Pos()
				}
			}
			return true
		})
		if install != 0 {
			waitPos, installPos = wait, install
			return false
		}
		return true
	})

	require.NotZero(t, installPos,
		"SetReindexAuditDeps must be called from a goroutine in MakeAppState")
	require.NotZero(t, waitPos,
		"the goroutine installing SetReindexAuditDeps must wait for the metastore first; "+
			"without that wait, eager shard init races the deps install")
	require.Less(t, int(waitPos), int(installPos),
		"SetReindexAuditDeps must come after waitForMetaStore, or shards loaded during "+
			"RAFT replay start resolving task liveness as Dead instead of Unknown")
}

// Pins the context both post-startup audit entry points run on. Each one
// starts with a query to the leader, and both are reached from the RAFT
// FSM apply path — the class-dir restore hook directly, the deferred
// replay through the audits that hook requested before the deps landed.
// A background context there leaves a leader that is reachable but not
// answering holding up RAFT apply with nothing, not even SIGTERM, to
// release it. Source-level check since MakeAppState needs a real cluster
// to run.
func TestReindexAuditCallsAreCancellableOnShutdown(t *testing.T) {
	calls := []string{"AuditOrphanReindexTrackersIfReady", "SetReindexAuditDeps"}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "configure_api.go", nil, 0)
	require.NoError(t, err)

	var makeAppState *ast.FuncDecl
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "MakeAppState" {
			makeAppState = fn
		}
	}
	require.NotNil(t, makeAppState, "MakeAppState is where the audit calls are wired")

	// The context expression each call passes.
	ctxArg := map[string]ast.Expr{}
	ast.Inspect(makeAppState.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		for _, name := range calls {
			if sel.Sel.Name == name {
				ctxArg[name] = call.Args[0]
			}
		}
		return true
	})

	// The property is "not a fresh root context", not any particular
	// variable name: `context.WithTimeout(serverShutdownCtx, …)` stored in
	// a local is a better call than passing serverShutdownCtx straight
	// through, and must pass too.
	for _, name := range calls {
		require.NotNilf(t, ctxArg[name], "%s must be called in MakeAppState", name)
		require.Falsef(t, isFreshRootContext(ctxArg[name]),
			"%s must run on a context a server shutdown cancels, got %s",
			name, types.ExprString(ctxArg[name]))
	}
}

// isFreshRootContext reports whether expr is a literal call to
// context.Background() or context.TODO() — a context nothing can cancel.
func isFreshRootContext(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "context" {
		return false
	}
	return sel.Sel.Name == "Background" || sel.Sel.Name == "TODO"
}
