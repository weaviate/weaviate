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
	"fmt"
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

// TestEveryPayloadReadIsBounded pins which readers of payload.mig bound their
// read and which one must not. The bound is a latency bound for the readers a
// RAFT apply reaches, where refusing is fail-open and costs a stat. The startup
// recovery walk is neither: refusing there arms no double-write mirror, and the
// flip that follows takes the canonical directory away with every write since
// the restart.
//
// A guard rather than a behavioral test: nothing in the outcome of a stat and a
// read distinguishes them.
func TestEveryPayloadReadIsBounded(t *testing.T) {
	offTheApplyPath := map[string]bool{"loadReindexRecoveryRecord": true}

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	checked := 0
	var unbounded []string

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
			if names["refuseOversizedRecoveryPayload"] {
				require.Falsef(t, offTheApplyPath[fn.Name.Name],
					"%s: %s bounds its read of payload.mig, which drops the writes taken since the restart",
					fset.Position(fn.Pos()), fn.Name.Name)
				continue
			}
			unbounded = append(unbounded, fmt.Sprintf("%s: %s", fset.Position(fn.Pos()), fn.Name.Name))
			require.Truef(t, offTheApplyPath[fn.Name.Name],
				"%s: %s reads payload.mig without bounding it first, and it runs where a RAFT apply "+
					"can reach it", fset.Position(fn.Pos()), fn.Name.Name)
		}
	}

	require.GreaterOrEqual(t, checked, 3, "the guard has to be finding the readers of this file")
	require.Len(t, unbounded, len(offTheApplyPath), "every exempt reader has to still be one")
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

// Reconciliation removes a migration's directories once its task goes
// terminal, and it seals the unit first. That interlock once answered from the
// re-entry guard, which is claimed under `if semantic` and only around the
// iteration — so it read false for four migration types and for the prep and
// swap of all nine, and the removal went ahead under a running worker.
//
// Three things keep it honest, and none is observable at runtime without a
// full cluster: the seal must not read the re-entry guard's map, every span
// that writes into those directories must register unconditionally, and
// entering must consult the seals so a late entrant is refused rather than
// admitted alongside a teardown.
func TestLocalUnitSealIsNotTheReEntryGuard(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "reindex_provider.go", nil, 0)
	require.NoError(t, err)

	bodies := map[string]*ast.FuncDecl{}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
			bodies[fn.Name.Name] = fn
		}
	}

	seal, ok := bodies["SealLocalUnit"]
	require.True(t, ok, "the seal reconciliation gates its removals on")
	names := identsIn(seal.Body)
	require.True(t, names["liveUnits"], "SealLocalUnit must answer from the liveness registry")
	require.True(t, names["sealedUnits"], "SealLocalUnit must record the seal it granted")
	require.False(t, names["activeWorkers"],
		"SealLocalUnit must not answer from the re-entry guard: that map is claimed only "+
			"for semantic migrations and only around the iteration")

	enter, ok := bodies["enterLocalUnit"]
	require.True(t, ok, "the claim every writing span takes")
	require.True(t, identsIn(enter.Body)["sealedUnits"],
		"enterLocalUnit must refuse a unit a teardown holds: the phase decided to run from a "+
			"task snapshot frozen at the start of a tick, so the teardown can have started since")

	// processOneUnit is the iteration; runPerUnitPhase drives both the prep
	// and the swap for every callback that reaches a shard.
	for _, fn := range []string{"processOneUnit", "runPerUnitPhase"} {
		decl, ok := bodies[fn]
		require.Truef(t, ok, "%s is where a span that writes migration directories lives", fn)
		require.Truef(t, identsIn(decl.Body)["enterLocalUnit"],
			"%s does work through handles taken before it starts, so it must register the unit as live", fn)
		require.Falsef(t, callIsGuardedBySemantic(decl.Body),
			"%s registers the unit only for semantic migrations; the other four types write "+
				"into the same directories", fn)
	}
}

// callIsGuardedBySemantic reports an enterLocalUnit call reachable only when a
// migration is semantic, which is the shape the re-entry guard has and the
// liveness registration must not.
func callIsGuardedBySemantic(body *ast.BlockStmt) bool {
	guarded := false
	ast.Inspect(body, func(n ast.Node) bool {
		stmt, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		if !identsIn(stmt.Cond)["semantic"] && !identsIn(stmt.Cond)["IsSemanticMigration"] {
			return true
		}
		if identsIn(stmt.Body)["enterLocalUnit"] {
			guarded = true
		}
		return true
	})
	return guarded
}
