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
	"go/printer"
	"go/token"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSecondaryBatchConfigWiring pins the config→db.Config hop for the gh#309
// GetBySecondaryBatch knobs. The runtime toggles are read from the environment
// into config.Config (usecases/config/environment.go), then must be copied into
// the db.Config{} literal that MakeAppState passes to db.New. When a knob is
// omitted from that literal it stays nil and WithSecondaryBatchPipeline(nil) /
// WithSecondaryBatchReadConcurrency(nil) makes the toggle a dead no-op in a real
// server - exactly the defect that shipped for SecondaryBatchPipelineEnabled
// (unit tests missed it because they construct buckets with the option directly,
// bypassing this wiring).
//
// The wiring lives inside the ~90-line db.Config{} literal in MakeAppState, so
// this asserts the invariant at the source level rather than booting the full
// app: each knob must appear as a keyed field assigned from
// appState.ServerConfig.Config.<sameName>.
func TestSecondaryBatchConfigWiring(t *testing.T) {
	const configFieldRoot = "appState.ServerConfig.Config"

	fields := dbConfigLiteralFields(t, "configure_api.go")

	knobs := []string{
		"SecondaryBatchReadConcurrency",
		"SecondaryBatchPipelineEnabled",
	}

	for _, knob := range knobs {
		t.Run(knob, func(t *testing.T) {
			rhs, ok := fields[knob]
			require.Truef(t, ok,
				"db.Config{} literal in configure_api.go must wire %s from the server "+
					"config; it is missing. Omitting it leaves the field nil so the "+
					"runtime toggle is a no-op in a real server.", knob)
			require.Equalf(t, configFieldRoot+"."+knob, rhs,
				"%s must be copied from %s.%s (mirroring its sibling), got %q",
				knob, configFieldRoot, knob, rhs)
		})
	}
}

// dbConfigLiteralFields parses the given source file in this package and returns
// the keyed fields of the (single) db.Config{} composite literal it contains,
// mapping field name → rendered right-hand-side expression.
func dbConfigLiteralFields(t *testing.T, filename string) map[string]string {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, nil, parser.SkipObjectResolution)
	require.NoErrorf(t, err, "parse %s", filename)

	var lit *ast.CompositeLit
	ast.Inspect(file, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := cl.Type.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		if pkg.Name == "db" && sel.Sel.Name == "Config" {
			lit = cl
			return false
		}
		return true
	})
	require.NotNil(t, lit, "no db.Config{} composite literal found in %s", filename)

	fields := make(map[string]string, len(lit.Elts))
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}
		fields[key.Name] = renderExpr(t, fset, kv.Value)
	}
	return fields
}

func renderExpr(t *testing.T, fset *token.FileSet, expr ast.Expr) string {
	t.Helper()
	var sb strings.Builder
	require.NoError(t, printer.Fprint(&sb, fset, expr))
	return sb.String()
}
