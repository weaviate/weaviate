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

package lsmkv

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

// A prepend publishes each copied segment by renaming its .tmp away, and the
// caller writes a durable record saying the staged data is complete. A rename
// is a directory entry, which reaches disk only when the directory holding it
// is synced — so without the sync a machine crash can take the segments while
// leaving the claim behind, and the next load promotes a bucket that is
// missing them.
//
// An fsync has no observable effect a test can assert, so this asserts the
// call instead: no function in the file may publish a rename without syncing.
func TestPrependPublishesEveryRenameDurably(t *testing.T) {
	const fileName = "segment_group_prepend.go"

	file, err := parser.ParseFile(token.NewFileSet(), fileName, nil, 0)
	require.NoError(t, err)

	renames := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}

		var renamesHere, syncs int
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			sel, ok := selectorOfCall(n)
			if !ok {
				return true
			}
			switch sel {
			case "os.Rename":
				renamesHere++
			case "diskio.Fsync":
				syncs++
			}
			return true
		})

		renames += renamesHere
		if renamesHere > 0 {
			require.NotZerof(t, syncs,
				"%s renames a published file but never syncs the directory holding the entry", fn.Name.Name)
		}
	}
	require.NotZero(t, renames, "the guard is watching a file that no longer renames anything")
}

// selectorOfCall returns "pkg.Fn" for a call of that shape.
func selectorOfCall(n ast.Node) (string, bool) {
	call, ok := n.(*ast.CallExpr)
	if !ok {
		return "", false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok {
		return "", false
	}
	return pkg.Name + "." + sel.Sel.Name, true
}
