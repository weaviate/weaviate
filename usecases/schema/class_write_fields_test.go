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

package schema

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
)

func TestClassWritesNameTheirFields(t *testing.T) {
	stored := func() *models.Class {
		return &models.Class{Class: "C", Vectorizer: "none", ReplicationConfig: &models.ReplicationConfig{Factor: 1}}
	}
	tests := []struct {
		name       string
		write      func(t *testing.T) *fakeSchemaManager
		wantFields []string // nil: the write replaces the class
	}{
		{
			name: "REST update replaces the class",
			write: func(t *testing.T) *fakeSchemaManager {
				h, fake := newTestHandler(t, nil)
				fake.On("ReadOnlyClass", mock.Anything).Return(stored())
				fake.On("QueryReadOnlyClasses", mock.Anything).Return(nil, nil)
				expectClassWrites(fake)
				require.NoError(t, h.UpdateClass(context.Background(), nil, "C", stored()))
				return fake
			},
		},
		{
			name: "internal update sends its field",
			write: func(t *testing.T) *fakeSchemaManager {
				h, fake := newTestHandler(t, nil)
				fake.On("ReadOnlyClass", mock.Anything).Return(stored())
				expectClassWrites(fake)
				require.NoError(t, UpdateClassInternal(h, context.Background(), "C", stored(), command.ClassFieldUsingBlockMaxWAND))
				return fake
			},
			wantFields: []string{command.ClassFieldUsingBlockMaxWAND},
		},
		{
			name: "drop-vector marker sends its vector",
			write: func(t *testing.T) *fakeSchemaManager {
				h, fake, _ := newDropVectorHandler(t, classWithVectors(map[string]models.VectorConfig{
					"foo": {VectorIndexType: "hnsw"},
				}))
				expectClassWrites(fake)
				require.NoError(t, h.DeleteClassVectorIndex(context.Background(), nil, "C", "foo"))
				return fake
			},
			wantFields: []string{command.ClassFieldVectorConfig("foo")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := tt.write(t)
			if tt.wantFields == nil {
				fake.AssertCalled(t, "ReplaceClass", mock.Anything, mock.Anything)
				fake.AssertNotCalled(t, "UpdateClass", mock.Anything, mock.Anything, mock.Anything)
				return
			}
			fake.AssertCalled(t, "UpdateClass", mock.Anything, mock.Anything, tt.wantFields)
			fake.AssertNotCalled(t, "ReplaceClass", mock.Anything, mock.Anything)
		})
	}
}

func expectClassWrites(fake *fakeSchemaManager) {
	fake.On("ReplaceClass", mock.Anything, mock.Anything).Return(nil).Maybe()
	fake.On("UpdateClass", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
}

// A whole-class replace reverts every field changed since its payload was read,
// so only the REST update, whose payload is the user's class, may send one.
func TestOnlyRESTReplacesTheWholeClass(t *testing.T) {
	type call struct{ file, fn string }
	const thisPackage = "usecases/schema"
	var replaceCalls, emptyFieldCalls []call
	fset := token.NewFileSet()
	visit := func(path string, src []byte) {
		f, err := parser.ParseFile(fset, path, src, 0)
		require.NoError(t, err)
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := funcName(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				ce, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := ce.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch {
				case sel.Sel.Name == "ReplaceClass":
					replaceCalls = append(replaceCalls, call{path, name})
				case sel.Sel.Name == "updateClass" && filepath.Dir(path) == thisPackage && isEmptyList(ce.Args[len(ce.Args)-1]):
					emptyFieldCalls = append(emptyFieldCalls, call{path, name})
				}
				return true
			})
		}
	}

	repoRoot := filepath.Join("..", "..")
	require.NoError(t, filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != repoRoot && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(src, []byte("ReplaceClass(")) || bytes.Contains(src, []byte("updateClass(")) {
			rel, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			visit(rel, src)
		}
		return nil
	}))

	require.Equal(t, []call{{"usecases/schema/class.go", "Handler.updateClass"}}, replaceCalls)
	require.Equal(t, []call{{"usecases/schema/class.go", "Handler.UpdateClass"}}, emptyFieldCalls)
}

func funcName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	recv := fn.Recv.List[0].Type
	if star, ok := recv.(*ast.StarExpr); ok {
		recv = star.X
	}
	if id, ok := recv.(*ast.Ident); ok {
		return id.Name + "." + fn.Name.Name
	}
	return fn.Name.Name
}

func isEmptyList(arg ast.Expr) bool {
	if id, ok := arg.(*ast.Ident); ok {
		return id.Name == "nil"
	}
	lit, ok := arg.(*ast.CompositeLit)
	return ok && len(lit.Elts) == 0
}
