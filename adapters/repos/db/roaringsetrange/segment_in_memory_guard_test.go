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

package roaringsetrange

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
)

// planeAccessAllowed lists the functions that may name s.bitmaps, with the
// reason each one is exempt. Everything else must go through mutateBitmaps.
var planeAccessAllowed = map[string]string{
	"NewSegmentInMemory": "builds the planes before the value is published",
	"mutateBitmaps":      "the sanctioned mutation scope; bumps the generation",
	"Size":               "unlocked read, predates the cache and is unchanged by it",
	"Readers":            "snapshots the planes under RLock",
}

// writeLockAllowed lists the functions that may take bitmapsLock for writing.
// Restricting this to mutateBitmaps is what closes the gap: a plane mutation
// either takes the write lock, in which case it must come through the scope
// that bumps the generation, or it does not, in which case it is a data race
// the -race concurrency tests are there to catch.
var writeLockAllowed = map[string]string{
	"mutateBitmaps": "bumps the generation inside the same critical section",
}

type planeViolation struct {
	function string
	kind     string
	position string
}

// findPlaneViolations reports functions owned by SegmentInMemory that reach for
// the bit planes or the write lock outside the allowlists above. It only
// considers SegmentInMemory, since segmentInMemoryReader holds a read-only
// snapshot and naming its own bitmaps field is not a hazard.
func findPlaneViolations(fset *token.FileSet, files []*ast.File) []planeViolation {
	var out []planeViolation

	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !ownedBySegmentInMemory(fn) {
				continue
			}

			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.SelectorExpr:
					if node.Sel.Name == "bitmaps" {
						if _, allowed := planeAccessAllowed[fn.Name.Name]; !allowed {
							out = append(out, planeViolation{
								function: fn.Name.Name,
								kind:     "names s.bitmaps",
								position: fset.Position(node.Pos()).String(),
							})
						}
					}
				case *ast.CallExpr:
					if !isWriteLockCall(node) {
						return true
					}
					if _, allowed := writeLockAllowed[fn.Name.Name]; !allowed {
						out = append(out, planeViolation{
							function: fn.Name.Name,
							kind:     "takes bitmapsLock for writing",
							position: fset.Position(node.Pos()).String(),
						})
					}
				}
				return true
			})
		}
	}
	return out
}

func ownedBySegmentInMemory(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		// the constructor has no receiver but does build the planes
		return fn.Name.Name == "NewSegmentInMemory"
	}

	typ := fn.Recv.List[0].Type
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	ident, ok := typ.(*ast.Ident)
	return ok && ident.Name == "SegmentInMemory"
}

// isWriteLockCall matches x.bitmapsLock.Lock().
func isWriteLockCall(call *ast.CallExpr) bool {
	method, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || method.Sel.Name != "Lock" {
		return false
	}
	receiver, ok := method.X.(*ast.SelectorExpr)
	return ok && receiver.Sel.Name == "bitmapsLock"
}

func parsePackageSources(t *testing.T) (*token.FileSet, []*ast.File) {
	t.Helper()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		require.NoError(t, err)
		files = append(files, file)
	}
	require.NotEmpty(t, files)
	return fset, files
}

// TestPlanesAreOnlyMutatedThroughMutateBitmaps is the structural guard for this
// package's one silent-failure mode. The leaf cache is invalidated by a
// generation counter; a writer that mutates the bit planes without bumping it
// serves a stale allow-list, which means missing or extra objects in query
// results with no panic, no log and no error metric. This test goes red the
// moment a new function reaches for the planes or takes the write lock, so the
// mistake is caught when it is written rather than in production.
func TestPlanesAreOnlyMutatedThroughMutateBitmaps(t *testing.T) {
	fset, files := parsePackageSources(t)

	for _, v := range findPlaneViolations(fset, files) {
		t.Errorf("%s %s at %s: mutate the planes through (*SegmentInMemory).mutateBitmaps, "+
			"which bumps the generation that invalidates the leaf cache. If this access is "+
			"genuinely read-only, add it to planeAccessAllowed with a reason.",
			v.function, v.kind, v.position)
	}
}

// TestPlaneGuardDetectsAnUnguardedWriter proves the guard above is not vacuous:
// against a package that does have a rogue writer, it fires.
func TestPlaneGuardDetectsAnUnguardedWriter(t *testing.T) {
	const rogue = `package roaringsetrange

func (s *SegmentInMemory) mergeSomethingNew(deletions *sroar.Bitmap) {
	s.bitmapsLock.Lock()
	defer s.bitmapsLock.Unlock()

	for key := range s.bitmaps {
		s.bitmaps[key].AndNotConc(deletions, 1)
	}
}

// a reader holding its own snapshot is not a hazard and must not be flagged
func (r *segmentInMemoryReader) readSomethingNew() int {
	return r.bitmaps[0].LenInBytes()
}
`

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "rogue.go", rogue, 0)
	require.NoError(t, err)

	violations := findPlaneViolations(fset, []*ast.File{file})
	require.NotEmpty(t, violations, "the guard would have missed an unguarded writer")

	kinds := map[string]bool{}
	for _, v := range violations {
		assert.Equal(t, "mergeSomethingNew", v.function,
			"the reader's own snapshot must not be flagged")
		kinds[v.kind] = true
	}
	assert.True(t, kinds["names s.bitmaps"])
	assert.True(t, kinds["takes bitmapsLock for writing"])
}

func TestMutateBitmapsBumpsTheGeneration(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("on a plain call", func(t *testing.T) {
		s := NewSegmentInMemory(logger)
		before := s.generation

		s.mutateBitmaps(func(bitmaps *rangeBitmaps) { bitmaps[0].Set(1) })

		assert.Equal(t, before+1, s.generation)
	})

	t.Run("on an early return", func(t *testing.T) {
		s := NewSegmentInMemory(logger)
		before := s.generation

		s.mutateBitmaps(func(bitmaps *rangeBitmaps) {
			if bitmaps[0] != nil {
				bitmaps[0].Set(1)
				return
			}
			t.Fatal("planes must be initialised")
		})

		assert.Equal(t, before+1, s.generation)
	})

	t.Run("on a panic", func(t *testing.T) {
		s := NewSegmentInMemory(logger)
		before := s.generation

		require.Panics(t, func() {
			s.mutateBitmaps(func(bitmaps *rangeBitmaps) {
				bitmaps[0].Set(1)
				panic("boom")
			})
		})

		assert.Equal(t, before+1, s.generation, "a panicking writer must still invalidate")
	})

	t.Run("holds the write lock while fn runs", func(t *testing.T) {
		s := NewSegmentInMemory(logger)

		var inside sync.WaitGroup
		inside.Add(1)
		s.mutateBitmaps(func(bitmaps *rangeBitmaps) {
			defer inside.Done()
			assert.False(t, s.bitmapsLock.TryRLock(), "readers must be excluded")
		})
		inside.Wait()

		require.True(t, s.bitmapsLock.TryRLock())
		s.bitmapsLock.RUnlock()
	})

	t.Run("the mutation is visible to a later reader", func(t *testing.T) {
		s := newCachedSegment(logger, 1<<20)

		mt := NewMemtable(logger)
		mt.Insert(20, []uint64{200})
		require.NoError(t, s.MergeSegmentByCursor(newFakeSegmentCursor(mt)))
		for round := 0; round < 3; round++ {
			query(t, s, 15, filters.OperatorGreaterThanEqual)
		}
		require.NotZero(t, cachedEntries(s))

		s.mutateBitmaps(func(bitmaps *rangeBitmaps) {
			bitmaps[0].Set(300)
			bitmaps[5].Set(300) // value 16, above the 15 threshold
		})

		assert.ElementsMatch(t, []uint64{200, 300},
			query(t, s, 15, filters.OperatorGreaterThanEqual))
	})
}
