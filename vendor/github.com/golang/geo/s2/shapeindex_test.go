/*
Copyright 2016 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package s2

import (
	"testing"

	"github.com/golang/geo/r3"
	"github.com/golang/geo/s1"
)

// TODO(roberts): This can be removed once s2shapeutil_test is added which has
// various Shape types and tests that validate the Shape interface.

// testShape is a minimal implementation of the Shape interface for use in testing
// until such time as there are other s2 types that implement it.
type testShape struct {
	a, b  Point
	edges int
}

func newTestShape() *testShape                              { return &testShape{} }
func (s *testShape) NumEdges() int                          { return s.edges }
func (s *testShape) Edge(id int) Edge                       { return Edge{s.a, s.b} }
func (s *testShape) HasInterior() bool                      { return false }
func (s *testShape) ContainsOrigin() bool                   { return false }
func (s *testShape) NumChains() int                         { return 0 }
func (s *testShape) Chain(chainID int) Chain                { return Chain{0, s.NumEdges()} }
func (s *testShape) ChainEdge(chainID, offset int) Edge     { return Edge{s.a, s.b} }
func (s *testShape) ChainPosition(edgeID int) ChainPosition { return ChainPosition{0, edgeID} }
func (s *testShape) dimension() dimension                   { return pointGeometry }

func TestShapeIndexBasics(t *testing.T) {
	index := NewShapeIndex()
	s := newTestShape()

	if index.Len() != 0 {
		t.Errorf("initial index should be empty after creation")
	}
	index.Add(s)

	if index.Len() == 0 {
		t.Errorf("index should not be empty after adding shape")
	}

	index.Reset()
	if index.Len() != 0 {
		t.Errorf("index should be empty after reset, got %v %+v", index.Len(), index)
	}
}

func TestShapeEdgeComparisons(t *testing.T) {
	tests := []struct {
		a, b Edge
		want int
	}{
		{
			// a.V0 < b.V0
			a:    Edge{PointFromCoords(-1, 0, 0), PointFromCoords(0, 0, 0)},
			b:    Edge{PointFromCoords(0, 0, 0), PointFromCoords(0, 0, 0)},
			want: -1,
		},
		{
			// a.V0 = b.V0
			a:    Edge{PointFromCoords(0, 2, 0), PointFromCoords(0, 0, 5)},
			b:    Edge{PointFromCoords(0, 2, 0), PointFromCoords(0, 0, 5)},
			want: 0,
		},
		{
			// a.V0 > b.V0
			a:    Edge{PointFromCoords(1, 0, 0), PointFromCoords(-6, 7, 8)},
			b:    Edge{PointFromCoords(0, 0, 0), PointFromCoords(1, 3, 5)},
			want: 1,
		},
		{
			// a.V0 = b.V0 && a.V1 < b.V1
			a:    Edge{PointFromCoords(5, -2, -0.4), PointFromCoords(-1, 0, 0)},
			b:    Edge{PointFromCoords(5, -2, -0.4), PointFromCoords(0, -1, -1)},
			want: -1,
		},
		{
			// a.V0 = b.V0 && a.V1 = b.V1
			a:    Edge{PointFromCoords(9, 8, 7), PointFromCoords(12, 3, -4)},
			b:    Edge{PointFromCoords(9, 8, 7), PointFromCoords(12, 3, -4)},
			want: 0,
		},
		{
			// a.V0 = b.V0 && a.V1 > b.V1
			a:    Edge{PointFromCoords(-11, 7.2, -4.6), PointFromCoords(0, 1, 0)},
			b:    Edge{PointFromCoords(-11, 7.2, -4.6), PointFromCoords(0, 0, 0.9)},
			want: 1,
		},
	}

	for _, test := range tests {
		if got := test.a.Cmp(test.b); got != test.want {
			t.Errorf("%v.Cmp(%v) = %v, want %v", test.a, test.b, got, test.want)
		}
	}
}

func TestShapeIndexCellBasics(t *testing.T) {
	s := &ShapeIndexCell{}

	if len(s.shapes) != 0 {
		t.Errorf("len(s.shapes) = %v, want %d", len(s.shapes), 0)
	}

	// create some clipped shapes to add.
	c1 := &clippedShape{}
	s.add(c1)

	c2 := newClippedShape(7, 1)
	s.add(c2)

	c3 := &clippedShape{}
	s.add(c3)

	// look up the element at a given index
	if got := s.shapes[1]; got != c2 {
		t.Errorf("%v.shapes[%d] = %v, want %v", s, 1, got, c2)
	}

	// look for the clipped shape that is part of the given shape.
	if got := s.findByShapeID(7); got != c2 {
		t.Errorf("%v.findByShapeID(%v) = %v, want %v", s, 7, got, c2)
	}
}

// validateEdge determines whether or not the edge defined by points A and B should be
// present in that CellID and verify that this matches hasEdge.
func validateEdge(t *testing.T, a, b Point, ci CellID, hasEdge bool) {
	// Expand or shrink the padding slightly to account for errors in the
	// function we use to test for intersection (IntersectsRect).
	padding := cellPadding
	sign := 1.0
	if !hasEdge {
		sign = -1
	}
	padding += sign * intersectsRectErrorUVDist
	bound := ci.boundUV().ExpandedByMargin(padding)
	aUV, bUV, ok := ClipToPaddedFace(a, b, ci.Face(), padding)

	if got := ok && edgeIntersectsRect(aUV, bUV, bound); got != hasEdge {
		t.Errorf("edgeIntersectsRect(%v, %v, %v) = %v && clip = %v, want %v", aUV, bUV, bound, edgeIntersectsRect(aUV, bUV, bound), ok, hasEdge)
	}
}

// validateInterior tests if the given Shape contains the center of the given CellID,
// and that this matches the expected value of indexContainsCenter.
func validateInterior(t *testing.T, shape Shape, ci CellID, indexContainsCenter bool) {
	if shape == nil || !shape.HasInterior() {
		if indexContainsCenter {
			t.Errorf("%v was nil or does not have an interior, but should have", shape)
		}
		return
	}

	crosser := NewEdgeCrosser(OriginPoint(), ci.Point())
	containsCenter := shape.ContainsOrigin()
	for e := 0; e < shape.NumEdges(); e++ {
		edge := shape.Edge(e)
		containsCenter = containsCenter != crosser.EdgeOrVertexCrossing(edge.V0, edge.V1)
	}

	if containsCenter != indexContainsCenter {
		t.Errorf("validating interior of shape containsCenter = %v, want %v", containsCenter, indexContainsCenter)
	}
}

// quadraticValidate verifies that that every cell of the index contains the correct
// edges, and that no cells are missing from the index.  The running time of this
// function is quadratic in the number of edges.
func quadraticValidate(t *testing.T, index *ShapeIndex) {
	// Iterate through a sequence of nonoverlapping cell ids that cover the
	// sphere and include as a subset all the cell ids used in the index.  For
	// each cell id, verify that the expected set of edges is present.
	// "minCellID" is the first CellID that has not been validated yet.
	minCellID := CellIDFromFace(0).ChildBeginAtLevel(maxLevel)
	for it := index.Iterator(); ; it.Next() {
		// Generate a list of CellIDs ("skipped cells") that cover the gap
		// between the last cell we validated and the next cell in the index.
		var skipped CellUnion
		if !it.Done() {
			cellID := it.CellID()
			if cellID < minCellID {
				t.Errorf("cell ID below min")
			}
			skipped = CellUnionFromRange(minCellID, cellID.RangeMin())
			minCellID = cellID.RangeMax().Next()
		} else {
			// Validate the empty cells beyond the last cell in the index.
			skipped = CellUnionFromRange(minCellID,
				CellIDFromFace(5).ChildEndAtLevel(maxLevel))
		}

		// Iterate through all the shapes, simultaneously validating the current
		// index cell and all the skipped cells.
		shortEdges := 0 // number of edges counted toward subdivision
		for id, shape := range index.shapes {
			for j := 0; j < len(skipped); j++ {
				validateInterior(t, shape, skipped[j], false)
			}

			// First check that containsCenter() is set correctly.
			var clipped *clippedShape
			if !it.Done() {
				clipped = it.IndexCell().findByShapeID(id)
				containsCenter := clipped != nil && clipped.containsCenter
				validateInterior(t, shape, it.CellID(), containsCenter)
			}
			// If this shape has been removed, it should not be present at all.
			if shape == nil {
				if clipped != nil {
					t.Errorf("clipped should be nil when shape is nil")
				}
				continue
			}

			// Otherwise check that the appropriate edges are present.
			for e := 0; e < shape.NumEdges(); e++ {
				edge := shape.Edge(e)
				for j := 0; j < len(skipped); j++ {
					validateEdge(t, edge.V0, edge.V1, skipped[j], false)
				}
				if !it.Done() {
					hasEdge := clipped != nil && clipped.containsEdge(e)
					validateEdge(t, edge.V0, edge.V1, it.CellID(), hasEdge)
					if hasEdge && it.CellID().Level() < maxLevelForEdge(edge) {
						shortEdges++
					}
				}
			}
		}

		if shortEdges > index.maxEdgesPerCell {
			t.Errorf("too many edges")
		}

		if it.Done() {
			break
		}
	}
}

func testIteratorMethods(t *testing.T, index *ShapeIndex) {
	it := index.Iterator()
	if !it.AtBegin() {
		t.Errorf("new iterator should start positioned at beginning")
	}

	it = index.End()
	if !it.Done() {
		t.Errorf("iterator positioned at end should report as done")
	}

	var ids []CellID
	// minCellID is the first CellID in a complete traversal.
	minCellID := CellIDFromFace(0).ChildBeginAtLevel(maxLevel)

	for it.Reset(); !it.Done(); it.Next() {
		// Get the next cell in the iterator.
		ci := it.CellID()
		skipped := CellUnionFromRange(minCellID, ci.RangeMin())

		it2 := index.Iterator()
		for i := 0; i < len(skipped); i++ {
			if it2.LocatePoint(skipped[i].Point()) {
				t.Errorf("iterator should not have been able to find the cell %v wihich was not in the index", skipped[i].Point())
			}

			if got := it2.LocateCellID(skipped[i]); got != Disjoint {
				t.Errorf("CellID location should be Disjoint for non-existent entry, got %v", got)
			}
		}

		if len(ids) != 0 {
			if it.AtBegin() {
				t.Errorf("an iterator from a non-empty set of cells should not be positioned at the beginning")
			}

			it2 = index.Iterator()
			it2.Prev()
			// jump back one spot.
			if ids[len(ids)-1] != it2.CellID() {
				t.Errorf("ShapeIndexIterator should be positioned at the beginning and not equal to last entry")
			}

			it2.Next()
			if ci != it2.CellID() {
				t.Errorf("advancing one spot should put us at the end")
			}

			it2.seek(ids[len(ids)-1])
			if ids[len(ids)-1] != it2.CellID() {
				t.Errorf("seek from beginning for the first entry (%v) should not put us at the end %v", ids[len(ids)-1], it.CellID())
			}

			it2.seekForward(ci)
			if ci != it2.CellID() {
				t.Errorf("%v.seekForward(%v) = %v, want %v", it2, ci, it2.CellID(), ci)
			}

			it2.seekForward(ids[len(ids)-1])
			if ci != it2.CellID() {
				t.Errorf("%v.seekForward(%v) (to last entry) = %v, want %v", it2, ids[len(ids)-1], it2.CellID(), ci)
			}
		}

		it2.Reset()
		if ci.Point() != it.Center() {
			t.Errorf("point at center of current position should equal center of the crrent CellID")
		}

		if !it2.LocatePoint(it.Center()) {
			t.Fatalf("it.LocatePoint(it.Center()) should have been able to locate the point it is currently at")
		}

		if ci != it2.CellID() {
			t.Errorf("CellID of the Point we just located should be equal. got %v, want %v", it2.CellID(), ci)
		}

		it2.Reset()
		if got := it2.LocateCellID(ci); got != Indexed {
			t.Errorf("it.LocateCellID(%v) = %v, want %v", ci, got, Indexed)
		}

		if ci != it2.CellID() {
			t.Errorf("CellID of the CellID we just located should match. got %v, want %v", it2.CellID(), ci)
		}

		if !ci.isFace() {
			it2.Reset()
			if got := it2.LocateCellID(ci.immediateParent()); Subdivided != got {
				t.Errorf("it2.LocateCellID(%v) = %v, want %v", ci.immediateParent(), got, Subdivided)
			}

			if it2.CellID() > ci {
				t.Errorf("CellID of the immediate parent should be above the current cell, got %v, want %v", it2.CellID(), ci)
			}

			if it2.CellID() < ci.immediateParent().RangeMin() {
				t.Errorf("CellID of the current position should fall below the RangeMin of the parent. got %v, want %v", it2.CellID(), ci.immediateParent().RangeMin())
			}
		}

		if !ci.IsLeaf() {
			for i := 0; i < 4; i++ {
				it2.Reset()
				if got, want := it2.LocateCellID(ci.Children()[i]), Indexed; got != want {
					t.Errorf("it2.LocateCellID(%v.Children[%d]) = %v, want %v", ci, i, got, want)
				}

				if ci != it2.CellID() {
					t.Errorf("it2.CellID() = %v, want %v", it2.CellID(), ci)
				}
			}
		}
		// Add this cellID to the set of cells to examine.
		ids = append(ids, ci)
		// Move the minimal CellID to the next CellID past our current position.
		minCellID = ci.RangeMax().Next()
	}
}

func TestShapeIndexNoEdges(t *testing.T) {
	index := NewShapeIndex()
	iter := index.Iterator()

	if !iter.Done() {
		t.Errorf("iterator for empty index should start at Done but did not")
	}
	testIteratorMethods(t, index)
}

func TestShapeIndexOneEdge(t *testing.T) {
	index := NewShapeIndex()
	e := edgeVectorShapeFromPoints(PointFromCoords(1, 0, 0), PointFromCoords(0, 1, 0))
	index.Add(e)
	quadraticValidate(t, index)
	testIteratorMethods(t, index)
}

func TestShapeIndexManyIdenticalEdges(t *testing.T) {
	const numEdges = 100
	a := PointFromCoords(0.99, 0.99, 1)
	b := PointFromCoords(-0.99, -0.99, 1)

	index := NewShapeIndex()
	for i := 0; i < numEdges; i++ {
		index.Add(edgeVectorShapeFromPoints(a, b))
	}
	quadraticValidate(t, index)
	testIteratorMethods(t, index)

	// Since all edges span the diagonal of a face, no subdivision should
	// have occurred (with the default index options).
	for it := index.Iterator(); !it.Done(); it.Next() {
		if it.CellID().Level() != 0 {
			t.Errorf("it.CellID.Level() = %v, want nonzero", it.CellID().Level())
		}
	}
}

func TestShapeIndexManyTinyEdges(t *testing.T) {
	// This test adds many edges to a single leaf cell, to check that
	// subdivision stops when no further subdivision is possible.

	// Construct two points in the same leaf cell.
	a := cellIDFromPoint(PointFromCoords(1, 0, 0)).Point()
	b := Point{a.Add(r3.Vector{0, 1e-12, 0}).Normalize()}
	shape := &edgeVectorShape{}
	for i := 0; i < 100; i++ {
		shape.Add(a, b)
	}

	index := NewShapeIndex()
	index.Add(shape)
	quadraticValidate(t, index)

	// Check that there is exactly one index cell and that it is a leaf cell.
	it := index.Iterator()
	if it.Done() {
		t.Errorf("ShapeIndexIterator should not be positioned at the end for %v", index)
		return
	}
	if !(it.CellID().IsLeaf()) {
		t.Errorf("there should be only one leaf cell in the index but it.CellID().IsLeaf() returned false")
	}
	it.Next()
	if !(it.Done()) {
		t.Errorf("ShapeIndexIterator should be positioned at the end now since there should have been only one element")
	}
}

func TestShapeIndexShrinkToFitOptimization(t *testing.T) {
	// This used to trigger a bug in the ShrinkToFit optimization. The loop
	// below contains almost all of face 0 except for a small region in the
	// 0/00000 subcell. That subcell is the only one that contains any edges.
	// This caused the index to be built only in that subcell. However, all the
	// other cells on that face should also have index entries, in order to
	// indicate that they are contained by the loop.
	loop := RegularLoop(PointFromCoords(1, 0.5, 0.5), s1.Degree*89, 100)
	index := NewShapeIndex()
	index.Add(loop)
	quadraticValidate(t, index)
}

// TODO(roberts): Differences from C++:
// TestShapeIndexLoopSpanningThreeFaces(t *testing.T) {}
// TestShapeIndexSimpleUpdates(t *testing.T) {}
// TestShapeIndexRandomUpdates(t *testing.T) {}
// TestShapeIndexContainingShapes(t *testing.T) {}
// TestShapeIndexMixedGeometry(t *testing.T) {}
// TestShapeIndexHasCrossing(t *testing.T) {}
