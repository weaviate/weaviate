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
	"math"
	"testing"

	"github.com/golang/geo/r1"
	"github.com/golang/geo/r2"
)

func TestPaddedCellMethods(t *testing.T) {
	// Test the PaddedCell methods that have approximate Cell equivalents.
	for i := 0; i < 1000; i++ {
		cid := randomCellID()
		padding := math.Pow(1e-15, randomFloat64())
		cell := CellFromCellID(cid)
		pCell := PaddedCellFromCellID(cid, padding)

		if cell.id != pCell.id {
			t.Errorf("%v.id = %v, want %v", pCell, pCell.id, cell.id)
		}
		if cell.id.Level() != pCell.Level() {
			t.Errorf("%v.Level() = %v, want %v", pCell, pCell.Level(), cell.id.Level())
		}

		if padding != pCell.Padding() {
			t.Errorf("%v.Padding() = %v, want %v", pCell, pCell.Padding(), padding)
		}

		if got, want := pCell.Bound(), cell.BoundUV().ExpandedByMargin(padding); got != want {
			t.Errorf("%v.BoundUV() = %v, want %v", pCell, got, want)
		}

		r := r2.RectFromPoints(cell.id.centerUV()).ExpandedByMargin(padding)
		if r != pCell.Middle() {
			t.Errorf("%v.Middle() = %v, want %v", pCell, pCell.Middle(), r)
		}

		if cell.id.Point() != pCell.Center() {
			t.Errorf("%v.Center() = %v, want %v", pCell, pCell.Center(), cell.id.Point())
		}
		if cid.IsLeaf() {
			continue
		}

		children, ok := cell.Children()
		if !ok {
			t.Errorf("%v.Children() failed but should not have", cell)
			continue
		}
		for pos := 0; pos < 4; pos++ {
			i, j := pCell.ChildIJ(pos)

			cellChild := children[pos]
			pCellChild := PaddedCellFromParentIJ(pCell, i, j)
			if cellChild.id != pCellChild.id {
				t.Errorf("%v.id = %v, want %v", pCellChild, pCellChild.id, cellChild.id)
			}
			if cellChild.id.Level() != pCellChild.Level() {
				t.Errorf("%v.Level() = %v, want %v", pCellChild, pCellChild.Level(), cellChild.id.Level())
			}

			if padding != pCellChild.Padding() {
				t.Errorf("%v.Padding() = %v, want %v", pCellChild, pCellChild.Padding(), padding)
			}

			if got, want := pCellChild.Bound(), cellChild.BoundUV().ExpandedByMargin(padding); got != want {
				t.Errorf("%v.BoundUV() = %v, want %v", pCellChild, got, want)
			}

			r := r2.RectFromPoints(cellChild.id.centerUV()).ExpandedByMargin(padding)
			if got := pCellChild.Middle(); !r.ApproxEquals(got) {
				t.Errorf("%v.Middle() = %v, want %v", pCellChild, got, r)
			}

			if cellChild.id.Point() != pCellChild.Center() {
				t.Errorf("%v.Center() = %v, want %v", pCellChild, pCellChild.Center(), cellChild.id.Point())
			}

		}
	}
}

func TestPaddedCellEntryExitVertices(t *testing.T) {
	for i := 0; i < 1000; i++ {
		id := randomCellID()
		unpadded := PaddedCellFromCellID(id, 0)
		padded := PaddedCellFromCellID(id, 0.5)

		// Check that entry/exit vertices do not depend on padding.
		if unpadded.EntryVertex() != padded.EntryVertex() {
			t.Errorf("entry vertex should not depend on padding; %v != %v", unpadded.EntryVertex(), padded.EntryVertex())
		}

		if unpadded.ExitVertex() != padded.ExitVertex() {
			t.Errorf("exit vertex should not depend on padding; %v != %v", unpadded.ExitVertex(), padded.ExitVertex())
		}

		// Check that the exit vertex of one cell is the same as the entry vertex
		// of the immediately following cell. This also tests wrapping from the
		// end to the start of the CellID curve with high probability.
		if got := PaddedCellFromCellID(id.NextWrap(), 0).EntryVertex(); unpadded.ExitVertex() != got {
			t.Errorf("PaddedCellFromCellID(%v.NextWrap(), 0).EntryVertex() = %v, want %v", id, got, unpadded.ExitVertex())
		}

		// Check that the entry vertex of a cell is the same as the entry vertex
		// of its first child, and similarly for the exit vertex.
		if id.IsLeaf() {
			continue
		}
		if got := PaddedCellFromCellID(id.Children()[0], 0).EntryVertex(); unpadded.EntryVertex() != got {
			t.Errorf("PaddedCellFromCellID(%v.Children()[0], 0).EntryVertex() = %v, want %v", id, got, unpadded.EntryVertex())
		}
		if got := PaddedCellFromCellID(id.Children()[3], 0).ExitVertex(); unpadded.ExitVertex() != got {
			t.Errorf("PaddedCellFromCellID(%v.Children()[3], 0).ExitVertex() = %v, want %v", id, got, unpadded.ExitVertex())
		}
	}
}

func TestPaddedCellShrinkToFit(t *testing.T) {
	for iter := 0; iter < 1000; iter++ {
		// Start with the desired result and work backwards.
		result := randomCellID()
		resultUV := result.boundUV()
		sizeUV := resultUV.Size()

		// Find the biggest rectangle that fits in "result" after padding.
		// (These calculations ignore numerical errors.)
		maxPadding := 0.5 * math.Min(sizeUV.X, sizeUV.Y)
		padding := maxPadding * randomFloat64()
		maxRect := resultUV.ExpandedByMargin(-padding)

		// Start with a random subset of the maximum rectangle.
		a := r2.Point{
			randomUniformFloat64(maxRect.X.Lo, maxRect.X.Hi),
			randomUniformFloat64(maxRect.Y.Lo, maxRect.Y.Hi),
		}
		b := r2.Point{
			randomUniformFloat64(maxRect.X.Lo, maxRect.X.Hi),
			randomUniformFloat64(maxRect.Y.Lo, maxRect.Y.Hi),
		}

		if !result.IsLeaf() {
			// If the result is not a leaf cell, we must ensure that no child of
			// result also satisfies the conditions of ShrinkToFit().  We do this
			// by ensuring that rect intersects at least two children of result
			// (after padding).
			useY := oneIn(2)
			center := result.centerUV().X
			if useY {
				center = result.centerUV().Y
			}

			// Find the range of coordinates that are shared between child cells
			// along that axis.
			shared := r1.Interval{center - padding, center + padding}
			if useY {
				shared = shared.Intersection(maxRect.Y)
			} else {
				shared = shared.Intersection(maxRect.X)
			}
			mid := randomUniformFloat64(shared.Lo, shared.Hi)

			if useY {
				a.Y = randomUniformFloat64(maxRect.Y.Lo, mid)
				b.Y = randomUniformFloat64(mid, maxRect.Y.Hi)
			} else {
				a.X = randomUniformFloat64(maxRect.X.Lo, mid)
				b.X = randomUniformFloat64(mid, maxRect.X.Hi)
			}
		}
		rect := r2.RectFromPoints(a, b)

		// Choose an arbitrary ancestor as the PaddedCell.
		initialID := result.Parent(randomUniformInt(result.Level() + 1))
		pCell := PaddedCellFromCellID(initialID, padding)
		if got := pCell.ShrinkToFit(rect); got != result {
			t.Errorf("%v.ShrinkToFit(%v) = %v, want %v", pCell, rect, got, result)
		}
	}
}
