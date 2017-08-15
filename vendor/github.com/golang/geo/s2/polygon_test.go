/*
Copyright 2015 Google Inc. All rights reserved.

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

import "testing"

const (
	// A set of nested loops around the LatLng point 0:0.
	// Every vertex of nearLoop0 is also a vertex of nearLoop1.
	nearPoint    = "0:0"
	nearLoop0    = "-1:0, 0:1, 1:0, 0:-1;"
	nearLoop1    = "-1:-1, -1:0, -1:1, 0:1, 1:1, 1:0, 1:-1, 0:-1;"
	nearLoop2    = "-1:-2, -2:5, 5:-2;"
	nearLoop3    = "-2:-2, -3:6, 6:-3;"
	nearLoopHemi = "0:-90, -90:0, 0:90, 90:0;"

	// A set of nested loops around the LatLng point 0:180. Every vertex of
	// farLoop0 and farLoop2 belongs to farLoop1, and all the loops except
	// farLoop2 are non-convex.
	farPoint    = "0:180"
	farLoop0    = "0:179, 1:180, 0:-179, 2:-180;"
	farLoop1    = "0:179, -1:179, 1:180, -1:-179, 0:-179, 3:-178, 2:-180, 3:178;"
	farLoop2    = "3:-178, 3:178, -1:179, -1:-179;"
	farLoop3    = "-3:-178, 4:-177, 4:177, -3:178, -2:179;"
	farLoopHemi = "0:-90, 60:90, -60:90;"

	// A set of nested loops around the LatLng point -90:0.
	southLoopPoint = "-89.9999:0.001"
	southLoop0a    = "-90:0, -89.99:0.01, -89.99:0;"
	southLoop0b    = "-90:0, -89.99:0.03, -89.99:0.02;"
	southLoop0c    = "-90:0, -89.99:0.05, -89.99:0.04;"
	southLoop1     = "-90:0, -89.9:0.1, -89.9:-0.1;"
	southLoop2     = "-90:0, -89.8:0.2, -89.8:-0.2;"
	southLoopHemi  = "0:-180, 0:60, 0:-60;"

	// Two different loops that surround all the near and far loops except
	// for the hemispheres.
	nearFarLoop1 = "-1:-9, -9:-9, -9:9, 9:9, 9:-9, 1:-9, " +
		"1:-175, 9:-175, 9:175, -9:175, -9:-175, -1:-175;"
	nearFarLoop2 = "-2:15, -2:170, -8:-175, 8:-175, " +
		"2:170, 2:15, 8:-4, -8:-4;"

	// Loop that results from intersection of other loops.
	farHemiSouthHemiLoop = "0:-180, 0:90, -60:90, 0:-90;"

	// Rectangles that form a cross, with only shared vertices, no crossing edges.
	// Optional holes outside the intersecting region. 1 is the horizontal rectangle,
	// and 2 is the vertical. The intersections are shared vertices.
	//       x---x
	//       | 2 |
	//   +---*---*---+
	//   | 1 |1+2| 1 |
	//   +---*---*---+
	//       | 2 |
	//       x---x
	loopCross1          = "-2:1, -1:1, 1:1, 2:1, 2:-1, 1:-1, -1:-1, -2:-1;"
	loopCross1SideHole  = "-1.5:0.5, -1.2:0.5, -1.2:-0.5, -1.5:-0.5;"
	loopCrossCenterHole = "-0.5:0.5, 0.5:0.5, 0.5:-0.5, -0.5:-0.5;"
	loopCross2SideHole  = "0.5:-1.5, 0.5:-1.2, -0.5:-1.2, -0.5:-1.5;"
	loopCross2          = "1:-2, 1:-1, 1:1, 1:2, -1:2, -1:1, -1:-1, -1:-2;"

	// Two rectangles that intersect, but no edges cross and there's always
	// local containment (rather than crossing) at each shared vertex.
	// In this ugly ASCII art, 1 is A+B, 2 is B+C:
	//   +---+---+---+
	//   | A | B | C |
	//   +---+---+---+
	loopOverlap1          = "0:1, 1:1, 2:1, 2:0, 1:0, 0:0;"
	loopOverlap1SideHole  = "0.2:0.8, 0.8:0.8, 0.8:0.2, 0.2:0.2;"
	loopOverlapCenterHole = "1.2:0.8, 1.8:0.8, 1.8:0.2, 1.2:0.2;"
	loopOverlap2SideHole  = "2.2:0.8, 2.8:0.8, 2.8:0.2, 2.2:0.2;"
	loopOverlap2          = "1:1, 2:1, 3:1, 3:0, 2:0, 1:0;"

	// By symmetry, the intersection of the two polygons has almost half the area
	// of either polygon.
	//   +---+
	//   | 3 |
	//   +---+---+
	//   |3+4| 4 |
	//   +---+---+
	loopOverlap3 = "-10:10, 0:10, 0:-10, -10:-10, -10:0"
	loopOverlap4 = "-10:0, 10:0, 10:-10, -10:-10"
)

// Some shared polygons used in the tests.
var (
	emptyPolygon = &Polygon{}
	fullPolygon  = FullPolygon()

	// TODO(roberts): Uncomment once Polygons with multiple loops are supported.
	/*
		near0Polygon     = makePolygon(nearLoop0, true)
		near01Polygon    = makePolygon(nearLoop0+nearLoop1, true)
		near30Polygon    = makePolygon(nearLoop3+nearLoop0, true)
		near23Polygon    = makePolygon(nearLoop2+nearLoop3, true)
		near0231Polygon  = makePolygon(nearLoop0+nearLoop2+nearLoop3+nearLoop1, true)
		near023H1Polygon = makePolygon(nearLoop0+nearLoop2+nearLoop3+nearLoopHemi+nearLoop1, true)

		far01Polygon    = makePolygon(farLoop0+farLoop1, true)
		far21Polygon    = makePolygon(farLoop2+farLoop1, true)
		far231Polygon   = makePolygon(farLoop2+farLoop3+farLoop1, true)
		far2H0Polygon   = makePolygon(farLoop2+farLoopHemi+farLoop0, true)
		far2H013Polygon = makePolygon(farLoop2+farLoopHemi+farLoop0+farLoop1+farLoop3, true)

		south0abPolygon     = makePolygon(southLoop0a+southLoop0b, true)
		south2Polygon       = makePolygon(southLoop2, true)
		south20b1Polygon    = makePolygon(southLoop2+southLoop0b+southLoop1, true)
		south2H1Polygon     = makePolygon(southLoop2+southLoopHemi+southLoop1, true)
		south20bH0acPolygon = makePolygon(southLoop2+southLoop0b+southLoopHemi+
			southLoop0a+southLoop0c, true)

		nf1N10F2S10abcPolygon = makePolygon(southLoop0c+farLoop2+nearLoop1+
			nearFarLoop1+nearLoop0+southLoop1+southLoop0b+southLoop0a, true)

		nf2N2F210S210abPolygon = makePolygon(farLoop2+southLoop0a+farLoop1+
			southLoop1+farLoop0+southLoop0b+nearFarLoop2+southLoop2+nearLoop2, true)

		f32n0Polygon  = makePolygon(farLoop2+nearLoop0+farLoop3, true)
		n32s0bPolygon = makePolygon(nearLoop3+southLoop0b+nearLoop2, true)

		cross1Polygon           = makePolygon(loopCross1, true)
		cross1SideHolePolygon   = makePolygon(loopCross1+loopCross1SideHole, true)
		cross1CenterHolePolygon = makePolygon(loopCross1+loopCrossCenterHole, true)
		cross2Polygon           = makePolygon(loopCross2, true)
		cross2SideHolePolygon   = makePolygon(loopCross2+loopCross2SideHole, true)
		cross2CenterHolePolygon = makePolygon(loopCross2+loopCrossCenterHole, true)

		overlap1Polygon           = makePolygon(loopOverlap1, true)
		overlap1SideHolePolygon   = makePolygon(loopOverlap1+loopOverlap1SideHole, true)
		overlap1CenterHolePolygon = makePolygon(loopOverlap1+loopOverlapCenterHole, true)
		overlap2Polygon           = makePolygon(loopOverlap2, true)
		overlap2SideHolePolygon   = makePolygon(loopOverlap2+loopOverlap2SideHole, true)
		overlap2CenterHolePolygon = makePolygon(loopOverlap2+loopOverlapCenterHole, true)

		overlap3Polygon = makePolygon(loopOverlap3, true)
		overlap4Polygon = makePolygon(loopOverlap4, true)

		farHemiPolygon      = makePolygon(farLoopHemi, true)
		southHemiPolygon    = makePolygon(southLoopHemi, true)
		farSouthHemiPolygon = makePolygon(farHemiSouthHemiLoop, true)
	*/
)

func TestPolygonEmptyAndFull(t *testing.T) {
	if !emptyPolygon.IsEmpty() {
		t.Errorf("empty polygon should be empty")
	}
	if emptyPolygon.IsFull() {
		t.Errorf("empty polygon should not be full")
	}

	if emptyPolygon.ContainsOrigin() {
		t.Errorf("emptyPolygon.ContainsOrigin() = true, want false")
	}
	if got, want := emptyPolygon.NumEdges(), 0; got != want {
		t.Errorf("emptyPolygon.NumEdges() = %v, want %v", got, want)
	}

	if got := emptyPolygon.dimension(); got != polygonGeometry {
		t.Errorf("emptyPolygon.dimension() = %v, want %v", got, polygonGeometry)
	}
	if got, want := emptyPolygon.NumChains(), 0; got != want {
		t.Errorf("emptyPolygon.NumChains() = %v, want %v", got, want)
	}

	if fullPolygon.IsEmpty() {
		t.Errorf("full polygon should not be emtpy")
	}
	if !fullPolygon.IsFull() {
		t.Errorf("full polygon should be full")
	}

	if !fullPolygon.ContainsOrigin() {
		t.Errorf("fullPolygon.ContainsOrigin() = false, want true")
	}
	if got, want := fullPolygon.NumEdges(), 0; got != want {
		t.Errorf("fullPolygon.NumEdges() = %v, want %v", got, want)
	}

	if got := fullPolygon.dimension(); got != polygonGeometry {
		t.Errorf("emptyPolygon.dimension() = %v, want %v", got, polygonGeometry)
	}
	if got, want := fullPolygon.NumChains(), 0; got != want {
		t.Errorf("emptyPolygon.NumChains() = %v, want %v", got, want)
	}
}

func TestPolygonShape(t *testing.T) {
	p := makePolygon("0:0, 1:0, 1:1, 2:1", true)
	shape := Shape(p)

	if got, want := shape.NumEdges(), 4; got != want {
		t.Errorf("%v.NumEdges() = %v, want %d", shape, got, want)
	}

	if p.numVertices != shape.NumEdges() {
		t.Errorf("the number of vertices in a polygon should equal the number of edges")
	}
	if p.NumLoops() != shape.NumChains() {
		t.Errorf("the number of loops in a polygon should equal the number of chains")
	}

	edgeID := 0
	for i, l := range p.loops {
		if edgeID != shape.Chain(i).Start {
			t.Errorf("the edge id of the start of loop(%d) should equal the sum of vertices so far in the polygon. got %d, want %d", i, shape.Chain(i).Start, edgeID)
		}
		if len(l.vertices) != shape.Chain(i).Length {
			t.Errorf("the length of Chain(%d) should equal the length of loop(%d), got %v, want %v", i, i, shape.Chain(i).Length, len(l.vertices))
		}
		for j := 0; j < len(l.Vertices()); j++ {
			edge := shape.Edge(edgeID)
			if l.OrientedVertex(j) != edge.V0 {
				t.Errorf("l.OrientedVertex(%d) = %v, want %v", j, l.OrientedVertex(j), edge.V0)
			}
			if l.OrientedVertex(j+1) != edge.V1 {
				t.Errorf("l.OrientedVertex(%d) = %v, want %v", j+1, l.OrientedVertex(j+1), edge.V1)
			}
			edgeID++
		}
	}
	if shape.dimension() != polygonGeometry {
		t.Errorf("polygon.dimension() = %v, want %v", shape.dimension(), polygonGeometry)
	}
	if !shape.HasInterior() {
		t.Errorf("polygons should always have interiors")
	}
	if !shape.ContainsOrigin() {
		t.Errorf("polygon %v should contain the origin", shape)
	}

	if got, want := p.ContainsPoint(OriginPoint()), shape.ContainsOrigin(); got != want {
		t.Errorf("p.ContainsPoint(OriginPoint()) != shape.ContainsOrigin()")
	}
}

func TestPolygonLoop(t *testing.T) {
	if fullPolygon.NumLoops() != 1 {
		t.Errorf("full polygon should have one loop")
	}

	l := &Loop{}
	p1 := PolygonFromLoops([]*Loop{l})
	if p1.NumLoops() != 1 {
		t.Errorf("polygon with one loop should have one loop")
	}
	if p1.Loop(0) != l {
		t.Errorf("polygon with one loop should return it")
	}

	// TODO: When multiple loops are supported, add more test cases.
}

func TestPolygonParent(t *testing.T) {
	p1 := PolygonFromLoops([]*Loop{&Loop{}})
	tests := []struct {
		p    *Polygon
		have int
		want int
		ok   bool
	}{
		{fullPolygon, 0, -1, false},
		{p1, 0, -1, false},

		// TODO: When multiple loops are supported, add more test cases to
		// more fully show the parent levels.
	}

	for _, test := range tests {
		if got, ok := test.p.Parent(test.have); ok != test.ok || got != test.want {
			t.Errorf("%v.Parent(%d) = %d,%v, want %d,%v", test.p, test.have, got, ok, test.want, test.ok)
		}
	}
}

func TestPolygonLastDescendant(t *testing.T) {
	p1 := PolygonFromLoops([]*Loop{&Loop{}})

	tests := []struct {
		p    *Polygon
		have int
		want int
	}{
		{fullPolygon, 0, 0},
		{fullPolygon, -1, 0},

		{p1, 0, 0},
		{p1, -1, 0},

		// TODO: When multiple loops are supported, add more test cases.
	}

	for _, test := range tests {
		if got := test.p.LastDescendant(test.have); got != test.want {
			t.Errorf("%v.LastDescendant(%d) = %d, want %d", test.p, test.have, got, test.want)
		}
	}
}

func TestPolygonLoopIsHoleAndLoopSign(t *testing.T) {
	if fullPolygon.loopIsHole(0) {
		t.Errorf("the full polygons only loop should not be a hole")
	}
	if fullPolygon.loopSign(0) != 1 {
		t.Errorf("the full polygons only loop should be postitive")
	}

	loop := LoopFromPoints(parsePoints("30:20, 40:20, 39:43, 33:35"))
	p := PolygonFromLoops([]*Loop{loop})

	if p.loopIsHole(0) {
		t.Errorf("first loop in a polygon should not start out as a hole")
	}
	if p.loopSign(0) != 1 {
		t.Errorf("first loop in a polygon should start out as positive")
	}

	// TODO: When multiple loops are supported, add more test cases to
	// more fully show the parent levels.
}

func TestPolygonContainsPoint(t *testing.T) {
	tests := []struct {
		polygon string
		point   string
	}{
		{nearLoop0, nearPoint},
		{nearLoop1, nearPoint},
		{nearLoop2, nearPoint},
		{nearLoop3, nearPoint},
		{nearLoopHemi, nearPoint},
		{southLoop0a, southLoopPoint},
		{southLoop1, southLoopPoint},
		{southLoop2, southLoopPoint},
		{southLoopHemi, southLoopPoint},
	}

	for _, test := range tests {
		poly := makePolygon(test.polygon, true)
		pt := parsePoint(test.point)
		if !poly.ContainsPoint(pt) {
			t.Errorf("%v.ContainsPoint(%v) = false, want true", test.polygon, test.point)
		}
	}
}

// TODO(roberts): Remaining Tests
// TestInit
// TestMultipleInit
// TestInitSingleLoop
// TestCellConstructorAndContains
// TestUninitializedIsValid
// TestOverlapFractions
// TestOriginNearPole
// TestTestApproxContainsAndDisjoint
// TestRelations
// TestOneLoopPolygonShape
// TestSeveralLoopPolygonShape
// TestManyLoopPolygonShape
// TestPointInBigLoop
// TestOperations
// TestIntersectionSnapFunction
// TestIntersectionPreservesLoopOrder
// TestLoopPointers
// TestBug1 - Bug14
// TestPolylineIntersection
// TestSplitting
// TestInitToCellUnionBorder
// TestUnionWithAmbgiuousCrossings
// TestInitToSloppySupportsEmptyPolygons
// TestInitToSnappedDoesNotRotateVertices
// TestInitToSnappedWithSnapLevel
// TestInitToSnappedIsValid_A
// TestInitToSnappedIsValid_B
// TestInitToSnappedIsValid_C
// TestInitToSnappedIsValid_D
// TestProject
// TestDistance
//
// IsValidTests
//   TestUnitLength
//   TestVertexCount
//   TestDuplicateVertex
//   TestSelfIntersection
//   TestEmptyLoop
//   TestFullLoop
//   TestLoopsCrossing
//   TestDuplicateEdge
//   TestInconsistentOrientations
//   TestLoopDepthNegative
//   TestLoopNestingInvalid
//   TestFuzzTest
//
// PolygonSimplifier
//   TestNoSimplification
//   TestSimplifiedLoopSelfIntersects
//   TestNoSimplificationManyLoops
//   TestTinyLoopDisappears
//   TestStraightLinesAreSimplified
//   TestEdgeSplitInManyPieces
//   TestEdgesOverlap
//   TestLargeRegularPolygon
//
// InitToSimplifiedInCell
//   TestPointsOnCellBoundaryKept
//   TestPointsInsideCellSimplified
//   TestCellCornerKept
//   TestNarrowStripRemoved
//   TestNarrowGapRemoved
//   TestCloselySpacedEdgeVerticesKept
//   TestPolylineAssemblyBug
