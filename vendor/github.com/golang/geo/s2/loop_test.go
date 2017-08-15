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

import (
	"fmt"
	"math"
	"testing"

	"github.com/golang/geo/r1"
	"github.com/golang/geo/r3"
	"github.com/golang/geo/s1"
)

var (
	// The northern hemisphere, defined using two pairs of antipodal points.
	northHemi = LoopFromPoints(parsePoints("0:-180, 0:-90, 0:0, 0:90"))

	// The northern hemisphere, defined using three points 120 degrees apart.
	northHemi3 = LoopFromPoints(parsePoints("0:-180, 0:-60, 0:60"))

	// The southern hemisphere, defined using two pairs of antipodal points.
	southHemi = LoopFromPoints(parsePoints("0:90, 0:0, 0:-90, 0:-180"))

	// The western hemisphere, defined using two pairs of antipodal points.
	westHemi = LoopFromPoints(parsePoints("0:-180, -90:0, 0:0, 90:0"))

	// The eastern hemisphere, defined using two pairs of antipodal points.
	eastHemi = LoopFromPoints(parsePoints("90:0, 0:0, -90:0, 0:-180"))

	// The "near" hemisphere, defined using two pairs of antipodal points.
	nearHemi = LoopFromPoints(parsePoints("0:-90, -90:0, 0:90, 90:0"))

	// The "far" hemisphere, defined using two pairs of antipodal points.
	farHemi = LoopFromPoints(parsePoints("90:0, 0:90, -90:0, 0:-90"))

	// A spiral stripe that slightly over-wraps the equator.
	candyCane = LoopFromPoints(parsePoints("-20:150, -20:-70, 0:70, 10:-150, 10:70, -10:-70"))

	// A small clockwise loop in the northern & eastern hemisperes.
	smallNECW = LoopFromPoints(parsePoints("35:20, 45:20, 40:25"))

	// Loop around the north pole at 80 degrees.
	arctic80 = LoopFromPoints(parsePoints("80:-150, 80:-30, 80:90"))

	// Loop around the south pole at 80 degrees.
	antarctic80 = LoopFromPoints(parsePoints("-80:120, -80:0, -80:-120"))

	// A completely degenerate triangle along the equator that RobustCCW()
	// considers to be CCW.
	lineTriangle = LoopFromPoints(parsePoints("0:1, 0:2, 0:3"))

	// A nearly-degenerate CCW chevron near the equator with very long sides
	// (about 80 degrees).  Its area is less than 1e-640, which is too small
	// to represent in double precision.
	skinnyChevron = LoopFromPoints(parsePoints("0:0, -1e-320:80, 0:1e-320, 1e-320:80"))

	// A diamond-shaped loop around the point 0:180.
	loopA = LoopFromPoints(parsePoints("0:178, -1:180, 0:-179, 1:-180"))

	// Like loopA, but the vertices are at leaf cell centers.
	snappedLoopA = LoopFromPoints([]Point{
		cellIDFromPoint(parsePoint("0:178")).Point(),
		cellIDFromPoint(parsePoint("-1:180")).Point(),
		cellIDFromPoint(parsePoint("0:-179")).Point(),
		cellIDFromPoint(parsePoint("1:-180")).Point(),
	})

	// A different diamond-shaped loop around the point 0:180.
	loopB = LoopFromPoints(parsePoints("0:179, -1:180, 0:-178, 1:-180"))

	// The intersection of A and B.
	aIntersectB = LoopFromPoints(parsePoints("0:179, -1:180, 0:-179, 1:-180"))

	// The union of A and B.
	aUnionB = LoopFromPoints(parsePoints("0:178, -1:180, 0:-178, 1:-180"))

	// A minus B (concave).
	aMinusB = LoopFromPoints(parsePoints("0:178, -1:180, 0:179, 1:-180"))

	// B minus A (concave).
	bMinusA = LoopFromPoints(parsePoints("0:-179, -1:180, 0:-178, 1:-180"))

	// A shape gotten from A by adding a triangle to one edge, and
	// subtracting a triangle from the opposite edge.
	loopC = LoopFromPoints(parsePoints("0:178, 0:180, -1:180, 0:-179, 1:-179, 1:-180"))

	// A shape gotten from A by adding a triangle to one edge, and
	// adding another triangle to the opposite edge.
	loopD = LoopFromPoints(parsePoints("0:178, -1:178, -1:180, 0:-179, 1:-179, 1:-180"))

	//   3------------2
	//   |            |               ^
	//   |  7-8  b-c  |               |
	//   |  | |  | |  |      Latitude |
	//   0--6-9--a-d--1               |
	//   |  | |       |               |
	//   |  f-e       |               +----------->
	//   |            |                 Longitude
	//   4------------5
	//
	// Important: It is not okay to skip over collinear vertices when
	// defining these loops (e.g. to define loop E as "0,1,2,3") because S2
	// uses symbolic perturbations to ensure that no three vertices are
	// *ever* considered collinear (e.g., vertices 0, 6, 9 are not
	// collinear).  In other words, it is unpredictable (modulo knowing the
	// details of the symbolic perturbations) whether 0123 contains 06123
	// for example.

	// Loop E:  0,6,9,a,d,1,2,3
	// Loop F:  0,4,5,1,d,a,9,6
	// Loop G:  0,6,7,8,9,a,b,c,d,1,2,3
	// Loop H:  0,6,f,e,9,a,b,c,d,1,2,3
	// Loop I:  7,6,f,e,9,8
	loopE = LoopFromPoints(parsePoints("0:30, 0:34, 0:36, 0:39, 0:41, 0:44, 30:44, 30:30"))
	loopF = LoopFromPoints(parsePoints("0:30, -30:30, -30:44, 0:44, 0:41, 0:39, 0:36, 0:34"))
	loopG = LoopFromPoints(parsePoints("0:30, 0:34, 10:34, 10:36, 0:36, 0:39, 10:39, 10:41, 0:41, 0:44, 30:44, 30:30"))
	loopH = LoopFromPoints(parsePoints("0:30, 0:34, -10:34, -10:36, 0:36, 0:39, 10:39, 10:41, 0:41, 0:44, 30:44, 30:30"))
	loopI = LoopFromPoints(parsePoints("10:34, 0:34, -10:34, -10:36, 0:36, 10:36"))

	// The set of all test loops.
	allLoops = []*Loop{
		EmptyLoop(),
		FullLoop(),
		northHemi,
		northHemi3,
		southHemi,
		westHemi,
		eastHemi,
		nearHemi,
		farHemi,
		candyCane,
		smallNECW,
		arctic80,
		antarctic80,
		lineTriangle,
		skinnyChevron,
		loopA,
		//snappedLoopA, // Fails TestAreaConsistentWithTurningAngle
		loopB,
		aIntersectB,
		aUnionB,
		aMinusB,
		bMinusA,
		loopC,
		loopD,
		loopE,
		loopF,
		loopG,
		loopH,
		loopI,
	}
)

func TestLoopEmptyAndFull(t *testing.T) {
	emptyLoop := EmptyLoop()

	if !emptyLoop.IsEmpty() {
		t.Errorf("empty loop should be empty")
	}
	if emptyLoop.IsFull() {
		t.Errorf("empty loop should not be full")
	}
	if !emptyLoop.isEmptyOrFull() {
		t.Errorf("empty loop should pass IsEmptyOrFull")
	}

	fullLoop := FullLoop()

	if fullLoop.IsEmpty() {
		t.Errorf("full loop should not be empty")
	}
	if !fullLoop.IsFull() {
		t.Errorf("full loop should be full")
	}
	if !fullLoop.isEmptyOrFull() {
		t.Errorf("full loop should pass IsEmptyOrFull")
	}
	if emptyLoop.NumEdges() != 0 {
		t.Errorf("empty loops should have no edges")
	}
	if emptyLoop.NumChains() != 0 {
		t.Errorf("empty loops should have no edge chains")
	}
	if fullLoop.NumEdges() != 0 {
		t.Errorf("full loops should have no edges")
	}
	if fullLoop.NumChains() != 0 {
		t.Errorf("full loops should have no edge chains")
	}
}

func TestLoopBasic(t *testing.T) {
	shape := Shape(makeLoop("0:0, 0:1, 1:0"))

	if got := shape.NumEdges(); got != 3 {
		t.Errorf("shape.NumEdges = %d, want 3", got)
	}
	if got := shape.NumChains(); got != 1 {
		t.Errorf("shape.NumChains = %d, want 1", got)
	}
	if got := shape.Chain(0).Start; got != 0 {
		t.Errorf("shape.Chain(0).Start = %d, want 3", got)
	}
	if got := shape.Chain(0).Length; got != 3 {
		t.Errorf("shape.Chain(0).Length = %d, want 3", got)
	}

	e := shape.Edge(2)
	if want := PointFromLatLng(LatLngFromDegrees(1, 0)); !e.V0.ApproxEqual(want) {
		t.Errorf("shape.Edge(2) end A = %v, want %v", e.V0, want)
	}
	if want := PointFromLatLng(LatLngFromDegrees(0, 0)); !e.V1.ApproxEqual(want) {

		t.Errorf("shape.Edge(2) end B = %v, want %v", e.V1, want)
	}

	if got := shape.dimension(); got != polygonGeometry {
		t.Errorf("shape.dimension() = %d, want %v", got, polygonGeometry)
	}
	if !shape.HasInterior() {
		t.Errorf("shape.HasInterior() = false, want true")
	}
	if shape.ContainsOrigin() {
		t.Errorf("shape.ContainsOrigin() = true, want false")
	}
}

func TestLoopHoleAndSign(t *testing.T) {
	l := LoopFromPoints(parsePoints("0:-180, 0:-90, 0:0, 0:90"))

	if l.IsHole() {
		t.Errorf("loop with default depth should not be a hole")
	}
	if l.Sign() == -1 {
		t.Errorf("loop with default depth should have a sign of +1")
	}

	l.depth = 3
	if !l.IsHole() {
		t.Errorf("loop with odd depth should be a hole")
	}
	if l.Sign() != -1 {
		t.Errorf("loop with odd depth should have a sign of -1")
	}

	l.depth = 2
	if l.IsHole() {
		t.Errorf("loop with even depth should not be a hole")
	}
	if l.Sign() == -1 {
		t.Errorf("loop with even depth should have a sign of +1")
	}

}

func TestLoopRectBound(t *testing.T) {
	if !EmptyLoop().RectBound().IsEmpty() {
		t.Errorf("empty loop's RectBound should be empty")
	}
	if !FullLoop().RectBound().IsFull() {
		t.Errorf("full loop's RectBound should be full")
	}
	if !candyCane.RectBound().Lng.IsFull() {
		t.Errorf("candy cane loop's RectBound should have a full longitude range")
	}
	if got := candyCane.RectBound().Lat.Lo; got >= -0.349066 {
		t.Errorf("candy cane loop's RectBound should have a lower latitude (%v) under -0.349066 radians", got)
	}
	if got := candyCane.RectBound().Lat.Hi; got <= 0.174533 {
		t.Errorf("candy cane loop's RectBound should have an upper latitude (%v) over 0.174533 radians", got)
	}
	if !smallNECW.RectBound().IsFull() {
		t.Errorf("small northeast clockwise loop's RectBound should be full")
	}
	if got, want := arctic80.RectBound(), rectFromDegrees(80, -180, 90, 180); !rectsApproxEqual(got, want, rectErrorLat, rectErrorLng) {
		t.Errorf("arctic 80 loop's RectBound (%v) should be %v", got, want)
	}
	if got, want := antarctic80.RectBound(), rectFromDegrees(-90, -180, -80, 180); !rectsApproxEqual(got, want, rectErrorLat, rectErrorLng) {
		t.Errorf("antarctic 80 loop's RectBound (%v) should be %v", got, want)
	}
	if !southHemi.RectBound().Lng.IsFull() {
		t.Errorf("south hemi loop's RectBound should have a full longitude range")
	}
	got, want := southHemi.RectBound().Lat, r1.Interval{-math.Pi / 2, 0}
	if !got.ApproxEqual(want) {
		t.Errorf("south hemi loop's RectBound latitude interval (%v) should be %v", got, want)
	}

	// Create a loop that contains the complement of the arctic80 loop.
	arctic80Inv := cloneLoop(arctic80)
	arctic80Inv.Invert()
	// The highest latitude of each edge is attained at its midpoint.
	mid := Point{arctic80Inv.vertices[0].Vector.Add(arctic80Inv.vertices[1].Vector).Mul(.5)}
	if got, want := arctic80Inv.RectBound().Lat.Hi, float64(LatLngFromPoint(mid).Lat); math.Abs(got-want) > 10*dblEpsilon {
		t.Errorf("arctic 80 inverse loop's RectBound should have a latutude hi of %v, got %v", got, want)
	}
}

func TestLoopCapBound(t *testing.T) {
	if !EmptyLoop().CapBound().IsEmpty() {
		t.Errorf("empty loop's CapBound should be empty")
	}
	if !FullLoop().CapBound().IsFull() {
		t.Errorf("full loop's CapBound should be full")
	}
	if !smallNECW.CapBound().IsFull() {
		t.Errorf("small northeast clockwise loop's CapBound should be full")
	}
	if got, want := arctic80.CapBound(), rectFromDegrees(80, -180, 90, 180).CapBound(); !got.ApproxEqual(want) {
		t.Errorf("arctic 80 loop's CapBound (%v) should be %v", got, want)
	}
	if got, want := antarctic80.CapBound(), rectFromDegrees(-90, -180, -80, 180).CapBound(); !got.ApproxEqual(want) {
		t.Errorf("antarctic 80 loop's CapBound (%v) should be %v", got, want)
	}
}

func TestLoopOriginInside(t *testing.T) {
	if !northHemi.originInside {
		t.Errorf("north hemisphere polygon should include origin")
	}
	if !northHemi3.originInside {
		t.Errorf("north hemisphere 3 polygon should include origin")
	}
	if southHemi.originInside {
		t.Errorf("south hemisphere polygon should not include origin")
	}
	if westHemi.originInside {
		t.Errorf("west hemisphere polygon should not include origin")
	}
	if !eastHemi.originInside {
		t.Errorf("east hemisphere polygon should include origin")
	}
	if nearHemi.originInside {
		t.Errorf("near hemisphere polygon should not include origin")
	}
	if !farHemi.originInside {
		t.Errorf("far hemisphere polygon should include origin")
	}
	if candyCane.originInside {
		t.Errorf("candy cane polygon should not include origin")
	}
	if !smallNECW.originInside {
		t.Errorf("smallNECW polygon should include origin")
	}
	if !arctic80.originInside {
		t.Errorf("arctic 80 polygon should include origin")
	}
	if antarctic80.originInside {
		t.Errorf("antarctic 80 polygon should not include origin")
	}
	if loopA.originInside {
		t.Errorf("loop A polygon should not include origin")
	}
}

func rotate(l *Loop) *Loop {
	vertices := make([]Point, 0, len(l.vertices))
	for i := 1; i < len(l.vertices); i++ {
		vertices = append(vertices, l.vertices[i])
	}
	vertices = append(vertices, l.vertices[0])
	return LoopFromPoints(vertices)
}

func TestLoopContainsPoint(t *testing.T) {
	north := Point{r3.Vector{0, 0, 1}}
	south := Point{r3.Vector{0, 0, -1}}
	east := PointFromCoords(0, 1, 0)
	west := PointFromCoords(0, -1, 0)

	if EmptyLoop().ContainsPoint(north) {
		t.Errorf("empty loop should not not have any points")
	}
	if !FullLoop().ContainsPoint(south) {
		t.Errorf("full loop should have full point vertex")
	}

	for _, tc := range []struct {
		name string
		l    *Loop
		in   Point
		out  Point
	}{
		{
			"north hemisphere",
			northHemi,
			north,
			south,
		},
		{
			"south hemisphere",
			southHemi,
			south,
			north,
		},
		{
			"west hemisphere",
			westHemi,
			west,
			east,
		},
		{
			"east hemisphere",
			eastHemi,
			east,
			west,
		},
		{
			"candy cane",
			candyCane,
			PointFromLatLng(LatLngFromDegrees(5, 71)),
			PointFromLatLng(LatLngFromDegrees(-8, 71)),
		},
	} {
		l := tc.l
		for i := 0; i < 4; i++ {
			if !l.ContainsPoint(tc.in) {
				t.Errorf("%s loop should contain %v at rotation %d", tc.name, tc.in, i)
			}
			if l.ContainsPoint(tc.out) {
				t.Errorf("%s loop shouldn't contain %v at rotation %d", tc.name, tc.out, i)
			}
			l = rotate(l)
		}
	}

	// This code checks each cell vertex is contained by exactly one of
	// the adjacent cells.
	for level := 0; level < 3; level++ {
		// set of unique points across all loops at this level.
		points := make(map[Point]bool)
		var loops []*Loop
		for id := CellIDFromFace(0).ChildBeginAtLevel(level); id != CellIDFromFace(5).ChildEndAtLevel(level); id = id.Next() {
			var vertices []Point
			cell := CellFromCellID(id)
			points[cell.Center()] = true
			for k := 0; k < 4; k++ {
				vertices = append(vertices, cell.Vertex(k))
				points[cell.Vertex(k)] = true
			}
			loops = append(loops, LoopFromPoints(vertices))
		}

		for point := range points {
			count := 0
			for _, loop := range loops {
				if loop.ContainsPoint(point) {
					count++
				}
			}
			if count != 1 {
				t.Errorf("point %v should only be contained by one loop at level %d, got %d", point, level, count)
			}
		}
	}
}

func TestLoopVertex(t *testing.T) {
	tests := []struct {
		loop   *Loop
		vertex int
		want   Point
	}{
		{EmptyLoop(), 0, Point{r3.Vector{0, 0, 1}}},
		{EmptyLoop(), 1, Point{r3.Vector{0, 0, 1}}},
		{FullLoop(), 0, Point{r3.Vector{0, 0, -1}}},
		{FullLoop(), 1, Point{r3.Vector{0, 0, -1}}},
		{arctic80, 0, parsePoint("80:-150")},
		{arctic80, 1, parsePoint("80:-30")},
		{arctic80, 2, parsePoint("80:90")},
		{arctic80, 3, parsePoint("80:-150")},
	}

	for _, test := range tests {
		if got := test.loop.Vertex(test.vertex); !pointsApproxEquals(got, test.want, epsilon) {
			t.Errorf("%v.Vertex(%d) = %v, want %v", test.loop, test.vertex, got, test.want)
		}
	}

	// Check that wrapping is correct.
	if !pointsApproxEquals(arctic80.Vertex(2), arctic80.Vertex(5), epsilon) {
		t.Errorf("Vertex should wrap values. %v.Vertex(2) = %v != %v.Vertex(5) = %v",
			arctic80, arctic80.Vertex(2), arctic80, arctic80.Vertex(5))
	}

	loopAroundThrice := 2 + 3*len(arctic80.vertices)
	if !pointsApproxEquals(arctic80.Vertex(2), arctic80.Vertex(loopAroundThrice), epsilon) {
		t.Errorf("Vertex should wrap values. %v.Vertex(2) = %v != %v.Vertex(%d) = %v",
			arctic80, arctic80.Vertex(2), arctic80, loopAroundThrice, arctic80.Vertex(loopAroundThrice))
	}
}

func TestLoopNumEdges(t *testing.T) {
	tests := []struct {
		loop *Loop
		want int
	}{
		{EmptyLoop(), 0},
		{FullLoop(), 0},
		{farHemi, 4},
		{candyCane, 6},
		{smallNECW, 3},
		{arctic80, 3},
		{antarctic80, 3},
		{lineTriangle, 3},
		{skinnyChevron, 4},
	}

	for _, test := range tests {
		if got := test.loop.NumEdges(); got != test.want {
			t.Errorf("%v.NumEdges() = %v, want %v", test.loop, got, test.want)
		}
	}
}

func TestLoopEdge(t *testing.T) {
	tests := []struct {
		loop  *Loop
		edge  int
		wantA Point
		wantB Point
	}{
		{
			loop:  farHemi,
			edge:  2,
			wantA: Point{r3.Vector{0, 0, -1}},
			wantB: Point{r3.Vector{0, -1, 0}},
		},
		{
			loop: candyCane,
			edge: 0,

			wantA: parsePoint("-20:150"),
			wantB: parsePoint("-20:-70"),
		},
		{
			loop:  candyCane,
			edge:  1,
			wantA: parsePoint("-20:-70"),
			wantB: parsePoint("0:70"),
		},
		{
			loop:  candyCane,
			edge:  2,
			wantA: parsePoint("0:70"),
			wantB: parsePoint("10:-150"),
		},
		{
			loop:  candyCane,
			edge:  3,
			wantA: parsePoint("10:-150"),
			wantB: parsePoint("10:70"),
		},
		{
			loop:  candyCane,
			edge:  4,
			wantA: parsePoint("10:70"),
			wantB: parsePoint("-10:-70"),
		},
		{
			loop:  candyCane,
			edge:  5,
			wantA: parsePoint("-10:-70"),
			wantB: parsePoint("-20:150"),
		},
		{
			loop:  skinnyChevron,
			edge:  2,
			wantA: parsePoint("0:1e-320"),
			wantB: parsePoint("1e-320:80"),
		},
		{
			loop:  skinnyChevron,
			edge:  3,
			wantA: parsePoint("1e-320:80"),
			wantB: parsePoint("0:0"),
		},
	}

	for _, test := range tests {
		if e := test.loop.Edge(test.edge); !(pointsApproxEquals(e.V0, test.wantA, epsilon) && pointsApproxEquals(e.V1, test.wantB, epsilon)) {
			t.Errorf("%v.Edge(%d) = %v, want (%v, %v)", test.loop, test.edge, e, test.wantA, test.wantB)
		}
	}
}

func TestLoopFromCell(t *testing.T) {
	cell := CellFromCellID(CellIDFromLatLng(LatLng{40.565459 * s1.Degree, -74.645276 * s1.Degree}))
	loopFromCell := LoopFromCell(cell)

	// Demonstrates the reason for this test; the cell bounds are more
	// conservative than the resulting loop bounds.
	if loopFromCell.RectBound().Contains(cell.RectBound()) {
		t.Errorf("loopFromCell's RectBound countains the original cells RectBound, but should not")
	}
}

func TestLoopRegularLoop(t *testing.T) {
	loop := RegularLoop(PointFromLatLng(LatLngFromDegrees(80, 135)), 20*s1.Degree, 4)
	if len(loop.vertices) != 4 {
		t.Errorf("RegularLoop with 4 vertices should have 4 vertices, got %d", len(loop.vertices))
	}
	// The actual Points values are already tested in the s2point_test method TestRegularPoints.
}

// cloneLoop creates a new copy of the given loop including all of its vertices
// so that when tests modify vertices in it, it won't ruin the original loop.
func cloneLoop(l *Loop) *Loop {
	c := &Loop{
		vertices:       make([]Point, len(l.vertices)),
		originInside:   l.originInside,
		bound:          l.bound,
		subregionBound: l.subregionBound,
		index:          NewShapeIndex(),
	}
	copy(c.vertices, l.vertices)
	c.index.Add(c)

	return c
}

func TestLoopContainsMatchesCrossingSign(t *testing.T) {
	// This test demonstrates a former incompatibility between CrossingSign
	// and ContainsPoint. It constructs a Cell-based loop L and
	// an edge E from Origin to a0 that crosses exactly one edge of L.  Yet
	// previously, Contains() returned false for both endpoints of E.
	//
	// The reason for the bug was that the loop bound was sometimes too tight.
	// The Contains() code for a0 bailed out early because a0 was found not to
	// be inside the bound of L.

	// Start with a cell that ends up producing the problem.
	cellID := cellIDFromPoint(Point{r3.Vector{1, 1, 1}}).Parent(21)
	children, ok := CellFromCellID(cellID).Children()
	if !ok {
		t.Fatalf("error subdividing cell")
	}

	points := make([]Point, 4)
	for i := 0; i < 4; i++ {
		// Note extra normalization. Center() is already normalized.
		// The test results will no longer be inconsistent if the extra
		// Normalize() is removed.
		points[i] = Point{children[i].Center().Normalize()}
	}

	// Get a vertex from a grandchild cell.
	// +---------------+---------------+
	// |               |               |
	// |    points[3]  |   points[2]   |
	// |       v       |       v       |
	// |       +-------+------ +       |
	// |       |       |       |       |
	// |       |       |       |       |
	// |       |       |       |       |
	// +-------+-------+-------+-------+
	// |       |       |       |       |
	// |       |    <----------------------- grandchild_cell
	// |       |       |       |       |
	// |       +-------+------ +       |
	// |       ^       |       ^       | <-- cell
	// | points[0]/a0  |     points[1] |
	// |               |               |
	// +---------------+---------------+
	loop := LoopFromPoints(points)
	grandchildren, ok := children[0].Children()
	if !ok {
		t.Fatalf("error subdividing cell")
	}

	grandchildCell := grandchildren[2]

	a0 := grandchildCell.Vertex(0)

	// This test depends on rounding errors that should make a0 slightly different from points[0]
	if points[0] == a0 {
		t.Errorf("%v not different enough from %v to successfully test", points[0], a0)
	}

	// The edge from a0 to the origin crosses one boundary.
	if got, want := NewChainEdgeCrosser(a0, OriginPoint(), loop.Vertex(0)).ChainCrossingSign(loop.Vertex(1)), DoNotCross; got != want {
		t.Errorf("crossingSign(%v, %v, %v, %v) = %v, want %v", a0, OriginPoint(), loop.Vertex(0), loop.Vertex(1), got, want)
	}

	if got, want := NewChainEdgeCrosser(a0, OriginPoint(), loop.Vertex(1)).ChainCrossingSign(loop.Vertex(2)), Cross; got != want {
		t.Errorf("crossingSign(%v, %v, %v, %v) = %v, want %v", a0, OriginPoint(), loop.Vertex(1), loop.Vertex(2), got, want)
	}

	if got, want := NewChainEdgeCrosser(a0, OriginPoint(), loop.Vertex(2)).ChainCrossingSign(loop.Vertex(3)), DoNotCross; got != want {
		t.Errorf("crossingSign(%v, %v, %v, %v) = %v, want %v", a0, OriginPoint(), loop.Vertex(2), loop.Vertex(3), got, want)
	}

	if got, want := NewChainEdgeCrosser(a0, OriginPoint(), loop.Vertex(3)).ChainCrossingSign(loop.Vertex(4)), DoNotCross; got != want {
		t.Errorf("crossingSign(%v, %v, %v, %v) = %v, want %v", a0, OriginPoint(), loop.Vertex(3), loop.Vertex(4), got, want)
	}

	// Contains should return false for the origin, and true for a0.
	if loop.ContainsPoint(OriginPoint()) {
		t.Errorf("%v.ContainsPoint(%v) = true, want false", loop, OriginPoint())
	}
	if !loop.ContainsPoint(a0) {
		t.Errorf("%v.ContainsPoint(%v) = false, want true", loop, a0)
	}

	// Since a0 is inside the loop, it should be inside the bound.
	bound := loop.RectBound()
	if !bound.ContainsPoint(a0) {
		t.Errorf("%v.RectBound().ContainsPoint(%v) = false, want true", loop, a0)
	}
}

func TestLoopTurningAngle(t *testing.T) {
	tests := []struct {
		loop *Loop
		want float64
	}{
		{EmptyLoop(), 2 * math.Pi},
		{FullLoop(), -2 * math.Pi},
		{northHemi3, 0},
		{westHemi, 0},
		{candyCane, 4.69364376125922},
		{lineTriangle, 2 * math.Pi},
		{skinnyChevron, 2 * math.Pi},
	}

	for _, test := range tests {
		if got := test.loop.TurningAngle(); !float64Near(got, test.want, epsilon) {
			t.Errorf("%v.TurningAngle() = %v, want %v", test.loop, got, test.want)
		}

		// Check that the turning angle is *identical* when the vertex order is
		// rotated, and that the sign is inverted when the vertices are reversed.
		expected := test.loop.TurningAngle()
		loopCopy := cloneLoop(test.loop)
		for i := 0; i < len(test.loop.vertices); i++ {
			loopCopy.Invert()
			if got := loopCopy.TurningAngle(); got != -expected {
				t.Errorf("loop.Invert().TurningAngle() = %v, want %v", got, -expected)
			}
			// Invert it back to normal.
			loopCopy.Invert()

			loopCopy = rotate(loopCopy)
			if got := loopCopy.TurningAngle(); got != expected {
				t.Errorf("loop.TurningAngle() = %v, want %v", got, expected)
			}
		}
	}

	// TODO(roberts): Uncomment once Area is implemented.
	// Build a narrow spiral loop starting at the north pole. This is designed
	// to test that the error in TurningAngle is linear in the number of
	// vertices even when the partial sum of the turning angles gets very large.
	// The spiral consists of two arms defining opposite sides of the loop.
	const armPoints = 10000 // Number of vertices in each "arm"
	const armRadius = 0.01  // Radius of spiral.
	var vertices = make([]Point, 2*armPoints)

	// Set the center point of the spiral.
	vertices[armPoints] = PointFromCoords(0, 0, 1)

	for i := 0; i < armPoints; i++ {
		angle := (2 * math.Pi / 3) * float64(i)
		x := math.Cos(angle)
		y := math.Sin(angle)
		r1 := float64(i) * armRadius / armPoints
		r2 := (float64(i) + 1.5) * armRadius / armPoints
		vertices[armPoints-i-1] = PointFromCoords(r1*x, r1*y, 1)
		vertices[armPoints+i] = PointFromCoords(r2*x, r2*y, 1)
	}
	// This is a pathological loop that contains many long parallel edges.
	spiral := LoopFromPoints(vertices)

	// Check that TurningAngle is consistent with Area to within the
	// error bound of the former. We actually use a tiny fraction of the
	// worst-case error bound, since the worst case only happens when all the
	// roundoff errors happen in the same direction and this test is not
	// designed to achieve that. The error in Area can be ignored for the
	// purposes of this test since it is generally much smaller.
	if got, want := spiral.TurningAngle(), (2*math.Pi - spiral.Area()); !float64Near(got, want, 0.01*spiral.turningAngleMaxError()) {
		t.Errorf("spiral.TurningAngle() = %v, want %v", got, want)
	}
}

func TestLoopAreaAndCentroid(t *testing.T) {
	var p Point

	if got, want := EmptyLoop().Area(), 0.0; got != want {
		t.Errorf("EmptyLoop.Area() = %v, want %v", got, want)
	}
	if got, want := FullLoop().Area(), 4*math.Pi; got != want {
		t.Errorf("FullLoop.Area() = %v, want %v", got, want)
	}
	if got := EmptyLoop().Centroid(); !p.ApproxEqual(got) {
		t.Errorf("EmptyLoop.Centroid() = %v, want %v", got, p)
	}
	if got := FullLoop().Centroid(); !p.ApproxEqual(got) {
		t.Errorf("FullLoop.Centroid() = %v, want %v", got, p)
	}

	if got, want := northHemi.Area(), 2*math.Pi; !float64Eq(got, want) {
		t.Errorf("northHemi.Area() = %v, want %v", got, want)
	}

	eastHemiArea := eastHemi.Area()
	if eastHemiArea < 2*math.Pi-1e-12 || eastHemiArea > 2*math.Pi+1e-12 {
		t.Errorf("eastHemi.Area() = %v, want between [%v, %v]", eastHemiArea, 2*math.Pi-1e-12, 2*math.Pi+1e-12)
	}

	// Construct spherical caps of random height, and approximate their boundary
	// with closely spaces vertices. Then check that the area and centroid are
	// correct.
	for i := 0; i < 50; i++ {
		// Choose a coordinate frame for the spherical cap.
		f := randomFrame()
		x := f.col(0)
		y := f.col(1)
		z := f.col(2)

		// Given two points at latitude phi and whose longitudes differ by dtheta,
		// the geodesic between the two points has a maximum latitude of
		// atan(tan(phi) / cos(dtheta/2)). This can be derived by positioning
		// the two points at (-dtheta/2, phi) and (dtheta/2, phi).
		//
		// We want to position the vertices close enough together so that their
		// maximum distance from the boundary of the spherical cap is maxDist.
		// Thus we want abs(atan(tan(phi) / cos(dtheta/2)) - phi) <= maxDist.
		const maxDist = 1e-6
		height := 2 * randomFloat64()
		phi := math.Asin(1.0 - height)
		maxDtheta := 2 * math.Acos(math.Tan(math.Abs(phi))/math.Tan(math.Abs(phi)+maxDist))
		maxDtheta = math.Min(math.Pi, maxDtheta)

		var vertices []Point
		for theta := 0.0; theta < 2*math.Pi; theta += randomFloat64() * maxDtheta {
			vertices = append(vertices,
				Point{x.Mul(math.Cos(theta) * math.Cos(phi)).Add(
					y.Mul(math.Sin(theta) * math.Cos(phi))).Add(
					z.Mul(math.Sin(phi)))},
			)
		}

		loop := LoopFromPoints(vertices)
		area := loop.Area()
		centroid := loop.Centroid()
		expectedArea := 2 * math.Pi * height
		if delta, want := math.Abs(area-expectedArea), 2*math.Pi*maxDist; delta > want {
			t.Errorf("%v.Area() = %v, want to be within %v of %v, got %v", loop, area, want, expectedArea, delta)
		}
		expectedCentroid := z.Mul(expectedArea * (1 - 0.5*height))
		if delta, want := centroid.Sub(expectedCentroid).Norm(), 2*maxDist; delta > want {
			t.Errorf("%v.Centroid() = %v, want to be within %v of %v, got %v", loop, centroid, want, expectedCentroid, delta)
		}
	}
}

// TODO(roberts): Test that Area() has an accuracy significantly better
// than 1e-15 on loops whose area is small.

func TestLoopAreaConsistentWithTurningAngle(t *testing.T) {
	// Check that the area computed using GetArea() is consistent with the
	// turning angle of the loop computed using GetTurnAngle().  According to
	// the Gauss-Bonnet theorem, the area of the loop should be equal to 2*Pi
	// minus its turning angle.
	for x, loop := range allLoops {
		area := loop.Area()
		gaussArea := 2*math.Pi - loop.TurningAngle()

		// TODO(roberts): The error bound below is much larger than it should be.
		if math.Abs(area-gaussArea) > 1e-9 {
			t.Errorf("%d. %v.Area() = %v want %v", x, loop, area, gaussArea)
		}
	}
}

func TestLoopGetAreaConsistentWithSign(t *testing.T) {
	// TODO(roberts): Uncomment when Loop has IsValid
	/*
		// Test that Area() returns an area near 0 for degenerate loops that
		// contain almost no points, and an area near 4*pi for degenerate loops that
		// contain almost all points.
		const maxVertices = 6

		for i := 0; i < 50; i++ {
			numVertices := 3 + randomUniformInt(maxVertices-3+1)
			// Repeatedly choose N vertices that are exactly on the equator until we
			// find some that form a valid loop.
			var loop = Loop{}
			for !loop.IsValid() {
				var vertices []Point
				for i := 0; i < numVertices; i++ {
					// We limit longitude to the range [0, 90] to ensure that the loop is
					// degenerate (as opposed to following the entire equator).
					vertices = append(vertices,
						PointFromLatLng(LatLng{0, s1.Angle(randomFloat64()) * math.Pi / 2}))
				}
				loop.vertices = vertices
				break
			}

			ccw := loop.IsNormalized()
			want := 0.0
			if !ccw {
				want = 4 * math.Pi
			}

			// TODO(roberts): The error bound below is much larger than it should be.
			if got := loop.Area(); !float64Near(got, want, 1e-8) {
				t.Errorf("%v.Area() = %v, want %v", loop, got, want)
			}
			p := PointFromCoords(0, 0, 1)
			if got := loop.ContainsPoint(p); got != !ccw {
				t.Errorf("%v.ContainsPoint(%v) = %v, want %v", got, p, !ccw)
			}
		}
	*/
}

func TestLoopNormalizedCompatibleWithContains(t *testing.T) {
	p := parsePoint("40:40")

	tests := []*Loop{
		lineTriangle,
		skinnyChevron,
	}

	// Checks that if a loop is normalized, it doesn't contain a
	// point outside of it, and vice versa.
	for _, loop := range tests {
		flip := cloneLoop(loop)

		flip.Invert()
		if norm, contains := loop.IsNormalized(), loop.ContainsPoint(p); norm == contains {
			t.Errorf("loop.IsNormalized() = %b == loop.ContainsPoint(%v) = %v, want !=", norm, p, contains)
		}
		if norm, contains := flip.IsNormalized(), flip.ContainsPoint(p); norm == contains {
			t.Errorf("flip.IsNormalized() = %b == flip.ContainsPoint(%v) = %v, want !=", norm, p, contains)
		}
		if a, b := loop.IsNormalized(), flip.IsNormalized(); a == b {
			t.Errorf("a loop and it's invert can not both be normalized")
		}
		flip.Normalize()
		if flip.ContainsPoint(p) {
			t.Errorf("%v.ContainsPoint(%v) = true, want false", flip, p)
		}
	}
}

const (
	// TODO(roberts): Convert these into changeable flags or parameters.
	// A loop with a 10km radius and 4096 vertices has an edge length of 15 meters.
	defaultRadiusKm   = 10.0
	numLoopSamples    = 16
	numQueriesPerLoop = 100
)

func BenchmarkLoopContainsPoint(b *testing.B) {
	// Benchmark ContainsPoint() on regular loops. The query points for a loop are
	// chosen so that they all lie in the loop's bounding rectangle (to avoid the
	// quick-rejection code path).

	// C++ ranges from 4 -> 256k by powers of 2 for number of vertices for benchmarking.
	vertices := 4
	for n := 1; n <= 17; n++ {
		b.Run(fmt.Sprintf("%d", vertices),
			func(b *testing.B) {
				b.StopTimer()
				loops := make([]*Loop, numLoopSamples)
				for i := 0; i < numLoopSamples; i++ {
					loops[i] = RegularLoop(randomPoint(), kmToAngle(10.0), vertices)
				}

				queries := make([][]Point, numLoopSamples)

				for i, loop := range loops {
					queries[i] = make([]Point, numQueriesPerLoop)
					for j := 0; j < numQueriesPerLoop; j++ {
						queries[i][j] = samplePointFromRect(loop.RectBound())
					}
				}

				b.StartTimer()
				for i := 0; i < b.N; i++ {
					loops[i%numLoopSamples].ContainsPoint(queries[i%numLoopSamples][i%numQueriesPerLoop])
				}
			})
		vertices *= 2
	}
}
