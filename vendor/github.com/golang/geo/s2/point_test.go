/*
Copyright 2014 Google Inc. All rights reserved.

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

	"github.com/golang/geo/r3"
	"github.com/golang/geo/s1"
)

func TestOriginPoint(t *testing.T) {
	if math.Abs(OriginPoint().Norm()-1) > 1e-15 {
		t.Errorf("Origin point norm = %v, want 1", OriginPoint().Norm())
	}

	// The point chosen below is about 66km from the north pole towards the East
	// Siberian Sea. The purpose of the stToUV(2/3) calculation is to keep the
	// origin as far away as possible from the longitudinal edges of large
	// Cells. (The line of longitude through the chosen point is always 1/3
	// or 2/3 of the way across any Cell with longitudinal edges that it
	// passes through.)
	p := Point{r3.Vector{-0.01, 0.01 * stToUV(2.0/3), 1}}
	if !p.ApproxEqual(OriginPoint()) {
		t.Errorf("Origin point should fall in the Siberian Sea, but does not.")
	}

	// Check that the origin is not too close to either pole.
	// The Earth's mean radius in kilometers (according to NASA).
	const earthRadiusKm = 6371.01
	if dist := math.Acos(OriginPoint().Z) * earthRadiusKm; dist <= 50 {
		t.Errorf("Origin point is to close to the North Pole. Got %v, want >= 50km", dist)
	}
}

func TestPointCross(t *testing.T) {
	tests := []struct {
		p1x, p1y, p1z, p2x, p2y, p2z, norm float64
	}{
		{1, 0, 0, 1, 0, 0, 1},
		{1, 0, 0, 0, 1, 0, 2},
		{0, 1, 0, 1, 0, 0, 2},
		{1, 2, 3, -4, 5, -6, 2 * math.Sqrt(934)},
	}
	for _, test := range tests {
		p1 := Point{r3.Vector{test.p1x, test.p1y, test.p1z}}
		p2 := Point{r3.Vector{test.p2x, test.p2y, test.p2z}}
		result := p1.PointCross(p2)
		if !float64Eq(result.Norm(), test.norm) {
			t.Errorf("|%v ⨯ %v| = %v, want %v", p1, p2, result.Norm(), test.norm)
		}
		if x := result.Dot(p1.Vector); !float64Eq(x, 0) {
			t.Errorf("|(%v ⨯ %v) · %v| = %v, want 0", p1, p2, p1, x)
		}
		if x := result.Dot(p2.Vector); !float64Eq(x, 0) {
			t.Errorf("|(%v ⨯ %v) · %v| = %v, want 0", p1, p2, p2, x)
		}
	}
}

func TestPointDistance(t *testing.T) {
	tests := []struct {
		x1, y1, z1 float64
		x2, y2, z2 float64
		want       float64 // radians
	}{
		{1, 0, 0, 1, 0, 0, 0},
		{1, 0, 0, 0, 1, 0, math.Pi / 2},
		{1, 0, 0, 0, 1, 1, math.Pi / 2},
		{1, 0, 0, -1, 0, 0, math.Pi},
		{1, 2, 3, 2, 3, -1, 1.2055891055045298},
	}
	for _, test := range tests {
		p1 := Point{r3.Vector{test.x1, test.y1, test.z1}}
		p2 := Point{r3.Vector{test.x2, test.y2, test.z2}}
		if a := p1.Distance(p2).Radians(); !float64Eq(a, test.want) {
			t.Errorf("%v.Distance(%v) = %v, want %v", p1, p2, a, test.want)
		}
		if a := p2.Distance(p1).Radians(); !float64Eq(a, test.want) {
			t.Errorf("%v.Distance(%v) = %v, want %v", p2, p1, a, test.want)
		}
	}
}

func TestChordAngleBetweenPoints(t *testing.T) {
	for iter := 0; iter < 10; iter++ {
		m := randomFrame()
		x := m.col(0)
		y := m.col(1)
		z := m.col(2)

		if got := ChordAngleBetweenPoints(z, z).Angle(); got != 0 {
			t.Errorf("ChordAngleBetweenPoints(%v, %v) = %v, want 0", z, z, got)
		}
		if got, want := ChordAngleBetweenPoints(Point{z.Mul(-1)}, z).Angle().Radians(), math.Pi; !float64Near(got, want, 1e-7) {
			t.Errorf("ChordAngleBetweenPoints(%v, %v) = %v, want %v", z.Mul(-1), z, got, want)
		}
		if got, want := ChordAngleBetweenPoints(x, z).Angle().Radians(), math.Pi/2; !float64Eq(got, want) {
			t.Errorf("ChordAngleBetweenPoints(%v, %v) = %v, want %v", x, z, got, want)
		}
		w := Point{y.Add(z.Vector).Normalize()}
		if got, want := ChordAngleBetweenPoints(w, z).Angle().Radians(), math.Pi/4; !float64Eq(got, want) {
			t.Errorf("ChordAngleBetweenPoints(%v, %v) = %v, want %v", w, z, got, want)
		}
	}
}

func TestPointApproxEqual(t *testing.T) {
	tests := []struct {
		x1, y1, z1 float64
		x2, y2, z2 float64
		want       bool
	}{
		{1, 0, 0, 1, 0, 0, true},
		{1, 0, 0, 0, 1, 0, false},
		{1, 0, 0, 0, 1, 1, false},
		{1, 0, 0, -1, 0, 0, false},
		{1, 2, 3, 2, 3, -1, false},
		{1, 0, 0, 1 * (1 + epsilon), 0, 0, true},
		{1, 0, 0, 1 * (1 - epsilon), 0, 0, true},
		{1, 0, 0, 1 + epsilon, 0, 0, true},
		{1, 0, 0, 1 - epsilon, 0, 0, true},
		{1, 0, 0, 1, epsilon, 0, true},
		{1, 0, 0, 1, epsilon, epsilon, false},
		{1, epsilon, 0, 1, -epsilon, epsilon, false},
	}
	for _, test := range tests {
		p1 := Point{r3.Vector{test.x1, test.y1, test.z1}}
		p2 := Point{r3.Vector{test.x2, test.y2, test.z2}}
		if got := p1.ApproxEqual(p2); got != test.want {
			t.Errorf("%v.ApproxEqual(%v), got %v want %v", p1, p2, got, test.want)
		}
	}
}

var (
	pz   = Point{r3.Vector{0, 0, 1}}
	p000 = Point{r3.Vector{1, 0, 0}}
	p045 = Point{r3.Vector{1, 1, 0}.Normalize()}
	p090 = Point{r3.Vector{0, 1, 0}}
	p180 = Point{r3.Vector{-1, 0, 0}}
	// Degenerate triangles.
	pr = Point{r3.Vector{0.257, -0.5723, 0.112}}
	pq = Point{r3.Vector{-0.747, 0.401, 0.2235}}

	// For testing the Girard area fall through case.
	g1 = Point{r3.Vector{1, 1, 1}}
	g2 = Point{g1.Add(pr.Mul(1e-15)).Normalize()}
	g3 = Point{g1.Add(pq.Mul(1e-15)).Normalize()}
)

func TestPointArea(t *testing.T) {
	epsilon := 1e-10
	tests := []struct {
		a, b, c  Point
		want     float64
		nearness float64
	}{
		{p000, p090, pz, math.Pi / 2.0, 0},
		// This test case should give 0 as the epsilon, but either Go or C++'s value for Pi,
		// or the accuracy of the multiplications along the way, cause a difference ~15 decimal
		// places into the result, so it is not quite a difference of 0.
		{p045, pz, p180, 3.0 * math.Pi / 4.0, 1e-14},
		// Make sure that Area has good *relative* accuracy even for very small areas.
		{Point{r3.Vector{epsilon, 0, 1}}, Point{r3.Vector{0, epsilon, 1}}, pz, 0.5 * epsilon * epsilon, 1e-14},
		// Make sure that it can handle degenerate triangles.
		{pr, pr, pr, 0.0, 0},
		{pr, pq, pr, 0.0, 1e-15},
		{p000, p045, p090, 0.0, 0},
		// Try a very long and skinny triangle.
		{p000, Point{r3.Vector{1, 1, epsilon}}, p090, 5.8578643762690495119753e-11, 1e-9},
		{g1, g2, g3, 0.0, 1e-15},
	}
	for _, test := range tests {
		if got := PointArea(test.a, test.b, test.c); !float64Near(got, test.want, test.nearness) {
			t.Errorf("PointArea(%v, %v, %v), got %v want %v", test.a, test.b, test.c, got, test.want)
		}
	}

	maxGirard := 0.0
	for i := 0; i < 10000; i++ {
		p0 := randomPoint()
		d1 := randomPoint()
		d2 := randomPoint()
		p1 := Point{p0.Add(d1.Mul(1e-15)).Normalize()}
		p2 := Point{p0.Add(d2.Mul(1e-15)).Normalize()}
		// The actual displacement can be as much as 1.2e-15 due to roundoff.
		// This yields a maximum triangle area of about 0.7e-30.
		if got := PointArea(p0, p1, p2); got > 0.7e-30 {
			t.Errorf("PointArea(%v, %v, %v) = %v, want <= %v", p1, p1, p2, got, 0.7e-30)
		}
		if a := GirardArea(p0, p1, p2); a > maxGirard {
			maxGirard = a
		}
	}
	// This check only passes if GirardArea() uses RobustCrossProd().
	if maxGirard > 1e-14 {
		t.Errorf("maximum GirardArea = %v, want <= %v", maxGirard, 1e-14)
	}
}

func TestPointAreaQuarterHemisphere(t *testing.T) {
	tests := []struct {
		a, b, c, d, e Point
		want          float64
	}{
		// Triangles with near-180 degree edges that sum to a quarter-sphere.
		{Point{r3.Vector{1, 0.1 * epsilon, epsilon}}, p000, p045, p180, pz, math.Pi},
		// Four other triangles that sum to a quarter-sphere.
		{Point{r3.Vector{1, 1, epsilon}}, p000, p045, p180, pz, math.Pi},
	}
	for _, test := range tests {
		area := PointArea(test.a, test.b, test.c) +
			PointArea(test.a, test.c, test.d) +
			PointArea(test.a, test.d, test.e) +
			PointArea(test.a, test.e, test.b)

		if !float64Eq(area, test.want) {
			t.Errorf("Adding up 4 quarter hemispheres with PointArea(), got %v want %v", area, test.want)
		}
	}

	// Compute the area of a hemisphere using four triangles with one near-180
	// degree edge and one near-degenerate edge.
	for i := 0; i < 100; i++ {
		lng := s1.Angle(2 * math.Pi * randomFloat64())
		p2Lng := lng + s1.Angle(randomFloat64())
		p0 := PointFromLatLng(LatLng{1e-20, lng}.Normalized())
		p1 := PointFromLatLng(LatLng{0, lng}.Normalized())
		p2 := PointFromLatLng(LatLng{0, p2Lng}.Normalized())
		p3 := PointFromLatLng(LatLng{0, lng + math.Pi}.Normalized())
		p4 := PointFromLatLng(LatLng{0, lng + 5.0}.Normalized())
		area := PointArea(p0, p1, p2) + PointArea(p0, p2, p3) + PointArea(p0, p3, p4) + PointArea(p0, p4, p1)
		if !float64Near(area, 2*math.Pi, 2e-15) {
			t.Errorf("hemisphere area of %v, %v, %v, %v, %v = %v, want %v", p1, p1, p2, p3, p4, area, 2*math.Pi)
		}
	}
}

func TestPointPlanarCentroid(t *testing.T) {
	tests := []struct {
		name             string
		p0, p1, p2, want Point
	}{
		{
			name: "xyz axis",
			p0:   Point{r3.Vector{0, 0, 1}},
			p1:   Point{r3.Vector{0, 1, 0}},
			p2:   Point{r3.Vector{1, 0, 0}},
			want: Point{r3.Vector{1. / 3, 1. / 3, 1. / 3}},
		},
		{
			name: "Same point",
			p0:   Point{r3.Vector{1, 0, 0}},
			p1:   Point{r3.Vector{1, 0, 0}},
			p2:   Point{r3.Vector{1, 0, 0}},
			want: Point{r3.Vector{1, 0, 0}},
		},
	}

	for _, test := range tests {
		got := PlanarCentroid(test.p0, test.p1, test.p2)
		if !got.ApproxEqual(test.want) {
			t.Errorf("%s: PlanarCentroid(%v, %v, %v) = %v, want %v", test.name, test.p0, test.p1, test.p2, got, test.want)
		}
	}
}

func TestPointTrueCentroid(t *testing.T) {
	// Test TrueCentroid with very small triangles. This test assumes that
	// the triangle is small enough so that it is nearly planar.
	// The centroid of a planar triangle is at the intersection of its
	// medians, which is two-thirds of the way along each median.
	for i := 0; i < 100; i++ {
		f := randomFrame()
		p := f.col(0)
		x := f.col(1)
		y := f.col(2)
		d := 1e-4 * math.Pow(1e-4, randomFloat64())

		// Make a triangle with two equal sides.
		p0 := Point{p.Sub(x.Mul(d)).Normalize()}
		p1 := Point{p.Add(x.Mul(d)).Normalize()}
		p2 := Point{p.Add(y.Mul(d * 3)).Normalize()}
		want := Point{p.Add(y.Mul(d)).Normalize()}

		got := TrueCentroid(p0, p1, p2).Normalize()
		if got.Distance(want.Vector) >= 2e-8 {
			t.Errorf("TrueCentroid(%v, %v, %v).Normalize() = %v, want %v", p0, p1, p2, got, want)
		}

		// Make a triangle with a right angle.
		p0 = p
		p1 = Point{p.Add(x.Mul(d * 3)).Normalize()}
		p2 = Point{p.Add(y.Mul(d * 6)).Normalize()}
		want = Point{p.Add(x.Add(y.Mul(2)).Mul(d)).Normalize()}

		got = TrueCentroid(p0, p1, p2).Normalize()
		if got.Distance(want.Vector) >= 2e-8 {
			t.Errorf("TrueCentroid(%v, %v, %v).Normalize() = %v, want %v", p0, p1, p2, got, want)
		}
	}
}

func TestPointRegularPoints(t *testing.T) {
	// Conversion to/from degrees has a little more variability than the default epsilon.
	const epsilon = 1e-13
	center := PointFromLatLng(LatLngFromDegrees(80, 135))
	radius := s1.Degree * 20
	pts := regularPoints(center, radius, 4)

	if len(pts) != 4 {
		t.Errorf("regularPoints with 4 vertices should have 4 vertices, got %d", len(pts))
	}

	lls := []LatLng{
		LatLngFromPoint(pts[0]),
		LatLngFromPoint(pts[1]),
		LatLngFromPoint(pts[2]),
		LatLngFromPoint(pts[3]),
	}
	cll := LatLngFromPoint(center)

	// Make sure that the radius is correct.
	wantDist := 20.0
	for i, ll := range lls {
		if got := cll.Distance(ll).Degrees(); !float64Near(got, wantDist, epsilon) {
			t.Errorf("Vertex %d distance from center = %v, want %v", i, got, wantDist)
		}
	}

	// Make sure the angle between each point is correct.
	wantAngle := math.Pi / 2
	for i := 0; i < len(pts); i++ {
		// Mod the index by 4 to wrap the values at each end.
		v0, v1, v2 := pts[(4+i+1)%4], pts[(4+i)%4], pts[(4+i-1)%4]
		if got := float64(v0.Sub(v1.Vector).Angle(v2.Sub(v1.Vector))); !float64Eq(got, wantAngle) {
			t.Errorf("(%v-%v).Angle(%v-%v) = %v, want %v", v0, v1, v1, v2, got, wantAngle)
		}
	}

	// Make sure that all edges of the polygon have the same length.
	wantLength := 27.990890717782829
	for i := 0; i < len(lls); i++ {
		ll1, ll2 := lls[i], lls[(i+1)%4]
		if got := ll1.Distance(ll2).Degrees(); !float64Near(got, wantLength, epsilon) {
			t.Errorf("%v.Distance(%v) = %v, want %v", ll1, ll2, got, wantLength)
		}
	}

	// Spot check an actual coordinate now that we know the points are spaced
	// evenly apart at the same angles and radii.
	if got, want := lls[0].Lat.Degrees(), 62.162880741097204; !float64Near(got, want, epsilon) {
		t.Errorf("%v.Lat = %v, want %v", lls[0], got, want)
	}
	if got, want := lls[0].Lng.Degrees(), 103.11051028343407; !float64Near(got, want, epsilon) {
		t.Errorf("%v.Lng = %v, want %v", lls[0], got, want)
	}
}

func TestPointRegion(t *testing.T) {
	p := Point{r3.Vector{1, 0, 0}}
	r := Point{r3.Vector{1, 0, 0}}
	if !r.Contains(p) {
		t.Errorf("%v.Contains(%v) = false, want true", r, p)
	}
	if !r.ContainsPoint(p) {
		t.Errorf("%v.ContainsPoint(%v) = false, want true", r, p)
	}
	if !r.Contains(r) {
		t.Errorf("%v.Contains(%v) = false, want true", r, r)
	}
	if !r.ContainsPoint(r) {
		t.Errorf("%v.ContainsPoint(%v) = false, want true", r, r)
	}
	if s := (Point{r3.Vector{1, 0, 1}}); r.Contains(s) {
		t.Errorf("%v.Contains(%v) = true, want false", r, s)
	}
	if got, want := r.CapBound(), CapFromPoint(p); !got.ApproxEqual(want) {
		t.Errorf("%v.CapBound() = %v, want %v", r, got, want)
	}
	if got, want := r.RectBound(), RectFromLatLng(LatLngFromPoint(p)); !rectsApproxEqual(got, want, epsilon, epsilon) {
		t.Errorf("%v.RectBound() = %v, want %v", r, got, want)
	}

	// The leaf cell containing a point is still much larger than the point.
	cell := CellFromPoint(p)
	if r.ContainsCell(cell) {
		t.Errorf("%v.ContainsCell(%v) = true, want false", r, cell)
	}
	if !r.IntersectsCell(cell) {
		t.Errorf("%v.IntersectsCell(%v) = false, want true", r, cell)
	}

}

func TestPointAngleMethods(t *testing.T) {

	tests := []struct {
		a, b, c       Point
		wantAngle     s1.Angle
		wantTurnAngle s1.Angle
	}{
		{p000, pz, p045, math.Pi / 4, -3 * math.Pi / 4},
		{p045, pz, p180, 3 * math.Pi / 4, -math.Pi / 4},
		{p000, pz, p180, math.Pi, 0},
		{pz, p000, p045, math.Pi / 2, math.Pi / 2},
		{pz, p000, pz, 0, -math.Pi},
	}

	for _, test := range tests {
		if got := Angle(test.a, test.b, test.c); math.Abs(float64(got-test.wantAngle)) > epsilon {
			t.Errorf("Angle(%v, %v, %v) = %v, want %v", test.a, test.b, test.c, got, test.wantAngle)
		}
		if got := TurnAngle(test.a, test.b, test.c); math.Abs(float64(got-test.wantTurnAngle)) > epsilon {
			t.Errorf("TurnAngle(%v, %v, %v) = %v, want %v", test.a, test.b, test.c, got, test.wantTurnAngle)
		}
	}
}

func TestPointRotate(t *testing.T) {
	for iter := 0; iter < 1000; iter++ {
		axis := randomPoint()
		target := randomPoint()
		// Choose a distance whose logarithm is uniformly distributed.
		distance := s1.Angle(math.Pi * math.Pow(1e-15, randomFloat64()))
		// Sometimes choose points near the far side of the axis.
		if oneIn(5) {
			distance = math.Pi - distance
		}
		p := InterpolateAtDistance(distance, axis, target)
		// Choose the rotation angle.
		angle := s1.Angle(2 * math.Pi * math.Pow(1e-15, randomFloat64()))
		if oneIn(3) {
			angle = -angle
		}
		if oneIn(10) {
			angle = 0
		}

		got := Rotate(p, axis, angle)

		if !got.IsUnit() {
			t.Errorf("%v should be unit length", got)
		}

		// got and p should be the same distance from axis.
		const maxPositionError = 1e-15
		if (got.Distance(axis) - p.Distance(axis)).Abs().Radians() > maxPositionError {
			t.Errorf("rotated point %v should be same distance as %v, got %v, want %v", got, p, got.Distance(axis), p.Distance(axis))
		}

		// Check that the rotation angle is correct. We allow a fixed error in the
		// *position* of the result, so we need to convert this into a rotation
		// angle. The allowable error can be very large as "p" approaches "axis".
		axisDistance := p.Cross(axis.Vector).Norm()
		maxRotationError := 0.0
		if axisDistance < maxPositionError {
			maxRotationError = 2 * math.Pi
		} else {
			maxRotationError = math.Asin(maxPositionError / axisDistance)
		}
		actualRotation := TurnAngle(p, axis, got) + math.Pi
		rotationError := math.Remainder((angle - actualRotation).Radians(), 2*math.Pi)
		if rotationError > maxRotationError {
			t.Errorf("rotational angle of %v = %v, want %v", got, actualRotation, angle)
		}
	}
}

func BenchmarkPointArea(b *testing.B) {
	for i := 0; i < b.N; i++ {
		PointArea(p000, p090, pz)
	}
}

func BenchmarkPointAreaGirardCase(b *testing.B) {
	for i := 0; i < b.N; i++ {
		PointArea(g1, g2, g3)
	}
}
