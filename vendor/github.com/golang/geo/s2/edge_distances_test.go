/*
Copyright 2017 Google Inc. All rights reserved.

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

func TestEdgeDistancesCheckDistance(t *testing.T) {
	tests := []struct {
		x, a, b r3.Vector
		distRad float64
		want    r3.Vector
	}{
		{
			x:       r3.Vector{1, 0, 0},
			a:       r3.Vector{1, 0, 0},
			b:       r3.Vector{0, 1, 0},
			distRad: 0,
			want:    r3.Vector{1, 0, 0},
		},
		{
			x:       r3.Vector{0, 1, 0},
			a:       r3.Vector{1, 0, 0},
			b:       r3.Vector{0, 1, 0},
			distRad: 0,
			want:    r3.Vector{0, 1, 0},
		},
		{
			x:       r3.Vector{1, 3, 0},
			a:       r3.Vector{1, 0, 0},
			b:       r3.Vector{0, 1, 0},
			distRad: 0,
			want:    r3.Vector{1, 3, 0},
		},
		{
			x:       r3.Vector{0, 0, 1},
			a:       r3.Vector{1, 0, 0},
			b:       r3.Vector{0, 1, 0},
			distRad: math.Pi / 2,
			want:    r3.Vector{1, 0, 0},
		},
		{
			x:       r3.Vector{0, 0, -1},
			a:       r3.Vector{1, 0, 0},
			b:       r3.Vector{0, 1, 0},
			distRad: math.Pi / 2,
			want:    r3.Vector{1, 0, 0},
		},
		{
			x:       r3.Vector{-1, -1, 0},
			a:       r3.Vector{1, 0, 0},
			b:       r3.Vector{0, 1, 0},
			distRad: 0.75 * math.Pi,
			want:    r3.Vector{1, 0, 0},
		},
		{
			x:       r3.Vector{0, 1, 0},
			a:       r3.Vector{1, 0, 0},
			b:       r3.Vector{1, 1, 0},
			distRad: math.Pi / 4,
			want:    r3.Vector{1, 1, 0},
		},
		{
			x:       r3.Vector{0, -1, 0},
			a:       r3.Vector{1, 0, 0},
			b:       r3.Vector{1, 1, 0},
			distRad: math.Pi / 2,
			want:    r3.Vector{1, 0, 0},
		},
		{
			x:       r3.Vector{0, -1, 0},
			a:       r3.Vector{1, 0, 0},
			b:       r3.Vector{-1, 1, 0},
			distRad: math.Pi / 2,
			want:    r3.Vector{1, 0, 0},
		},
		{
			x:       r3.Vector{-1, -1, 0},
			a:       r3.Vector{1, 0, 0},
			b:       r3.Vector{-1, 1, 0},
			distRad: math.Pi / 2,
			want:    r3.Vector{-1, 1, 0},
		},
		{
			x:       r3.Vector{1, 1, 1},
			a:       r3.Vector{1, 0, 0},
			b:       r3.Vector{0, 1, 0},
			distRad: math.Asin(math.Sqrt(1.0 / 3.0)),
			want:    r3.Vector{1, 1, 0},
		},
		{
			x:       r3.Vector{1, 1, -1},
			a:       r3.Vector{1, 0, 0},
			b:       r3.Vector{0, 1, 0},
			distRad: math.Asin(math.Sqrt(1.0 / 3.0)),
			want:    r3.Vector{1, 1, 0}},
		{
			x:       r3.Vector{-1, 0, 0},
			a:       r3.Vector{1, 1, 0},
			b:       r3.Vector{1, 1, 0},
			distRad: 0.75 * math.Pi,
			want:    r3.Vector{1, 1, 0},
		},
		{
			x:       r3.Vector{0, 0, -1},
			a:       r3.Vector{1, 1, 0},
			b:       r3.Vector{1, 1, 0},
			distRad: math.Pi / 2,
			want:    r3.Vector{1, 1, 0},
		},
		{
			x:       r3.Vector{-1, 0, 0},
			a:       r3.Vector{1, 0, 0},
			b:       r3.Vector{1, 0, 0},
			distRad: math.Pi,
			want:    r3.Vector{1, 0, 0},
		},
	}

	for _, test := range tests {
		x := Point{test.x.Normalize()}
		a := Point{test.a.Normalize()}
		b := Point{test.b.Normalize()}
		want := Point{test.want.Normalize()}

		if d := DistanceFromSegment(x, a, b).Radians(); !float64Near(d, test.distRad, 1e-15) {
			t.Errorf("DistanceFromSegment(%v, %v, %v) = %v, want %v", x, a, b, d, test.distRad)
		}

		closest := Project(x, a, b)
		if !closest.ApproxEqual(want) {
			t.Errorf("ClosestPoint(%v, %v, %v) = %v, want %v", x, a, b, closest, want)
		}

		if minDistance, ok := UpdateMinDistance(x, a, b, 0); ok {
			t.Errorf("UpdateMinDistance(%v, %v, %v, %v) = %v, want %v", x, a, b, 0, minDistance, 0)
		}

		minDistance, ok := UpdateMinDistance(x, a, b, s1.InfChordAngle())
		if !ok {
			t.Errorf("UpdateMinDistance(%v, %v, %v, %v) = %v, want %v", x, a, b, s1.InfChordAngle(), minDistance, s1.InfChordAngle())
		}

		if !float64Near(test.distRad, minDistance.Angle().Radians(), 1e-15) {
			t.Errorf("MinDistance between %v and %v,%v = %v, want %v within %v", x, a, b, minDistance.Angle().Radians(), test.distRad, 1e-15)
		}
	}
}
func TestEdgeDistancesInterpolate(t *testing.T) {
	// Choose test points designed to expose floating-point errors.
	p1 := PointFromCoords(0.1, 1e-30, 0.3)
	p2 := PointFromCoords(-0.7, -0.55, -1e30)

	tests := []struct {
		a, b Point
		dist float64
		want Point
	}{
		// A zero-length edge.
		{p1, p1, 0, p1},
		{p1, p1, 1, p1},
		// Start, end, and middle of a medium-length edge.
		{p1, p2, 0, p1},
		{p1, p2, 1, p2},
		{p1, p2, 0.5, Point{(p1.Add(p2.Vector)).Mul(0.5)}},

		// Test that interpolation is done using distances on the sphere
		// rather than linear distances.
		{
			Point{r3.Vector{1, 0, 0}},
			Point{r3.Vector{0, 1, 0}},
			1.0 / 3.0,
			Point{r3.Vector{math.Sqrt(3), 1, 0}},
		},
		{
			Point{r3.Vector{1, 0, 0}},
			Point{r3.Vector{0, 1, 0}},
			2.0 / 3.0,
			Point{r3.Vector{1, math.Sqrt(3), 0}},
		},
	}

	for _, test := range tests {
		test.a = Point{test.a.Normalize()}
		test.b = Point{test.b.Normalize()}
		test.want = Point{test.want.Normalize()}

		// We allow a bit more than the usual 1e-15 error tolerance because
		// Interpolate() uses trig functions.
		if got := Interpolate(test.dist, test.a, test.b); !pointsApproxEquals(got, test.want, 3e-15) {
			t.Errorf("Interpolate(%v, %v, %v) = %v, want %v", test.dist, test.a, test.b, got, test.want)
		}
	}
}

func TestEdgeDistancesInterpolateOverLongEdge(t *testing.T) {
	lng := math.Pi - 1e-2
	a := Point{PointFromLatLng(LatLng{0, 0}).Normalize()}
	b := Point{PointFromLatLng(LatLng{0, s1.Angle(lng)}).Normalize()}

	for f := 0.4; f > 1e-15; f *= 0.1 {
		// Test that interpolation is accurate on a long edge (but not so long that
		// the definition of the edge itself becomes too unstable).
		want := Point{PointFromLatLng(LatLng{0, s1.Angle(f * lng)}).Normalize()}
		if got := Interpolate(f, a, b); !pointsApproxEquals(got, want, 3e-15) {
			t.Errorf("long edge Interpolate(%v, %v, %v) = %v, want %v", f, a, b, got, want)
		}

		// Test the remainder of the dist also matches.
		wantRem := Point{PointFromLatLng(LatLng{0, s1.Angle((1 - f) * lng)}).Normalize()}
		if got := Interpolate(1-f, a, b); !pointsApproxEquals(got, wantRem, 3e-15) {
			t.Errorf("long edge Interpolate(%v, %v, %v) = %v, want %v", 1-f, a, b, got, wantRem)
		}
	}
}

func TestEdgeDistancesInterpolateAntipodal(t *testing.T) {
	p1 := PointFromCoords(0.1, 1e-30, 0.3)

	// Test that interpolation on a 180 degree edge (antipodal endpoints) yields
	// a result with the correct distance from each endpoint.
	for dist := 0.0; dist <= 1.0; dist += 0.125 {
		actual := Interpolate(dist, p1, Point{p1.Mul(-1)})
		if !float64Near(actual.Distance(p1).Radians(), dist*math.Pi, 3e-15) {
			t.Errorf("antipodal points Interpolate(%v, %v, %v) = %v, want %v", dist, p1, Point{p1.Mul(-1)}, actual, dist*math.Pi)
		}
	}
}

func TestEdgeDistancesRepeatedInterpolation(t *testing.T) {
	// Check that points do not drift away from unit length when repeated
	// interpolations are done.
	for i := 0; i < 100; i++ {
		a := randomPoint()
		b := randomPoint()
		for j := 0; j < 1000; j++ {
			a = Interpolate(0.01, a, b)
		}
		if !a.Vector.IsUnit() {
			t.Errorf("repeated Interpolate(%v, %v, %v) calls did not stay unit length for", 0.01, a, b)
		}
	}
}

// TODO(roberts):
// TestEdgeDistancesInterpolateCanExtrapolate
// TestEdgeDistanceUpdateMinDistanceMaxError
// TestEdgeDistancesEdgePairDistance
// TestEdgeDistancesEdgeBNearEdgeA
