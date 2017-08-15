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
	"reflect"
	"testing"

	"github.com/golang/geo/r3"
	"github.com/golang/geo/s1"
)

func TestPolylineBasics(t *testing.T) {
	empty := Polyline{}
	if empty.RectBound() != EmptyRect() {
		t.Errorf("empty.RectBound() = %v, want %v", empty.RectBound(), EmptyRect())
	}
	if len(empty) != 0 {
		t.Errorf("empty Polyline should have no vertices")
	}
	empty.Reverse()
	if len(empty) != 0 {
		t.Errorf("reveresed empty Polyline should have no vertices")
	}

	latlngs := []LatLng{
		LatLngFromDegrees(0, 0),
		LatLngFromDegrees(0, 90),
		LatLngFromDegrees(0, 180),
	}

	semiEquator := PolylineFromLatLngs(latlngs)
	//if got, want := semiEquator.Interpolate(0.5), Point{r3.Vector{0, 1, 0}}; !got.ApproxEqual(want) {
	//	t.Errorf("semiEquator.Interpolate(0.5) = %v, want %v", got, want)
	//}
	semiEquator.Reverse()
	if got, want := (*semiEquator)[2], (Point{r3.Vector{1, 0, 0}}); !got.ApproxEqual(want) {
		t.Errorf("semiEquator[2] = %v, want %v", got, want)
	}
}

func TestPolylineShape(t *testing.T) {
	var shape Shape = makePolyline("0:0, 1:0, 1:1, 2:1")
	if got, want := shape.NumEdges(), 3; got != want {
		t.Errorf("%v.NumEdges() = %v, want %d", shape, got, want)
	}
	if got, want := shape.NumChains(), 1; got != want {
		t.Errorf("%v.NumChains() = %d, want %d", shape, got, want)
	}
	if got, want := shape.Chain(0).Start, 0; got != want {
		t.Errorf("%v.Chain(0).Start = %d, want %d", shape, got, want)
	}
	if got, want := shape.Chain(0).Length, 3; got != want {
		t.Errorf("%v.Chain(0).Length = %d, want %d", shape, got, want)
	}

	e := shape.Edge(2)
	if want := PointFromLatLng(LatLngFromDegrees(1, 1)); !e.V0.ApproxEqual(want) {
		t.Errorf("%v.Edge(%d) point A = %v  want %v", shape, 2, e.V0, want)
	}
	if want := PointFromLatLng(LatLngFromDegrees(2, 1)); !e.V1.ApproxEqual(want) {
		t.Errorf("%v.Edge(%d) point B = %v  want %v", shape, 2, e.V1, want)
	}

	if shape.HasInterior() {
		t.Errorf("polylines should not have an interior")
	}
	if shape.ContainsOrigin() {
		t.Errorf("polylines should not contain the origin")
	}

	if shape.dimension() != polylineGeometry {
		t.Errorf("polylines should have PolylineGeometry")
	}

	empty := &Polyline{}
	if got, want := empty.NumEdges(), 0; got != want {
		t.Errorf("%v.NumEdges() = %d, want %d", empty, got, want)
	}
	if got, want := empty.NumChains(), 0; got != want {
		t.Errorf("%v.NumChains() = %d, want %d", empty, got, want)
	}
}

func TestPolylineLengthAndCentroid(t *testing.T) {
	// Construct random great circles and divide them randomly into segments.
	// Then make sure that the length and centroid are correct.  Note that
	// because of the way the centroid is computed, it does not matter how
	// we split the great circle into segments.

	for i := 0; i < 100; i++ {
		// Choose a coordinate frame for the great circle.
		f := randomFrame()

		var line Polyline
		for theta := 0.0; theta < 2*math.Pi; theta += math.Pow(randomFloat64(), 10) {
			p := Point{f.row(0).Mul(math.Cos(theta)).Add(f.row(1).Mul(math.Sin(theta)))}
			if len(line) == 0 || !p.ApproxEqual(line[len(line)-1]) {
				line = append(line, p)
			}
		}

		// Close the circle.
		line = append(line, line[0])

		length := line.Length()
		if got, want := math.Abs(length.Radians()-2*math.Pi), 2e-14; got > want {
			t.Errorf("%v.Length() = %v, want < %v", line, got, want)
		}

		centroid := line.Centroid()
		if got, want := centroid.Norm(), 2e-14; got > want {
			t.Errorf("%v.Norm() = %v, want < %v", centroid, got, want)
		}
	}
}

func TestPolylineIntersectsCell(t *testing.T) {
	pline := Polyline{
		Point{r3.Vector{1, -1.1, 0.8}.Normalize()},
		Point{r3.Vector{1, -0.8, 1.1}.Normalize()},
	}

	for face := 0; face < 6; face++ {
		cell := CellFromCellID(CellIDFromFace(face))
		if got, want := pline.IntersectsCell(cell), face&1 == 0; got != want {
			t.Errorf("%v.IntersectsCell(%v) = %v, want %v", pline, cell, got, want)
		}
	}
}

func TestPolylineSubsample(t *testing.T) {
	const polyStr = "0:0, 0:1, -1:2, 0:3, 0:4, 1:4, 2:4.5, 3:4, 3.5:4, 4:4"

	tests := []struct {
		have      string
		tolerance float64
		want      []int
	}{
		{
			// No vertices.
			tolerance: 1.0,
		},
		{
			// One vertex.
			have:      "0:1",
			tolerance: 1.0,
			want:      []int{0},
		},
		{
			// Two vertices.
			have:      "10:10, 11:11",
			tolerance: 5.0,
			want:      []int{0, 1},
		},
		{
			// Three points on a straight line. In theory, zero tolerance
			// should work, but in practice there are floating point errors.
			have:      "-1:0, 0:0, 1:0",
			tolerance: 1e-15,
			want:      []int{0, 2},
		},
		{
			// Zero tolerance on a non-straight line.
			have:      "-1:0, 0:0, 1:1",
			tolerance: 0.0,
			want:      []int{0, 1, 2},
		},
		{
			// Negative tolerance should return all vertices.
			have:      "-1:0, 0:0, 1:1",
			tolerance: -1.0,
			want:      []int{0, 1, 2},
		},
		{
			// Non-zero tolerance with a straight line.
			have:      "0:1, 0:2, 0:3, 0:4, 0:5",
			tolerance: 1.0,
			want:      []int{0, 4},
		},
		{
			// And finally, verify that we still do something
			// reasonable if the client passes in an invalid polyline
			// with two or more adjacent vertices.
			have:      "0:1, 0:1, 0:1, 0:2",
			tolerance: 0.0,
			want:      []int{0, 3},
		},

		// Simple examples
		{
			have:      polyStr,
			tolerance: 3.0,
			want:      []int{0, 9},
		},
		{
			have:      polyStr,
			tolerance: 2.0,
			want:      []int{0, 6, 9},
		},
		{
			have:      polyStr,
			tolerance: 0.9,
			want:      []int{0, 2, 6, 9},
		},
		{
			have:      polyStr,
			tolerance: 0.4,
			want:      []int{0, 1, 2, 3, 4, 6, 9},
		},
		{
			have:      polyStr,
			tolerance: 0,
			want:      []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},

		// Check that duplicate vertices are never generated.
		{
			have:      "10:10, 12:12, 10:10",
			tolerance: 5.0,
			want:      []int{0},
		},
		{
			have:      "0:0, 1:1, 0:0, 0:120, 0:130",
			tolerance: 5.0,
			want:      []int{0, 3, 4},
		},

		// Check that points are not collapsed if they would create a line segment
		// longer than 90 degrees, and also that the code handles original polyline
		// segments longer than 90 degrees.
		{
			have:      "90:0, 50:180, 20:180, -20:180, -50:180, -90:0, 30:0, 90:0",
			tolerance: 5.0,
			want:      []int{0, 2, 4, 5, 6, 7},
		},

		// Check that the output polyline is parametrically equivalent and not just
		// geometrically equivalent, i.e. that backtracking is preserved.  The
		// algorithm achieves this by requiring that the points must be encountered
		// in increasing order of distance along each output segment, except for
		// points that are within "tolerance" of the first vertex of each segment.
		{
			have:      "10:10, 10:20, 10:30, 10:15, 10:40",
			tolerance: 5.0,
			want:      []int{0, 2, 3, 4},
		},
		{
			have:      "10:10, 10:20, 10:30, 10:10, 10:30, 10:40",
			tolerance: 5.0,
			want:      []int{0, 2, 3, 5},
		},
		{
			have:      "10:10, 12:12, 9:9, 10:20, 10:30",
			tolerance: 5.0,
			want:      []int{0, 4},
		},
	}

	for _, test := range tests {
		p := makePolyline(test.have)
		got := p.SubsampleVertices(s1.Angle(test.tolerance) * s1.Degree)
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("%q.SubsampleVertices(%v°) = %v, want %v", test.have, test.tolerance, got, test.want)
		}
	}
}
