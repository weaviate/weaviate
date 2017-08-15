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

	"github.com/golang/geo/r1"
	"github.com/golang/geo/r3"
	"github.com/golang/geo/s1"
)

func rectBoundForPoints(a, b Point) Rect {
	bounder := NewRectBounder()
	bounder.AddPoint(a)
	bounder.AddPoint(b)
	return bounder.RectBound()
}

func TestRectBounderMaxLatitudeSimple(t *testing.T) {
	cubeLat := math.Asin(1 / math.Sqrt(3)) // 35.26 degrees
	cubeLatRect := Rect{r1.IntervalFromPoint(-cubeLat).AddPoint(cubeLat),
		s1.IntervalFromEndpoints(-math.Pi/4, math.Pi/4)}

	tests := []struct {
		a, b Point
		want Rect
	}{
		// Check cases where the min/max latitude is attained at a vertex.
		{
			a:    Point{r3.Vector{1, 1, 1}},
			b:    Point{r3.Vector{1, -1, -1}},
			want: cubeLatRect,
		},
		{
			a:    Point{r3.Vector{1, -1, 1}},
			b:    Point{r3.Vector{1, 1, -1}},
			want: cubeLatRect,
		},
	}

	for _, test := range tests {
		if got := rectBoundForPoints(test.a, test.b); !rectsApproxEqual(got, test.want, rectErrorLat, rectErrorLng) {
			t.Errorf("RectBounder for points (%v, %v) near max lat failed: got %v, want %v", test.a, test.b, got, test.want)
		}
	}
}

func TestRectBounderMaxLatitudeEdgeInterior(t *testing.T) {
	// Check cases where the min/max latitude occurs in the edge interior.
	// These tests expect the result to be pretty close to the middle of the
	// allowable error range (i.e., by adding 0.5 * kRectError).

	tests := []struct {
		got  float64
		want float64
	}{
		// Max latitude, CW edge
		{
			math.Pi/4 + 0.5*rectErrorLat,
			rectBoundForPoints(Point{r3.Vector{1, 1, 1}}, Point{r3.Vector{1, -1, 1}}).Lat.Hi,
		},
		// Min latitude, CW edge
		{
			-math.Pi/4 - 0.5*rectErrorLat,
			rectBoundForPoints(Point{r3.Vector{1, -1, -1}}, Point{r3.Vector{-1, -1, -1}}).Lat.Lo,
		},
		// Max latitude, CCW edge
		{
			math.Pi/4 + 0.5*rectErrorLat,
			rectBoundForPoints(Point{r3.Vector{1, -1, 1}}, Point{r3.Vector{1, 1, 1}}).Lat.Hi,
		},
		// Min latitude, CCW edge
		{
			-math.Pi/4 - 0.5*rectErrorLat,
			rectBoundForPoints(Point{r3.Vector{-1, 1, -1}}, Point{r3.Vector{-1, -1, -1}}).Lat.Lo,
		},

		// Check cases where the edge passes through one of the poles.
		{
			math.Pi / 2,
			rectBoundForPoints(Point{r3.Vector{.3, .4, 1}}, Point{r3.Vector{-.3, -.4, 1}}).Lat.Hi,
		},
		{
			-math.Pi / 2,
			rectBoundForPoints(Point{r3.Vector{.3, .4, -1}}, Point{r3.Vector{-.3, -.4, -1}}).Lat.Lo,
		},
	}

	for _, test := range tests {
		if !float64Eq(test.got, test.want) {
			t.Errorf("RectBound for max lat on interior of edge failed; got %v want %v", test.got, test.want)
		}
	}
}

func TestRectBounderMaxLatitudeRandom(t *testing.T) {
	// Check that the maximum latitude of edges is computed accurately to within
	// 3 * dblEpsilon (the expected maximum error). We concentrate on maximum
	// latitudes near the equator and north pole since these are the extremes.

	for i := 0; i < 100; i++ {
		// Construct a right-handed coordinate frame (U,V,W) such that U points
		// slightly above the equator, V points at the equator, and W is slightly
		// offset from the north pole.
		u := randomPoint()
		u.Z = dblEpsilon * 1e-6 * math.Pow(1e12, randomFloat64())

		u = Point{u.Normalize()}
		v := Point{PointFromCoords(0, 0, 1).PointCross(u).Normalize()}
		w := Point{u.PointCross(v).Normalize()}

		// Construct a line segment AB that passes through U, and check that the
		// maximum latitude of this segment matches the latitude of U.
		a := Point{u.Sub(v.Mul(randomFloat64())).Normalize()}
		b := Point{u.Add(v.Mul(randomFloat64())).Normalize()}
		abBound := rectBoundForPoints(a, b)
		if !float64Near(latitude(u).Radians(), abBound.Lat.Hi, rectErrorLat) {
			t.Errorf("bound for line AB not near enough to the latitude of point %v. got %v, want %v",
				u, latitude(u).Radians(), abBound.Lat.Hi)
		}

		// Construct a line segment CD that passes through W, and check that the
		// maximum latitude of this segment matches the latitude of W.
		c := Point{w.Sub(v.Mul(randomFloat64())).Normalize()}
		d := Point{w.Add(v.Mul(randomFloat64())).Normalize()}
		cdBound := rectBoundForPoints(c, d)
		if !float64Near(latitude(w).Radians(), cdBound.Lat.Hi, rectErrorLat) {
			t.Errorf("bound for line CD not near enough to the lat of point %v. got %v, want %v",
				v, latitude(w).Radians(), cdBound.Lat.Hi)
		}
	}
}

func TestRectBounderExpandForSubregions(t *testing.T) {
	// Test the full and empty bounds.
	if !ExpandForSubregions(FullRect()).IsFull() {
		t.Errorf("Subregion Bound of full rect should be full")
	}
	if !ExpandForSubregions(EmptyRect()).IsEmpty() {
		t.Errorf("Subregion Bound of empty rect should be empty")
	}

	tests := []struct {
		xLat, xLng, yLat, yLng float64
		wantFull               bool
	}{
		// Cases where the bound does not straddle the equator (but almost does),
		// and spans nearly 180 degrees in longitude.
		{3e-16, 0, 1e-14, math.Pi, true},
		{9e-16, 0, 1e-14, math.Pi, false},
		{1e-16, 7e-16, 1e-14, math.Pi, true},
		{3e-16, 14e-16, 1e-14, math.Pi, false},
		{1e-100, 14e-16, 1e-14, math.Pi, true},
		{1e-100, 22e-16, 1e-14, math.Pi, false},
		// Cases where the bound spans at most 90 degrees in longitude, and almost
		// 180 degrees in latitude.  Note that DBL_EPSILON is about 2.22e-16, which
		// implies that the double-precision value just below Pi/2 can be written as
		// (math.Pi/2 - 2e-16).
		{-math.Pi / 2, -1e-15, math.Pi/2 - 7e-16, 0, true},
		{-math.Pi / 2, -1e-15, math.Pi/2 - 30e-16, 0, false},
		{-math.Pi/2 + 4e-16, 0, math.Pi/2 - 2e-16, 1e-7, true},
		{-math.Pi/2 + 30e-16, 0, math.Pi / 2, 1e-7, false},
		{-math.Pi/2 + 4e-16, 0, math.Pi/2 - 4e-16, math.Pi / 2, true},
		{-math.Pi / 2, 0, math.Pi/2 - 30e-16, math.Pi / 2, false},
		// Cases where the bound straddles the equator and spans more than 90
		// degrees in longitude.  These are the cases where the critical distance is
		// between a corner of the bound and the opposite longitudinal edge.  Unlike
		// the cases above, here the bound may contain nearly-antipodal points (to
		// within 3.055 * DBL_EPSILON) even though the latitude and longitude ranges
		// are both significantly less than (math.Pi - 3.055 * DBL_EPSILON).
		{-math.Pi / 2, 0, math.Pi/2 - 1e-8, math.Pi - 1e-7, true},
		{-math.Pi / 2, 0, math.Pi/2 - 1e-7, math.Pi - 1e-7, false},
		{-math.Pi/2 + 1e-12, -math.Pi + 1e-4, math.Pi / 2, 0, true},
		{-math.Pi/2 + 1e-11, -math.Pi + 1e-4, math.Pi / 2, 0, true},
	}

	for _, tc := range tests {
		in := RectFromLatLng(LatLng{s1.Angle(tc.xLat), s1.Angle(tc.xLng)})
		in = in.AddPoint(LatLng{s1.Angle(tc.yLat), s1.Angle(tc.yLng)})
		got := ExpandForSubregions(in)

		// Test that the bound is actually expanded.
		if !got.Contains(in) {
			t.Errorf("Subregion bound of (%f, %f, %f, %f) should contain original rect", tc.xLat, tc.xLng, tc.yLat, tc.yLng)
		}
		if in.Lat == validRectLatRange && in.Lat.ContainsInterval(got.Lat) {
			t.Errorf("Subregion bound of (%f, %f, %f, %f) shouldn't be contained by original rect", tc.xLat, tc.xLng, tc.yLat, tc.yLng)
		}

		// We check the various situations where the bound contains nearly-antipodal points. The tests are organized into pairs
		// where the two bounds are similar except that the first bound meets the nearly-antipodal criteria while the second does not.
		if got.IsFull() != tc.wantFull {
			t.Errorf("Subregion Bound of (%f, %f, %f, %f).IsFull should be %t", tc.xLat, tc.xLng, tc.yLat, tc.yLng, tc.wantFull)
		}
	}

	rectTests := []struct {
		xLat, xLng, yLat, yLng float64
		wantRect               Rect
	}{
		{1.5, -math.Pi / 2, 1.5, math.Pi/2 - 2e-16, Rect{r1.Interval{1.5, 1.5}, s1.FullInterval()}},
		{1.5, -math.Pi / 2, 1.5, math.Pi/2 - 7e-16, Rect{r1.Interval{1.5, 1.5}, s1.Interval{-math.Pi / 2, math.Pi/2 - 7e-16}}},
		// Check for cases where the bound is expanded to include one of the poles
		{-math.Pi/2 + 1e-15, 0, -math.Pi/2 + 1e-15, 0, Rect{r1.Interval{-math.Pi / 2, -math.Pi/2 + 1e-15}, s1.FullInterval()}},
		{math.Pi/2 - 1e-15, 0, math.Pi/2 - 1e-15, 0, Rect{r1.Interval{math.Pi/2 - 1e-15, math.Pi / 2}, s1.FullInterval()}},
	}

	for _, tc := range rectTests {
		// Now we test cases where the bound does not contain nearly-antipodal
		// points, but it does contain points that are approximately 180 degrees
		// apart in latitude.
		in := RectFromLatLng(LatLng{s1.Angle(tc.xLat), s1.Angle(tc.xLng)})
		in = in.AddPoint(LatLng{s1.Angle(tc.yLat), s1.Angle(tc.yLng)})
		got := ExpandForSubregions(in)
		if !rectsApproxEqual(got, tc.wantRect, rectErrorLat, rectErrorLng) {
			t.Errorf("Subregion Bound of (%f, %f, %f, %f) = (%v) should be %v", tc.xLat, tc.xLng, tc.yLat, tc.yLng, got, tc.wantRect)
		}
	}
}

// TODO(roberts): TestRectBounderNearlyIdenticalOrAntipodalPoints(t *testing.T)
