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

package s1

import (
	"math"
	"testing"
)

// Some standard intervals for use throughout the tests.
var (
	empty = EmptyInterval()
	full  = FullInterval()
	// Single-point intervals:
	zero  = IntervalFromEndpoints(0, 0)
	pi2   = IntervalFromEndpoints(math.Pi/2, math.Pi/2)
	pi    = IntervalFromEndpoints(math.Pi, math.Pi)
	mipi  = IntervalFromEndpoints(-math.Pi, -math.Pi) // same as pi after normalization
	mipi2 = IntervalFromEndpoints(-math.Pi/2, -math.Pi/2)
	// Single quadrants:
	quad1 = IntervalFromEndpoints(0, math.Pi/2)
	quad2 = IntervalFromEndpoints(math.Pi/2, -math.Pi) // equivalent to (pi/2, pi)
	quad3 = IntervalFromEndpoints(math.Pi, -math.Pi/2)
	quad4 = IntervalFromEndpoints(-math.Pi/2, 0)
	// Quadrant pairs:
	quad12 = IntervalFromEndpoints(0, -math.Pi)
	quad23 = IntervalFromEndpoints(math.Pi/2, -math.Pi/2)
	quad34 = IntervalFromEndpoints(-math.Pi, 0)
	quad41 = IntervalFromEndpoints(-math.Pi/2, math.Pi/2)
	// Quadrant triples:
	quad123 = IntervalFromEndpoints(0, -math.Pi/2)
	quad234 = IntervalFromEndpoints(math.Pi/2, 0)
	quad341 = IntervalFromEndpoints(math.Pi, math.Pi/2)
	quad412 = IntervalFromEndpoints(-math.Pi/2, -math.Pi)
	// Small intervals around the midpoints between quadrants,
	// such that the center of each interval is offset slightly CCW from the midpoint.
	mid12 = IntervalFromEndpoints(math.Pi/2-0.01, math.Pi/2+0.02)
	mid23 = IntervalFromEndpoints(math.Pi-0.01, -math.Pi+0.02)
	mid34 = IntervalFromEndpoints(-math.Pi/2-0.01, -math.Pi/2+0.02)
	mid41 = IntervalFromEndpoints(-0.01, 0.02)
)

func TestConstructors(t *testing.T) {
	// Check that [-π,-π] is normalized to [π,π].
	if mipi.Lo != math.Pi {
		t.Errorf("mipi.Lo = %v, want π", mipi.Lo)
	}
	if mipi.Hi != math.Pi {
		t.Errorf("mipi.Hi = %v, want π", mipi.Lo)
	}

	var i Interval
	if !i.IsValid() {
		t.Errorf("Zero value Interval is not valid")
	}
}

func TestIntervalFromPointPair(t *testing.T) {
	tests := []struct {
		a, b float64
		want Interval
	}{
		{-math.Pi, math.Pi, pi},
		{math.Pi, -math.Pi, pi},
		{mid34.Hi, mid34.Lo, mid34},
		{mid23.Lo, mid23.Hi, mid23},
	}
	for _, test := range tests {
		got := IntervalFromPointPair(test.a, test.b)
		if got != test.want {
			t.Errorf("IntervalFromPointPair(%f, %f) = %v, want %v", test.a, test.b, got, test.want)
		}
	}
}

func TestSimplePredicates(t *testing.T) {
	if !zero.IsValid() || zero.IsEmpty() || zero.IsFull() {
		t.Errorf("Zero interval is invalid or empty or full")
	}
	if !empty.IsValid() || !empty.IsEmpty() || empty.IsFull() {
		t.Errorf("Empty interval is invalid or not empty or full")
	}
	if !empty.IsInverted() {
		t.Errorf("Empty interval is not inverted")
	}
	if !full.IsValid() || full.IsEmpty() || !full.IsFull() {
		t.Errorf("Full interval is invalid or empty or not full")
	}
	if !pi.IsValid() || pi.IsEmpty() || pi.IsInverted() {
		t.Errorf("pi is invalid or empty or inverted")
	}
	if !mipi.IsValid() || mipi.IsEmpty() || mipi.IsInverted() {
		t.Errorf("mipi is invalid or empty or inverted")
	}
}

func TestAlmostFullOrEmpty(t *testing.T) {
	// Test that rounding errors don't cause intervals that are almost empty or
	// full to be considered empty or full.  The following value is the greatest
	// representable value less than Pi.
	almostPi := math.Pi - 2*dblEpsilon

	i := Interval{-almostPi, math.Pi}
	if i.IsFull() {
		t.Errorf("%v.IsFull should not be true", i)
	}

	i = Interval{-math.Pi, almostPi}
	if i.IsFull() {
		t.Errorf("%v.IsFull should not be true", i)
	}

	i = Interval{math.Pi, -almostPi}
	if i.IsEmpty() {
		t.Errorf("%v.IsEmpty should not be true", i)
	}

	i = Interval{almostPi, -math.Pi}
	if i.IsEmpty() {
		t.Errorf("%v.IsEmpty should not be true", i)
	}
}

func TestCenter(t *testing.T) {
	tests := []struct {
		interval Interval
		want     float64
	}{
		{quad12, math.Pi / 2},
		{IntervalFromEndpoints(3.1, 2.9), 3 - math.Pi},
		{IntervalFromEndpoints(-2.9, -3.1), math.Pi - 3},
		{IntervalFromEndpoints(2.1, -2.1), math.Pi},
		{pi, math.Pi},
		{mipi, math.Pi},
		// TODO(dsymonds): The C++ test for quad23 uses fabs. Why?
		{quad23, math.Pi},
		// TODO(dsymonds): The C++ test for quad123 uses EXPECT_DOUBLE_EQ. Why?
		{quad123, 0.75 * math.Pi},
	}
	for _, test := range tests {
		got := test.interval.Center()
		// TODO(dsymonds): Some are inaccurate in the 16th decimal place. Track it down.
		if math.Abs(got-test.want) > 1e-15 {
			t.Errorf("%v.Center() = %v, want %v", test.interval, got, test.want)
		}
	}
}

func TestLength(t *testing.T) {
	tests := []struct {
		interval Interval
		want     float64
	}{
		{quad12, math.Pi},
		{pi, 0},
		{mipi, 0},
		// TODO(dsymonds): The C++ test for quad123 uses DOUBLE_EQ. Why?
		{quad123, 1.5 * math.Pi},
		// TODO(dsymonds): The C++ test for quad23 uses fabs. Why?
		{quad23, math.Pi},
		{full, 2 * math.Pi},
	}
	for _, test := range tests {
		if l := test.interval.Length(); l != test.want {
			t.Errorf("%v.Length() got %v, want %v", test.interval, l, test.want)
		}
	}
	if l := empty.Length(); l >= 0 {
		t.Errorf("empty interval has non-negative length %v", l)
	}
}

func TestContains(t *testing.T) {
	tests := []struct {
		interval  Interval
		in, out   []float64 // points that should be inside/outside the interval
		iIn, iOut []float64 // points that should be inside/outside the interior
	}{
		{empty, nil, []float64{0, math.Pi, -math.Pi}, nil, []float64{math.Pi, -math.Pi}},
		{full, []float64{0, math.Pi, -math.Pi}, nil, []float64{math.Pi, -math.Pi}, nil},
		{quad12, []float64{0, math.Pi, -math.Pi}, nil,
			[]float64{math.Pi / 2}, []float64{0, math.Pi, -math.Pi}},
		{quad23, []float64{math.Pi / 2, -math.Pi / 2, math.Pi, -math.Pi}, []float64{0},
			[]float64{math.Pi, -math.Pi}, []float64{math.Pi / 2, -math.Pi / 2, 0}},
		{pi, []float64{math.Pi, -math.Pi}, []float64{0}, nil, []float64{math.Pi, -math.Pi}},
		{mipi, []float64{math.Pi, -math.Pi}, []float64{0}, nil, []float64{math.Pi, -math.Pi}},
		{zero, []float64{0}, nil, nil, []float64{0}},
	}
	for _, test := range tests {
		for _, p := range test.in {
			if !test.interval.Contains(p) {
				t.Errorf("%v should contain %v", test.interval, p)
			}
		}
		for _, p := range test.out {
			if test.interval.Contains(p) {
				t.Errorf("%v should not contain %v", test.interval, p)
			}
		}
		for _, p := range test.iIn {
			if !test.interval.InteriorContains(p) {
				t.Errorf("interior of %v should contain %v", test.interval, p)
			}
		}
		for _, p := range test.iOut {
			if test.interval.InteriorContains(p) {
				t.Errorf("interior %v should not contain %v", test.interval, p)
			}
		}
	}
}

func TestIntervalOperations(t *testing.T) {
	quad12eps := IntervalFromEndpoints(quad12.Lo, mid23.Hi)
	quad2hi := IntervalFromEndpoints(mid23.Lo, quad12.Hi)
	quad412eps := IntervalFromEndpoints(mid34.Lo, quad12.Hi)
	quadeps12 := IntervalFromEndpoints(mid41.Lo, quad12.Hi)
	quad1lo := IntervalFromEndpoints(quad12.Lo, mid41.Hi)
	quad2lo := IntervalFromEndpoints(quad23.Lo, mid12.Hi)
	quad3hi := IntervalFromEndpoints(mid34.Lo, quad23.Hi)
	quadeps23 := IntervalFromEndpoints(mid12.Lo, quad23.Hi)
	quad23eps := IntervalFromEndpoints(quad23.Lo, mid34.Hi)
	quadeps123 := IntervalFromEndpoints(mid41.Lo, quad23.Hi)

	// This massive list of test cases is ported directly from the C++ test case.
	tests := []struct {
		x, y                               Interval
		xContainsY, xInteriorContainsY     bool
		xIntersectsY, xInteriorIntersectsY bool
		wantUnion, wantIntersection        Interval
	}{
		// 0
		{empty, empty, true, true, false, false, empty, empty},
		{empty, full, false, false, false, false, full, empty},
		{empty, zero, false, false, false, false, zero, empty},
		{empty, pi, false, false, false, false, pi, empty},
		{empty, mipi, false, false, false, false, mipi, empty},

		// 5
		{full, empty, true, true, false, false, full, empty},
		{full, full, true, true, true, true, full, full},
		{full, zero, true, true, true, true, full, zero},
		{full, pi, true, true, true, true, full, pi},
		{full, mipi, true, true, true, true, full, mipi},
		{full, quad12, true, true, true, true, full, quad12},
		{full, quad23, true, true, true, true, full, quad23},

		// 12
		{zero, empty, true, true, false, false, zero, empty},
		{zero, full, false, false, true, false, full, zero},
		{zero, zero, true, false, true, false, zero, zero},
		{zero, pi, false, false, false, false, IntervalFromEndpoints(0, math.Pi), empty},
		{zero, pi2, false, false, false, false, quad1, empty},
		{zero, mipi, false, false, false, false, quad12, empty},
		{zero, mipi2, false, false, false, false, quad4, empty},
		{zero, quad12, false, false, true, false, quad12, zero},
		{zero, quad23, false, false, false, false, quad123, empty},

		// 21
		{pi2, empty, true, true, false, false, pi2, empty},
		{pi2, full, false, false, true, false, full, pi2},
		{pi2, zero, false, false, false, false, quad1, empty},
		{pi2, pi, false, false, false, false, IntervalFromEndpoints(math.Pi/2, math.Pi), empty},
		{pi2, pi2, true, false, true, false, pi2, pi2},
		{pi2, mipi, false, false, false, false, quad2, empty},
		{pi2, mipi2, false, false, false, false, quad23, empty},
		{pi2, quad12, false, false, true, false, quad12, pi2},
		{pi2, quad23, false, false, true, false, quad23, pi2},

		// 30
		{pi, empty, true, true, false, false, pi, empty},
		{pi, full, false, false, true, false, full, pi},
		{pi, zero, false, false, false, false, IntervalFromEndpoints(math.Pi, 0), empty},
		{pi, pi, true, false, true, false, pi, pi},
		{pi, pi2, false, false, false, false, IntervalFromEndpoints(math.Pi/2, math.Pi), empty},
		{pi, mipi, true, false, true, false, pi, pi},
		{pi, mipi2, false, false, false, false, quad3, empty},
		{pi, quad12, false, false, true, false, IntervalFromEndpoints(0, math.Pi), pi},
		{pi, quad23, false, false, true, false, quad23, pi},

		// 39
		{mipi, empty, true, true, false, false, mipi, empty},
		{mipi, full, false, false, true, false, full, mipi},
		{mipi, zero, false, false, false, false, quad34, empty},
		{mipi, pi, true, false, true, false, mipi, mipi},
		{mipi, pi2, false, false, false, false, quad2, empty},
		{mipi, mipi, true, false, true, false, mipi, mipi},
		{mipi, mipi2, false, false, false, false, IntervalFromEndpoints(-math.Pi, -math.Pi/2), empty},
		{mipi, quad12, false, false, true, false, quad12, mipi},
		{mipi, quad23, false, false, true, false, quad23, mipi},

		// 48
		{quad12, empty, true, true, false, false, quad12, empty},
		{quad12, full, false, false, true, true, full, quad12},
		{quad12, zero, true, false, true, false, quad12, zero},
		{quad12, pi, true, false, true, false, quad12, pi},
		{quad12, mipi, true, false, true, false, quad12, mipi},
		{quad12, quad12, true, false, true, true, quad12, quad12},
		{quad12, quad23, false, false, true, true, quad123, quad2},
		{quad12, quad34, false, false, true, false, full, quad12},

		// 56
		{quad23, empty, true, true, false, false, quad23, empty},
		{quad23, full, false, false, true, true, full, quad23},
		{quad23, zero, false, false, false, false, quad234, empty},
		{quad23, pi, true, true, true, true, quad23, pi},
		{quad23, mipi, true, true, true, true, quad23, mipi},
		{quad23, quad12, false, false, true, true, quad123, quad2},
		{quad23, quad23, true, false, true, true, quad23, quad23},
		{quad23, quad34, false, false, true, true, quad234, IntervalFromEndpoints(-math.Pi, -math.Pi/2)},

		// 64
		{quad1, quad23, false, false, true, false, quad123, IntervalFromEndpoints(math.Pi/2, math.Pi/2)},
		{quad2, quad3, false, false, true, false, quad23, mipi},
		{quad3, quad2, false, false, true, false, quad23, pi},
		{quad2, pi, true, false, true, false, quad2, pi},
		{quad2, mipi, true, false, true, false, quad2, mipi},
		{quad3, pi, true, false, true, false, quad3, pi},
		{quad3, mipi, true, false, true, false, quad3, mipi},

		// 71
		{quad12, mid12, true, true, true, true, quad12, mid12},
		{mid12, quad12, false, false, true, true, quad12, mid12},

		// 73
		{quad12, mid23, false, false, true, true, quad12eps, quad2hi},
		{mid23, quad12, false, false, true, true, quad12eps, quad2hi},

		// This test checks that the union of two disjoint intervals is the smallest
		// interval that contains both of them.  Note that the center of "mid34"
		// slightly CCW of -Pi/2 so that there is no ambiguity about the result.
		// 75
		{quad12, mid34, false, false, false, false, quad412eps, empty},
		{mid34, quad12, false, false, false, false, quad412eps, empty},

		// 77
		{quad12, mid41, false, false, true, true, quadeps12, quad1lo},
		{mid41, quad12, false, false, true, true, quadeps12, quad1lo},

		// 79
		{quad23, mid12, false, false, true, true, quadeps23, quad2lo},
		{mid12, quad23, false, false, true, true, quadeps23, quad2lo},
		{quad23, mid23, true, true, true, true, quad23, mid23},
		{mid23, quad23, false, false, true, true, quad23, mid23},
		{quad23, mid34, false, false, true, true, quad23eps, quad3hi},
		{mid34, quad23, false, false, true, true, quad23eps, quad3hi},
		{quad23, mid41, false, false, false, false, quadeps123, empty},
		{mid41, quad23, false, false, false, false, quadeps123, empty},
	}
	should := func(b bool) string {
		if b {
			return "should"
		}
		return "should not"
	}
	for _, test := range tests {
		if test.x.ContainsInterval(test.y) != test.xContainsY {
			t.Errorf("%v %s contain %v", test.x, should(test.xContainsY), test.y)
		}
		if test.x.InteriorContainsInterval(test.y) != test.xInteriorContainsY {
			t.Errorf("interior of %v %s contain %v", test.x, should(test.xInteriorContainsY), test.y)
		}
		if test.x.Intersects(test.y) != test.xIntersectsY {
			t.Errorf("%v %s intersect %v", test.x, should(test.xIntersectsY), test.y)
		}
		if test.x.InteriorIntersects(test.y) != test.xInteriorIntersectsY {
			t.Errorf("interior of %v %s intersect %v", test.x, should(test.xInteriorIntersectsY), test.y)
		}
		if u := test.x.Union(test.y); u != test.wantUnion {
			t.Errorf("%v ∪ %v was %v, want %v", test.x, test.y, u, test.wantUnion)
		}
		if u := test.x.Intersection(test.y); u != test.wantIntersection {
			t.Errorf("%v ∩ %v was %v, want %v", test.x, test.y, u, test.wantIntersection)
		}
	}
}

func TestAddPoint(t *testing.T) {
	tests := []struct {
		interval Interval
		points   []float64
		want     Interval
	}{
		{empty, []float64{0}, zero},
		{empty, []float64{math.Pi}, pi},
		{empty, []float64{-math.Pi}, mipi},
		{empty, []float64{math.Pi, -math.Pi}, pi},
		{empty, []float64{-math.Pi, math.Pi}, mipi},
		{empty, []float64{mid12.Lo, mid12.Hi}, mid12},
		{empty, []float64{mid23.Lo, mid23.Hi}, mid23},

		{quad1, []float64{-0.9 * math.Pi, -math.Pi / 2}, quad123},
		{full, []float64{0}, full},
		{full, []float64{math.Pi}, full},
		{full, []float64{-math.Pi}, full},
	}
	for _, test := range tests {
		got := test.interval
		for _, point := range test.points {
			got = got.AddPoint(point)
		}
		want := test.want
		if math.Abs(got.Lo-want.Lo) > 1e-15 || math.Abs(got.Hi-want.Hi) > 1e-15 {
			t.Errorf("%v.AddPoint(%v) = %v, want %v", test.interval, test.points, got, want)
		}
	}
}

func TestExpanded(t *testing.T) {
	tests := []struct {
		interval Interval
		margin   float64
		want     Interval
	}{
		{empty, 1, empty},
		{full, 1, full},
		{zero, 1, Interval{-1, 1}},
		{mipi, 0.01, Interval{math.Pi - 0.01, -math.Pi + 0.01}},
		{pi, 27, full},
		{pi, math.Pi / 2, quad23},
		{pi2, math.Pi / 2, quad12},
		{mipi2, math.Pi / 2, quad34},

		{empty, -1, empty},
		{full, -1, full},
		{quad123, -27, empty},
		{quad234, -27, empty},
		{quad123, -math.Pi / 2, quad2},
		{quad341, -math.Pi / 2, quad4},
		{quad412, -math.Pi / 2, quad1},
	}
	for _, test := range tests {
		if got, want := test.interval.Expanded(test.margin), test.want; math.Abs(got.Lo-want.Lo) > 1e-15 || math.Abs(got.Hi-want.Hi) > 1e-15 {
			t.Errorf("%v.Expanded(%v) = %v, want %v", test.interval, test.margin, got, want)
		}
	}
}

func TestIntervalString(t *testing.T) {
	if s, exp := pi.String(), "[3.1415927, 3.1415927]"; s != exp {
		t.Errorf("pi.String() = %q, want %q", s, exp)
	}
}
