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

// Most of the Rect methods have trivial implementations in terms of the
// Interval class, so most of the testing is done in that unit test.

package r2

import (
	"math"
	"reflect"
	"testing"

	"github.com/golang/geo/r1"
)

var (
	sw = Point{0, 0.25}
	se = Point{0.5, 0.25}
	ne = Point{0.5, 0.75}
	nw = Point{0, 0.75}

	empty   = EmptyRect()
	rect    = RectFromPoints(sw, ne)
	rectMid = RectFromPoints(Point{0.25, 0.5}, Point{0.25, 0.5})
	rectSW  = RectFromPoints(sw, sw)
	rectNE  = RectFromPoints(ne, ne)
)

func float64Eq(x, y float64) bool { return math.Abs(x-y) < 1e-14 }

func pointsApproxEqual(a, b Point) bool {
	return float64Eq(a.X, b.X) && float64Eq(a.Y, b.Y)
}

func TestOrtho(t *testing.T) {
	tests := []struct {
		p    Point
		want Point
	}{
		{Point{0, 0}, Point{0, 0}},
		{Point{0, 1}, Point{-1, 0}},
		{Point{1, 1}, Point{-1, 1}},
		{Point{-4, 7}, Point{-7, -4}},
		{Point{1, math.Sqrt(3)}, Point{-math.Sqrt(3), 1}},
	}
	for _, test := range tests {
		if got := test.p.Ortho(); !pointsApproxEqual(got, test.want) {
			t.Errorf("%v.Ortho() = %v, want %v", test.p, got, test.want)
		}
	}
}

func TestDot(t *testing.T) {
	tests := []struct {
		p    Point
		op   Point
		want float64
	}{
		{Point{0, 0}, Point{0, 0}, 0},
		{Point{0, 1}, Point{0, 0}, 0},
		{Point{1, 1}, Point{4, 3}, 7},
		{Point{-4, 7}, Point{1, 5}, 31},
	}
	for _, test := range tests {
		if got := test.p.Dot(test.op); !float64Eq(got, test.want) {
			t.Errorf("%v.Dot(%v) = %v, want %v", test.p, test.op, got, test.want)
		}
	}
}

func TestCross(t *testing.T) {
	tests := []struct {
		p    Point
		op   Point
		want float64
	}{
		{Point{0, 0}, Point{0, 0}, 0},
		{Point{0, 1}, Point{0, 0}, 0},
		{Point{1, 1}, Point{-1, -1}, 0},
		{Point{1, 1}, Point{4, 3}, -1},
		{Point{1, 5}, Point{-2, 3}, 13},
	}

	for _, test := range tests {
		if got := test.p.Cross(test.op); !float64Eq(got, test.want) {
			t.Errorf("%v.Cross(%v) = %v, want %v", test.p, test.op, got, test.want)
		}
	}
}

func TestNorm(t *testing.T) {
	tests := []struct {
		p    Point
		want float64
	}{
		{Point{0, 0}, 0},
		{Point{0, 1}, 1},
		{Point{-1, 0}, 1},
		{Point{3, 4}, 5},
		{Point{3, -4}, 5},
		{Point{2, 2}, 2 * math.Sqrt(2)},
		{Point{1, math.Sqrt(3)}, 2},
		{Point{29, 29 * math.Sqrt(3)}, 29 * 2},
		{Point{1, 1e15}, 1e15},
		{Point{1e14, math.MaxFloat32 - 1}, math.MaxFloat32},
	}

	for _, test := range tests {
		if !float64Eq(test.p.Norm(), test.want) {
			t.Errorf("%v.Norm() = %v, want %v", test.p, test.p.Norm(), test.want)
		}
	}
}

func TestNormalize(t *testing.T) {
	tests := []struct {
		have Point
		want Point
	}{
		{Point{}, Point{}},
		{Point{0, 0}, Point{0, 0}},
		{Point{0, 1}, Point{0, 1}},
		{Point{-1, 0}, Point{-1, 0}},
		{Point{3, 4}, Point{0.6, 0.8}},
		{Point{3, -4}, Point{0.6, -0.8}},
		{Point{2, 2}, Point{math.Sqrt(2) / 2, math.Sqrt(2) / 2}},
		{Point{7, 7 * math.Sqrt(3)}, Point{0.5, math.Sqrt(3) / 2}},
		{Point{1e21, 1e21 * math.Sqrt(3)}, Point{0.5, math.Sqrt(3) / 2}},
		{Point{1, 1e16}, Point{0, 1}},
		{Point{1e4, math.MaxFloat32 - 1}, Point{0, 1}},
	}

	for _, test := range tests {
		if got := test.have.Normalize(); !pointsApproxEqual(got, test.want) {
			t.Errorf("%v.Normalize() = %v, want %v", test.have, got, test.want)
		}
	}

}

func TestEmptyRect(t *testing.T) {
	if !empty.IsValid() {
		t.Errorf("empty Rect should be valid: %v", empty)
	}
	if !empty.IsEmpty() {
		t.Errorf("empty Rect should be empty: %v", empty)
	}
}

func TestFromVariousTypes(t *testing.T) {
	d1 := RectFromPoints(Point{0.1, 0}, Point{0.25, 1})
	tests := []struct {
		r1, r2 Rect
	}{
		{
			RectFromCenterSize(Point{0.3, 0.5}, Point{0.2, 0.4}),
			RectFromPoints(Point{0.2, 0.3}, Point{0.4, 0.7}),
		},
		{
			RectFromCenterSize(Point{1, 0.1}, Point{0, 2}),
			RectFromPoints(Point{1, -0.9}, Point{1, 1.1}),
		},
		{
			d1,
			Rect{d1.X, d1.Y},
		},
		{
			RectFromPoints(Point{0.15, 0.3}, Point{0.35, 0.9}),
			RectFromPoints(Point{0.15, 0.9}, Point{0.35, 0.3}),
		},
		{
			RectFromPoints(Point{0.12, 0}, Point{0.83, 0.5}),
			RectFromPoints(Point{0.83, 0}, Point{0.12, 0.5}),
		},
	}

	for _, test := range tests {
		if got := test.r1.ApproxEquals(test.r2); !got {
			t.Errorf("%v.ApproxEquals(%v); got %v want true", test.r1, test.r2, got)
		}
	}
}

func TestCenter(t *testing.T) {
	tests := []struct {
		rect Rect
		want Point
	}{
		{empty, Point{0.5, 0.5}},
		{rect, Point{0.25, 0.5}},
	}
	for _, test := range tests {
		if got := test.rect.Center(); got != test.want {
			t.Errorf("%v.Center(); got %v want %v", test.rect, got, test.want)
		}
	}
}

func TestVertices(t *testing.T) {
	want := [4]Point{sw, se, ne, nw}
	got := rect.Vertices()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%v.Vertices(); got %v want %v", rect, got, want)
	}
}

func TestContainsPoint(t *testing.T) {
	tests := []struct {
		rect Rect
		p    Point
		want bool
	}{
		{rect, Point{0.2, 0.4}, true},
		{rect, Point{0.2, 0.8}, false},
		{rect, Point{-0.1, 0.4}, false},
		{rect, Point{0.6, 0.1}, false},
		{rect, Point{rect.X.Lo, rect.Y.Lo}, true},
		{rect, Point{rect.X.Hi, rect.Y.Hi}, true},
	}
	for _, test := range tests {
		if got := test.rect.ContainsPoint(test.p); got != test.want {
			t.Errorf("%v.ContainsPoint(%v); got %v want %v", test.rect, test.p, got, test.want)
		}
	}
}

func TestInteriorContainsPoint(t *testing.T) {
	tests := []struct {
		rect Rect
		p    Point
		want bool
	}{
		// Check corners are not contained.
		{rect, sw, false},
		{rect, ne, false},
		// Check a point on the border is not contained.
		{rect, Point{0, 0.5}, false},
		{rect, Point{0.25, 0.25}, false},
		{rect, Point{0.5, 0.5}, false},
		// Check points inside are contained.
		{rect, Point{0.125, 0.6}, true},
	}
	for _, test := range tests {
		if got := test.rect.InteriorContainsPoint(test.p); got != test.want {
			t.Errorf("%v.InteriorContainsPoint(%v); got %v want %v",
				test.rect, test.p, got, test.want)
		}
	}
}

func TestIntervalOps(t *testing.T) {
	tests := []struct {
		r1, r2                                           Rect
		contains, intContains, intersects, intIntersects bool
		wantUnion, wantIntersection                      Rect
	}{
		{
			rect, rectMid,
			true, true, true, true,
			rect, rectMid,
		},
		{
			rect, rectSW,
			true, false, true, false,
			rect, rectSW,
		},
		{
			rect, rectNE,
			true, false, true, false,
			rect, rectNE,
		},
		{
			rect,
			RectFromPoints(Point{0.45, 0.1}, Point{0.75, 0.3}),
			false, false, true, true,
			RectFromPoints(Point{0, 0.1}, Point{0.75, 0.75}),
			RectFromPoints(Point{0.45, 0.25}, Point{0.5, 0.3}),
		},
		{
			rect,
			RectFromPoints(Point{0.5, 0.1}, Point{0.7, 0.3}),
			false, false, true, false,
			RectFromPoints(Point{0, 0.1}, Point{0.7, 0.75}),
			RectFromPoints(Point{0.5, 0.25}, Point{0.5, 0.3}),
		},
		{
			rect,
			RectFromPoints(Point{0.45, 0.1}, Point{0.7, 0.25}),
			false, false, true, false,
			RectFromPoints(Point{0, 0.1}, Point{0.7, 0.75}),
			RectFromPoints(Point{0.45, 0.25}, Point{0.5, 0.25}),
		},
		{
			RectFromPoints(Point{0.1, 0.2}, Point{0.1, 0.3}),
			RectFromPoints(Point{0.15, 0.7}, Point{0.2, 0.8}),
			false, false, false, false,
			RectFromPoints(Point{0.1, 0.2}, Point{0.2, 0.8}),
			EmptyRect(),
		},
		// Check that the intersection of two rectangles that overlap in x but not y
		// is valid, and vice versa.
		{
			RectFromPoints(Point{0.1, 0.2}, Point{0.4, 0.5}),
			RectFromPoints(Point{0, 0}, Point{0.2, 0.1}),
			false, false, false, false,
			RectFromPoints(Point{0, 0}, Point{0.4, 0.5}),
			EmptyRect(),
		},
		{
			RectFromPoints(Point{0, 0}, Point{0.1, 0.3}),
			RectFromPoints(Point{0.2, 0.1}, Point{0.3, 0.4}),
			false, false, false, false,
			RectFromPoints(Point{0, 0}, Point{0.3, 0.4}),
			EmptyRect(),
		},
	}
	for _, test := range tests {
		if got := test.r1.Contains(test.r2); got != test.contains {
			t.Errorf("%v.Contains(%v); got %v want %v",
				test.r1, test.r2, got, test.contains)
		}

		if got := test.r1.InteriorContains(test.r2); got != test.intContains {
			t.Errorf("%v.InteriorContains(%v); got %v want %v",
				test.r1, test.r2, got, test.contains)
		}

		if got := test.r1.Intersects(test.r2); got != test.intersects {
			t.Errorf("%v.Intersects(%v); got %v want %v",
				test.r1, test.r2, got, test.intersects)
		}

		if got := test.r1.InteriorIntersects(test.r2); got != test.intIntersects {
			t.Errorf("%v.InteriorIntersects(%v); got %v want %v",
				test.r1, test.r2, got, test.intIntersects)
		}

		tCon := test.r1.Contains(test.r2)
		if got := test.r1.Union(test.r2).ApproxEquals(test.r1); got != tCon {
			t.Errorf("%v.Union(%v) == %v.Contains(%v); got %v want %v",
				test.r1, test.r2, test.r1, test.r2, got, tCon)
		}

		tInter := test.r1.Intersects(test.r2)
		if got := !test.r1.Intersection(test.r2).IsEmpty(); got != tInter {
			t.Errorf("%v.Intersection(%v).IsEmpty() == %v.Intersects(%v); got %v want %v",
				test.r1, test.r2, test.r1, test.r2, got, tInter)
		}

		if got := test.r1.Union(test.r2); got != test.wantUnion {
			t.Errorf("%v.Union(%v); got %v want %v",
				test.r1, test.r2, got, test.wantUnion)
		}

		if got := test.r1.Intersection(test.r2); got != test.wantIntersection {
			t.Errorf("%v.Intersection(%v); got %v want %v",
				test.r1, test.r2, got, test.wantIntersection)
		}

		r := test.r1.AddRect(test.r2)

		if r != test.wantUnion {
			t.Errorf("%v.AddRect(%v); got %v want %v", test.r1, test.r2, r, test.wantUnion)
		}
	}
}

func TestAddPoint(t *testing.T) {
	r1 := rect
	r2 := EmptyRect()

	r2 = r2.AddPoint(sw)
	r2 = r2.AddPoint(se)
	r2 = r2.AddPoint(nw)
	r2 = r2.AddPoint(Point{0.1, 0.4})

	if !r1.ApproxEquals(r2) {
		t.Errorf("%v.AddPoint(%v); got false want true", r1, r2)
	}
}

func TestClampPoint(t *testing.T) {
	r := Rect{r1.Interval{Lo: 0, Hi: 0.5}, r1.Interval{Lo: 0.25, Hi: 0.75}}
	tests := []struct {
		p    Point
		want Point
	}{
		{Point{-0.01, 0.24}, Point{0, 0.25}},
		{Point{-5.0, 0.48}, Point{0, 0.48}},
		{Point{-5.0, 2.48}, Point{0, 0.75}},
		{Point{0.19, 2.48}, Point{0.19, 0.75}},

		{Point{6.19, 2.48}, Point{0.5, 0.75}},
		{Point{6.19, 0.53}, Point{0.5, 0.53}},
		{Point{6.19, -2.53}, Point{0.5, 0.25}},
		{Point{0.33, -2.53}, Point{0.33, 0.25}},
		{Point{0.33, 0.37}, Point{0.33, 0.37}},
	}
	for _, test := range tests {
		if got := r.ClampPoint(test.p); got != test.want {
			t.Errorf("%v.ClampPoint(%v); got %v want %v", r, test.p, got, test.want)
		}
	}
}

func TestExpandedEmpty(t *testing.T) {
	tests := []struct {
		rect Rect
		p    Point
	}{
		{
			EmptyRect(),
			Point{0.1, 0.3},
		},
		{
			EmptyRect(),
			Point{-0.1, -0.3},
		},
		{
			RectFromPoints(Point{0.2, 0.4}, Point{0.3, 0.7}),
			Point{-0.1, 0.3},
		},
		{
			RectFromPoints(Point{0.2, 0.4}, Point{0.3, 0.7}),
			Point{0.1, -0.2},
		},
	}
	for _, test := range tests {
		if got := test.rect.Expanded(test.p); !got.IsEmpty() {
			t.Errorf("%v.Expanded(%v); got %v want true", test.rect, test.p, got.IsEmpty())
		}
	}
}

func TestExpandedEquals(t *testing.T) {
	tests := []struct {
		rect Rect
		p    Point
		want Rect
	}{
		{
			RectFromPoints(Point{0.2, 0.4}, Point{0.3, 0.7}),
			Point{0.1, 0.3},
			RectFromPoints(Point{0.1, 0.1}, Point{0.4, 1.0}),
		},
		{
			RectFromPoints(Point{0.2, 0.4}, Point{0.3, 0.7}),
			Point{0.1, -0.1},
			RectFromPoints(Point{0.1, 0.5}, Point{0.4, 0.6}),
		},
		{
			RectFromPoints(Point{0.2, 0.4}, Point{0.3, 0.7}),
			Point{0.1, 0.1},
			RectFromPoints(Point{0.1, 0.3}, Point{0.4, 0.8}),
		},
	}
	for _, test := range tests {
		if got := test.rect.Expanded(test.p); !got.ApproxEquals(test.want) {
			t.Errorf("%v.Expanded(%v); got %v want %v", test.rect, test.p, got, test.want)
		}
	}
}
