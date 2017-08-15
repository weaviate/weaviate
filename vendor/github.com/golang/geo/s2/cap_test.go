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

const (
	tinyRad = 1e-10
)

var (
	emptyCap   = EmptyCap()
	fullCap    = FullCap()
	defaultCap = EmptyCap()

	zeroHeight  = 0.0
	fullHeight  = 2.0
	emptyHeight = -1.0

	xAxisPt = Point{r3.Vector{1, 0, 0}}
	yAxisPt = Point{r3.Vector{0, 1, 0}}

	xAxis = CapFromPoint(xAxisPt)
	yAxis = CapFromPoint(yAxisPt)
	xComp = xAxis.Complement()

	hemi = CapFromCenterHeight(PointFromCoords(1, 0, 1), 1)
	tiny = CapFromCenterAngle(PointFromCoords(1, 2, 3), s1.Angle(tinyRad))

	// A concave cap. Note that the error bounds for point containment tests
	// increase with the cap angle, so we need to use a larger error bound
	// here.
	concaveCenter = PointFromLatLng(LatLngFromDegrees(80, 10))
	concaveRadius = s1.ChordAngleFromAngle(150 * s1.Degree)
	maxCapError   = concaveRadius.MaxPointError() + concaveRadius.MaxAngleError() + 3*dblEpsilon
	concave       = CapFromCenterChordAngle(concaveCenter, concaveRadius)
	concaveMin    = CapFromCenterChordAngle(concaveCenter, concaveRadius.Expanded(-maxCapError))
	concaveMax    = CapFromCenterChordAngle(concaveCenter, concaveRadius.Expanded(maxCapError))
)

func TestCapBasicEmptyFullValid(t *testing.T) {
	tests := []struct {
		got                Cap
		empty, full, valid bool
	}{
		{Cap{}, false, false, false},

		{emptyCap, true, false, true},
		{emptyCap.Complement(), false, true, true},
		{fullCap, false, true, true},
		{fullCap.Complement(), true, false, true},
		{defaultCap, true, false, true},

		{xComp, false, true, true},
		{xComp.Complement(), true, false, true},

		{tiny, false, false, true},
		{concave, false, false, true},
		{hemi, false, false, true},
		{tiny, false, false, true},
	}
	for _, test := range tests {
		if e := test.got.IsEmpty(); e != test.empty {
			t.Errorf("%v.IsEmpty() = %t; want %t", test.got, e, test.empty)
		}
		if f := test.got.IsFull(); f != test.full {
			t.Errorf("%v.IsFull() = %t; want %t", test.got, f, test.full)
		}
		if v := test.got.IsValid(); v != test.valid {
			t.Errorf("%v.IsValid() = %t; want %t", test.got, v, test.valid)
		}
	}
}

func TestCapCenterHeightRadius(t *testing.T) {
	if xAxis == xAxis.Complement().Complement() {
		t.Errorf("the complement of the complement is not the original. %v == %v",
			xAxis, xAxis.Complement().Complement())
	}

	if fullCap.Height() != fullHeight {
		t.Error("full Caps should be full height")
	}
	if fullCap.Radius().Degrees() != 180.0 {
		t.Error("radius of x-axis cap should be 180 degrees")
	}

	if emptyCap.center != defaultCap.center {
		t.Error("empty Caps should be have the same center as the default")
	}
	if emptyCap.Height() != defaultCap.Height() {
		t.Error("empty Caps should be have the same height as the default")
	}

	if yAxis.Height() != zeroHeight {
		t.Error("y-axis cap should not be empty height")
	}

	if xAxis.Height() != zeroHeight {
		t.Error("x-axis cap should not be empty height")
	}
	if xAxis.Radius().Radians() != zeroHeight {
		t.Errorf("radius of x-axis cap got %f want %f", xAxis.Radius().Radians(), emptyHeight)
	}

	hc := Point{hemi.center.Mul(-1.0)}
	if hc != hemi.Complement().center {
		t.Error("hemi center and its complement should have the same center")
	}
	if hemi.Height() != 1.0 {
		t.Error("hemi cap should be 1.0 in height")
	}
}

func TestCapContains(t *testing.T) {
	tests := []struct {
		c1, c2 Cap
		want   bool
	}{
		{emptyCap, emptyCap, true},
		{fullCap, emptyCap, true},
		{fullCap, fullCap, true},
		{emptyCap, xAxis, false},
		{fullCap, xAxis, true},
		{xAxis, fullCap, false},
		{xAxis, xAxis, true},
		{xAxis, emptyCap, true},
		{hemi, tiny, true},
		{hemi, CapFromCenterAngle(xAxisPt, s1.Angle(math.Pi/4-epsilon)), true},
		{hemi, CapFromCenterAngle(xAxisPt, s1.Angle(math.Pi/4+epsilon)), false},
		{concave, hemi, true},
		{concave, CapFromCenterHeight(Point{concave.center.Mul(-1.0)}, 0.1), false},
	}
	for _, test := range tests {
		if got := test.c1.Contains(test.c2); got != test.want {
			t.Errorf("%v.Contains(%v) = %t; want %t", test.c1, test.c2, got, test.want)
		}
	}
}

func TestCapContainsPoint(t *testing.T) {
	tangent := tiny.center.Cross(r3.Vector{3, 2, 1}).Normalize()
	tests := []struct {
		c    Cap
		p    Point
		want bool
	}{
		{xAxis, xAxisPt, true},
		{xAxis, Point{r3.Vector{1, 1e-20, 0}}, false},
		{yAxis, xAxis.center, false},
		{xComp, xAxis.center, true},
		{xComp.Complement(), xAxis.center, false},
		{tiny, Point{tiny.center.Add(tangent.Mul(tinyRad * 0.99))}, true},
		{tiny, Point{tiny.center.Add(tangent.Mul(tinyRad * 1.01))}, false},
		{hemi, PointFromCoords(1, 0, -(1 - epsilon)), true},
		{hemi, xAxisPt, true},
		{hemi.Complement(), xAxisPt, false},
		{concaveMax, PointFromLatLng(LatLngFromDegrees(-70*(1-epsilon), 10)), true},
		{concaveMin, PointFromLatLng(LatLngFromDegrees(-70*(1+epsilon), 10)), false},
		{concaveMax, PointFromLatLng(LatLngFromDegrees(-50*(1-epsilon), -170)), true},
		{concaveMin, PointFromLatLng(LatLngFromDegrees(-50*(1+epsilon), -170)), false},
	}
	for _, test := range tests {
		if got := test.c.ContainsPoint(test.p); got != test.want {
			t.Errorf("%v.ContainsPoint(%v) = %t, want %t", test.c, test.p, got, test.want)
		}
	}
}

func TestCapInteriorIntersects(t *testing.T) {
	tests := []struct {
		c1, c2 Cap
		want   bool
	}{
		{emptyCap, emptyCap, false},
		{emptyCap, xAxis, false},
		{fullCap, emptyCap, false},
		{fullCap, fullCap, true},
		{fullCap, xAxis, true},
		{xAxis, fullCap, false},
		{xAxis, xAxis, false},
		{xAxis, emptyCap, false},
		{concave, hemi.Complement(), true},
	}
	for _, test := range tests {
		if got := test.c1.InteriorIntersects(test.c2); got != test.want {
			t.Errorf("%v.InteriorIntersects(%v); got %t want %t", test.c1, test.c2, got, test.want)
		}
	}
}

func TestCapInteriorContains(t *testing.T) {
	if hemi.InteriorContainsPoint(Point{r3.Vector{1, 0, -(1 + epsilon)}}) {
		t.Errorf("hemi (%v) should not contain point just past half way(%v)", hemi,
			Point{r3.Vector{1, 0, -(1 + epsilon)}})
	}
}

func TestCapExpanded(t *testing.T) {
	cap50 := CapFromCenterAngle(xAxisPt, 50.0*s1.Degree)
	cap51 := CapFromCenterAngle(xAxisPt, 51.0*s1.Degree)

	if !emptyCap.Expanded(s1.Angle(fullHeight)).IsEmpty() {
		t.Error("Expanding empty cap should return an empty cap")
	}
	if !fullCap.Expanded(s1.Angle(fullHeight)).IsFull() {
		t.Error("Expanding a full cap should return an full cap")
	}

	if !cap50.Expanded(0).ApproxEqual(cap50) {
		t.Error("Expanding a cap by 0° should be equal to the original")
	}
	if !cap50.Expanded(1 * s1.Degree).ApproxEqual(cap51) {
		t.Error("Expanding 50° by 1° should equal the 51° cap")
	}

	if cap50.Expanded(129.99 * s1.Degree).IsFull() {
		t.Error("Expanding 50° by 129.99° should not give a full cap")
	}
	if !cap50.Expanded(130.01 * s1.Degree).IsFull() {
		t.Error("Expanding 50° by 130.01° should give a full cap")
	}
}

func TestCapRadiusToHeight(t *testing.T) {
	tests := []struct {
		got  s1.Angle
		want float64
	}{
		// Above/below boundary checks.
		{s1.Angle(-0.5), emptyHeight},
		{s1.Angle(0), 0},
		{s1.Angle(math.Pi), fullHeight},
		{s1.Angle(2 * math.Pi), fullHeight},
		// Degree tests.
		{-7.0 * s1.Degree, emptyHeight},
		{-0.0 * s1.Degree, 0},
		{0.0 * s1.Degree, 0},
		{12.0 * s1.Degree, 0.0218523992661943},
		{30.0 * s1.Degree, 0.1339745962155613},
		{45.0 * s1.Degree, 0.2928932188134525},
		{90.0 * s1.Degree, 1.0},
		{179.99 * s1.Degree, 1.9999999847691292},
		{180.0 * s1.Degree, fullHeight},
		{270.0 * s1.Degree, fullHeight},
		// Radians tests.
		{-1.0 * s1.Radian, emptyHeight},
		{-0.0 * s1.Radian, 0},
		{0.0 * s1.Radian, 0},
		{1.0 * s1.Radian, 0.45969769413186},
		{math.Pi / 2.0 * s1.Radian, 1.0},
		{2.0 * s1.Radian, 1.4161468365471424},
		{3.0 * s1.Radian, 1.9899924966004454},
		{math.Pi * s1.Radian, fullHeight},
		{4.0 * s1.Radian, fullHeight},
	}
	for _, test := range tests {
		// float64Eq comes from s2latlng_test.go
		if got := radiusToHeight(test.got); !float64Eq(got, test.want) {
			t.Errorf("radiusToHeight(%v) = %v; want %v", test.got, got, test.want)
		}
	}
}

func TestCapRectBounds(t *testing.T) {
	const epsilon = 1e-13
	var tests = []struct {
		desc     string
		have     Cap
		latLoDeg float64
		latHiDeg float64
		lngLoDeg float64
		lngHiDeg float64
		isFull   bool
	}{
		{
			"Cap that includes South Pole.",
			CapFromCenterAngle(PointFromLatLng(LatLngFromDegrees(-45, 57)), s1.Degree*50),
			-90, 5, -180, 180, true,
		},
		{
			"Cap that is tangent to the North Pole.",
			CapFromCenterAngle(PointFromCoords(1, 0, 1), s1.Radian*(math.Pi/4.0+1e-16)),
			0, 90, -180, 180, true,
		},
		{
			"Cap that at 45 degree center that goes from equator to the pole.",
			CapFromCenterAngle(PointFromCoords(1, 0, 1), s1.Degree*(45+5e-15)),
			0, 90, -180, 180, true,
		},
		{
			"The eastern hemisphere.",
			CapFromCenterAngle(Point{r3.Vector{0, 1, 0}}, s1.Radian*(math.Pi/2+2e-16)),
			-90, 90, -180, 180, true,
		},
		{
			"A cap centered on the equator.",
			CapFromCenterAngle(PointFromLatLng(LatLngFromDegrees(0, 50)), s1.Degree*20),
			-20, 20, 30, 70, false,
		},
		{
			"A cap centered on the North Pole.",
			CapFromCenterAngle(PointFromLatLng(LatLngFromDegrees(90, 123)), s1.Degree*10),
			80, 90, -180, 180, true,
		},
	}

	for _, test := range tests {
		r := test.have.RectBound()
		if !float64Near(s1.Angle(r.Lat.Lo).Degrees(), test.latLoDeg, epsilon) {
			t.Errorf("%s: %v.RectBound(), Lat.Lo not close enough, got %0.20f, want %0.20f",
				test.desc, test.have, s1.Angle(r.Lat.Lo).Degrees(), test.latLoDeg)
		}
		if !float64Near(s1.Angle(r.Lat.Hi).Degrees(), test.latHiDeg, epsilon) {
			t.Errorf("%s: %v.RectBound(), Lat.Hi not close enough, got %0.20f, want %0.20f",
				test.desc, test.have, s1.Angle(r.Lat.Hi).Degrees(), test.latHiDeg)
		}
		if !float64Near(s1.Angle(r.Lng.Lo).Degrees(), test.lngLoDeg, epsilon) {
			t.Errorf("%s: %v.RectBound(), Lng.Lo not close enough, got %0.20f, want %0.20f",
				test.desc, test.have, s1.Angle(r.Lng.Lo).Degrees(), test.lngLoDeg)
		}
		if !float64Near(s1.Angle(r.Lng.Hi).Degrees(), test.lngHiDeg, epsilon) {
			t.Errorf("%s: %v.RectBound(), Lng.Hi not close enough, got %0.20f, want %0.20f",
				test.desc, test.have, s1.Angle(r.Lng.Hi).Degrees(), test.lngHiDeg)
		}
		if got := r.Lng.IsFull(); got != test.isFull {
			t.Errorf("%s: RectBound(%v).isFull() = %t, want %t", test.desc, test.have, got, test.isFull)
		}
	}

	// Empty and full caps.
	if !EmptyCap().RectBound().IsEmpty() {
		t.Errorf("RectBound() on EmptyCap should be empty.")
	}

	if !FullCap().RectBound().IsFull() {
		t.Errorf("RectBound() on FullCap should be full.")
	}
}

func TestCapAddPoint(t *testing.T) {
	const epsilon = 1e-14
	tests := []struct {
		have Cap
		p    Point
		want Cap
	}{
		// Cap plus its center equals itself.
		{xAxis, xAxisPt, xAxis},
		{yAxis, yAxisPt, yAxis},

		// Cap plus opposite point equals full.
		{xAxis, Point{r3.Vector{-1, 0, 0}}, fullCap},
		{yAxis, Point{r3.Vector{0, -1, 0}}, fullCap},

		// Cap plus orthogonal axis equals half cap.
		{xAxis, Point{r3.Vector{0, 0, 1}}, CapFromCenterAngle(xAxisPt, s1.Angle(math.Pi/2.0))},
		{xAxis, Point{r3.Vector{0, 0, -1}}, CapFromCenterAngle(xAxisPt, s1.Angle(math.Pi/2.0))},

		// The 45 degree angled hemisphere plus some points.
		{
			hemi,
			PointFromCoords(0, 1, -1),
			CapFromCenterAngle(Point{r3.Vector{1, 0, 1}},
				s1.Angle(120.0)*s1.Degree),
		},
		{
			hemi,
			PointFromCoords(0, -1, -1),
			CapFromCenterAngle(Point{r3.Vector{1, 0, 1}},
				s1.Angle(120.0)*s1.Degree),
		},
		{
			hemi,
			PointFromCoords(-1, -1, -1),
			CapFromCenterAngle(Point{r3.Vector{1, 0, 1}},
				s1.Angle(math.Acos(-math.Sqrt(2.0/3.0)))),
		},
		{hemi, Point{r3.Vector{0, 1, 1}}, hemi},
		{hemi, Point{r3.Vector{1, 0, 0}}, hemi},
	}

	for _, test := range tests {
		got := test.have.AddPoint(test.p)
		if !got.ApproxEqual(test.want) {
			t.Errorf("%v.AddPoint(%v) = %v, want %v", test.have, test.p, got, test.want)
		}

		if !got.ContainsPoint(test.p) {
			t.Errorf("%v.AddPoint(%v) did not contain added point", test.have, test.p)
		}
	}
}

func TestCapAddCap(t *testing.T) {
	tests := []struct {
		have  Cap
		other Cap
		want  Cap
	}{
		// Identity cases.
		{emptyCap, emptyCap, emptyCap},
		{fullCap, fullCap, fullCap},

		// Anything plus empty equals itself.
		{fullCap, emptyCap, fullCap},
		{emptyCap, fullCap, fullCap},
		{xAxis, emptyCap, xAxis},
		{emptyCap, xAxis, xAxis},
		{yAxis, emptyCap, yAxis},
		{emptyCap, yAxis, yAxis},

		// Two halves make a whole.
		{xAxis, xComp, fullCap},

		// Two zero-height orthogonal axis caps make a half-cap.
		{xAxis, yAxis, CapFromCenterAngle(xAxisPt, s1.Angle(math.Pi/2.0))},
	}

	for _, test := range tests {
		got := test.have.AddCap(test.other)
		if !got.ApproxEqual(test.want) {
			t.Errorf("%v.AddCap(%v) = %v, want %v", test.have, test.other, got, test.want)
		}
	}
}

func TestCapContainsCell(t *testing.T) {
	faceRadius := math.Atan(math.Sqrt2)
	for face := 0; face < 6; face++ {
		// The cell consisting of the entire face.
		rootCell := CellFromCellID(CellIDFromFace(face))

		// A leaf cell at the midpoint of the v=1 edge.
		edgeCell := CellFromPoint(Point{faceUVToXYZ(face, 0, 1-epsilon)})

		// A leaf cell at the u=1, v=1 corner
		cornerCell := CellFromPoint(Point{faceUVToXYZ(face, 1-epsilon, 1-epsilon)})

		// Quick check for full and empty caps.
		if !fullCap.ContainsCell(rootCell) {
			t.Errorf("Cap(%v).ContainsCell(%v) = %t; want = %t", fullCap, rootCell, false, true)
		}

		// Check intersections with the bounding caps of the leaf cells that are adjacent to
		// cornerCell along the Hilbert curve.  Because this corner is at (u=1,v=1), the curve
		// stays locally within the same cube face.
		first := cornerCell.id.Advance(-3)
		last := cornerCell.id.Advance(4)
		for id := first; id < last; id = id.Next() {
			c := CellFromCellID(id).CapBound()
			if got, want := c.ContainsCell(cornerCell), id == cornerCell.id; got != want {
				t.Errorf("Cap(%v).ContainsCell(%v) = %t; want = %t", c, cornerCell, got, want)
			}
		}

		for capFace := 0; capFace < 6; capFace++ {
			// A cap that barely contains all of capFace.
			center := unitNorm(capFace)
			covering := CapFromCenterAngle(center, s1.Angle(faceRadius+epsilon))
			if got, want := covering.ContainsCell(rootCell), capFace == face; got != want {
				t.Errorf("Cap(%v).ContainsCell(%v) = %t; want = %t", covering, rootCell, got, want)
			}
			if got, want := covering.ContainsCell(edgeCell), center.Vector.Dot(edgeCell.id.Point().Vector) > 0.1; got != want {
				t.Errorf("Cap(%v).ContainsCell(%v) = %t; want = %t", covering, edgeCell, got, want)
			}
			if got, want := covering.ContainsCell(edgeCell), covering.IntersectsCell(edgeCell); got != want {
				t.Errorf("Cap(%v).ContainsCell(%v) = %t; want = %t", covering, edgeCell, got, want)
			}
			if got, want := covering.ContainsCell(cornerCell), capFace == face; got != want {
				t.Errorf("Cap(%v).ContainsCell(%v) = %t; want = %t", covering, cornerCell, got, want)
			}

			// A cap that barely intersects the edges of capFace.
			bulging := CapFromCenterAngle(center, s1.Angle(math.Pi/4+epsilon))
			if bulging.ContainsCell(rootCell) {
				t.Errorf("Cap(%v).ContainsCell(%v) = %t; want = %t", bulging, rootCell, true, false)
			}
			if got, want := bulging.ContainsCell(edgeCell), capFace == face; got != want {
				t.Errorf("Cap(%v).ContainsCell(%v) = %t; want = %t", bulging, edgeCell, got, want)
			}
			if bulging.ContainsCell(cornerCell) {
				t.Errorf("Cap(%v).ContainsCell(%v) = %t; want = %t", bulging, cornerCell, true, false)
			}
		}
	}
}

func TestCapIntersectsCell(t *testing.T) {
	faceRadius := math.Atan(math.Sqrt2)
	for face := 0; face < 6; face++ {
		// The cell consisting of the entire face.
		rootCell := CellFromCellID(CellIDFromFace(face))

		// A leaf cell at the midpoint of the v=1 edge.
		edgeCell := CellFromPoint(Point{faceUVToXYZ(face, 0, 1-epsilon)})

		// A leaf cell at the u=1, v=1 corner
		cornerCell := CellFromPoint(Point{faceUVToXYZ(face, 1-epsilon, 1-epsilon)})

		// Quick check for full and empty caps.
		if emptyCap.IntersectsCell(rootCell) {
			t.Errorf("Cap(%v).IntersectsCell(%v) = %t; want = %t", emptyCap, rootCell, true, false)
		}

		// Check intersections with the bounding caps of the leaf cells that are adjacent to
		// cornerCell along the Hilbert curve.  Because this corner is at (u=1,v=1), the curve
		// stays locally within the same cube face.
		first := cornerCell.id.Advance(-3)
		last := cornerCell.id.Advance(4)
		for id := first; id < last; id = id.Next() {
			c := CellFromCellID(id).CapBound()
			if got, want := c.IntersectsCell(cornerCell), id.immediateParent().Contains(cornerCell.id); got != want {
				t.Errorf("Cap(%v).IntersectsCell(%v) = %t; want = %t", c, cornerCell, got, want)
			}
		}

		antiFace := (face + 3) % 6
		for capFace := 0; capFace < 6; capFace++ {
			// A cap that barely contains all of capFace.
			center := unitNorm(capFace)
			covering := CapFromCenterAngle(center, s1.Angle(faceRadius+epsilon))
			if got, want := covering.IntersectsCell(rootCell), capFace != antiFace; got != want {
				t.Errorf("Cap(%v).IntersectsCell(%v) = %t; want = %t", covering, rootCell, got, want)
			}
			if got, want := covering.IntersectsCell(edgeCell), covering.ContainsCell(edgeCell); got != want {
				t.Errorf("Cap(%v).IntersectsCell(%v) = %t; want = %t", covering, edgeCell, got, want)
			}
			if got, want := covering.IntersectsCell(cornerCell), center.Vector.Dot(cornerCell.id.Point().Vector) > 0; got != want {
				t.Errorf("Cap(%v).IntersectsCell(%v) = %t; want = %t", covering, cornerCell, got, want)
			}

			// A cap that barely intersects the edges of capFace.
			bulging := CapFromCenterAngle(center, s1.Angle(math.Pi/4+epsilon))
			if got, want := bulging.IntersectsCell(rootCell), capFace != antiFace; got != want {
				t.Errorf("Cap(%v).IntersectsCell(%v) = %t; want = %t", bulging, rootCell, got, want)
			}
			if got, want := bulging.IntersectsCell(edgeCell), center.Vector.Dot(edgeCell.id.Point().Vector) > 0.1; got != want {
				t.Errorf("Cap(%v).IntersectsCell(%v) = %t; want = %t", bulging, edgeCell, got, want)
			}
			if bulging.IntersectsCell(cornerCell) {
				t.Errorf("Cap(%v).IntersectsCell(%v) = %t; want = %t", bulging, cornerCell, true, false)
			}

			// A singleton cap.
			singleton := CapFromCenterAngle(center, 0)
			if got, want := singleton.IntersectsCell(rootCell), capFace == face; got != want {
				t.Errorf("Cap(%v).IntersectsCell(%v) = %t; want = %t", singleton, rootCell, got, want)
			}
			if singleton.IntersectsCell(edgeCell) {
				t.Errorf("Cap(%v).IntersectsCell(%v) = %t; want = %t", singleton, edgeCell, true, false)
			}
			if singleton.IntersectsCell(cornerCell) {
				t.Errorf("Cap(%v).IntersectsCell(%v) = %t; want = %t", singleton, cornerCell, true, false)
			}
		}
	}
}

func TestCapCentroid(t *testing.T) {
	// Empty and full caps.
	if got, want := EmptyCap().Centroid(), (Point{}); !got.ApproxEqual(want) {
		t.Errorf("Centroid of EmptyCap should be zero point, got %v", want)
	}
	if got, want := FullCap().Centroid().Norm(), 1e-15; got > want {
		t.Errorf("Centroid of FullCap should have a Norm of 0, got %v", want)
	}

	// Random caps.
	for i := 0; i < 100; i++ {
		center := randomPoint()
		height := randomUniformFloat64(0.0, 2.0)
		c := CapFromCenterHeight(center, height)
		got := c.Centroid()
		want := center.Mul((1.0 - height/2.0) * c.Area())
		if delta := got.Sub(want).Norm(); delta > 1e-15 {
			t.Errorf("%v.Sub(%v).Norm() = %v, want %v", got, want, delta, 1e-15)
		}
	}
}

func TestCapUnion(t *testing.T) {
	// Two caps which have the same center but one has a larger radius.
	a := CapFromCenterAngle(PointFromLatLng(LatLngFromDegrees(50.0, 10.0)), s1.Degree*0.2)
	b := CapFromCenterAngle(PointFromLatLng(LatLngFromDegrees(50.0, 10.0)), s1.Degree*0.3)
	if !b.Contains(a) {
		t.Errorf("%v.Contains(%v) = false, want true", b, a)
	}
	if got := b.ApproxEqual(a.Union(b)); !got {
		t.Errorf("%v.ApproxEqual(%v) = %v, want true", b, a.Union(b), got)
	}

	// Two caps where one is the full cap.
	if got := a.Union(FullCap()); !got.IsFull() {
		t.Errorf("%v.Union(%v).IsFull() = %v, want true", a, got, got.IsFull())
	}

	// Two caps where one is the empty cap.
	if got := a.Union(EmptyCap()); !a.ApproxEqual(got) {
		t.Errorf("%v.Union(EmptyCap) = %v, want %v", a, got, a)
	}

	// Two caps which have different centers, one entirely encompasses the other.
	c := CapFromCenterAngle(PointFromLatLng(LatLngFromDegrees(51.0, 11.0)), s1.Degree*1.5)
	if !c.Contains(a) {
		t.Errorf("%v.Contains(%v) = false, want true", c, a)
	}
	if got := a.Union(c).center; !got.ApproxEqual(c.center) {
		t.Errorf("%v.Union(%v).center = %v, want %v", a, c, got, c.center)
	}
	if got := a.Union(c); !float64Eq(float64(got.Radius()), float64(c.Radius())) {
		t.Errorf("%v.Union(%v).Radius = %v, want %v", a, c, got.Radius(), c.Radius())
	}

	// Two entirely disjoint caps.
	d := CapFromCenterAngle(PointFromLatLng(LatLngFromDegrees(51.0, 11.0)), s1.Degree*0.1)
	if d.Contains(a) {
		t.Errorf("%v.Contains(%v) = true, want false", d, a)
	}
	if d.Intersects(a) {
		t.Errorf("%v.Intersects(%v) = true, want false", d, a)
	}

	// Check union and reverse direction are the same.
	aUnionD := a.Union(d)
	if !aUnionD.ApproxEqual(d.Union(a)) {
		t.Errorf("%v.Union(%v).ApproxEqual(%v.Union(%v)) = false, want true", a, d, d, a)
	}
	if got, want := LatLngFromPoint(aUnionD.center).Lat.Degrees(), 50.4588; !float64Near(got, want, 0.001) {
		t.Errorf("%v.Center.Lat = %v, want %v", aUnionD, got, want)
	}
	if got, want := LatLngFromPoint(aUnionD.center).Lng.Degrees(), 10.4525; !float64Near(got, want, 0.001) {
		t.Errorf("%v.Center.Lng = %v, want %v", aUnionD, got, want)
	}
	if got, want := aUnionD.Radius().Degrees(), 0.7425; !float64Near(got, want, 0.001) {
		t.Errorf("%v.Radius = %v, want %v", aUnionD, got, want)
	}

	// Two partially overlapping caps.
	e := CapFromCenterAngle(PointFromLatLng(LatLngFromDegrees(50.3, 10.3)), s1.Degree*0.2)
	aUnionE := a.Union(e)
	if e.Contains(a) {
		t.Errorf("%v.Contains(%v) = false, want true", e, a)
	}
	if !e.Intersects(a) {
		t.Errorf("%v.Intersects(%v) = false, want true", e, a)
	}
	if !aUnionE.ApproxEqual(e.Union(a)) {
		t.Errorf("%v.Union(%v).ApproxEqual(%v.Union(%v)) = false, want true", a, e, e, a)
	}
	if got, want := LatLngFromPoint(aUnionE.center).Lat.Degrees(), 50.1500; !float64Near(got, want, 0.001) {
		t.Errorf("%v.Center.Lat = %v, want %v", aUnionE, got, want)
	}
	if got, want := LatLngFromPoint(aUnionE.center).Lng.Degrees(), 10.1495; !float64Near(got, want, 0.001) {
		t.Errorf("%v.Center.Lng = %v, want %v", aUnionE, got, want)
	}
	if got, want := aUnionE.Radius().Degrees(), 0.3781; !float64Near(got, want, 0.001) {
		t.Errorf("%v.Radius = %v, want %v", aUnionE, got, want)
	}

	p1 := Point{r3.Vector{0, 0, 1}}
	p2 := Point{r3.Vector{0, 1, 0}}
	// Two very large caps, whose radius sums to in excess of 180 degrees, and
	// whose centers are not antipodal.
	f := CapFromCenterAngle(p1, s1.Degree*150)
	g := CapFromCenterAngle(p2, s1.Degree*150)
	if !f.Union(g).IsFull() {
		t.Errorf("%v.Union(%v).IsFull() = false, want true", f, g)
	}

	// Two non-overlapping hemisphere caps with antipodal centers.
	hemi := CapFromCenterHeight(p1, 1)
	if !hemi.Union(hemi.Complement()).IsFull() {
		t.Errorf("%v.Union(%v).Complement().IsFull() = false, want true", hemi, hemi.Complement())
	}
}

func TestCapEqual(t *testing.T) {
	tests := []struct {
		a, b Cap
		want bool
	}{
		{EmptyCap(), EmptyCap(), true},
		{EmptyCap(), FullCap(), false},
		{FullCap(), FullCap(), true},
		{
			CapFromCenterAngle(PointFromCoords(0, 0, 1), s1.Degree*150),
			CapFromCenterAngle(PointFromCoords(0, 0, 1), s1.Degree*151),
			false,
		},
		{xAxis, xAxis, true},
		{xAxis, yAxis, false},
		{xComp, xAxis.Complement(), true},
	}

	for _, test := range tests {
		if got := test.a.Equal(test.b); got != test.want {
			t.Errorf("%v.Equal(%v) = %t, want %t", test.a, test.b, got, test.want)
		}
	}
}
