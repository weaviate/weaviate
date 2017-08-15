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
	"fmt"
	"math"
	"testing"

	"github.com/golang/geo/r1"
	"github.com/golang/geo/r2"
	"github.com/golang/geo/r3"
	"github.com/golang/geo/s1"
)

func TestEdgeClippingIntersectsFace(t *testing.T) {
	tests := []struct {
		a    pointUVW
		want bool
	}{
		{pointUVW{r3.Vector{2.05335e-06, 3.91604e-22, 2.90553e-06}}, false},
		{pointUVW{r3.Vector{-3.91604e-22, -2.05335e-06, -2.90553e-06}}, false},
		{pointUVW{r3.Vector{0.169258, -0.169258, 0.664013}}, false},
		{pointUVW{r3.Vector{0.169258, -0.169258, -0.664013}}, false},
		{pointUVW{r3.Vector{math.Sqrt(2.0 / 3.0), -math.Sqrt(2.0 / 3.0), 3.88578e-16}}, true},
		{pointUVW{r3.Vector{-3.88578e-16, -math.Sqrt(2.0 / 3.0), math.Sqrt(2.0 / 3.0)}}, true},
	}

	for _, test := range tests {
		if got := test.a.intersectsFace(); got != test.want {
			t.Errorf("%v.intersectsFace() = %v, want %v", test.a, got, test.want)
		}
	}
}

func TestEdgeClippingIntersectsOppositeEdges(t *testing.T) {
	tests := []struct {
		a    pointUVW
		want bool
	}{
		{pointUVW{r3.Vector{0.169258, -0.169258, 0.664013}}, false},
		{pointUVW{r3.Vector{0.169258, -0.169258, -0.664013}}, false},

		{pointUVW{r3.Vector{-math.Sqrt(4.0 / 3.0), 0, -math.Sqrt(4.0 / 3.0)}}, true},
		{pointUVW{r3.Vector{math.Sqrt(4.0 / 3.0), 0, math.Sqrt(4.0 / 3.0)}}, true},

		{pointUVW{r3.Vector{-math.Sqrt(2.0 / 3.0), -math.Sqrt(2.0 / 3.0), 1.66533453694e-16}}, false},
		{pointUVW{r3.Vector{math.Sqrt(2.0 / 3.0), math.Sqrt(2.0 / 3.0), -1.66533453694e-16}}, false},
	}
	for _, test := range tests {
		if got := test.a.intersectsOppositeEdges(); got != test.want {
			t.Errorf("%v.intersectsOppositeEdges() = %v, want %v", test.a, got, test.want)
		}
	}
}

func TestEdgeClippingExitAxis(t *testing.T) {
	tests := []struct {
		a    pointUVW
		want axis
	}{
		{pointUVW{r3.Vector{0, -math.Sqrt(2.0 / 3.0), math.Sqrt(2.0 / 3.0)}}, axisU},
		{pointUVW{r3.Vector{0, math.Sqrt(4.0 / 3.0), -math.Sqrt(4.0 / 3.0)}}, axisU},
		{pointUVW{r3.Vector{-math.Sqrt(4.0 / 3.0), -math.Sqrt(4.0 / 3.0), 0}}, axisV},
		{pointUVW{r3.Vector{math.Sqrt(4.0 / 3.0), math.Sqrt(4.0 / 3.0), 0}}, axisV},
		{pointUVW{r3.Vector{math.Sqrt(2.0 / 3.0), -math.Sqrt(2.0 / 3.0), 0}}, axisV},
		{pointUVW{r3.Vector{1.67968702783622, 0, 0.870988820096491}}, axisV},
		{pointUVW{r3.Vector{0, math.Sqrt2, math.Sqrt2}}, axisU},
	}

	for _, test := range tests {
		if got := test.a.exitAxis(); got != test.want {
			t.Errorf("%v.exitAxis() = %v, want %v", test.a, got, test.want)
		}
	}
}

func TestEdgeClippingExitPoint(t *testing.T) {
	tests := []struct {
		a        pointUVW
		exitAxis axis
		want     r2.Point
	}{
		{pointUVW{r3.Vector{-3.88578058618805e-16, -math.Sqrt(2.0 / 3.0), math.Sqrt(2.0 / 3.0)}}, axisU, r2.Point{-1, 1}},
		{pointUVW{r3.Vector{math.Sqrt(4.0 / 3.0), -math.Sqrt(4.0 / 3.0), 0}}, axisV, r2.Point{-1, -1}},
		{pointUVW{r3.Vector{-math.Sqrt(4.0 / 3.0), -math.Sqrt(4.0 / 3.0), 0}}, axisV, r2.Point{-1, 1}},
		{pointUVW{r3.Vector{-6.66134e-16, math.Sqrt(4.0 / 3.0), -math.Sqrt(4.0 / 3.0)}}, axisU, r2.Point{1, 1}},
	}

	for _, test := range tests {
		if got := test.a.exitPoint(test.exitAxis); !r2PointsApproxEquals(got, test.want, epsilon) {
			t.Errorf("%v.exitPoint() = %v, want %v", test.a, got, test.want)
		}
	}
}

// testClipToPaddedFace performs a comprehensive set of tests across all faces and
// with random padding for the given points.
//
// We do this by defining an (x,y) coordinate system for the plane containing AB,
// and converting points along the great circle AB to angles in the range
// [-Pi, Pi]. We then accumulate the angle intervals spanned by each
// clipped edge; the union over all 6 faces should approximately equal the
// interval covered by the original edge.
func testClipToPaddedFace(t *testing.T, a, b Point) {
	a = Point{a.Normalize()}
	b = Point{b.Normalize()}
	if a.Vector == b.Mul(-1) {
		return
	}

	norm := Point{a.PointCross(b).Normalize()}
	aTan := Point{norm.Cross(a.Vector)}

	padding := 0.0
	if !oneIn(10) {
		padding = 1e-10 * math.Pow(1e-5, randomFloat64())
	}

	xAxis := a
	yAxis := aTan

	// Given the points A and B, we expect all angles generated from the clipping
	// to fall within this range.
	expectedAngles := s1.Interval{0, float64(a.Angle(b.Vector))}
	if expectedAngles.IsInverted() {
		expectedAngles = s1.Interval{expectedAngles.Hi, expectedAngles.Lo}
	}
	maxAngles := expectedAngles.Expanded(faceClipErrorRadians)
	var actualAngles s1.Interval

	for face := 0; face < 6; face++ {
		aUV, bUV, intersects := ClipToPaddedFace(a, b, face, padding)
		if !intersects {
			continue
		}

		aClip := Point{faceUVToXYZ(face, aUV.X, aUV.Y).Normalize()}
		bClip := Point{faceUVToXYZ(face, bUV.X, bUV.Y).Normalize()}

		desc := fmt.Sprintf("on face %d, a=%v, b=%v, aClip=%v, bClip=%v,", face, a, b, aClip, bClip)

		if got := math.Abs(aClip.Dot(norm.Vector)); got > faceClipErrorRadians {
			t.Errorf("%s abs(%v.Dot(%v)) = %v, want <= %v", desc, aClip, norm, got, faceClipErrorRadians)
		}
		if got := math.Abs(bClip.Dot(norm.Vector)); got > faceClipErrorRadians {
			t.Errorf("%s abs(%v.Dot(%v)) = %v, want <= %v", desc, bClip, norm, got, faceClipErrorRadians)
		}

		if float64(aClip.Angle(a.Vector)) > faceClipErrorRadians {
			if got := math.Max(math.Abs(aUV.X), math.Abs(aUV.Y)); !float64Eq(got, 1+padding) {
				t.Errorf("%s the largest component of %v = %v, want %v", desc, aUV, got, 1+padding)
			}
		}
		if float64(bClip.Angle(b.Vector)) > faceClipErrorRadians {
			if got := math.Max(math.Abs(bUV.X), math.Abs(bUV.Y)); !float64Eq(got, 1+padding) {
				t.Errorf("%s the largest component of %v = %v, want %v", desc, bUV, got, 1+padding)
			}
		}

		aAngle := math.Atan2(aClip.Dot(yAxis.Vector), aClip.Dot(xAxis.Vector))
		bAngle := math.Atan2(bClip.Dot(yAxis.Vector), bClip.Dot(xAxis.Vector))

		// Rounding errors may cause bAngle to be slightly less than aAngle.
		// We handle this by constructing the interval with FromPointPair,
		// which is okay since the interval length is much less than math.Pi.
		faceAngles := s1.IntervalFromEndpoints(aAngle, bAngle)
		if faceAngles.IsInverted() {
			faceAngles = s1.Interval{faceAngles.Hi, faceAngles.Lo}
		}
		if !maxAngles.ContainsInterval(faceAngles) {
			t.Errorf("%s %v.ContainsInterval(%v) = false, but should have contained this interval", desc, maxAngles, faceAngles)
		}
		actualAngles = actualAngles.Union(faceAngles)
	}
	if !actualAngles.Expanded(faceClipErrorRadians).ContainsInterval(expectedAngles) {
		t.Errorf("the union of all angle segments should be larger than the expected angle")
	}
}

func TestEdgeClippingClipToPaddedFace(t *testing.T) {
	// Start with a few simple cases.
	// An edge that is entirely contained within one cube face:
	testClipToPaddedFace(t, Point{r3.Vector{1, -0.5, -0.5}}, Point{r3.Vector{1, 0.5, 0.5}})
	testClipToPaddedFace(t, Point{r3.Vector{1, 0.5, 0.5}}, Point{r3.Vector{1, -0.5, -0.5}})
	// An edge that crosses one cube edge:
	testClipToPaddedFace(t, Point{r3.Vector{1, 0, 0}}, Point{r3.Vector{0, 1, 0}})
	testClipToPaddedFace(t, Point{r3.Vector{0, 1, 0}}, Point{r3.Vector{1, 0, 0}})
	// An edge that crosses two opposite edges of face 0:
	testClipToPaddedFace(t, Point{r3.Vector{0.75, 0, -1}}, Point{r3.Vector{0.75, 0, 1}})
	testClipToPaddedFace(t, Point{r3.Vector{0.75, 0, 1}}, Point{r3.Vector{0.75, 0, -1}})
	// An edge that crosses two adjacent edges of face 2:
	testClipToPaddedFace(t, Point{r3.Vector{1, 0, 0.75}}, Point{r3.Vector{0, 1, 0.75}})
	testClipToPaddedFace(t, Point{r3.Vector{0, 1, 0.75}}, Point{r3.Vector{1, 0, 0.75}})
	// An edges that crosses three cube edges (four faces):
	testClipToPaddedFace(t, Point{r3.Vector{1, 0.9, 0.95}}, Point{r3.Vector{-1, 0.95, 0.9}})
	testClipToPaddedFace(t, Point{r3.Vector{-1, 0.95, 0.9}}, Point{r3.Vector{1, 0.9, 0.95}})

	// Comprehensively test edges that are difficult to handle, especially those
	// that nearly follow one of the 12 cube edges.
	biunit := r2.Rect{r1.Interval{-1, 1}, r1.Interval{-1, 1}}

	for i := 0; i < 1000; i++ {
		// Choose two adjacent cube corners P and Q.
		face := randomUniformInt(6)
		i := randomUniformInt(4)
		j := (i + 1) & 3
		p := Point{faceUVToXYZ(face, biunit.Vertices()[i].X, biunit.Vertices()[i].Y)}
		q := Point{faceUVToXYZ(face, biunit.Vertices()[j].X, biunit.Vertices()[j].Y)}

		// Now choose two points that are nearly in the plane of PQ, preferring
		// points that are near cube corners, face midpoints, or edge midpoints.
		a := perturbedCornerOrMidpoint(p, q)
		b := perturbedCornerOrMidpoint(p, q)
		testClipToPaddedFace(t, a, b)
	}
}

// getFraction returns the fraction t of the given point X on the line AB such that
// x = (1-t)*a + t*b. Returns 0 if A = B.
func getFraction(t *testing.T, x, a, b r2.Point) float64 {
	// A bound for the error in edge clipping plus the error in the calculation
	// (which is similar to EdgeIntersectsRect).
	errorDist := (edgeClipErrorUVDist + intersectsRectErrorUVDist)
	if a == b {
		return 0.0
	}
	dir := b.Sub(a).Normalize()
	if got := math.Abs(x.Sub(a).Dot(dir.Ortho())); got > errorDist {
		t.Errorf("getFraction(%v, %v, %v) = %v, which exceeds errorDist %v", x, a, b, got, errorDist)
	}
	return x.Sub(a).Dot(dir)
}

// randomPointFromInterval returns a randomly selected point from the given interval
// with one of three possible choices. All cases have reasonable probability for any
// interval. The choices are: randomly choose a value inside the interval, choose a
// value outside the interval, or select one of the two endpoints.
func randomPointFromInterval(clip r1.Interval) float64 {
	if oneIn(5) {
		if oneIn(2) {
			return clip.Lo
		}
		return clip.Hi
	}

	switch randomUniformInt(3) {
	case 0:
		return clip.Lo - randomFloat64()
	case 1:
		return clip.Hi + randomFloat64()
	default:
		return clip.Lo + randomFloat64()*clip.Length()
	}
}

// Given a rectangle "clip", choose a point that may lie in the rectangle interior, along an extended edge, exactly at a vertex, or in one of the eight regions exterior to "clip" that are separated by its extended edges.  Also sometimes return points that are exactly on one of the extended diagonals of "clip". All cases are reasonably likely to occur for any given rectangle "clip".
func chooseRectEndpoint(clip r2.Rect) r2.Point {
	if oneIn(10) {
		// Return a point on one of the two extended diagonals.
		diag := randomUniformInt(2)
		t := randomUniformFloat64(-1, 2)
		return clip.Vertices()[diag].Mul(1 - t).Add(clip.Vertices()[diag+2].Mul(t))
	}
	return r2.Point{randomPointFromInterval(clip.X), randomPointFromInterval(clip.Y)}
}

// Choose a random point in the rectangle defined by points A and B, sometimes
// returning a point on the edge AB or the points A and B themselves.
func choosePointInRect(a, b r2.Point) r2.Point {
	if oneIn(5) {
		if oneIn(2) {
			return a
		}
		return b
	}

	if oneIn(3) {
		return a.Add(b.Sub(a).Mul(randomFloat64()))
	}
	return r2.Point{randomUniformFloat64(a.X, b.X), randomUniformFloat64(a.Y, b.Y)}
}

// Given a point P representing a possibly clipped endpoint A of an edge AB,
// verify that clip contains P, and that if clipping occurred (i.e., P != A)
// then P is on the boundary of clip.
func checkPointOnBoundary(t *testing.T, p, a r2.Point, clip r2.Rect) {
	if got := clip.ContainsPoint(p); !got {
		t.Errorf("%v.ContainsPoint(%v) = %v, want true", clip, p, got)
	}
	if p != a {
		p1 := r2.Point{math.Nextafter(p.X, a.X), math.Nextafter(p.Y, a.Y)}
		if got := clip.ContainsPoint(p1); got {
			t.Errorf("%v.ContainsPoint(%v) = %v, want false", clip, p1, got)
		}
	}
}

func TestEdgeClippingClipEdge(t *testing.T) {
	// A bound for the error in edge clipping plus the error in the
	// EdgeIntersectsRect calculation below.
	errorDist := (edgeClipErrorUVDist + intersectsRectErrorUVDist)
	testRects := []r2.Rect{
		// Test clipping against random rectangles.
		r2.RectFromPoints(
			r2.Point{randomUniformFloat64(-1, 1), randomUniformFloat64(-1, 1)},
			r2.Point{randomUniformFloat64(-1, 1), randomUniformFloat64(-1, 1)}),
		r2.RectFromPoints(
			r2.Point{randomUniformFloat64(-1, 1), randomUniformFloat64(-1, 1)},
			r2.Point{randomUniformFloat64(-1, 1), randomUniformFloat64(-1, 1)}),
		r2.RectFromPoints(
			r2.Point{randomUniformFloat64(-1, 1), randomUniformFloat64(-1, 1)},
			r2.Point{randomUniformFloat64(-1, 1), randomUniformFloat64(-1, 1)}),
		r2.RectFromPoints(
			r2.Point{randomUniformFloat64(-1, 1), randomUniformFloat64(-1, 1)},
			r2.Point{randomUniformFloat64(-1, 1), randomUniformFloat64(-1, 1)}),
		r2.RectFromPoints(
			r2.Point{randomUniformFloat64(-1, 1), randomUniformFloat64(-1, 1)},
			r2.Point{randomUniformFloat64(-1, 1), randomUniformFloat64(-1, 1)}),

		// Also clip against one-dimensional, singleton, and empty rectangles.
		r2.Rect{r1.Interval{-0.7, -0.7}, r1.Interval{0.3, 0.35}},
		r2.Rect{r1.Interval{0.2, 0.5}, r1.Interval{0.3, 0.3}},
		r2.Rect{r1.Interval{-0.7, 0.3}, r1.Interval{0, 0}},
		r2.RectFromPoints(r2.Point{0.3, 0.8}),
		r2.EmptyRect(),
	}

	for _, r := range testRects {
		for i := 0; i < 1000; i++ {
			a := chooseRectEndpoint(r)
			b := chooseRectEndpoint(r)

			aClip, bClip, intersects := ClipEdge(a, b, r)
			if !intersects {
				if edgeIntersectsRect(a, b, r.ExpandedByMargin(-errorDist)) {
					t.Errorf("edgeIntersectsRect(%v, %v, %v.ExpandedByMargin(%v) = true, want false", a, b, r, -errorDist)
				}
			} else {
				if !edgeIntersectsRect(a, b, r.ExpandedByMargin(errorDist)) {
					t.Errorf("edgeIntersectsRect(%v, %v, %v.ExpandedByMargin(%v) = false, want true", a, b, r, errorDist)
				}

				// Check that the clipped points lie on the edge AB, and
				// that the points have the expected order along the segment AB.
				if gotA, gotB := getFraction(t, aClip, a, b), getFraction(t, bClip, a, b); gotA > gotB {
					t.Errorf("getFraction(%v,%v,%v) = %v, getFraction(%v, %v, %v) = %v; %v < %v = false, want true", aClip, a, b, gotA, bClip, a, b, gotB, gotA, gotB)
				}

				// Check that the clipped portion of AB is as large as possible.
				checkPointOnBoundary(t, aClip, a, r)
				checkPointOnBoundary(t, bClip, b, r)
			}

			// Choose an random initial bound to pass to clipEdgeBound.
			initialClip := r2.RectFromPoints(choosePointInRect(a, b), choosePointInRect(a, b))
			bound := clippedEdgeBound(a, b, initialClip)
			if bound.IsEmpty() {
				// Precondition of clipEdgeBound not met
				continue
			}
			maxBound := bound.Intersection(r)
			if bound, intersects := clipEdgeBound(a, b, r, bound); !intersects {
				if edgeIntersectsRect(a, b, maxBound.ExpandedByMargin(-errorDist)) {
					t.Errorf("edgeIntersectsRect(%v, %v, %v.ExpandedByMargin(%v) = true, want false", a, b, maxBound.ExpandedByMargin(-errorDist), -errorDist)
				}
			} else {
				if !edgeIntersectsRect(a, b, maxBound.ExpandedByMargin(errorDist)) {
					t.Errorf("edgeIntersectsRect(%v, %v, %v.ExpandedByMargin(%v) = false, want true", a, b, maxBound.ExpandedByMargin(errorDist), errorDist)
				}
				// check that the bound is as large as possible.
				ai := 0
				if a.X > b.X {
					ai = 1
				}
				aj := 0
				if a.Y > b.Y {
					aj = 1
				}
				checkPointOnBoundary(t, bound.VertexIJ(ai, aj), a, maxBound)
				checkPointOnBoundary(t, bound.VertexIJ(1-ai, 1-aj), b, maxBound)
			}
		}
	}
}
