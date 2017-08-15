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

	"github.com/golang/geo/r3"
)

func TestEdgeCrosserCrossings(t *testing.T) {
	na1 := math.Nextafter(1, 0)
	na2 := math.Nextafter(1, 2)

	tests := []struct {
		msg          string
		a, b, c, d   Point
		robust       Crossing
		edgeOrVertex bool
		simple       bool
	}{
		{
			msg:          "two regular edges that cross",
			a:            Point{r3.Vector{1, 2, 1}},
			b:            Point{r3.Vector{1, -3, 0.5}},
			c:            Point{r3.Vector{1, -0.5, -3}},
			d:            Point{r3.Vector{0.1, 0.5, 3}},
			robust:       Cross,
			edgeOrVertex: true,
			simple:       true,
		},
		{
			msg:          "two regular edges that intersect antipodal points",
			a:            Point{r3.Vector{1, 2, 1}},
			b:            Point{r3.Vector{1, -3, 0.5}},
			c:            Point{r3.Vector{-1, 0.5, 3}},
			d:            Point{r3.Vector{-0.1, -0.5, -3}},
			robust:       DoNotCross,
			edgeOrVertex: false,
			simple:       true,
		},
		{
			msg:          "two edges on the same great circle that start at antipodal points",
			a:            Point{r3.Vector{0, 0, -1}},
			b:            Point{r3.Vector{0, 1, 0}},
			c:            Point{r3.Vector{0, 0, 1}},
			d:            Point{r3.Vector{0, 1, 1}},
			robust:       DoNotCross,
			edgeOrVertex: false,
			simple:       true,
		},
		{
			msg:          "two edges that cross where one vertex is the OriginPoint",
			a:            Point{r3.Vector{1, 0, 0}},
			b:            OriginPoint(),
			c:            Point{r3.Vector{1, -0.1, 1}},
			d:            Point{r3.Vector{1, 1, -0.1}},
			robust:       Cross,
			edgeOrVertex: true,
			simple:       true,
		},
		{
			msg:          "two edges that intersect antipodal points where one vertex is the OriginPoint",
			a:            Point{r3.Vector{1, 0, 0}},
			b:            OriginPoint(),
			c:            Point{r3.Vector{1, 0.1, -1}},
			d:            Point{r3.Vector{1, 1, -0.1}},
			robust:       DoNotCross,
			edgeOrVertex: false,
			simple:       true,
		},
		{
			msg:          "two edges that cross antipodal points",
			a:            Point{r3.Vector{1, 0, 0}},
			b:            Point{r3.Vector{0, 1, 0}},
			c:            Point{r3.Vector{0, 0, -1}},
			d:            Point{r3.Vector{-1, -1, 1}},
			robust:       DoNotCross,
			edgeOrVertex: false,
			simple:       true,
		},
		{
			// The Ortho() direction is (-4,0,2) and edge CD
			// is further CCW around (2,3,4) than AB.
			msg:          "two edges that share an endpoint",
			a:            Point{r3.Vector{2, 3, 4}},
			b:            Point{r3.Vector{-1, 2, 5}},
			c:            Point{r3.Vector{7, -2, 3}},
			d:            Point{r3.Vector{2, 3, 4}},
			robust:       MaybeCross,
			edgeOrVertex: false,
			simple:       true,
		},
		{
			// The edge AB is approximately in the x=y plane, while CD is approximately
			// perpendicular to it and ends exactly at the x=y plane.
			msg:          "two edges that barely cross near the middle of one edge",
			a:            Point{r3.Vector{1, 1, 1}},
			b:            Point{r3.Vector{1, na1, -1}},
			c:            Point{r3.Vector{11, -12, -1}},
			d:            Point{r3.Vector{10, 10, 1}},
			robust:       Cross,
			edgeOrVertex: true,
			simple:       false,
		},
		{
			msg:          "two edges that barely cross near the middle separated by a distance of about 1e-15",
			a:            Point{r3.Vector{1, 1, 1}},
			b:            Point{r3.Vector{1, na2, -1}},
			c:            Point{r3.Vector{1, -1, 0}},
			d:            Point{r3.Vector{1, 1, 0}},
			robust:       DoNotCross,
			edgeOrVertex: false,
			simple:       false,
		},
		{
			// This example cannot be handled using regular double-precision
			// arithmetic due to floating-point underflow.
			msg:          "two edges that barely cross each other near the end of both edges",
			a:            Point{r3.Vector{0, 0, 1}},
			b:            Point{r3.Vector{2, -1e-323, 1}},
			c:            Point{r3.Vector{1, -1, 1}},
			d:            Point{r3.Vector{1e-323, 0, 1}},
			robust:       Cross,
			edgeOrVertex: true,
			simple:       false,
		},
		{
			msg:          "two edges that barely cross each other near the end separated by a distance of about 1e-640",
			a:            Point{r3.Vector{0, 0, 1}},
			b:            Point{r3.Vector{2, 1e-323, 1}},
			c:            Point{r3.Vector{1, -1, 1}},
			d:            Point{r3.Vector{1e-323, 0, 1}},
			robust:       DoNotCross,
			edgeOrVertex: false,
			simple:       false,
		},
		{
			msg: "two edges that barely cross each other near the middle of one edge",
			// Computing the exact determinant of some of the triangles in this test
			// requires more than 2000 bits of precision.
			a:            Point{r3.Vector{1, -1e-323, -1e-323}},
			b:            Point{r3.Vector{1e-323, 1, 1e-323}},
			c:            Point{r3.Vector{1, -1, 1e-323}},
			d:            Point{r3.Vector{1, 1, 0}},
			robust:       Cross,
			edgeOrVertex: true,
			simple:       false,
		},
		{
			msg:          "two edges that barely cross each other near the middle separated by a distance of about 1e-640",
			a:            Point{r3.Vector{1, 1e-323, -1e-323}},
			b:            Point{r3.Vector{-1e-323, 1, 1e-323}},
			c:            Point{r3.Vector{1, -1, 1e-323}},
			d:            Point{r3.Vector{1, 1, 0}},
			robust:       DoNotCross,
			edgeOrVertex: false,
			simple:       false,
		},
	}

	for _, test := range tests {
		// just normalize them once
		a := Point{test.a.Normalize()}
		b := Point{test.b.Normalize()}
		c := Point{test.c.Normalize()}
		d := Point{test.d.Normalize()}
		testCrossing(t, test.msg, a, b, c, d, test.robust, test.edgeOrVertex, test.simple)
		testCrossing(t, test.msg, b, a, c, d, test.robust, test.edgeOrVertex, test.simple)
		testCrossing(t, test.msg, a, b, d, c, test.robust, test.edgeOrVertex, test.simple)
		testCrossing(t, test.msg, b, a, d, c, test.robust, test.edgeOrVertex, test.simple)

		// test degenerate cases
		testCrossing(t, test.msg, a, a, c, d, DoNotCross, false, false)
		testCrossing(t, test.msg, a, b, c, c, DoNotCross, false, false)
		testCrossing(t, test.msg, a, a, c, c, DoNotCross, false, false)

		testCrossing(t, test.msg, a, b, a, b, MaybeCross, true, false)
		testCrossing(t, test.msg, c, d, a, b, test.robust, test.edgeOrVertex != (test.robust == MaybeCross), test.simple)
	}
}

func testCrossing(t *testing.T, msg string, a, b, c, d Point, robust Crossing, edgeOrVertex, simple bool) {
	// Modify the expected result if two vertices from different edges match.
	if a == c || a == d || b == c || b == d {
		robust = MaybeCross
	}

	input := fmt.Sprintf("%s: a: %v, b: %v, c: %v, d: %v", msg, a, b, c, d)
	if simple {
		if got, want := SimpleCrossing(a, b, c, d), robust == Cross; got != want {
			t.Errorf("%v, SimpleCrossing(a, b, c, d) = %t, want %t", input, got, want)
		}
	}

	crosser := NewChainEdgeCrosser(a, b, c)
	if got, want := crosser.ChainCrossingSign(d), robust; got != want {
		t.Errorf("%v, ChainCrossingSign(d) = %d, want %d", input, got, want)
	}
	if got, want := crosser.ChainCrossingSign(c), robust; got != want {
		t.Errorf("%v, ChainCrossingSign(c) = %d, want %d", input, got, want)
	}
	if got, want := crosser.CrossingSign(d, c), robust; got != want {
		t.Errorf("%v, CrossingSign(d, c) = %d, want %d", input, got, want)
	}
	if got, want := crosser.CrossingSign(c, d), robust; got != want {
		t.Errorf("%v, CrossingSign(c, d) = %d, want %d", input, got, want)
	}

	crosser.RestartAt(c)
	if got, want := crosser.EdgeOrVertexChainCrossing(d), edgeOrVertex; got != want {
		t.Errorf("%v, EdgeOrVertexChainCrossing(d) = %t, want %t", input, got, want)
	}
	if got, want := crosser.EdgeOrVertexChainCrossing(c), edgeOrVertex; got != want {
		t.Errorf("%v, EdgeOrVertexChainCrossing(c) = %t, want %t", input, got, want)
	}
	if got, want := crosser.EdgeOrVertexCrossing(d, c), edgeOrVertex; got != want {
		t.Errorf("%v, EdgeOrVertexCrossing(d, c) = %t, want %t", input, got, want)
	}
	if got, want := crosser.EdgeOrVertexCrossing(c, d), edgeOrVertex; got != want {
		t.Errorf("%v, EdgeOrVertexCrossing(c, d) = %t, want %t", input, got, want)
	}
}

// TODO(roberts): Differences from C++:
// TestEdgeCrosserCollinearEdgesThatDontTouch
// TestEdgeCrosserCoincidentZeroLengthEdgesThatDontTouch
