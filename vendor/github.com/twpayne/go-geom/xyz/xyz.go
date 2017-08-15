// Package xyz contains operations in 3d coordinate space.  Each
// layout must have 3 ordinates (and thus each coordinate)
// it is assumed that x,y,z are ordinates with indexes 0,1,2 respectively
package xyz

import (
	"math"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy"
)

// Distance calculates the distance between the two coordinates in 3d space.
func Distance(point1, point2 geom.Coord) float64 {
	// default to 2D distance if either Z is not set
	if math.IsNaN(point1[2]) || math.IsNaN(point2[2]) {
		return xy.Distance(point1, point2)
	}

	dx := point1[0] - point2[0]
	dy := point1[1] - point2[1]
	dz := point1[2] - point2[2]
	return math.Sqrt(dx*dx + dy*dy + dz*dz)
}

// DistancePointToLine calculates the distance from point to a point on the line
func DistancePointToLine(point, lineStart, lineEnd geom.Coord) float64 {
	// if start = end, then just compute distance to one of the endpoints
	if Equals(lineStart, lineEnd) {
		return Distance(point, lineStart)
	}

	// otherwise use comp.graphics.algorithms Frequently Asked Questions method
	/*
	 * (1) r = AC dot AB
	 *         ---------
	 *         ||AB||^2
	 *
	 * r has the following meaning:
	 *   r=0 P = A
	 *   r=1 P = B
	 *   r<0 P is on the backward extension of AB
	 *   r>1 P is on the forward extension of AB
	 *   0<r<1 P is interior to AB
	 */

	len2 := (lineEnd[0]-lineStart[0])*(lineEnd[0]-lineStart[0]) + (lineEnd[1]-lineStart[1])*(lineEnd[1]-lineStart[1]) + (lineEnd[2]-lineStart[2])*(lineEnd[2]-lineStart[2])
	if math.IsNaN(len2) {
		panic("Ordinates must not be NaN")
	}
	r := ((point[0]-lineStart[0])*(lineEnd[0]-lineStart[0]) + (point[1]-lineStart[1])*(lineEnd[1]-lineStart[1]) + (point[2]-lineStart[2])*(lineEnd[2]-lineStart[2])) / len2

	if r <= 0.0 {
		return Distance(point, lineStart)
	}
	if r >= 1.0 {
		return Distance(point, lineEnd)
	}

	// compute closest point q on line segment
	qx := lineStart[0] + r*(lineEnd[0]-lineStart[0])
	qy := lineStart[1] + r*(lineEnd[1]-lineStart[1])
	qz := lineStart[2] + r*(lineEnd[2]-lineStart[2])
	// result is distance from p to q
	dx := point[0] - qx
	dy := point[1] - qy
	dz := point[2] - qz
	return math.Sqrt(dx*dx + dy*dy + dz*dz)
}

// Equals determines if the two coordinates have equal in 3d space
func Equals(point1, other geom.Coord) bool {
	return (point1[0] == other[0]) && (point1[1] == other[1]) &&
		((point1[2] == other[2]) ||
			(math.IsNaN(point1[2]) && math.IsNaN(other[2])))
}

// DistanceLineToLine computes the distance between two 3D segments
func DistanceLineToLine(line1Start, line1End, line2Start, line2End geom.Coord) float64 {
	/**
	 * This calculation is susceptible to roundoff errors when
	 * passed large ordinate values.
	 * It may be possible to improve this by using {@link DD} arithmetic.
	 */
	if Equals(line1Start, line1End) {
		return DistancePointToLine(line1Start, line2Start, line2End)
	}
	if Equals(line2Start, line1End) {
		return DistancePointToLine(line2Start, line1Start, line1End)
	}

	/**
	 * Algorithm derived from http://softsurfer.com/Archive/algorithm_0106/algorithm_0106.htm
	 */
	a := VectorDot(line1Start, line1End, line1Start, line1End)
	b := VectorDot(line1Start, line1End, line2Start, line2End)
	c := VectorDot(line2Start, line2End, line2Start, line2End)
	d := VectorDot(line1Start, line1End, line2Start, line1Start)
	e := VectorDot(line2Start, line2End, line2Start, line1Start)

	denom := a*c - b*b
	if math.IsNaN(denom) {
		panic("Ordinates must not be NaN")
	}

	var s, t float64
	if denom <= 0.0 {
		/**
		 * The lines are parallel.
		 * In this case solve for the parameters s and t by assuming s is 0.
		 */
		s = 0
		// choose largest denominator for optimal numeric conditioning
		if b > c {
			t = d / b
		} else {
			t = e / c
		}
	} else {
		s = (b*e - c*d) / denom
		t = (a*e - b*d) / denom
	}
	if s < 0 {
		return DistancePointToLine(line1Start, line2Start, line2End)
	} else if s > 1 {
		return DistancePointToLine(line1End, line2Start, line2End)
	} else if t < 0 {
		return DistancePointToLine(line2Start, line1Start, line1End)
	} else if t > 1 {
		return DistancePointToLine(line2End, line1Start, line1End)
	}
	/**
	 * The closest points are in interiors of segments,
	 * so compute them directly
	 */
	x1 := line1Start[0] + s*(line1End[0]-line1Start[0])
	y1 := line1Start[1] + s*(line1End[1]-line1Start[1])
	z1 := line1Start[2] + s*(line1End[2]-line1Start[2])

	x2 := line2Start[0] + t*(line2End[0]-line2Start[0])
	y2 := line2Start[1] + t*(line2End[1]-line2Start[1])
	z2 := line2Start[2] + t*(line2End[2]-line2Start[2])

	// length (p1-p2)
	return Distance(geom.Coord{x1, y1, z1}, geom.Coord{x2, y2, z2})
}
