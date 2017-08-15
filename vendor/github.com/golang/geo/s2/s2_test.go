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
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"

	"github.com/golang/geo/r2"
	"github.com/golang/geo/r3"
	"github.com/golang/geo/s1"
)

// float64Eq reports whether the two values are within the default epsilon.
func float64Eq(x, y float64) bool { return float64Near(x, y, epsilon) }

// float64Near reports whether the two values are within the given epsilon.
func float64Near(x, y, ε float64) bool {
	return math.Abs(x-y) <= ε
}

// TODO(roberts): Add in flag to allow specifying the random seed for repeatable tests.

// kmToAngle converts a distance on the Earth's surface to an angle.
func kmToAngle(km float64) s1.Angle {
	// The Earth's mean radius in kilometers (according to NASA).
	const earthRadiusKm = 6371.01
	return s1.Angle(km / earthRadiusKm)
}

// randomBits returns a 64-bit random unsigned integer whose lowest "num" are random, and
// whose other bits are zero.
func randomBits(num uint32) uint64 {
	// Make sure the request is for not more than 63 bits.
	if num > 63 {
		num = 63
	}
	return uint64(rand.Int63()) & ((1 << num) - 1)
}

// Return a uniformly distributed 64-bit unsigned integer.
func randomUint64() uint64 {
	return uint64(rand.Int63() | (rand.Int63() << 63))
}

// Return a uniformly distributed 32-bit unsigned integer.
func randomUint32() uint32 {
	return uint32(randomBits(32))
}

// randomFloat64 returns a uniformly distributed value in the range [0,1).
// Note that the values returned are all multiples of 2**-53, which means that
// not all possible values in this range are returned.
func randomFloat64() float64 {
	const randomFloatBits = 53
	return math.Ldexp(float64(randomBits(randomFloatBits)), -randomFloatBits)
}

// randomUniformInt returns a uniformly distributed integer in the range [0,n).
// NOTE: This is replicated here to stay in sync with how the C++ code generates
// uniform randoms. (instead of using Go's math/rand package directly).
func randomUniformInt(n int) int {
	return int(randomFloat64() * float64(n))
}

// randomUniformFloat64 returns a uniformly distributed value in the range [min, max).
func randomUniformFloat64(min, max float64) float64 {
	return min + randomFloat64()*(max-min)
}

// oneIn returns true with a probability of 1/n.
func oneIn(n int) bool {
	return randomUniformInt(n) == 0
}

// randomPoint returns a random unit-length vector.
func randomPoint() Point {
	return PointFromCoords(randomUniformFloat64(-1, 1),
		randomUniformFloat64(-1, 1), randomUniformFloat64(-1, 1))
}

// randomFrame returns a right-handed coordinate frame (three orthonormal vectors) for
// a randomly generated point.
func randomFrame() *matrix3x3 {
	return randomFrameAtPoint(randomPoint())
}

// randomFrameAtPoint returns a right-handed coordinate frame using the given
// point as the z-axis. The x- and y-axes are computed such that (x,y,z) is a
// right-handed coordinate frame (three orthonormal vectors).
func randomFrameAtPoint(z Point) *matrix3x3 {
	x := Point{z.Cross(randomPoint().Vector).Normalize()}
	y := Point{z.Cross(x.Vector).Normalize()}

	m := &matrix3x3{}
	m.setCol(0, x)
	m.setCol(1, y)
	m.setCol(2, z)
	return m
}

// randomCellIDForLevel returns a random CellID at the given level.
// The distribution is uniform over the space of cell ids, but only
// approximately uniform over the surface of the sphere.
func randomCellIDForLevel(level int) CellID {
	face := randomUniformInt(numFaces)
	pos := randomUint64() & uint64((1<<posBits)-1)
	return CellIDFromFacePosLevel(face, pos, level)
}

// randomCellID returns a random CellID at a randomly chosen
// level. The distribution is uniform over the space of cell ids,
// but only approximately uniform over the surface of the sphere.
func randomCellID() CellID {
	return randomCellIDForLevel(randomUniformInt(maxLevel + 1))
}

// parsePoint returns an Point from the latitude-longitude coordinate in degrees
// in the given string, or the origin if the string was invalid.
// e.g., "-20:150"
func parsePoint(s string) Point {
	p := parsePoints(s)
	if len(p) > 0 {
		return p[0]
	}

	return Point{r3.Vector{0, 0, 0}}
}

// parseRect returns the minimal bounding Rect that contains the one or more
// latitude-longitude coordinates in degrees in the given string.
// Examples of input:
//   "-20:150"                     // one point
//   "-20:150, -20:151, -19:150"   // three points
func parseRect(s string) Rect {
	var rect Rect
	lls := parseLatLngs(s)
	if len(lls) > 0 {
		rect = RectFromLatLng(lls[0])
	}

	for _, ll := range lls[1:] {
		rect = rect.AddPoint(ll)
	}

	return rect
}

// parseLatLngs splits up a string of lat:lng points and returns the list of parsed
// entries.
func parseLatLngs(s string) []LatLng {
	pieces := strings.Split(s, ",")
	var lls []LatLng
	for _, piece := range pieces {
		piece = strings.TrimSpace(piece)

		// Skip empty strings.
		if piece == "" {
			continue
		}

		p := strings.Split(piece, ":")
		if len(p) != 2 {
			panic(fmt.Sprintf("invalid input string for parseLatLngs: %q", piece))
		}

		lat, err := strconv.ParseFloat(p[0], 64)
		if err != nil {
			panic(fmt.Sprintf("invalid float in parseLatLngs: %q, err: %v", p[0], err))
		}

		lng, err := strconv.ParseFloat(p[1], 64)
		if err != nil {
			panic(fmt.Sprintf("invalid float in parseLatLngs: %q, err: %v", p[1], err))
		}

		lls = append(lls, LatLngFromDegrees(lat, lng))
	}
	return lls
}

// parsePoints takes a string of lat:lng points and returns the set of Points it defines.
func parsePoints(s string) []Point {
	lls := parseLatLngs(s)
	points := make([]Point, len(lls))
	for i, ll := range lls {
		points[i] = PointFromLatLng(ll)
	}
	return points
}

// makeLoop constructs a loop from a comma separated string of lat:lng
// coordinates in degrees. Example of the input format:
//   "-20:150, 10:-120, 0.123:-170.652"
// The special strings "empty" or "full" create an empty or full loop respectively.
func makeLoop(s string) *Loop {
	if s == "full" {
		return FullLoop()
	}
	if s == "empty" {
		return EmptyLoop()
	}

	return LoopFromPoints(parsePoints(s))
}

// makePolygon constructs a polygon from the set of semicolon separated CSV
// strings of lat:lng points defining each loop in the polygon. If the normalize
// flag is set to true, loops are normalized by inverting them
// if necessary so that they enclose at most half of the unit sphere.
//
// Examples of the input format:
//     "10:20, 90:0, 20:30"                                  // one loop
//     "10:20, 90:0, 20:30; 5.5:6.5, -90:-180, -15.2:20.3"   // two loops
//     ""       // the empty polygon (consisting of no loops)
//     "full"   // the full polygon (consisting of one full loop)
//     "empty"  // **INVALID** (a polygon consisting of one empty loop)
func makePolygon(s string, normalize bool) *Polygon {
	strs := strings.Split(s, ";")
	var loops []*Loop
	for _, str := range strs {
		if str == "" {
			continue
		}
		loop := makeLoop(strings.TrimSpace(str))
		if normalize {
			// TODO(roberts): Uncomment once Normalize is implemented.
			// loop.Normalize()
		}
		loops = append(loops, loop)
	}
	return PolygonFromLoops(loops)
}

// makePolyline constructs a Polyline from the given string of lat:lng values.
func makePolyline(s string) *Polyline {
	p := Polyline(parsePoints(s))
	return &p
}

// concentricLoopsPolygon constructs a polygon with the specified center as a
// number of concentric loops and vertices per loop.
func concentricLoopsPolygon(center Point, numLoops, verticesPerLoop int) *Polygon {
	var loops []*Loop
	for li := 0; li < numLoops; li++ {
		radius := s1.Angle(0.005 * float64(li+1) / float64(numLoops))
		loops = append(loops, RegularLoop(center, radius, verticesPerLoop))
	}
	return PolygonFromLoops(loops)
}

// skewedInt returns a number in the range [0,2^max_log-1] with bias towards smaller numbers.
func skewedInt(maxLog int) int {
	base := uint32(rand.Int31n(int32(maxLog + 1)))
	return int(randomBits(31) & ((1 << base) - 1))
}

// randomCap returns a cap with a random axis such that the log of its area is
// uniformly distributed between the logs of the two given values. The log of
// the cap angle is also approximately uniformly distributed.
func randomCap(minArea, maxArea float64) Cap {
	capArea := maxArea * math.Pow(minArea/maxArea, randomFloat64())
	return CapFromCenterArea(randomPoint(), capArea)
}

// pointsApproxEquals reports whether the two points are within the given distance
// of each other. This is the same as Point.ApproxEquals but permits specifying
// the epsilon.
func pointsApproxEquals(a, b Point, epsilon float64) bool {
	return float64(a.Vector.Angle(b.Vector)) <= epsilon
}

var (
	rectErrorLat = 10 * dblEpsilon
	rectErrorLng = dblEpsilon
)

// r2PointsApproxEqual reports whether the two points are within the given epsilon.
func r2PointsApproxEquals(a, b r2.Point, epsilon float64) bool {
	return float64Near(a.X, b.X, epsilon) && float64Near(a.Y, b.Y, epsilon)
}

// rectsApproxEqual reports whether the two rect are within the given tolerances
// at each corner from each other. The tolerances are specific to each axis.
func rectsApproxEqual(a, b Rect, tolLat, tolLng float64) bool {
	return math.Abs(a.Lat.Lo-b.Lat.Lo) < tolLat &&
		math.Abs(a.Lat.Hi-b.Lat.Hi) < tolLat &&
		math.Abs(a.Lng.Lo-b.Lng.Lo) < tolLng &&
		math.Abs(a.Lng.Hi-b.Lng.Hi) < tolLng
}

// matricesApproxEqual reports whether all cells in both matrices are equal within
// the default floating point epsilon.
func matricesApproxEqual(m1, m2 *matrix3x3) bool {
	return float64Eq(m1[0][0], m2[0][0]) &&
		float64Eq(m1[0][1], m2[0][1]) &&
		float64Eq(m1[0][2], m2[0][2]) &&

		float64Eq(m1[1][0], m2[1][0]) &&
		float64Eq(m1[1][1], m2[1][1]) &&
		float64Eq(m1[1][2], m2[1][2]) &&

		float64Eq(m1[2][0], m2[2][0]) &&
		float64Eq(m1[2][1], m2[2][1]) &&
		float64Eq(m1[2][2], m2[2][2])
}

// samplePointFromRect returns a point chosen uniformly at random (with respect
// to area on the sphere) from the given rectangle.
func samplePointFromRect(rect Rect) Point {
	// First choose a latitude uniformly with respect to area on the sphere.
	sinLo := math.Sin(rect.Lat.Lo)
	sinHi := math.Sin(rect.Lat.Hi)
	lat := math.Asin(randomUniformFloat64(sinLo, sinHi))

	// Now choose longitude uniformly within the given range.
	lng := rect.Lng.Lo + randomFloat64()*rect.Lng.Length()

	return PointFromLatLng(LatLng{s1.Angle(lat), s1.Angle(lng)}.Normalized())
}

// samplePointFromCap returns a point chosen uniformly at random (with respect
// to area) from the given cap.
func samplePointFromCap(c Cap) Point {
	// We consider the cap axis to be the "z" axis. We choose two other axes to
	// complete the coordinate frame.
	m := getFrame(c.Center())

	// The surface area of a spherical cap is directly proportional to its
	// height. First we choose a random height, and then we choose a random
	// point along the circle at that height.
	h := randomFloat64() * c.Height()
	theta := 2 * math.Pi * randomFloat64()
	r := math.Sqrt(h * (2 - h))

	// The result should already be very close to unit-length, but we might as
	// well make it accurate as possible.
	return Point{fromFrame(m, PointFromCoords(math.Cos(theta)*r, math.Sin(theta)*r, 1-h)).Normalize()}
}

// perturbATowardsB returns a point that has been shifted some distance towards the
// second point based on a random number.
func perturbATowardsB(a, b Point) Point {
	choice := randomFloat64()
	if choice < 0.1 {
		return a
	}
	if choice < 0.3 {
		// Return a point that is exactly proportional to A and that still
		// satisfies IsUnitLength().
		for {
			b := Point{a.Mul(2 - a.Norm() + 5*(randomFloat64()-0.5)*dblEpsilon)}
			if !b.ApproxEqual(a) && b.IsUnit() {
				return b
			}
		}
	}
	if choice < 0.5 {
		// Return a point such that the distance squared to A will underflow.
		return InterpolateAtDistance(1e-300, a, b)
	}
	// Otherwise return a point whose distance from A is near dblEpsilon such
	// that the log of the pdf is uniformly distributed.
	distance := dblEpsilon * 1e-5 * math.Pow(1e6, randomFloat64())
	return InterpolateAtDistance(s1.Angle(distance), a, b)
}

// perturbedCornerOrMidpoint returns a Point from a line segment whose endpoints are
// difficult to handle correctly. Given two adjacent cube vertices P and Q,
// it returns either an edge midpoint, face midpoint, or corner vertex that is
// in the plane of PQ and that has been perturbed slightly. It also sometimes
// returns a random point from anywhere on the sphere.
func perturbedCornerOrMidpoint(p, q Point) Point {
	a := p.Mul(float64(randomUniformInt(3) - 1)).Add(q.Mul(float64(randomUniformInt(3) - 1)))
	if oneIn(10) {
		// This perturbation often has no effect except on coordinates that are
		// zero, in which case the perturbed value is so small that operations on
		// it often result in underflow.
		a = a.Add(randomPoint().Mul(math.Pow(1e-300, randomFloat64())))
	} else if oneIn(2) {
		// For coordinates near 1 (say > 0.5), this perturbation yields values
		// that are only a few representable values away from the initial value.
		a = a.Add(randomPoint().Mul(4 * dblEpsilon))
	} else {
		// A perturbation whose magnitude is in the range [1e-25, 1e-10].
		a = a.Add(randomPoint().Mul(1e-10 * math.Pow(1e-15, randomFloat64())))
	}

	if a.Norm2() < math.SmallestNonzeroFloat64 {
		// If a.Norm2() is denormalized, Normalize() loses too much precision.
		return perturbedCornerOrMidpoint(p, q)
	}
	return Point{a}
}

// TODO:
// Most of the other s2 testing methods.
