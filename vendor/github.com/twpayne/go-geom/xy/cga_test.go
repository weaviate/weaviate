package xy_test

import (
	"math"
	"math/rand"
	"testing"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy"
	"github.com/twpayne/go-geom/xy/internal"
)

func TestIsOnLinePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("This test is supposed to panic")
		}
		// good panic was expected
	}()

	xy.IsOnLine(geom.XY, geom.Coord{0, 0}, []float64{0, 0})
}

func TestIsOnLine(t *testing.T) {
	for i, tc := range []struct {
		desc         string
		p            geom.Coord
		lineSegments []float64
		layout       geom.Layout
		intersects   bool
	}{
		{
			desc:         "Point on center of line",
			p:            geom.Coord{0, 0},
			lineSegments: []float64{-1, 0, 1, 0},
			layout:       geom.XY,
			intersects:   true,
		},
		{
			desc:         "Point not on line",
			p:            geom.Coord{0, 0},
			lineSegments: []float64{-1, 1, 1, 0},
			layout:       geom.XY,
			intersects:   false,
		},
		{
			desc:         "Point not on second line segment",
			p:            geom.Coord{0, 0},
			lineSegments: []float64{-1, 1, 1, 0, -1, 0},
			layout:       geom.XY,
			intersects:   true,
		},
		{
			desc:         "Point not on any line segments",
			p:            geom.Coord{0, 0},
			lineSegments: []float64{-1, 1, 1, 0, 2, 0},
			layout:       geom.XY,
			intersects:   false,
		},
		{
			desc:         "Point in unclosed ring",
			p:            geom.Coord{0, 0},
			lineSegments: []float64{-1, 1, 1, 1, 1, -1, -1, -1, -1, 1.00000000000000000000000000001},
			layout:       geom.XY,
			intersects:   false,
		},
		{
			desc:         "Point in ring",
			p:            geom.Coord{0, 0},
			lineSegments: []float64{-1, 1, 1, 1, 1, -1, -1, -1, -1, 1},
			layout:       geom.XY,
			intersects:   false,
		},
	} {
		if tc.intersects != xy.IsOnLine(tc.layout, tc.p, tc.lineSegments) {
			t.Errorf("Test '%v' (%v) failed: expected \n%v but was \n%v", i+1, tc.desc, tc.intersects, !tc.intersects)
		}
	}
}

func TestIsRingCounterClockwiseNotEnoughPoints(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Errorf("Expected a panic because there are not enough points")
		}

	}()
	xy.IsRingCounterClockwise(geom.XY, []float64{0, 0, 1, 0, 1, 1})
}

func TestIsRingCounterClockwise(t *testing.T) {
	for i, tc := range []struct {
		desc         string
		lineSegments []float64
		ccw          bool
	}{
		{
			desc:         "counter-clockwise ring 3 points",
			lineSegments: []float64{0, 0, 1, 0, 1, 1, 0, 0},
			ccw:          true,
		},
		{
			desc:         "counter-clockwise ring 4 points, not closed, highest at end",
			lineSegments: []float64{0, 0, 1, 0, 1, .5, 0, 1},
			ccw:          true,
		},
		{
			desc:         "counter-clockwise ring 4 points",
			lineSegments: []float64{0, 0, 1, 0, 1, 1, 0, 1, 0, 0},
			ccw:          true,
		},
		{
			desc:         "clockwise ring 3 points",
			lineSegments: []float64{0, 0, 0, 1, 1, 1, 0, 0},
			ccw:          false,
		},
		{
			desc:         "clockwise ring 4 points",
			lineSegments: []float64{0, 0, 0, 1, 1, 1, 1, 0, 0, 0},
			ccw:          false,
		},
		{
			desc:         "clockwise ring many points",
			lineSegments: internal.RING.FlatCoords(),
			ccw:          false,
		},
		{
			desc:         "counter-clockwise tiny ring",
			lineSegments: []float64{0, 0, 1e55, 0, 1e55, 1e55, 0, 0},
			ccw:          true,
		},
	} {
		if tc.ccw != xy.IsRingCounterClockwise(geom.XY, tc.lineSegments) {
			t.Errorf("Test '%v' (%v) failed: expected \n%v but was \n%v", i+1, tc.desc, tc.ccw, !tc.ccw)
		}

		// test with another ordinate per point
		copied := make3DCopy(tc.lineSegments)
		if tc.ccw != xy.IsRingCounterClockwise(geom.XYZ, copied) {
			t.Errorf("Test '%v' (%v) failed: expected \n%v but was \n%v", i+1, tc.desc, tc.ccw, !tc.ccw)
		}

	}
}

func make3DCopy(coords []float64) []float64 {

	len := len(coords)
	copied := make([]float64, len+(len/2))

	j := 0
	for i := 0; i < len; i += 2 {
		copied[j] = coords[i]
		copied[j+1] = coords[i+1]
		copied[j+2] = rand.Float64()
		j += 3
	}

	return copied
}
func TestDistanceFromPointToLine(t *testing.T) {
	for i, tc := range []struct {
		p                  geom.Coord
		startLine, endLine geom.Coord
		distance           float64
	}{
		{
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{1, 0},
			endLine:   geom.Coord{1, 1},
			distance:  1,
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{1, 1},
			endLine:   geom.Coord{1, -1},
			distance:  1,
		},
		{
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{0, 1},
			endLine:   geom.Coord{0, -1},
			distance:  0,
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{1, 0},
			endLine:   geom.Coord{2, 0},
			distance:  1,
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{2, 0},
			endLine:   geom.Coord{1, 0},
			distance:  1,
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{2, 0},
			endLine:   geom.Coord{0, 0},
			distance:  0,
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{0, 0},
			endLine:   geom.Coord{0, 0},
			distance:  0,
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{1, 0},
			endLine:   geom.Coord{1, 0},
			distance:  1,
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{3, 4},
			endLine:   geom.Coord{0, 9},
			distance:  5,
		},
	} {
		calculatedDistance := xy.DistanceFromPointToLine(tc.p, tc.startLine, tc.endLine)
		if tc.distance != calculatedDistance {
			t.Errorf("Test '%v' failed: expected \n%v but was \n%v", i+1, tc.distance, calculatedDistance)
		}
	}
}

func TestPerpendicularDistanceFromPointToLine(t *testing.T) {
	for i, tc := range []struct {
		p                  geom.Coord
		startLine, endLine geom.Coord
		distance           float64
	}{
		{
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{1, 0},
			endLine:   geom.Coord{1, 1},
			distance:  1,
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{1, 1},
			endLine:   geom.Coord{1, -1},
			distance:  1,
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{0, 1},
			endLine:   geom.Coord{0, -1},
			distance:  0,
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{1, 0},
			endLine:   geom.Coord{2, 0},
			distance:  0,
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{2, 0},
			endLine:   geom.Coord{1, 0},
			distance:  0,
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{2, 0},
			endLine:   geom.Coord{0, 0},
			distance:  0,
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{0, 0},
			endLine:   geom.Coord{0, 0},
			distance:  math.NaN(),
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{1, 0},
			endLine:   geom.Coord{1, 0},
			distance:  math.NaN(),
		}, {
			p:         geom.Coord{0, 0},
			startLine: geom.Coord{3, 4},
			endLine:   geom.Coord{3, 9},
			distance:  3,
		},
	} {
		calculatedDistance := xy.PerpendicularDistanceFromPointToLine(tc.p, tc.startLine, tc.endLine)
		if math.IsNaN(tc.distance) {
			if !math.IsNaN(calculatedDistance) {
				t.Errorf("Test '%v' failed: expected Nan but was %v", i+1, calculatedDistance)
			}
		} else if tc.distance != calculatedDistance {
			t.Errorf("Test '%v' failed: expected \n%v but was \n%v", i+1, tc.distance, calculatedDistance)
		}
	}
}

func TestDistanceFromPointToMultiline(t *testing.T) {
	for i, tc := range []struct {
		p        geom.Coord
		lines    []float64
		layout   geom.Layout
		distance float64
	}{
		{
			p:        geom.Coord{0, 0},
			lines:    []float64{1, 0, 1, 1, 2, 0},
			layout:   geom.XY,
			distance: 1,
		},
		{
			p:        geom.Coord{0, 0},
			lines:    []float64{2, 0, 1, 1, 1, 0},
			layout:   geom.XY,
			distance: 1,
		},
	} {
		calculatedDistance := xy.DistanceFromPointToLineString(tc.layout, tc.p, tc.lines)
		if tc.distance != calculatedDistance {
			t.Errorf("Test '%v' failed: expected \n%v but was \n%v", i+1, tc.distance, calculatedDistance)
		}
	}
}

func TestDistanceFromLineToLine(t *testing.T) {
	for i, tc := range []struct {
		desc                                       string
		line1Start, line1End, line2Start, line2End geom.Coord
		distance                                   float64
	}{
		{
			desc:       "Both lines are the same",
			line1Start: geom.Coord{0, 0},
			line1End:   geom.Coord{1, 0},
			line2Start: geom.Coord{0, 0},
			line2End:   geom.Coord{1, 0},
			distance:   0,
		},
		{
			desc:       "Touching perpendicular lines",
			line1Start: geom.Coord{0, 0},
			line1End:   geom.Coord{1, 0},
			line2Start: geom.Coord{0, 0},
			line2End:   geom.Coord{0, 1},
			distance:   0,
		},
		{
			desc:       "Disjoint perpendicular lines",
			line1Start: geom.Coord{0, 0},
			line1End:   geom.Coord{1, 0},
			line2Start: geom.Coord{0, 1},
			line2End:   geom.Coord{0, 2},
			distance:   1,
		},
		{
			desc:       "Disjoint lines that have no distance",
			line1Start: geom.Coord{0, 0},
			line1End:   geom.Coord{0, 0},
			line2Start: geom.Coord{0, 1},
			line2End:   geom.Coord{0, 1},
			distance:   1,
		},
		{
			desc:       "X - cross at origin",
			line1Start: geom.Coord{1, 1},
			line1End:   geom.Coord{-1, -1},
			line2Start: geom.Coord{-1, 1},
			line2End:   geom.Coord{1, -1},
			distance:   0,
		},
		{
			desc:       "Parallel lines the same length and fully parallel",
			line1Start: geom.Coord{0, 0},
			line1End:   geom.Coord{1, 0},
			line2Start: geom.Coord{0, 1},
			line2End:   geom.Coord{1, 1},
			distance:   1,
		},
		{
			desc:       "Parallel lines the same length and only partial overlap (of parallelism)",
			line1Start: geom.Coord{0, 0},
			line1End:   geom.Coord{2, 0},
			line2Start: geom.Coord{-1, 1},
			line2End:   geom.Coord{1, 1},
			distance:   1,
		},
	} {
		calculatedDistance := xy.DistanceFromLineToLine(tc.line1Start, tc.line1End, tc.line2Start, tc.line2End)
		if tc.distance != calculatedDistance {
			t.Errorf("Test '%v' failed: expected \n%v but was \n%v", i+1, tc.distance, calculatedDistance)
		}
	}
}

func TestSignedArea(t *testing.T) {
	for i, tc := range []struct {
		desc        string
		lines       []float64
		area        float64
		areaReverse float64
	}{
		{
			desc:        "A line",
			lines:       []float64{1, 0, 1, 1},
			area:        0,
			areaReverse: 0,
		},
		{
			desc:        "A unclosed 2 line multiline, right angle, Counter Clockwise",
			lines:       []float64{0, 0, 3, 0, 3, 4},
			area:        -6,
			areaReverse: -6,
		},
		{
			desc:        "A square, Counter Clockwise",
			lines:       []float64{0, 0, 3, 0, 3, 3, 0, 3, 0, 0},
			area:        -9,
			areaReverse: -9,
		},
		{
			desc:        "A more complex ring, Counter Clockwise",
			lines:       internal.RING.FlatCoords(),
			area:        -0.024959177231354802,
			areaReverse: -0.024959177231354795,
		},
	} {
		calculatedArea := xy.SignedArea(geom.XY, tc.lines)
		if tc.area != calculatedArea {
			t.Errorf("Test '%v' failed: expected \n%v but was \n%v: \n %v", i+1, tc.area, calculatedArea, tc.lines)
		}

		calculatedArea = xy.SignedArea(geom.XY, reverseCopy(tc.lines))
		if tc.areaReverse != calculatedArea {
			t.Errorf("Reversed Test '%v' failed: expected \n%v but was \n%v: \n %v", i+1, tc.areaReverse, calculatedArea, reverseCopy(tc.lines))
		}

		// test with another ordinate per point
		copied := make3DCopy(tc.lines)
		calculatedArea = xy.SignedArea(geom.XYZ, copied)

		if tc.area != calculatedArea {
			t.Errorf("Test '%v' failed: expected \n%v but was \n%v: \n %v", i+1, tc.area, calculatedArea, tc.lines)
		}
	}
}

func reverseCopy(coords []float64) []float64 {
	copy := make([]float64, len(coords))

	for i := 0; i < len(coords); i++ {
		copy[i] = coords[len(coords)-1-i]
	}

	return copy
}
