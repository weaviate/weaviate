package hcoords

import (
	"fmt"
	"math"

	"github.com/twpayne/go-geom"
)

// GetIntersection Computes the (approximate) intersection point between two line segments
// using homogeneous coordinates.
//
// Note that this algorithm is not numerically stable; i.e. it can produce intersection points which
// lie outside the envelope of the line segments themselves.  In order to increase the precision of the calculation
// input points should be normalized before passing them to this routine.
func GetIntersection(line1End1, line1End2, line2End1, line2End2 geom.Coord) (geom.Coord, error) {
	// unrolled computation
	line1Xdiff := line1End1[1] - line1End2[1]
	line1Ydiff := line1End2[0] - line1End1[0]
	line1W := line1End1[0]*line1End2[1] - line1End2[0]*line1End1[1]

	line2X := line2End1[1] - line2End2[1]
	line2Y := line2End2[0] - line2End1[0]
	line2W := line2End1[0]*line2End2[1] - line2End2[0]*line2End1[1]

	x := line1Ydiff*line2W - line2Y*line1W
	y := line2X*line1W - line1Xdiff*line2W
	w := line1Xdiff*line2Y - line2X*line1Ydiff

	xIntersection := x / w
	yIntersection := y / w

	if math.IsNaN(xIntersection) || math.IsNaN(yIntersection) {
		return nil, fmt.Errorf("intersection cannot be calculated using the h-coords implementation")
	}

	if math.IsInf(xIntersection, 0) || math.IsInf(yIntersection, 0) {
		return nil, fmt.Errorf("intersection cannot be calculated using the h-coords implementation")
	}

	return geom.Coord{xIntersection, yIntersection}, nil
}
