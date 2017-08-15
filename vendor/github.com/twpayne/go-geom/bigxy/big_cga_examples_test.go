package bigxy_test

import (
	"fmt"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/bigxy"
)

func ExampleOrientationIndex() {
	vectorOrigin := geom.Coord{10.0, 10.0}
	vectorEnd := geom.Coord{20.0, 20.0}
	target := geom.Coord{10.0, 20.0}

	orientation := bigxy.OrientationIndex(vectorOrigin, vectorEnd, target)

	fmt.Println(orientation)
	// Output: CounterClockwise
}

func ExampleIntersection() {
	line1Start := geom.Coord{0, 1}
	line1End := geom.Coord{0, -1}
	line2Start := geom.Coord{-1, 0}
	line2End := geom.Coord{1, 0}

	intersection := bigxy.Intersection(line1Start, line1End, line2Start, line2End)

	fmt.Println(intersection)
	// Output: [0 0]
}
