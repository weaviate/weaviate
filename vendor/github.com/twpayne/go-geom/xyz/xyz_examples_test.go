package xyz_test

import (
	"fmt"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xyz"
)

func ExampleDistance() {
	p1 := geom.Coord{0, 0, 0}
	p2 := geom.Coord{10, 0, 0}

	distance := xyz.Distance(p1, p2)

	fmt.Println(distance)

	// Output: 10
}

func ExampleDistancePointToLine() {
	p1 := geom.Coord{0, 0, 0}
	lineStart := geom.Coord{10, 0, 10}
	lineEnd := geom.Coord{10, 0, -10}

	distance := xyz.DistancePointToLine(p1, lineStart, lineEnd)

	fmt.Println(distance)

	// Output: 10
}

func ExampleDistanceLineToLine() {

	line1Start := geom.Coord{10, 0, 10}
	line1End := geom.Coord{10, 0, -10}
	line2Start := geom.Coord{0, 0, 10}
	line2End := geom.Coord{0, 0, -10}

	distance := xyz.DistanceLineToLine(line1Start, line1End, line2Start, line2End)

	fmt.Println(distance)

	// Output: 10
}

func ExampleEquals() {
	p1 := geom.Coord{0, 0}
	p2 := geom.Coord{10, 0}

	distance := xyz.Equals(p1, p2)

	fmt.Println(distance)

	// Output: false
}
