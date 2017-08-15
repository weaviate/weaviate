package xy_test

import (
	"fmt"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy"
)

func ExampleOrientationIndex() {

	vectorOrigin := geom.Coord{10.0, 10.0}
	vectorEnd := geom.Coord{20.0, 20.0}
	target := geom.Coord{10.0, 20.0}

	orientation := xy.OrientationIndex(vectorOrigin, vectorEnd, target)

	fmt.Println(orientation)
	// Output: CounterClockwise
}

func ExampleIsOnLine() {
	line := geom.NewLineString(geom.XY)
	line.MustSetCoords([]geom.Coord{
		{0, 0}, {10, 0}, {10, 20},
	})
	onLine := xy.IsOnLine(line.Layout(), geom.Coord{5, 0}, line.FlatCoords())
	fmt.Println(onLine)
	// Output: true
}

func ExampleIsRingCounterClockwise() {
	ring := geom.NewLinearRingFlat(geom.XY, []float64{10, 10, 20, 10, 30, 30, 10, 30, 10, 10})
	clockwise := xy.IsRingCounterClockwise(ring.Layout(), ring.FlatCoords())
	fmt.Println(clockwise)
	// Output: true
}

func ExampleDistanceFromPointToLine() {
	p := geom.Coord{0, 0}
	lineStart := geom.Coord{10, -10}
	lineEnd := geom.Coord{10, 10}
	distance := xy.DistanceFromPointToLine(p, lineStart, lineEnd)
	fmt.Println(distance)
	// Output: 10
}

func ExamplePerpendicularDistanceFromPointToLine() {
	p := geom.Coord{0, 0}
	lineStart := geom.Coord{10, 5}
	lineEnd := geom.Coord{10, 10}
	distance := xy.PerpendicularDistanceFromPointToLine(p, lineStart, lineEnd)
	fmt.Println(distance)
	// Output: 10
}

func ExampleDistanceFromPointToLineString() {
	p := geom.Coord{50, 50}
	lineString := geom.NewLineStringFlat(geom.XY, []float64{0, 0, 10, 10, 10, 20, 10, 100})
	distance := xy.DistanceFromPointToLineString(lineString.Layout(), p, lineString.FlatCoords())
	fmt.Println(distance)
	// Output: 40
}

func ExampleDistanceFromLineToLine() {
	line1 := geom.NewLineStringFlat(geom.XY, []float64{0, 0, 10, 10})
	line2 := geom.NewLineStringFlat(geom.XY, []float64{-10, -10, 0, -10})
	distance := xy.DistanceFromLineToLine(line1.Coord(0), line1.Coord(1), line2.Coord(0), line2.Coord(1))
	fmt.Println(distance)
	// Output: 10
}

func ExampleSignedArea() {
	ring := geom.NewLinearRingFlat(geom.XY, []float64{10, 10, 20, 10, 30, 30, 10, 30, 10, 10})
	singedArea := xy.SignedArea(ring.Layout(), ring.FlatCoords())
	fmt.Println(singedArea)
	// Output: -300
}

func ExampleIsPointWithinLineBounds() {
	point := geom.Coord{0, 0}
	line := geom.NewLineStringFlat(geom.XY, []float64{-10, -10, 0, -10})
	isWithinLineBounds := xy.IsPointWithinLineBounds(point, line.Coord(0), line.Coord(1))
	fmt.Println(isWithinLineBounds)
	// Output: false
}

func ExampleDoLinesOverlap() {
	line1Start := geom.Coord{0, 0}
	line1End := geom.Coord{10, 10}
	line2Start := geom.Coord{0, -10}
	line2End := geom.Coord{10, 5}
	overlaps := xy.DoLinesOverlap(line1Start, line1End, line2Start, line2End)
	fmt.Println(overlaps)
	// Output: true
}

func ExampleEqual() {
	coords := []float64{10, 30, 30, 10}
	isEqual := xy.Equal(coords, 0, coords, 1)
	fmt.Println(isEqual)
	// Output: false
}

func ExampleDistance() {
	coords := []float64{10, 10, 10, -10}
	distance := xy.Distance(geom.Coord(coords[0:2]), geom.Coord(coords[2:4]))
	fmt.Println(distance)
	// Output: 20
}
