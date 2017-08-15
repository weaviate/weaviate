package xy_test

import (
	"fmt"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy"
)

func ExamplePointsCentroid() {
	centroid := xy.PointsCentroid(
		geom.NewPointFlat(geom.XY, []float64{0, 0}),
		geom.NewPointFlat(geom.XY, []float64{2, 0}),
		geom.NewPointFlat(geom.XY, []float64{2, 2}),
		geom.NewPointFlat(geom.XY, []float64{0, 2}))

	fmt.Println(centroid)
	// Output: [1 1]
}

func ExampleMultiPointCentroid() {
	multiPoint := geom.NewMultiPointFlat(geom.XY, []float64{
		0, 0,
		2, 0,
		2, 2,
		0, 2})
	centroid := xy.MultiPointCentroid(multiPoint)

	fmt.Println(centroid)
	// Output: [1 1]
}

func ExamplePointsCentroidFlat() {
	multiPoint := geom.NewMultiPointFlat(geom.XY, []float64{0, 0, 2, 0, 2, 2, 0, 2})
	centroid := xy.PointsCentroidFlat(multiPoint.Layout(), multiPoint.FlatCoords())
	fmt.Println(centroid)
	// Output: [1 1]
}

func ExampleNewPointCentroidCalculator() {
	polygon := geom.NewPolygonFlat(geom.XY, []float64{0, 0, 2, 0, 2, 2, 0, 2}, []int{8})
	calculator := xy.NewPointCentroidCalculator()
	coords := polygon.FlatCoords()
	stride := polygon.Layout().Stride()

	for i := 0; i < len(coords); i += stride {
		calculator.AddCoord(geom.Coord(coords[i : i+stride]))
	}

	fmt.Println(calculator.GetCentroid())
	//Output: [1 1]
}
