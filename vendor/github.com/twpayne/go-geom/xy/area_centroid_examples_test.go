package xy_test

import (
	"fmt"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy"
)

func ExamplePolygonsCentroid() {
	poly1 := geom.NewPolygonFlat(geom.XY, []float64{0, 0, -10, 0, -10, -10, 0, -10, 0, 0}, []int{10})
	poly2 := geom.NewPolygonFlat(geom.XY, []float64{0, 0, 10, 0, 10, 10, 0, 10, 0, 0}, []int{10})

	centroid := xy.PolygonsCentroid(poly1, poly2)

	fmt.Println(centroid)
	// Output: [0 0]
}

func ExampleAreaCentroidCalculator_AddPolygon() {
	polygons := []*geom.Polygon{
		geom.NewPolygonFlat(geom.XY, []float64{0, 0, -10, 0, -10, -10, 0, -10, 0, 0}, []int{10}),
		geom.NewPolygonFlat(geom.XY, []float64{0, 0, 10, 0, 10, 10, 0, 10, 0, 0}, []int{10}),
	}

	calculator := xy.NewAreaCentroidCalculator(geom.XY)

	for _, p := range polygons {
		calculator.AddPolygon(p)
	}

	fmt.Println(calculator.GetCentroid())

	// Output: [0 0]
}
