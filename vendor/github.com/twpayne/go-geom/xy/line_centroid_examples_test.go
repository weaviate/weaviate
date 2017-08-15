package xy_test

import (
	"fmt"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy"
)

func ExampleLinesCentroid() {
	line1 := geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 3, 3})
	line2 := geom.NewLineStringFlat(geom.XY, []float64{10, 10, 11, 11, 13, 13})
	centroid := xy.LinesCentroid(line1, line2)
	fmt.Println(centroid)
	//Output: [6.5 6.5]
}

func ExampleLinearRingsCentroid() {
	line1 := geom.NewLinearRingFlat(geom.XY, []float64{0, 0, 1, 1, 3, 3, 0, 0})
	line2 := geom.NewLinearRingFlat(geom.XY, []float64{10, 10, 11, 11, 13, 13, 10, 10})
	centroid := xy.LinearRingsCentroid(line1, line2)
	fmt.Println(centroid)
	//Output: [6.5 6.5]
}

func ExampleMultiLineCentroid() {
	line := geom.NewMultiLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 3, 3, 10, 10, 11, 11, 13, 13}, []int{6, 12})
	centroid := xy.MultiLineCentroid(line)
	fmt.Println(centroid)
	//Output: [6.5 6.5]

}

func ExampleNewLineCentroidCalculator() {

	calculator := xy.NewLineCentroidCalculator(geom.XY)
	calculator.AddLine(geom.NewLineStringFlat(geom.XY, []float64{0, 0, 1, 1, 3, 3}))
	calculator.AddLine(geom.NewLineStringFlat(geom.XY, []float64{10, 10, 11, 11, 13, 13}))
	centroid := calculator.GetCentroid()
	fmt.Println(centroid)
	//Output: [6.5 6.5]

}

func ExampleLineCentroidCalculator_AddPolygon() {

	calculator := xy.NewLineCentroidCalculator(geom.XY)
	calculator.AddPolygon(geom.NewPolygonFlat(geom.XY, []float64{0, 0, 1, 1, 3, 3}, []int{6}))
	centroid := calculator.GetCentroid()
	fmt.Println(centroid)
	//Output: [1.5 1.5]

}
