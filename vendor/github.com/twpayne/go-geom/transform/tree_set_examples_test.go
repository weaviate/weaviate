package transform_test

import (
	"fmt"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/sorting"
	"github.com/twpayne/go-geom/transform"
)

type treeSetExampleCompare struct{}

func (c treeSetExampleCompare) IsEquals(x, y geom.Coord) bool {
	return x[0] == y[0] && x[1] == y[1]
}
func (c treeSetExampleCompare) IsLess(x, y geom.Coord) bool {
	return sorting.IsLess2D(x, y)
}

func ExampleNewTreeSet() {
	set := transform.NewTreeSet(geom.XY, treeSetExampleCompare{})
	set.Insert([]float64{3, 1})
	set.Insert([]float64{3, 2})
	set.Insert([]float64{1, 2})

	fmt.Println(set.ToFlatArray())

	// Output: [1 2 3 1 3 2]
}
