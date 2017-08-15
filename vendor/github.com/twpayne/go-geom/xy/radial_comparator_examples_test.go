package xy_test

import (
	"fmt"
	"sort"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy"
)

func ExampleNewRadialSorting() {
	coords := []float64{10, 10, 20, 20, 20, 0, 30, 10, 0, 0, 1, 1}
	sorting := xy.NewRadialSorting(geom.XY, coords, geom.Coord{10, 10})
	sort.Sort(sorting)
	fmt.Println(coords)
	// Output: [10 10 20 20 30 10 20 0 1 1 0 0]
}
