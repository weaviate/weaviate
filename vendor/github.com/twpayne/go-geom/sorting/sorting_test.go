package sorting_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/sorting"
)

func TestCompare2D(t *testing.T) {
	for i, tc := range []struct {
		c1, c2 []float64
		result bool
	}{
		{
			c1:     []float64{0, 0},
			c2:     []float64{0, 0},
			result: false,
		},
		{
			c1:     []float64{1, 0},
			c2:     []float64{0, 1},
			result: false,
		},
		{
			c1:     []float64{1, 0},
			c2:     []float64{0, 0},
			result: false,
		},
		{
			c1:     []float64{0, 1},
			c2:     []float64{0, 0},
			result: false,
		},
		{
			c1:     []float64{0, 0},
			c2:     []float64{0, 1},
			result: true,
		},
		{
			c1:     []float64{0, 0},
			c2:     []float64{1, 0},
			result: true,
		},
	} {
		actual := sorting.IsLess2D(tc.c1, tc.c2)

		if actual != tc.result {
			t.Errorf("Test %d failed.  Expected %v but got %v", i+1, tc.result, actual)
		}
	}
}

func TestNewFlatCoordSorting2D(t *testing.T) {
	for i, tc := range []struct {
		c1, result []float64
		layout     geom.Layout
	}{
		{
			c1:     []float64{},
			result: []float64{},
			layout: geom.XY,
		},
		{
			c1:     []float64{0, 0, 1, 1},
			result: []float64{0, 0, 1, 1},
			layout: geom.XY,
		},
		{
			c1:     []float64{1, 0, 0, 1, 2, 2, 2, -2, -1, 0, 0, 0, 0, 0},
			result: []float64{-1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 2, -2, 2, 2},
			layout: geom.XY,
		},
		{
			c1:     []float64{1, 0, 6, 0, 1, 6, 2, 2, 6, 2, -2, 6, -1, 0, 8, 0, 0, 6, 0, 0, 6},
			result: []float64{-1, 0, 8, 0, 0, 6, 0, 0, 6, 0, 1, 6, 1, 0, 6, 2, -2, 6, 2, 2, 6},
			layout: geom.XYM,
		},
	} {
		actual := make([]float64, len(tc.c1))
		copy(actual, tc.c1)
		sort.Sort(sorting.NewFlatCoordSorting2D(tc.layout, actual))

		if !reflect.DeepEqual(tc.result, actual) {
			t.Errorf("Test %d: Failed to sort coordinates correctly. Expected: \n\t%v\nBut was:\n\t%v", i+1, tc.result, actual)
		}
	}
}
