package transform

import (
	"reflect"
	"testing"

	"github.com/twpayne/go-geom"
)

func TestUniqueCoords(t *testing.T) {
	for i, tc := range []struct {
		pts, expected []float64
		compare       Compare
		layout        geom.Layout
	}{
		{
			pts: []float64{
				0, 0, 1, 0, 2, 2, 0, 0, 2, 0, 2, 2, 1, 0,
			},
			expected: []float64{
				0, 0, 1, 0, 2, 2, 2, 0,
			},
			layout:  geom.XY,
			compare: testCompare{},
		},
	} {
		filteredCoords := UniqueCoords(tc.layout, tc.compare, tc.pts)

		if !reflect.DeepEqual(filteredCoords, tc.expected) {
			t.Errorf("Test %v Failed: FlatCoords(%v, ..., %v) didn't result in the expected result.  Expected\n\t%v\nbut was\n\t%v", i+1, tc.layout, tc.compare, tc.expected, filteredCoords)
		}
	}
}
