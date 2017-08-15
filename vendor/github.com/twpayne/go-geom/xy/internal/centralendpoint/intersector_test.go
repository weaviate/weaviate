package centralendpoint_test

import (
	"testing"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy/internal/centralendpoint"
)

func TestGetIntersection(t *testing.T) {
	for i, tc := range []struct {
		p1, p2, p3, p4 geom.Coord
		layout         geom.Layout
		result         geom.Coord
	}{
		{
			p1: geom.Coord{-1, 0}, p2: geom.Coord{1, 0}, p3: geom.Coord{0, -1}, p4: geom.Coord{0, 1},
			layout: geom.XY,
			result: geom.Coord{-1.0, 0.0},
		},
		{
			p1: geom.Coord{10, 10}, p2: geom.Coord{20, 20}, p3: geom.Coord{10, 20}, p4: geom.Coord{20, 10},
			layout: geom.XY,
			result: geom.Coord{10.0, 10.0},
		},
		{
			p1: geom.Coord{10, 10}, p2: geom.Coord{20, 20}, p3: geom.Coord{20, 20}, p4: geom.Coord{10, 10},
			layout: geom.XY,
			result: geom.Coord{10.0, 10.0},
		},
		{
			p1: geom.Coord{10, 10}, p2: geom.Coord{20, 20}, p3: geom.Coord{30, 20}, p4: geom.Coord{20, 10},
			layout: geom.XY,
			result: geom.Coord{20.0, 20.0},
		},
	} {
		calculatedResult := centralendpoint.GetIntersection(tc.p1, tc.p2, tc.p3, tc.p4)

		if !calculatedResult.Equal(tc.layout, tc.result) {
			t.Errorf("Test '%v' failed: expected \n%v but was \n%v", i+1, tc.result, calculatedResult)
		}

	}
}
