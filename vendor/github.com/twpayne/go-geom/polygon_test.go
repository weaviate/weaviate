package geom

import (
	"fmt"
	"reflect"
	"testing"
)

func ExampleNewPolygon() {
	unitSquare := NewPolygon(XY).MustSetCoords([][]Coord{
		{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
	})
	fmt.Printf("unitSquare.Area() == %f", unitSquare.Area())
	// Output: unitSquare.Area() == 1.000000
}

type testPolygon struct {
	layout     Layout
	stride     int
	coords     [][]Coord
	ends       []int
	flatCoords []float64
	bounds     *Bounds
}

func testPolygonEquals(t *testing.T, p *Polygon, tp *testPolygon) {
	if err := p.verify(); err != nil {
		t.Error(err)
	}
	if p.Layout() != tp.layout {
		t.Errorf("p.Layout() == %v, want %v", p.Layout(), tp.layout)
	}
	if p.Stride() != tp.stride {
		t.Errorf("p.Stride() == %v, want %v", p.Stride(), tp.stride)
	}
	if !reflect.DeepEqual(p.Coords(), tp.coords) {
		t.Errorf("p.Coords() == %v, want %v", p.Coords(), tp.coords)
	}
	if !reflect.DeepEqual(p.FlatCoords(), tp.flatCoords) {
		t.Errorf("p.FlatCoords() == %v, want %v", p.FlatCoords(), tp.flatCoords)
	}
	if !reflect.DeepEqual(p.Ends(), tp.ends) {
		t.Errorf("p.Ends() == %v, want %v", p.Ends(), tp.ends)
	}
	if !reflect.DeepEqual(p.Bounds(), tp.bounds) {
		t.Errorf("p.Bounds() == %v, want %v", p.Bounds(), tp.bounds)
	}
	if got := p.NumLinearRings(); got != len(tp.coords) {
		t.Errorf("p.NumLinearRings() == %v, want %v", got, len(tp.coords))
	}
	for i, c := range tp.coords {
		want := NewLinearRing(p.Layout()).MustSetCoords(c)
		if got := p.LinearRing(i); !reflect.DeepEqual(got, want) {
			t.Errorf("p.LinearRing(%v) == %v, want %v", i, got, want)
		}
	}
}

func TestPolygon(t *testing.T) {
	for _, c := range []struct {
		p  *Polygon
		tp *testPolygon
	}{
		{
			p: NewPolygon(XY).MustSetCoords([][]Coord{{{1, 2}, {3, 4}, {5, 6}}, {{7, 8}, {9, 10}, {11, 12}}}),
			tp: &testPolygon{
				layout:     XY,
				stride:     2,
				coords:     [][]Coord{{{1, 2}, {3, 4}, {5, 6}}, {{7, 8}, {9, 10}, {11, 12}}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
				ends:       []int{6, 12},
				bounds:     NewBounds(XY).Set(1, 2, 11, 12),
			},
		},
	} {

		testPolygonEquals(t, c.p, c.tp)
	}
}

func TestPolygonClone(t *testing.T) {
	p1 := NewPolygon(XY).MustSetCoords([][]Coord{{{1, 2}, {3, 4}, {5, 6}}})
	if p2 := p1.Clone(); aliases(p1.FlatCoords(), p2.FlatCoords()) {
		t.Error("Clone() should not alias flatCoords")
	}
}

func TestPolygonStrideMismatch(t *testing.T) {
	for _, c := range []struct {
		layout Layout
		coords [][]Coord
		err    error
	}{
		{
			layout: XY,
			coords: nil,
			err:    nil,
		},
		{
			layout: XY,
			coords: [][]Coord{},
			err:    nil,
		},
		{
			layout: XY,
			coords: [][]Coord{{{1, 2}, {}}},
			err:    ErrStrideMismatch{Got: 0, Want: 2},
		},
		{
			layout: XY,
			coords: [][]Coord{{{1, 2}, {1}}},
			err:    ErrStrideMismatch{Got: 1, Want: 2},
		},
		{
			layout: XY,
			coords: [][]Coord{{{1, 2}, {3, 4}}},
			err:    nil,
		},
		{
			layout: XY,
			coords: [][]Coord{{{1, 2}, {3, 4, 5}}},
			err:    ErrStrideMismatch{Got: 3, Want: 2},
		},
	} {
		p := NewPolygon(c.layout)
		if _, err := p.SetCoords(c.coords); err != c.err {
			t.Errorf("p.SetCoords(%v) == %v, want %v", c.coords, err, c.err)
		}
	}
}
