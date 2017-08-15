package geom

import (
	"reflect"
	"testing"
)

type testMultiPolygon struct {
	layout     Layout
	stride     int
	coords     [][][]Coord
	flatCoords []float64
	endss      [][]int
	bounds     *Bounds
}

func testMultiPolygonEquals(t *testing.T, mp *MultiPolygon, tmp *testMultiPolygon) {
	if err := mp.verify(); err != nil {
		t.Error(err)
	}
	if mp.Layout() != tmp.layout {
		t.Errorf("mp.Layout() == %v, want %v", mp.Layout(), tmp.layout)
	}
	if mp.Stride() != tmp.stride {
		t.Errorf("mp.Stride() == %v, want %v", mp.Stride(), tmp.stride)
	}
	if !reflect.DeepEqual(mp.Coords(), tmp.coords) {
		t.Errorf("mp.Coords() == %v, want %v", mp.Coords(), tmp.coords)
	}
	if !reflect.DeepEqual(mp.FlatCoords(), tmp.flatCoords) {
		t.Errorf("mp.FlatCoords() == %v, want %v", mp.FlatCoords(), tmp.flatCoords)
	}
	if !reflect.DeepEqual(mp.Endss(), tmp.endss) {
		t.Errorf("mp.Endss() == %v, want %v", mp.Endss(), tmp.endss)
	}
	if !reflect.DeepEqual(mp.Bounds(), tmp.bounds) {
		t.Errorf("mp.Bounds() == %v, want %v", mp.Bounds(), tmp.bounds)
	}
	if got := mp.NumPolygons(); got != len(tmp.coords) {
		t.Errorf("mp.NumPolygons() == %v, want %v", got, len(tmp.coords))
	}
	for i, c := range tmp.coords {
		want := NewPolygon(mp.Layout()).MustSetCoords(c)
		if got := mp.Polygon(i); !reflect.DeepEqual(got, want) {
			t.Errorf("mp.Polygon(%v) == %v, want %v", i, got, want)
		}
	}
}

func TestMultiPolygon(t *testing.T) {
	for _, c := range []struct {
		mp  *MultiPolygon
		tmp *testMultiPolygon
	}{
		{
			mp: NewMultiPolygon(XY).MustSetCoords([][][]Coord{{{{1, 2}, {3, 4}, {5, 6}}, {{7, 8}, {9, 10}, {11, 12}}}}),
			tmp: &testMultiPolygon{
				layout:     XY,
				stride:     2,
				coords:     [][][]Coord{{{{1, 2}, {3, 4}, {5, 6}}, {{7, 8}, {9, 10}, {11, 12}}}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
				endss:      [][]int{{6, 12}},
				bounds:     NewBounds(XY).Set(1, 2, 11, 12),
			},
		},
	} {

		testMultiPolygonEquals(t, c.mp, c.tmp)
	}
}

func TestMultiPolygonClone(t *testing.T) {
	p1 := NewMultiPolygon(XY).MustSetCoords([][][]Coord{{{{1, 2}, {3, 4}, {5, 6}}}})
	if p2 := p1.Clone(); aliases(p1.FlatCoords(), p2.FlatCoords()) {
		t.Error("Clone() should not alias flatCoords")
	}
}

func TestMultiPolygonStrideMismatch(t *testing.T) {
	for _, c := range []struct {
		layout Layout
		coords [][][]Coord
		err    error
	}{
		{
			layout: XY,
			coords: nil,
			err:    nil,
		},
		{
			layout: XY,
			coords: [][][]Coord{},
			err:    nil,
		},
		{
			layout: XY,
			coords: [][][]Coord{{{{1, 2}, {}}}},
			err:    ErrStrideMismatch{Got: 0, Want: 2},
		},
		{
			layout: XY,
			coords: [][][]Coord{{{{1, 2}, {1}}}},
			err:    ErrStrideMismatch{Got: 1, Want: 2},
		},
		{
			layout: XY,
			coords: [][][]Coord{{{{1, 2}, {3, 4}}}},
			err:    nil,
		},
		{
			layout: XY,
			coords: [][][]Coord{{{{1, 2}, {3, 4, 5}}}},
			err:    ErrStrideMismatch{Got: 3, Want: 2},
		},
	} {
		mp := NewMultiPolygon(c.layout)
		if _, err := mp.SetCoords(c.coords); err != c.err {
			t.Errorf("mp.SetCoords(%v) == %v, want %v", c.coords, err, c.err)
		}
	}
}
