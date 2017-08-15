package geom

import (
	"reflect"
	"testing"
)

type testMultiLineString struct {
	layout     Layout
	stride     int
	coords     [][]Coord
	flatCoords []float64
	ends       []int
	bounds     *Bounds
}

func testMultiLineStringEquals(t *testing.T, mls *MultiLineString, tmls *testMultiLineString) {
	if err := mls.verify(); err != nil {
		t.Error(err)
	}
	if mls.Layout() != tmls.layout {
		t.Errorf("mls.Layout() == %v, want %v", mls.Layout(), tmls.layout)
	}
	if mls.Stride() != tmls.stride {
		t.Errorf("mls.Stride() == %v, want %v", mls.Stride(), tmls.stride)
	}
	if !reflect.DeepEqual(mls.Coords(), tmls.coords) {
		t.Errorf("mls.Coords() == %v, want %v", mls.Coords(), tmls.coords)
	}
	if !reflect.DeepEqual(mls.FlatCoords(), tmls.flatCoords) {
		t.Errorf("mls.FlatCoords() == %v, want %v", mls.FlatCoords(), tmls.flatCoords)
	}
	if !reflect.DeepEqual(mls.Ends(), tmls.ends) {
		t.Errorf("mls.Ends() == %v, want %v", mls.Ends(), tmls.ends)
	}
	if !reflect.DeepEqual(mls.Bounds(), tmls.bounds) {
		t.Errorf("mls.Bounds() == %v, want %v", mls.Bounds(), tmls.bounds)
	}
	if got := mls.NumLineStrings(); got != len(tmls.coords) {
		t.Errorf("mls.NumLineStrings() == %v, want %v", got, len(tmls.coords))
	}
	for i, c := range tmls.coords {
		want := NewLineString(mls.Layout()).MustSetCoords(c)
		if got := mls.LineString(i); !reflect.DeepEqual(got, want) {
			t.Errorf("mls.LineString(%v) == %v, want %v", i, got, want)
		}
	}
}

func TestMultiLineString(t *testing.T) {
	for _, c := range []struct {
		mls  *MultiLineString
		tmls *testMultiLineString
	}{
		{
			mls: NewMultiLineString(XY).MustSetCoords([][]Coord{{{1, 2}, {3, 4}, {5, 6}}, {{7, 8}, {9, 10}, {11, 12}}}),
			tmls: &testMultiLineString{
				layout:     XY,
				stride:     2,
				coords:     [][]Coord{{{1, 2}, {3, 4}, {5, 6}}, {{7, 8}, {9, 10}, {11, 12}}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
				ends:       []int{6, 12},
				bounds:     NewBounds(XY).Set(1, 2, 11, 12),
			},
		},
	} {
		testMultiLineStringEquals(t, c.mls, c.tmls)
	}
}

func TestMultiLineStringClone(t *testing.T) {
	p1 := NewMultiLineString(XY).MustSetCoords([][]Coord{{{1, 2}, {3, 4}, {5, 6}}})
	if p2 := p1.Clone(); aliases(p1.FlatCoords(), p2.FlatCoords()) {
		t.Error("Clone() should not alias flatCoords")
	}
}

func TestMultiLineStringStrideMismatch(t *testing.T) {
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
		mls := NewMultiLineString(c.layout)
		if _, err := mls.SetCoords(c.coords); err != c.err {
			t.Errorf("mls.SetCoords(%v) == %v, want %v", c.coords, err, c.err)
		}
	}
}
