package geom

import (
	"reflect"
	"testing"
)

type testLinearRing struct {
	layout     Layout
	stride     int
	coords     []Coord
	flatCoords []float64
	bounds     *Bounds
}

func testLinearRingEquals(t *testing.T, lr *LinearRing, tlr *testLinearRing) {
	if err := lr.verify(); err != nil {
		t.Error(err)
	}
	if lr.Layout() != tlr.layout {
		t.Errorf("lr.Layout() == %v, want %v", lr.Layout(), tlr.layout)
	}
	if lr.Stride() != tlr.stride {
		t.Errorf("lr.Stride() == %v, want %v", lr.Stride(), tlr.stride)
	}
	if !reflect.DeepEqual(lr.Coords(), tlr.coords) {
		t.Errorf("lr.Coords() == %v, want %v", lr.Coords(), tlr.coords)
	}
	if !reflect.DeepEqual(lr.FlatCoords(), tlr.flatCoords) {
		t.Errorf("lr.FlatCoords() == %v, want %v", lr.FlatCoords(), tlr.flatCoords)
	}
	if !reflect.DeepEqual(lr.Bounds(), tlr.bounds) {
		t.Errorf("lr.Bounds() == %v, want %v", lr.Bounds(), tlr.bounds)
	}
	if got := lr.NumCoords(); got != len(tlr.coords) {
		t.Errorf("lr.NumCoords() == %v, want %v", got, len(tlr.coords))
	}
	for i, c := range tlr.coords {
		if !reflect.DeepEqual(lr.Coord(i), c) {
			t.Errorf("lr.Coord(%v) == %v, want %v", i, lr.Coord(i), c)
		}
	}
}

func TestLinearRing(t *testing.T) {
	for _, c := range []struct {
		lr  *LinearRing
		tlr *testLinearRing
	}{
		{
			lr: NewLinearRing(XY).MustSetCoords([]Coord{{1, 2}, {3, 4}, {5, 6}}),
			tlr: &testLinearRing{
				layout:     XY,
				stride:     2,
				coords:     []Coord{{1, 2}, {3, 4}, {5, 6}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6},
				bounds:     NewBounds(XY).Set(1, 2, 5, 6),
			},
		},
		{
			lr: NewLinearRing(XYZ).MustSetCoords([]Coord{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}),
			tlr: &testLinearRing{
				layout:     XYZ,
				stride:     3,
				coords:     []Coord{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9},
				bounds:     NewBounds(XYZ).Set(1, 2, 3, 7, 8, 9),
			},
		},
		{
			lr: NewLinearRing(XYM).MustSetCoords([]Coord{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}),
			tlr: &testLinearRing{
				layout:     XYM,
				stride:     3,
				coords:     []Coord{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9},
				bounds:     NewBounds(XYM).Set(1, 2, 3, 7, 8, 9),
			},
		},
		{
			lr: NewLinearRing(XYZM).MustSetCoords([]Coord{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}}),
			tlr: &testLinearRing{
				layout:     XYZM,
				stride:     4,
				coords:     []Coord{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
				bounds:     NewBounds(XYZM).Set(1, 2, 3, 4, 9, 10, 11, 12),
			},
		},
	} {
		testLinearRingEquals(t, c.lr, c.tlr)
	}
}

func TestLinearRingClone(t *testing.T) {
	p1 := NewLinearRing(XY).MustSetCoords([]Coord{{1, 2}, {3, 4}, {5, 6}})
	if p2 := p1.Clone(); aliases(p1.FlatCoords(), p2.FlatCoords()) {
		t.Error("Clone() should not alias flatCoords")
	}
}

func TestLinearRingStrideMismatch(t *testing.T) {
	for _, c := range []struct {
		layout Layout
		coords []Coord
		err    error
	}{
		{
			layout: XY,
			coords: nil,
			err:    nil,
		},
		{
			layout: XY,
			coords: []Coord{},
			err:    nil,
		},
		{
			layout: XY,
			coords: []Coord{{1, 2}, {}},
			err:    ErrStrideMismatch{Got: 0, Want: 2},
		},
		{
			layout: XY,
			coords: []Coord{{1, 2}, {1}},
			err:    ErrStrideMismatch{Got: 1, Want: 2},
		},
		{
			layout: XY,
			coords: []Coord{{1, 2}, {3, 4}},
			err:    nil,
		},
		{
			layout: XY,
			coords: []Coord{{1, 2}, {3, 4, 5}},
			err:    ErrStrideMismatch{Got: 3, Want: 2},
		},
	} {
		p := NewLinearRing(c.layout)
		if _, err := p.SetCoords(c.coords); err != c.err {
			t.Errorf("p.SetCoords(%v) == %v, want %v", c.coords, err, c.err)
		}
	}
}
