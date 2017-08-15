package geom

import (
	"reflect"
	"testing"
)

type testLineString struct {
	layout     Layout
	stride     int
	coords     []Coord
	flatCoords []float64
	bounds     *Bounds
}

func testLineStringEquals(t *testing.T, ls *LineString, tls *testLineString) {
	if err := ls.verify(); err != nil {
		t.Error(err)
	}
	if ls.Layout() != tls.layout {
		t.Errorf("ls.Layout() == %v, want %v", ls.Layout(), tls.layout)
	}
	if ls.Stride() != tls.stride {
		t.Errorf("ls.Stride() == %v, want %v", ls.Stride(), tls.stride)
	}
	if !reflect.DeepEqual(ls.Coords(), tls.coords) {
		t.Errorf("ls.Coords() == %v, want %v", ls.Coords(), tls.coords)
	}
	if !reflect.DeepEqual(ls.FlatCoords(), tls.flatCoords) {
		t.Errorf("ls.FlatCoords() == %v, want %v", ls.FlatCoords(), tls.flatCoords)
	}
	if !reflect.DeepEqual(ls.Bounds(), tls.bounds) {
		t.Errorf("ls.Bounds() == %v, want %v", ls.Bounds(), tls.bounds)
	}
	if got := ls.NumCoords(); got != len(tls.coords) {
		t.Errorf("ls.NumCoords() == %v, want %v", got, len(tls.coords))
	}
	for i, c := range tls.coords {
		if !reflect.DeepEqual(ls.Coord(i), c) {
			t.Errorf("ls.Coord(%v) == %v, want %v", i, ls.Coord(i), c)
		}
	}
}

func TestLineString(t *testing.T) {
	for _, c := range []struct {
		ls  *LineString
		tls *testLineString
	}{
		{
			ls: NewLineString(XY).MustSetCoords([]Coord{{1, 2}, {3, 4}, {5, 6}}),
			tls: &testLineString{
				layout:     XY,
				stride:     2,
				coords:     []Coord{{1, 2}, {3, 4}, {5, 6}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6},
				bounds:     NewBounds(XY).Set(1, 2, 5, 6),
			},
		},
		{
			ls: NewLineString(XYZ).MustSetCoords([]Coord{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}),
			tls: &testLineString{
				layout:     XYZ,
				stride:     3,
				coords:     []Coord{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9},
				bounds:     NewBounds(XYZ).Set(1, 2, 3, 7, 8, 9),
			},
		},
		{
			ls: NewLineString(XYM).MustSetCoords([]Coord{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}),
			tls: &testLineString{
				layout:     XYM,
				stride:     3,
				coords:     []Coord{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9},
				bounds:     NewBounds(XYM).Set(1, 2, 3, 7, 8, 9),
			},
		},
		{
			ls: NewLineString(XYZM).MustSetCoords([]Coord{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}}),
			tls: &testLineString{
				layout:     XYZM,
				stride:     4,
				coords:     []Coord{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
				bounds:     NewBounds(XYZM).Set(1, 2, 3, 4, 9, 10, 11, 12),
			},
		},
	} {

		testLineStringEquals(t, c.ls, c.tls)
	}
}

func TestLineStringClone(t *testing.T) {
	p1 := NewLineString(XY).MustSetCoords([]Coord{{1, 2}, {3, 4}, {5, 6}})
	if p2 := p1.Clone(); aliases(p1.FlatCoords(), p2.FlatCoords()) {
		t.Error("Clone() should not alias flatCoords")
	}
}

func TestLineStringInterpolate(t *testing.T) {
	ls := NewLineString(XYM).MustSetCoords([]Coord{{1, 2, 0}, {2, 4, 1}, {3, 8, 2}})
	for _, c := range []struct {
		val float64
		dim int
		i   int
		f   float64
	}{
		{val: -0.5, dim: 2, i: 0, f: 0.0},
		{val: 0.0, dim: 2, i: 0, f: 0.0},
		{val: 0.5, dim: 2, i: 0, f: 0.5},
		{val: 1.0, dim: 2, i: 1, f: 0.0},
		{val: 1.5, dim: 2, i: 1, f: 0.5},
		{val: 2.0, dim: 2, i: 2, f: 0.0},
		{val: 2.5, dim: 2, i: 2, f: 0.0},
	} {
		if i, f := ls.Interpolate(c.val, c.dim); i != c.i || f != c.f {
			t.Errorf("ls.Interpolate(%v, %v) == %v, %v, want %v, %v", c.val, c.dim, i, f, c.i, c.f)
		}

	}
}

func TestLineStringStrideMismatch(t *testing.T) {
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
		p := NewLineString(c.layout)
		if _, err := p.SetCoords(c.coords); err != c.err {
			t.Errorf("p.SetCoords(%v) == %v, want %v", c.coords, err, c.err)
		}
	}
}
