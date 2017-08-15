package geom

import (
	"reflect"
	"testing"
)

type testMultiPoint struct {
	layout     Layout
	stride     int
	coords     []Coord
	flatCoords []float64
	bounds     *Bounds
}

func testMultiPointEquals(t *testing.T, mp *MultiPoint, tmp *testMultiPoint) {
	if err := mp.verify(); err != nil {
		t.Error(err)
	}
	if mp.Layout() != tmp.layout {
		t.Errorf("mp.Layout() == %v, want %v", mp.Layout(), tmp.layout)
	}
	if mp.Stride() != tmp.stride {
		t.Errorf("mp.Stride() == %v, want %v", mp.Stride(), tmp.stride)
	}
	if !reflect.DeepEqual(mp.FlatCoords(), tmp.flatCoords) {
		t.Errorf("mp.FlatCoords() == %v, want %v", mp.FlatCoords(), tmp.flatCoords)
	}
	if !reflect.DeepEqual(mp.Coords(), tmp.coords) {
		t.Errorf("mp.Coords() == %v, want %v", mp.Coords(), tmp.coords)
	}
	if !reflect.DeepEqual(mp.Bounds(), tmp.bounds) {
		t.Errorf("mp.Bounds() == %v, want %v", mp.Bounds(), tmp.bounds)
	}
	if got := mp.NumCoords(); got != len(tmp.coords) {
		t.Errorf("mp.NumCoords() == %v, want %v", got, len(tmp.coords))
	}
	for i, c := range tmp.coords {
		if !reflect.DeepEqual(mp.Coord(i), c) {
			t.Errorf("mp.Coord(%v) == %v, want %v", i, mp.Coord(i), c)
		}
	}
}

func TestMultiPoint(t *testing.T) {
	for _, c := range []struct {
		mp  *MultiPoint
		tmp *testMultiPoint
	}{
		{
			mp: NewMultiPoint(XY).MustSetCoords([]Coord{{1, 2}, {3, 4}, {5, 6}}),
			tmp: &testMultiPoint{
				layout:     XY,
				stride:     2,
				coords:     []Coord{{1, 2}, {3, 4}, {5, 6}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6},
				bounds:     NewBounds(XY).Set(1, 2, 5, 6),
			},
		},
		{
			mp: NewMultiPoint(XYZ).MustSetCoords([]Coord{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}),
			tmp: &testMultiPoint{
				layout:     XYZ,
				stride:     3,
				coords:     []Coord{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9},
				bounds:     NewBounds(XYZ).Set(1, 2, 3, 7, 8, 9),
			},
		},
		{
			mp: NewMultiPoint(XYM).MustSetCoords([]Coord{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}),
			tmp: &testMultiPoint{
				layout:     XYM,
				stride:     3,
				coords:     []Coord{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9},
				bounds:     NewBounds(XYM).Set(1, 2, 3, 7, 8, 9),
			},
		},
		{
			mp: NewMultiPoint(XYZM).MustSetCoords([]Coord{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}}),
			tmp: &testMultiPoint{
				layout:     XYZM,
				stride:     4,
				coords:     []Coord{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}},
				flatCoords: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
				bounds:     NewBounds(XYZM).Set(1, 2, 3, 4, 9, 10, 11, 12),
			},
		},
	} {

		testMultiPointEquals(t, c.mp, c.tmp)
	}
}

func TestMultiPointClone(t *testing.T) {
	p1 := NewMultiPoint(XY).MustSetCoords([]Coord{{1, 2}, {3, 4}, {5, 6}})
	if p2 := p1.Clone(); aliases(p1.FlatCoords(), p2.FlatCoords()) {
		t.Error("Clone() should not alias flatCoords")
	}
}

func TestMultiPointPush(t *testing.T) {
	mp := NewMultiPoint(XY)
	testMultiPointEquals(t, mp, &testMultiPoint{
		layout: XY,
		stride: 2,
		coords: []Coord{},
		bounds: NewBounds(XY),
	})
	if err := mp.Push(NewPoint(XY).MustSetCoords(Coord{1, 2})); err != nil {
		t.Error(err)
	}
	testMultiPointEquals(t, mp, &testMultiPoint{
		layout:     XY,
		stride:     2,
		coords:     []Coord{{1, 2}},
		flatCoords: []float64{1, 2},
		bounds:     NewBounds(XY).Set(1, 2, 1, 2),
	})
	if err := mp.Push(NewPoint(XY).MustSetCoords(Coord{3, 4})); err != nil {
		t.Error(err)
	}
	testMultiPointEquals(t, mp, &testMultiPoint{
		layout:     XY,
		stride:     2,
		coords:     []Coord{{1, 2}, {3, 4}},
		flatCoords: []float64{1, 2, 3, 4},
		bounds:     NewBounds(XY).Set(1, 2, 3, 4),
	})
}

func TestMultiPointStrideMismatch(t *testing.T) {
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
		p := NewMultiPoint(c.layout)
		if _, err := p.SetCoords(c.coords); err != c.err {
			t.Errorf("p.SetCoords(%v) == %v, want %v", c.coords, err, c.err)
		}
	}
}
