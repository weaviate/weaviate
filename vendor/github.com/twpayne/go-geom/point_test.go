package geom

import (
	"reflect"
	"testing"
)

type testPoint struct {
	layout     Layout
	stride     int
	coords     Coord
	flatCoords []float64
	bounds     *Bounds
}

func testPointEquals(t *testing.T, p *Point, tp *testPoint) {
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
	if !reflect.DeepEqual(p.Bounds(), tp.bounds) {
		t.Errorf("p.Bounds() == %v, want %v", p.Bounds(), tp.bounds)
	}
}

func TestPoint(t *testing.T) {

	for _, c := range []struct {
		p  *Point
		tp *testPoint
	}{
		{
			p: NewPoint(XY),
			tp: &testPoint{
				layout:     XY,
				stride:     2,
				coords:     Coord{0, 0},
				flatCoords: []float64{0, 0},
				bounds:     NewBounds(XY).Set(0, 0, 0, 0),
			},
		},
		{
			p: NewPoint(XY).MustSetCoords(Coord{1, 2}),
			tp: &testPoint{
				layout:     XY,
				stride:     2,
				coords:     Coord{1, 2},
				flatCoords: []float64{1, 2},
				bounds:     NewBounds(XY).Set(1, 2, 1, 2),
			},
		},
		{
			p: NewPoint(XYZ),
			tp: &testPoint{
				layout:     XYZ,
				stride:     3,
				coords:     Coord{0, 0, 0},
				flatCoords: []float64{0, 0, 0},
				bounds:     NewBounds(XYZ).Set(0, 0, 0, 0, 0, 0),
			},
		},
		{
			p: NewPoint(XYZ).MustSetCoords(Coord{1, 2, 3}),
			tp: &testPoint{
				layout:     XYZ,
				stride:     3,
				coords:     Coord{1, 2, 3},
				flatCoords: []float64{1, 2, 3},
				bounds:     NewBounds(XYZ).Set(1, 2, 3, 1, 2, 3),
			},
		},
		{
			p: NewPoint(XYM),
			tp: &testPoint{
				layout:     XYM,
				stride:     3,
				coords:     Coord{0, 0, 0},
				flatCoords: []float64{0, 0, 0},
				bounds:     NewBounds(XYM).Set(0, 0, 0, 0, 0, 0),
			},
		},
		{
			p: NewPoint(XYM).MustSetCoords(Coord{1, 2, 3}),
			tp: &testPoint{
				layout:     XYM,
				stride:     3,
				coords:     Coord{1, 2, 3},
				flatCoords: []float64{1, 2, 3},
				bounds:     NewBounds(XYM).Set(1, 2, 3, 1, 2, 3),
			},
		},
		{
			p: NewPoint(XYZM),
			tp: &testPoint{
				layout:     XYZM,
				stride:     4,
				coords:     Coord{0, 0, 0, 0},
				flatCoords: []float64{0, 0, 0, 0},
				bounds:     NewBounds(XYZM).Set(0, 0, 0, 0, 0, 0, 0, 0),
			},
		},
		{
			p: NewPoint(XYZM).MustSetCoords(Coord{1, 2, 3, 4}),
			tp: &testPoint{
				layout:     XYZM,
				stride:     4,
				coords:     Coord{1, 2, 3, 4},
				flatCoords: []float64{1, 2, 3, 4},
				bounds:     NewBounds(XYZM).Set(1, 2, 3, 4, 1, 2, 3, 4),
			},
		},
	} {
		testPointEquals(t, c.p, c.tp)
	}
}

func TestPointClone(t *testing.T) {
	p1 := NewPoint(XY).MustSetCoords(Coord{1, 2})
	if p2 := p1.Clone(); aliases(p1.FlatCoords(), p2.FlatCoords()) {
		t.Error("Clone() should not alias flatCoords")
	}
}

func TestPointStrideMismatch(t *testing.T) {
	for _, c := range []struct {
		layout Layout
		coords Coord
		err    error
	}{
		{
			layout: XY,
			coords: nil,
			err:    ErrStrideMismatch{Got: 0, Want: 2},
		},
		{
			layout: XY,
			coords: Coord{},
			err:    ErrStrideMismatch{Got: 0, Want: 2},
		},
		{
			layout: XY,
			coords: Coord{1},
			err:    ErrStrideMismatch{Got: 1, Want: 2},
		},
		{
			layout: XY,
			coords: Coord{1, 2},
			err:    nil,
		},
		{
			layout: XY,
			coords: Coord{1, 2, 3},
			err:    ErrStrideMismatch{Got: 3, Want: 2},
		},
	} {
		p := NewPoint(c.layout)
		if _, err := p.SetCoords(c.coords); err != c.err {
			t.Errorf("p.SetCoords(%v) == %v, want %v", c.coords, err, c.err)
		}
	}
}

func TestPointXYZM(t *testing.T) {
	for _, tc := range []struct {
		p          *Point
		x, y, z, m float64
	}{
		{
			p: NewPoint(XY).MustSetCoords([]float64{1, 2}),
			x: 1,
			y: 2,
		},
		{
			p: NewPoint(XYZ).MustSetCoords([]float64{1, 2, 3}),
			x: 1,
			y: 2,
			z: 3,
		},
		{
			p: NewPoint(XYM).MustSetCoords([]float64{1, 2, 3}),
			x: 1,
			y: 2,
			m: 3,
		},
		{
			p: NewPoint(XYZM).MustSetCoords([]float64{1, 2, 3, 4}),
			x: 1,
			y: 2,
			z: 3,
			m: 4,
		},
	} {
		if got := tc.p.X(); got != tc.x {
			t.Errorf("%v.X() == %f, want %f", tc.p, got, tc.x)
		}
		if got := tc.p.Y(); got != tc.y {
			t.Errorf("%v.Y() == %f, want %f", tc.p, got, tc.y)
		}
		if got := tc.p.Z(); got != tc.z {
			t.Errorf("%v.Z() == %f, want %f", tc.p, got, tc.z)
		}
		if got := tc.p.M(); got != tc.m {
			t.Errorf("%v.M() == %f, want %f", tc.p, got, tc.m)
		}
	}
}
