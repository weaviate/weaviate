package geom

import (
	"testing"
)

func TestLength(t *testing.T) {
	for _, tc := range []struct {
		g interface {
			Length() float64
		}
		want float64
	}{
		{
			g:    NewPoint(XY),
			want: 0,
		},
		{
			g:    NewMultiPoint(XY),
			want: 0,
		},
		{
			g:    NewLineString(XY),
			want: 0,
		},
		{
			g: NewLineString(XY).MustSetCoords([]Coord{
				{0, 0}, {1, 0},
			}),
			want: 1,
		},
		{
			g:    NewLinearRing(XY),
			want: 0,
		},
		{
			g: NewLinearRing(XY).MustSetCoords([]Coord{
				{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0},
			}),
			want: 4,
		},
		{
			g:    NewPolygon(XY),
			want: 0,
		},
		{
			g: NewPolygon(XY).MustSetCoords([][]Coord{
				{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
			}),
			want: 4,
		},
		{
			g: NewPolygon(XY).MustSetCoords([][]Coord{
				{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
				{{0.25, 0.25}, {0.75, 0.25}, {0.75, 0.75}, {0.25, 0.75}, {0.25, 0.25}},
			}),
			want: 6,
		},
		{
			g:    NewMultiPolygon(XY),
			want: 0,
		},
		{
			g: NewMultiPolygon(XY).MustSetCoords([][][]Coord{
				{
					{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
					{{0.25, 0.25}, {0.75, 0.25}, {0.75, 0.75}, {0.25, 0.75}, {0.25, 0.25}},
				},
			}),
			want: 6,
		},
		{
			g: NewMultiPolygon(XY).MustSetCoords([][][]Coord{
				{
					{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
					{{0.25, 0.25}, {0.75, 0.25}, {0.75, 0.75}, {0.25, 0.75}, {0.25, 0.25}},
				},
				{
					{{2, 2}, {4, 2}, {4, 4}, {2, 4}, {2, 2}},
				},
			}),
			want: 14,
		},
	} {
		if got := tc.g.Length(); got != tc.want {
			t.Errorf("%#v.Length() == %f, want %f", tc.g, got, tc.want)
		}
	}
}
