package geom

import (
	"testing"
)

func TestArea(t *testing.T) {
	for _, tc := range []struct {
		g interface {
			Area() float64
		}
		want float64
	}{
		{
			g:    NewPoint(XY),
			want: 0,
		},
		{
			g:    NewLineString(XY),
			want: 0,
		},
		{
			g:    NewLinearRing(XY),
			want: 0,
		},
		{
			g: NewLinearRing(XY).MustSetCoords([]Coord{
				{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0},
			}),
			want: 1,
		},
		{
			g: NewLinearRing(XY).MustSetCoords([]Coord{
				{0, 0}, {1, 1}, {1, 0}, {0, 0},
			}),
			want: -0.5,
		},
		{
			g: NewLinearRing(XY).MustSetCoords([]Coord{
				{-3, -2}, {-1, 4}, {6, 1}, {3, 10}, {-4, 9}, {-3, -2},
			}),
			want: 60,
		},
		{
			g:    NewMultiLineString(XY),
			want: 0,
		},
		{
			g:    NewPolygon(XY),
			want: 0,
		},
		{
			g: NewPolygon(XY).MustSetCoords([][]Coord{
				{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
			}),
			want: 1,
		},
		{
			g: NewPolygon(XY).MustSetCoords([][]Coord{
				{{0, 0}, {1, 1}, {1, 0}, {0, 0}},
			}),
			want: -0.5,
		},
		{
			g: NewPolygon(XY).MustSetCoords([][]Coord{
				{{-3, -2}, {-1, 4}, {6, 1}, {3, 10}, {-4, 9}, {-3, -2}},
			}),
			want: 60,
		},
		{
			g: NewPolygon(XY).MustSetCoords([][]Coord{
				{{-3, -2}, {-1, 4}, {6, 1}, {3, 10}, {-4, 9}, {-3, -2}},
				{{0, 6}, {2, 6}, {2, 8}, {0, 8}, {0, 6}},
			}),
			want: 56,
		},
		{
			g:    NewMultiPolygon(XY),
			want: 0,
		},
		{
			g: NewMultiPolygon(XY).MustSetCoords([][][]Coord{
				{
					{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
				},
			}),
			want: 1,
		},
		{
			g: NewMultiPolygon(XY).MustSetCoords([][][]Coord{
				{
					{{0, 0}, {1, 1}, {1, 0}, {0, 0}},
				},
			}),
			want: -0.5,
		},
		{
			g: NewMultiPolygon(XY).MustSetCoords([][][]Coord{
				{
					{{-3, -2}, {-1, 4}, {6, 1}, {3, 10}, {-4, 9}, {-3, -2}},
				},
			}),
			want: 60,
		},
		{
			g: NewMultiPolygon(XY).MustSetCoords([][][]Coord{
				{
					{{-3, -2}, {-1, 4}, {6, 1}, {3, 10}, {-4, 9}, {-3, -2}},
					{{0, 6}, {2, 6}, {2, 8}, {0, 8}, {0, 6}},
				},
			}),
			want: 56,
		},
		{
			g: NewMultiPolygon(XY).MustSetCoords([][][]Coord{
				{
					{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
				},
				{
					{{-3, -2}, {-1, 4}, {6, 1}, {3, 10}, {-4, 9}, {-3, -2}},
					{{0, 6}, {2, 6}, {2, 8}, {0, 8}, {0, 6}},
				},
			}),
			want: 57,
		},
	} {
		if got := tc.g.Area(); got != tc.want {
			t.Errorf("%#v.Area() == %f, want %f", tc.g, got, tc.want)
		}
	}
}
