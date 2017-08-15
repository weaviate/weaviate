package kml

import (
	"bytes"
	"encoding/xml"
	"testing"

	"github.com/twpayne/go-geom"
)

func Test(t *testing.T) {
	for _, tc := range []struct {
		g    geom.T
		want string
	}{
		{
			g:    geom.NewPoint(geom.XY),
			want: `<Point><coordinates>0,0</coordinates></Point>`,
		},
		{
			g:    geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{0, 0}),
			want: `<Point><coordinates>0,0</coordinates></Point>`,
		},
		{
			g:    geom.NewPoint(geom.XYZ).MustSetCoords(geom.Coord{0, 0, 0}),
			want: `<Point><coordinates>0,0</coordinates></Point>`,
		},
		{
			g:    geom.NewPoint(geom.XYZ).MustSetCoords(geom.Coord{0, 0, 1}),
			want: `<Point><coordinates>0,0,1</coordinates></Point>`,
		},
		{
			g:    geom.NewPoint(geom.XYM).MustSetCoords(geom.Coord{0, 0, 1}),
			want: `<Point><coordinates>0,0</coordinates></Point>`,
		},
		{
			g:    geom.NewPoint(geom.XYZM).MustSetCoords(geom.Coord{0, 0, 0, 1}),
			want: `<Point><coordinates>0,0</coordinates></Point>`,
		},
		{
			g:    geom.NewPoint(geom.XYZM).MustSetCoords(geom.Coord{0, 0, 1, 1}),
			want: `<Point><coordinates>0,0,1</coordinates></Point>`,
		},
		{
			g: geom.NewMultiPoint(geom.XY).MustSetCoords([]geom.Coord{{1, 2}, {3, 4}, {5, 6}}),
			want: `<MultiGeometry>` +
				`<Point>` +
				`<coordinates>1,2</coordinates>` +
				`</Point>` +
				`<Point>` +
				`<coordinates>3,4</coordinates>` +
				`</Point>` +
				`<Point>` +
				`<coordinates>5,6</coordinates>` +
				`</Point>` +
				`</MultiGeometry>`,
		},
		{
			g: geom.NewLineString(geom.XY).MustSetCoords([]geom.Coord{
				{0, 0}, {1, 1},
			}),
			want: `<LineString><coordinates>0,0 1,1</coordinates></LineString>`,
		},
		{
			g: geom.NewLineString(geom.XYZ).MustSetCoords([]geom.Coord{
				{1, 2, 3}, {4, 5, 6},
			}),
			want: `<LineString><coordinates>1,2,3 4,5,6</coordinates></LineString>`,
		},
		{
			g: geom.NewLineString(geom.XYM).MustSetCoords([]geom.Coord{
				{1, 2, 3}, {4, 5, 6},
			}),
			want: `<LineString><coordinates>1,2 4,5</coordinates></LineString>`,
		},
		{
			g: geom.NewLineString(geom.XYZM).MustSetCoords([]geom.Coord{
				{1, 2, 3, 4}, {5, 6, 7, 8},
			}),
			want: `<LineString><coordinates>1,2,3 5,6,7</coordinates></LineString>`,
		},
		{
			g: geom.NewMultiLineString(geom.XY).MustSetCoords([][]geom.Coord{
				{{1, 2}, {3, 4}, {5, 6}, {7, 8}},
			}),
			want: `<MultiGeometry>` +
				`<LineString>` +
				`<coordinates>1,2 3,4 5,6 7,8</coordinates>` +
				`</LineString>` +
				`</MultiGeometry>`,
		},
		{
			g: geom.NewMultiLineString(geom.XY).MustSetCoords([][]geom.Coord{
				{{1, 2}, {3, 4}, {5, 6}, {7, 8}},
				{{9, 10}, {11, 12}, {13, 14}},
			}),
			want: `<MultiGeometry>` +
				`<LineString>` +
				`<coordinates>1,2 3,4 5,6 7,8</coordinates>` +
				`</LineString>` +
				`<LineString>` +
				`<coordinates>9,10 11,12 13,14</coordinates>` +
				`</LineString>` +
				`</MultiGeometry>`,
		},
		{
			g: geom.NewPolygon(geom.XY).MustSetCoords([][]geom.Coord{
				{{1, 2}, {3, 4}, {5, 6}, {1, 2}},
			}),
			want: `<Polygon>` +
				`<outerBoundaryIs>` +
				`<LinearRing>` +
				`<coordinates>1,2 3,4 5,6 1,2</coordinates>` +
				`</LinearRing>` +
				`</outerBoundaryIs>` +
				`</Polygon>`,
		},
		{
			g: geom.NewPolygon(geom.XYZ).MustSetCoords([][]geom.Coord{
				{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {1, 2, 3}},
			}),
			want: `<Polygon>` +
				`<outerBoundaryIs>` +
				`<LinearRing>` +
				`<coordinates>1,2,3 4,5,6 7,8,9 1,2,3</coordinates>` +
				`</LinearRing>` +
				`</outerBoundaryIs>` +
				`</Polygon>`,
		},
		{
			g: geom.NewPolygon(geom.XYZ).MustSetCoords([][]geom.Coord{
				{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {1, 2, 3}},
				{{0.4, 0.5, 0.6}, {0.7, 0.8, 0.9}, {0.1, 0.2, 0.3}, {0.4, 0.5, 0.6}},
			}),
			want: `<Polygon>` +
				`<outerBoundaryIs>` +
				`<LinearRing>` +
				`<coordinates>1,2,3 4,5,6 7,8,9 1,2,3</coordinates>` +
				`</LinearRing>` +
				`</outerBoundaryIs>` +
				`<innerBoundaryIs>` +
				`<LinearRing>` +
				`<coordinates>0.4,0.5,0.6 0.7,0.8,0.9 0.1,0.2,0.3 0.4,0.5,0.6</coordinates>` +
				`</LinearRing>` +
				`</innerBoundaryIs>` +
				`</Polygon>`,
		},
		{
			g: geom.NewMultiPolygon(geom.XYZ).MustSetCoords([][][]geom.Coord{
				{
					{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {1, 2, 3}},
					{{0.4, 0.5, 0.6}, {0.7, 0.8, 0.9}, {0.1, 0.2, 0.3}, {0.4, 0.5, 0.6}},
				},
			}),
			want: `<MultiGeometry>` +
				`<Polygon>` +
				`<outerBoundaryIs>` +
				`<LinearRing>` +
				`<coordinates>1,2,3 4,5,6 7,8,9 1,2,3</coordinates>` +
				`</LinearRing>` +
				`</outerBoundaryIs>` +
				`<innerBoundaryIs>` +
				`<LinearRing>` +
				`<coordinates>0.4,0.5,0.6 0.7,0.8,0.9 0.1,0.2,0.3 0.4,0.5,0.6</coordinates>` +
				`</LinearRing>` +
				`</innerBoundaryIs>` +
				`</Polygon>` +
				`</MultiGeometry>`,
		},
		{
			g: geom.NewMultiPolygon(geom.XYZ).MustSetCoords([][][]geom.Coord{
				{
					{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {1, 2, 3}},
				},
				{
					{{0.4, 0.5, 0.6}, {0.7, 0.8, 0.9}, {0.1, 0.2, 0.3}, {0.4, 0.5, 0.6}},
				},
			}),
			want: `<MultiGeometry>` +
				`<Polygon>` +
				`<outerBoundaryIs>` +
				`<LinearRing>` +
				`<coordinates>1,2,3 4,5,6 7,8,9 1,2,3</coordinates>` +
				`</LinearRing>` +
				`</outerBoundaryIs>` +
				`</Polygon>` +
				`<Polygon>` +
				`<outerBoundaryIs>` +
				`<LinearRing>` +
				`<coordinates>0.4,0.5,0.6 0.7,0.8,0.9 0.1,0.2,0.3 0.4,0.5,0.6</coordinates>` +
				`</LinearRing>` +
				`</outerBoundaryIs>` +
				`</Polygon>` +
				`</MultiGeometry>`,
		},
		{
			g: geom.NewGeometryCollection().MustPush(
				geom.NewLineString(geom.XY).MustSetCoords([]geom.Coord{
					{-122.4425587930444, 37.80666418607323},
					{-122.4428379594768, 37.80663578323093},
				}),
				geom.NewLineString(geom.XY).MustSetCoords([]geom.Coord{
					{-122.4425509770566, 37.80662588061205},
					{-122.4428340530617, 37.8065999493009},
				}),
			),
			want: `<MultiGeometry>` +
				`<LineString>` +
				`<coordinates>` +
				`-122.4425587930444,37.80666418607323 -122.4428379594768,37.80663578323093` +
				`</coordinates>` +
				`</LineString>` +
				`<LineString>` +
				`<coordinates>` +
				`-122.4425509770566,37.80662588061205 -122.4428340530617,37.8065999493009` +
				`</coordinates>` +
				`</LineString>` +
				`</MultiGeometry>`,
		},
	} {
		b := &bytes.Buffer{}
		e := xml.NewEncoder(b)
		element, err := Encode(tc.g)
		if err != nil {
			t.Errorf("Encode(%#v) == %#v, %v, want ..., nil", tc.g, element, err)
			continue
		}
		if err := e.Encode(element); err != nil {
			t.Errorf("Encode(%#v) == %v, want nil", element, err)
			continue
		}
		if got := b.String(); got != tc.want {
			t.Errorf("Encode(Encode(%#v))\nwrote %v\n want %v", tc.g, got, tc.want)
		}
	}
}
