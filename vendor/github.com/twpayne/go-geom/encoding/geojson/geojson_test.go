package geojson

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/d4l3k/messagediff"
	"github.com/twpayne/go-geom"
)

func TestGeometryDecode_NilCoordinates(t *testing.T) {
	for _, tc := range []struct {
		geometry Geometry
		want     geom.T
	}{
		{
			geometry: Geometry{Type: "Point"},
			want:     geom.NewPoint(geom.NoLayout),
		},
		{
			geometry: Geometry{Type: "LineString"},
			want:     geom.NewLineString(geom.NoLayout),
		},
		{
			geometry: Geometry{Type: "Polygon"},
			want:     geom.NewPolygon(geom.NoLayout),
		},
		{
			geometry: Geometry{Type: "MultiPoint"},
			want:     geom.NewMultiPoint(geom.NoLayout),
		},
		{
			geometry: Geometry{Type: "MultiLineString"},
			want:     geom.NewMultiLineString(geom.NoLayout),
		},
		{
			geometry: Geometry{Type: "MultiPolygon"},
			want:     geom.NewMultiPolygon(geom.NoLayout),
		},
		{
			geometry: Geometry{Type: "GeometryCollection"},
			want:     geom.NewGeometryCollection(),
		},
	} {
		if got, err := tc.geometry.Decode(); err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%+v.Decode() == %v, %v, want %v, nil", tc.geometry, got, err, tc.want)
		}
	}
}

func TestGeometry(t *testing.T) {
	for _, tc := range []struct {
		g geom.T
		s string
	}{
		{
			g: geom.NewPoint(DefaultLayout),
			s: `{"type":"Point","coordinates":[0,0]}`,
		},
		{
			g: geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{1, 2}),
			s: `{"type":"Point","coordinates":[1,2]}`,
		},
		{
			g: geom.NewPoint(geom.XYZ).MustSetCoords(geom.Coord{1, 2, 3}),
			s: `{"type":"Point","coordinates":[1,2,3]}`,
		},
		{
			g: geom.NewPoint(geom.XYZM).MustSetCoords(geom.Coord{1, 2, 3, 4}),
			s: `{"type":"Point","coordinates":[1,2,3,4]}`,
		},
		{
			g: geom.NewLineString(DefaultLayout),
			s: `{"type":"LineString","coordinates":[]}`,
		},
		{
			g: geom.NewLineString(geom.XY).MustSetCoords([]geom.Coord{{1, 2}, {3, 4}}),
			s: `{"type":"LineString","coordinates":[[1,2],[3,4]]}`,
		},
		{
			g: geom.NewLineString(geom.XYZ).MustSetCoords([]geom.Coord{{1, 2, 3}, {4, 5, 6}}),
			s: `{"type":"LineString","coordinates":[[1,2,3],[4,5,6]]}`,
		},
		{
			g: geom.NewLineString(geom.XYZM).MustSetCoords([]geom.Coord{{1, 2, 3, 4}, {5, 6, 7, 8}}),
			s: `{"type":"LineString","coordinates":[[1,2,3,4],[5,6,7,8]]}`,
		},
		{
			g: geom.NewPolygon(DefaultLayout),
			s: `{"type":"Polygon","coordinates":[]}`,
		},
		{
			g: geom.NewPolygon(geom.XY).MustSetCoords([][]geom.Coord{{{1, 2}, {3, 4}, {5, 6}, {1, 2}}}),
			s: `{"type":"Polygon","coordinates":[[[1,2],[3,4],[5,6],[1,2]]]}`,
		},
		{
			g: geom.NewPolygon(geom.XYZ).MustSetCoords([][]geom.Coord{{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {1, 2, 3}}}),
			s: `{"type":"Polygon","coordinates":[[[1,2,3],[4,5,6],[7,8,9],[1,2,3]]]}`,
		},
		{
			g: geom.NewMultiPoint(DefaultLayout),
			s: `{"type":"MultiPoint","coordinates":[]}`,
		},
		{
			g: geom.NewMultiPoint(geom.XY).MustSetCoords([]geom.Coord{{1, 2}, {3, 4}}),
			s: `{"type":"MultiPoint","coordinates":[[1,2],[3,4]]}`,
		},
		{
			g: geom.NewMultiPoint(geom.XYZ).MustSetCoords([]geom.Coord{{1, 2, 3}, {4, 5, 6}}),
			s: `{"type":"MultiPoint","coordinates":[[1,2,3],[4,5,6]]}`,
		},
		{
			g: geom.NewMultiPoint(geom.XYZM).MustSetCoords([]geom.Coord{{1, 2, 3, 4}, {5, 6, 7, 8}}),
			s: `{"type":"MultiPoint","coordinates":[[1,2,3,4],[5,6,7,8]]}`,
		},
		{
			g: geom.NewMultiLineString(DefaultLayout),
			s: `{"type":"MultiLineString","coordinates":[]}`,
		},
		{
			g: geom.NewMultiLineString(geom.XY).MustSetCoords([][]geom.Coord{{{1, 2}, {3, 4}, {5, 6}, {1, 2}}}),
			s: `{"type":"MultiLineString","coordinates":[[[1,2],[3,4],[5,6],[1,2]]]}`,
		},
		{
			g: geom.NewMultiLineString(geom.XYZ).MustSetCoords([][]geom.Coord{{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {1, 2, 3}}}),
			s: `{"type":"MultiLineString","coordinates":[[[1,2,3],[4,5,6],[7,8,9],[1,2,3]]]}`,
		},
		{
			g: geom.NewMultiPolygon(DefaultLayout),
			s: `{"type":"MultiPolygon","coordinates":[]}`,
		},
		{
			g: geom.NewMultiPolygon(geom.XYZ).MustSetCoords([][][]geom.Coord{{{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {1, 2, 3}}, {{-1, -2, -3}, {-4, -5, -6}, {-7, -8, -9}, {-1, -2, -3}}}}),
			s: `{"type":"MultiPolygon","coordinates":[[[[1,2,3],[4,5,6],[7,8,9],[1,2,3]],[[-1,-2,-3],[-4,-5,-6],[-7,-8,-9],[-1,-2,-3]]]]}`,
		},
		{
			g: geom.NewGeometryCollection().MustPush(
				geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{100, 0}),
				geom.NewLineString(geom.XY).MustSetCoords([]geom.Coord{{101, 0}, {102, 1}}),
			),
			s: `{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[100,0]},{"type":"LineString","coordinates":[[101,0],[102,1]]}]}`,
		},
	} {
		if got, err := Marshal(tc.g); err != nil || string(got) != tc.s {
			t.Errorf("Marshal(%#v) == %#v, %v, want %#v, nil", tc.g, string(got), err, tc.s)
		}
		var g geom.T
		if err := Unmarshal([]byte(tc.s), &g); err != nil || !reflect.DeepEqual(g, tc.g) {
			t.Errorf("Unmarshal(%#v, %#v) == %v, want %#v, nil", tc.s, g, err, tc.g)
		}
	}
}

func TestFeature(t *testing.T) {
	for _, tc := range []struct {
		f *Feature
		s string
	}{
		{
			f: &Feature{
				Geometry: geom.NewPoint(geom.XY).MustSetCoords([]float64{125.6, 10.1}),
				Properties: map[string]interface{}{
					"name": "Dinagat Islands",
				},
			},
			s: `{"type":"Feature","geometry":{"type":"Point","coordinates":[125.6,10.1]},"properties":{"name":"Dinagat Islands"}}`,
		},
		{
			f: &Feature{
				Geometry: geom.NewLineString(geom.XY).MustSetCoords([]geom.Coord{{102, 0}, {103, 1}, {104, 0}, {105, 1}}),
				Properties: map[string]interface{}{
					"prop0": "value0",
					"prop1": 0.0,
				},
			},
			s: `{"type":"Feature","geometry":{"type":"LineString","coordinates":[[102,0],[103,1],[104,0],[105,1]]},"properties":{"prop0":"value0","prop1":0}}`,
		},
		{
			f: &Feature{
				Geometry: geom.NewPolygon(geom.XY).MustSetCoords([][]geom.Coord{{{100, 0}, {101, 0}, {101, 1}, {100, 1}, {100, 0}}}),
				Properties: map[string]interface{}{
					"prop0": "value0",
					"prop1": map[string]interface{}{
						"this": "that",
					},
				},
			},
			s: `{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[100,0],[101,0],[101,1],[100,1],[100,0]]]},"properties":{"prop0":"value0","prop1":{"this":"that"}}}`,
		},
		{
			f: &Feature{
				ID:       "0",
				Geometry: geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{1, 2}),
			},
			s: `{"type":"Feature","id":"0","geometry":{"type":"Point","coordinates":[1,2]}}`,
		},
		{
			f: &Feature{
				ID:       "f",
				Geometry: geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{1, 2}),
			},
			s: `{"type":"Feature","id":"f","geometry":{"type":"Point","coordinates":[1,2]}}`,
		},
	} {
		if got, err := json.Marshal(tc.f); err != nil || string(got) != tc.s {
			t.Errorf("json.Marshal(%+v) == %v, %v, want %v, nil", tc.f, string(got), err, tc.s)
		}
		f := &Feature{}
		if err := json.Unmarshal([]byte(tc.s), f); err != nil {
			t.Errorf("json.Unmarshal(%v, ...) == %v, want nil", tc.s, err)
		}
		if diff, equal := messagediff.PrettyDiff(tc.f, f); !equal {
			t.Errorf("json.Unmarshal(%v, ...), diff\n%s", tc.s, diff)
		}
	}
}

func TestFeatureCollection(t *testing.T) {
	for _, tc := range []struct {
		fc *FeatureCollection
		s  string
	}{
		{
			fc: &FeatureCollection{
				Features: []*Feature{
					{
						Geometry: geom.NewPoint(geom.XY).MustSetCoords([]float64{125.6, 10.1}),
						Properties: map[string]interface{}{
							"name": "Dinagat Islands",
						},
					},
				},
			},
			s: `{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[125.6,10.1]},"properties":{"name":"Dinagat Islands"}}]}`,
		},
		{
			fc: &FeatureCollection{
				Features: []*Feature{
					{
						Geometry: geom.NewPoint(geom.XY).MustSetCoords([]float64{125.6, 10.1}),
						Properties: map[string]interface{}{
							"name": "Dinagat Islands",
						},
					},
					{
						Geometry: geom.NewLineString(geom.XY).MustSetCoords([]geom.Coord{{102, 0}, {103, 1}, {104, 0}, {105, 1}}),
						Properties: map[string]interface{}{
							"prop0": "value0",
							"prop1": 0.0,
						},
					},
					{
						Geometry: geom.NewPolygon(geom.XY).MustSetCoords([][]geom.Coord{{{100, 0}, {101, 0}, {101, 1}, {100, 1}, {100, 0}}}),
						Properties: map[string]interface{}{
							"prop0": "value0",
							"prop1": map[string]interface{}{
								"this": "that",
							},
						},
					},
				},
			},
			s: `{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[125.6,10.1]},"properties":{"name":"Dinagat Islands"}},{"type":"Feature","geometry":{"type":"LineString","coordinates":[[102,0],[103,1],[104,0],[105,1]]},"properties":{"prop0":"value0","prop1":0}},{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[100,0],[101,0],[101,1],[100,1],[100,0]]]},"properties":{"prop0":"value0","prop1":{"this":"that"}}}]}`,
		},
	} {
		if got, err := json.Marshal(tc.fc); err != nil || string(got) != tc.s {
			t.Errorf("json.Marshal(%+v) == %v, %v, want %v, nil", tc.fc, string(got), err, tc.s)
		}
		fc := &FeatureCollection{}
		if err := json.Unmarshal([]byte(tc.s), fc); err != nil {
			t.Errorf("json.Unmarshal(%v, ...) == %v, want nil", tc.s, err)
		}
		if diff, equal := messagediff.PrettyDiff(tc.fc, fc); !equal {
			t.Errorf("json.Unmarshal(%v, ...), diff\n%s", tc.s, diff)
		}
	}
}
