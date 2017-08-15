package geom

import (
	"reflect"
	"testing"
)

func TestGeometryCollectionBounds(t *testing.T) {
	for _, tc := range []struct {
		geoms []T
		want  *Bounds
	}{
		{
			want: NewBounds(NoLayout),
		},
		{
			geoms: []T{
				NewPoint(XY),
			},
			want: NewBounds(XY).SetCoords(Coord{0, 0}, Coord{0, 0}),
		},
		{
			geoms: []T{
				NewPoint(XY),
			},
			want: NewBounds(XY).SetCoords(Coord{0, 0}, Coord{0, 0}),
		},
		{
			geoms: []T{
				NewPoint(XY).MustSetCoords(Coord{1, 2}),
				NewPoint(XY).MustSetCoords(Coord{3, 4}),
			},
			want: NewBounds(XY).SetCoords(Coord{1, 2}, Coord{3, 4}),
		},
		{
			geoms: []T{
				NewPoint(XY).MustSetCoords(Coord{1, 2}),
				NewPoint(XYZ).MustSetCoords(Coord{3, 4, 5}),
			},
			want: NewBounds(XYZ).SetCoords(Coord{1, 2, 5}, Coord{3, 4, 5}),
		},
		{
			geoms: []T{
				NewPoint(XY).MustSetCoords(Coord{1, 2}),
				NewPoint(XYM).MustSetCoords(Coord{3, 4, 5}),
			},
			want: NewBounds(XYM).SetCoords(Coord{1, 2, 5}, Coord{3, 4, 5}),
		},
		{
			geoms: []T{
				NewPoint(XYZ).MustSetCoords(Coord{1, 2, 3}),
				NewPoint(XYM).MustSetCoords(Coord{4, 5, 6}),
			},
			want: NewBounds(XYZM).SetCoords(Coord{1, 2, 3, 6}, Coord{4, 5, 3, 6}),
		},
	} {
		if got := NewGeometryCollection().MustPush(tc.geoms...).Bounds(); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("NewGeometryCollection().MustPush(%+v).Bounds() == %+v, want %+v", tc.geoms, got, tc.want)
		}
	}
}

func TestGeometryCollectionLayout(t *testing.T) {
	for _, tc := range []struct {
		geoms []T
		want  Layout
	}{
		{
			want: NoLayout,
		},
		{
			geoms: []T{
				NewPoint(XY),
			},
			want: XY,
		},
		{
			geoms: []T{
				NewPoint(XYZ),
			},
			want: XYZ,
		},
		{
			geoms: []T{
				NewPoint(XYM),
			},
			want: XYM,
		},
		{
			geoms: []T{
				NewPoint(XYZM),
			},
			want: XYZM,
		},
		{
			geoms: []T{
				NewPoint(XY),
				NewPoint(XYZ),
			},
			want: XYZ,
		},
		{
			geoms: []T{
				NewPoint(XY),
				NewPoint(XYM),
			},
			want: XYM,
		},
		{
			geoms: []T{
				NewPoint(XY),
				NewPoint(XYZ),
				NewPoint(XYM),
			},
			want: XYZM,
		},
	} {
		if got := NewGeometryCollection().MustPush(tc.geoms...).Layout(); got != tc.want {
			t.Errorf("NewGeometryCollection().MustPush(%+v).Layout() == %s, want %s", tc.geoms, got, tc.want)
		}
	}
}

func TestGeometryCollectionPush(t *testing.T) {
	for _, tc := range []struct {
		srid    int
		geoms   []T
		g       T
		wantErr error
	}{
		{
			g: NewPoint(XY),
		},
		{
			g: NewPoint(XY).SetSRID(4326),
		},
		{
			srid: 4326,
			g:    NewPoint(XY).SetSRID(4326),
		},
		{
			geoms: []T{
				NewPoint(XY).SetSRID(4326),
			},
			g: NewPoint(XY).SetSRID(3857),
		},
		{
			srid: 4326,
			g:    NewPoint(XY).SetSRID(3857),
		},
	} {
		if gotErr := NewGeometryCollection().SetSRID(tc.srid).MustPush(tc.geoms...).Push(tc.g); gotErr != tc.wantErr {
			t.Errorf("NewGeometryCollection().SetSRID(%d).MustPush(%+v).Push(%+v) == %v, want %v", tc.srid, tc.geoms, tc.g, gotErr, tc.wantErr)
		}
	}
}
