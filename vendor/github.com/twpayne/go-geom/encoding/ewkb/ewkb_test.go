package ewkb

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/twpayne/go-geom"
)

func test(t *testing.T, g geom.T, xdr []byte, ndr []byte) {
	if xdr != nil {
		if got, err := Unmarshal(xdr); err != nil || !reflect.DeepEqual(got, g) {
			t.Errorf("Unmarshal(%s) == %#v, %#v, want %#v, nil", hex.EncodeToString(xdr), got, err, g)
		}
		if got, err := Marshal(g, XDR); err != nil || !reflect.DeepEqual(got, xdr) {
			t.Errorf("Marshal(%#v, XDR) == %s, %#v, want %s, nil", g, hex.EncodeToString(got), err, hex.EncodeToString(xdr))
		}
	}
	if ndr != nil {
		if got, err := Unmarshal(ndr); err != nil || !reflect.DeepEqual(got, g) {
			t.Errorf("Unmarshal(%s) == %#v, %#v, want %#v, nil", hex.EncodeToString(ndr), got, err, g)
		}
		if got, err := Marshal(g, NDR); err != nil || !reflect.DeepEqual(got, ndr) {
			t.Errorf("Marshal(%#v, NDR) == %s, %#v, want %#v, nil", g, hex.EncodeToString(got), err, hex.EncodeToString(ndr))
		}
	}
	switch g := g.(type) {
	case *geom.Point:
		var p Point
		if xdr != nil {
			if err := p.Scan(xdr); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", p, string(xdr), err)
			}
			if !reflect.DeepEqual(p, Point{g}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(xdr), p, Point{g})
			}
		}
		if ndr != nil {
			if err := p.Scan(ndr); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", p, string(ndr), err)
			}
			if !reflect.DeepEqual(p, Point{g}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(ndr), p, Point{g})
			}
		}
	case *geom.LineString:
		var ls LineString
		if xdr != nil {
			if err := ls.Scan(xdr); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", ls, string(xdr), err)
			}
			if !reflect.DeepEqual(ls, LineString{g}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(xdr), ls, LineString{g})
			}
		}
		if ndr != nil {
			if err := ls.Scan(ndr); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", ls, string(ndr), err)
			}
			if !reflect.DeepEqual(ls, LineString{g}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(ndr), ls, LineString{g})
			}
		}
	case *geom.Polygon:
		var p Polygon
		if xdr != nil {
			if err := p.Scan(xdr); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", p, string(xdr), err)
			}
			if !reflect.DeepEqual(p, Polygon{g}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(xdr), p, Polygon{g})
			}
		}
		if ndr != nil {
			if err := p.Scan(ndr); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", p, string(ndr), err)
			}
			if !reflect.DeepEqual(p, Polygon{g}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(ndr), p, Polygon{g})
			}
		}
	case *geom.MultiPoint:
		var mp MultiPoint
		if xdr != nil {
			if err := mp.Scan(xdr); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", mp, string(xdr), err)
			}
			if !reflect.DeepEqual(mp, MultiPoint{g}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(xdr), mp, MultiPoint{g})
			}
		}
		if ndr != nil {
			if err := mp.Scan(ndr); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", mp, string(ndr), err)
			}
			if !reflect.DeepEqual(mp, MultiPoint{g}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(ndr), mp, MultiPoint{g})
			}
		}
	case *geom.MultiLineString:
		var mls MultiLineString
		if xdr != nil {
			if err := mls.Scan(xdr); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", mls, string(xdr), err)
			}
			if !reflect.DeepEqual(mls, MultiLineString{g}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(xdr), mls, MultiLineString{g})
			}
		}
		if ndr != nil {
			if err := mls.Scan(ndr); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", mls, string(ndr), err)
			}
			if !reflect.DeepEqual(mls, MultiLineString{g}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(ndr), mls, MultiLineString{g})
			}
		}
	case *geom.MultiPolygon:
		var mp MultiPolygon
		if xdr != nil {
			if err := mp.Scan(xdr); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", mp, string(xdr), err)
			}
			if !reflect.DeepEqual(mp, MultiPolygon{g}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(xdr), mp, MultiPolygon{g})
			}
		}
		if ndr != nil {
			if err := mp.Scan(ndr); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", mp, string(ndr), err)
			}
			if !reflect.DeepEqual(mp, MultiPolygon{g}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(ndr), mp, MultiPolygon{g})
			}
		}
	case *geom.GeometryCollection:
		var gc GeometryCollection
		if xdr != nil {
			if err := gc.Scan(xdr); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", gc, string(xdr), err)
			}
			if !reflect.DeepEqual(gc, GeometryCollection{g}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(xdr), gc, GeometryCollection{g})
			}
		}
		if ndr != nil {
			if err := gc.Scan(ndr); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", gc, string(ndr), err)
			}
			if !reflect.DeepEqual(gc, GeometryCollection{g}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(ndr), gc, GeometryCollection{g})
			}
		}
	}
}

func mustDecodeString(s string) []byte {
	data, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return data
}

func Test(t *testing.T) {
	for _, tc := range []struct {
		g   geom.T
		xdr []byte
		ndr []byte
	}{
		{
			g:   geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{1, 2}),
			xdr: mustDecodeString("00000000013ff00000000000004000000000000000"),
			ndr: mustDecodeString("0101000000000000000000f03f0000000000000040"),
		},
		{
			g:   geom.NewPoint(geom.XYZ).MustSetCoords(geom.Coord{1, 2, 3}),
			xdr: mustDecodeString("00800000013ff000000000000040000000000000004008000000000000"),
			ndr: mustDecodeString("0101000080000000000000f03f00000000000000400000000000000840"),
		},
		{
			g:   geom.NewPoint(geom.XYM).MustSetCoords(geom.Coord{1, 2, 3}),
			xdr: mustDecodeString("00400000013ff000000000000040000000000000004008000000000000"),
			ndr: mustDecodeString("0101000040000000000000f03f00000000000000400000000000000840"),
		},
		{
			g:   geom.NewPoint(geom.XYZM).MustSetCoords(geom.Coord{1, 2, 3, 4}),
			xdr: mustDecodeString("00c00000013ff0000000000000400000000000000040080000000000004010000000000000"),
			ndr: mustDecodeString("01010000c0000000000000f03f000000000000004000000000000008400000000000001040"),
		},
		{
			g:   geom.NewPoint(geom.XY).SetSRID(4326).MustSetCoords(geom.Coord{1, 2}),
			xdr: mustDecodeString("0020000001000010e63ff00000000000004000000000000000"),
			ndr: mustDecodeString("0101000020e6100000000000000000f03f0000000000000040"),
		},
		{
			g:   geom.NewPoint(geom.XYZ).SetSRID(4326).MustSetCoords(geom.Coord{1, 2, 3}),
			xdr: mustDecodeString("00a0000001000010e63ff000000000000040000000000000004008000000000000"),
			ndr: mustDecodeString("01010000a0e6100000000000000000f03f00000000000000400000000000000840"),
		},
		{
			g:   geom.NewPoint(geom.XYM).SetSRID(4326).MustSetCoords(geom.Coord{1, 2, 3}),
			xdr: mustDecodeString("0060000001000010e63ff000000000000040000000000000004008000000000000"),
			ndr: mustDecodeString("0101000060e6100000000000000000f03f00000000000000400000000000000840"),
		},
		{
			g:   geom.NewPoint(geom.XYZM).SetSRID(4326).MustSetCoords(geom.Coord{1, 2, 3, 4}),
			xdr: mustDecodeString("00e0000001000010e63ff0000000000000400000000000000040080000000000004010000000000000"),
			ndr: mustDecodeString("01010000e0e6100000000000000000f03f000000000000004000000000000008400000000000001040"),
		},
		{
			g: geom.NewGeometryCollection().SetSRID(4326).MustPush(
				geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{1, 2}),
				geom.NewLineString(geom.XY).MustSetCoords([]geom.Coord{{3, 4}, {5, 6}}),
			),
			ndr: mustDecodeString("0107000020E6100000020000000101000000000000000000F03F00000000000000400102000000020000000000000000000840000000000000104000000000000014400000000000001840"),
			xdr: mustDecodeString("0020000007000010e60000000200000000013ff000000000000040000000000000000000000002000000024008000000000000401000000000000040140000000000004018000000000000"),
		},
	} {
		test(t, tc.g, tc.xdr, tc.ndr)
	}
}
