package ewkbhex

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
	"github.com/twpayne/go-geom/encoding/wkbcommon"
)

func test(t *testing.T, g geom.T, xdr string, ndr string) {
	if xdr != "" {
		if got, err := Decode(xdr); err != nil || !reflect.DeepEqual(got, g) {
			t.Errorf("Decode(%s) == %#v, %#v, want %#v, nil", xdr, got, err, g)
		}
		if got, err := Encode(g, wkbcommon.XDR); err != nil || !reflect.DeepEqual(got, xdr) {
			t.Errorf("Encode(%#v, XDR) == %s, %#v, want %s, nil", g, got, err, xdr)
		}
	}
	if ndr != "" {
		if got, err := Decode(ndr); err != nil || !reflect.DeepEqual(got, g) {
			t.Errorf("Decode(%s) == %#v, %#v, want %#v, nil", ndr, got, err, g)
		}
		if got, err := Encode(g, wkbcommon.NDR); err != nil || !reflect.DeepEqual(got, ndr) {
			t.Errorf("Encode(%#v, NDR) == %s, %#v, want %#v, nil", g, got, err, ndr)
		}
	}
	switch g.(type) {
	case *geom.Point:
		var p ewkb.Point
		if xdr != "" {
			if err := p.Scan(decodeString(xdr)); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", p, string(xdr), err)
			}
			if !reflect.DeepEqual(p, ewkb.Point{Point: g.(*geom.Point)}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(xdr), p, ewkb.Point{Point: g.(*geom.Point)})
			}
		}
		if ndr != "" {
			if err := p.Scan(decodeString(ndr)); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", p, string(ndr), err)
			}
			if !reflect.DeepEqual(p, ewkb.Point{Point: g.(*geom.Point)}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(ndr), p, ewkb.Point{Point: g.(*geom.Point)})
			}
		}
	case *geom.LineString:
		var ls ewkb.LineString
		if xdr != "" {
			if err := ls.Scan(decodeString(xdr)); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", ls, string(xdr), err)
			}
			if !reflect.DeepEqual(ls, ewkb.LineString{LineString: g.(*geom.LineString)}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(xdr), ls, ewkb.LineString{LineString: g.(*geom.LineString)})
			}
		}
		if ndr != "" {
			if err := ls.Scan(decodeString(ndr)); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", ls, string(ndr), err)
			}
			if !reflect.DeepEqual(ls, ewkb.LineString{LineString: g.(*geom.LineString)}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(ndr), ls, ewkb.LineString{LineString: g.(*geom.LineString)})
			}
		}
	case *geom.Polygon:
		var p ewkb.Polygon
		if xdr != "" {
			if err := p.Scan(decodeString(xdr)); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", p, string(xdr), err)
			}
			if !reflect.DeepEqual(p, ewkb.Polygon{Polygon: g.(*geom.Polygon)}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(xdr), p, ewkb.Polygon{Polygon: g.(*geom.Polygon)})
			}
		}
		if ndr != "" {
			if err := p.Scan(decodeString(ndr)); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", p, string(ndr), err)
			}
			if !reflect.DeepEqual(p, ewkb.Polygon{Polygon: g.(*geom.Polygon)}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(ndr), p, ewkb.Polygon{Polygon: g.(*geom.Polygon)})
			}
		}
	case *geom.MultiPoint:
		var mp ewkb.MultiPoint
		if xdr != "" {
			if err := mp.Scan(decodeString(xdr)); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", mp, string(xdr), err)
			}
			if !reflect.DeepEqual(mp, ewkb.MultiPoint{MultiPoint: g.(*geom.MultiPoint)}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(xdr), mp, ewkb.MultiPoint{MultiPoint: g.(*geom.MultiPoint)})
			}
		}
		if ndr != "" {
			if err := mp.Scan(decodeString(ndr)); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", mp, string(ndr), err)
			}
			if !reflect.DeepEqual(mp, ewkb.MultiPoint{MultiPoint: g.(*geom.MultiPoint)}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(ndr), mp, ewkb.MultiPoint{MultiPoint: g.(*geom.MultiPoint)})
			}
		}
	case *geom.MultiLineString:
		var mls ewkb.MultiLineString
		if xdr != "" {
			if err := mls.Scan(decodeString(xdr)); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", mls, string(xdr), err)
			}
			if !reflect.DeepEqual(mls, ewkb.MultiLineString{MultiLineString: g.(*geom.MultiLineString)}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(xdr), mls, ewkb.MultiLineString{MultiLineString: g.(*geom.MultiLineString)})
			}
		}
		if ndr != "" {
			if err := mls.Scan(decodeString(ndr)); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", mls, string(ndr), err)
			}
			if !reflect.DeepEqual(mls, ewkb.MultiLineString{MultiLineString: g.(*geom.MultiLineString)}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(ndr), mls, ewkb.MultiLineString{MultiLineString: g.(*geom.MultiLineString)})
			}
		}
	case *geom.MultiPolygon:
		var mp ewkb.MultiPolygon
		if xdr != "" {
			if err := mp.Scan(decodeString(xdr)); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", mp, string(xdr), err)
			}
			if !reflect.DeepEqual(mp, ewkb.MultiPolygon{MultiPolygon: g.(*geom.MultiPolygon)}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(xdr), mp, ewkb.MultiPolygon{MultiPolygon: g.(*geom.MultiPolygon)})
			}
		}
		if ndr != "" {
			if err := mp.Scan(decodeString(ndr)); err != nil {
				t.Errorf("%#v.Scan(%#v) == %v, want nil", mp, string(ndr), err)
			}
			if !reflect.DeepEqual(mp, ewkb.MultiPolygon{MultiPolygon: g.(*geom.MultiPolygon)}) {
				t.Errorf("Scan(%#v) got %#v, want %#v", string(ndr), mp, ewkb.MultiPolygon{MultiPolygon: g.(*geom.MultiPolygon)})
			}
		}
	}
}

func decodeString(s string) []byte {
	data, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return data
}

func Test(t *testing.T) {
	for _, tc := range []struct {
		g   geom.T
		xdr string
		ndr string
	}{
		{
			g:   geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{1, 2}),
			xdr: "00000000013ff00000000000004000000000000000",
			ndr: "0101000000000000000000f03f0000000000000040",
		},
		{
			g:   geom.NewPoint(geom.XYZ).MustSetCoords(geom.Coord{1, 2, 3}),
			xdr: "00800000013ff000000000000040000000000000004008000000000000",
			ndr: "0101000080000000000000f03f00000000000000400000000000000840",
		},
		{
			g:   geom.NewPoint(geom.XYM).MustSetCoords(geom.Coord{1, 2, 3}),
			xdr: "00400000013ff000000000000040000000000000004008000000000000",
			ndr: "0101000040000000000000f03f00000000000000400000000000000840",
		},
		{
			g:   geom.NewPoint(geom.XYZM).MustSetCoords(geom.Coord{1, 2, 3, 4}),
			xdr: "00c00000013ff0000000000000400000000000000040080000000000004010000000000000",
			ndr: "01010000c0000000000000f03f000000000000004000000000000008400000000000001040",
		},
		{
			g:   geom.NewPoint(geom.XY).SetSRID(4326).MustSetCoords(geom.Coord{1, 2}),
			xdr: "0020000001000010e63ff00000000000004000000000000000",
			ndr: "0101000020e6100000000000000000f03f0000000000000040",
		},
		{
			g:   geom.NewPoint(geom.XYZ).SetSRID(4326).MustSetCoords(geom.Coord{1, 2, 3}),
			xdr: "00a0000001000010e63ff000000000000040000000000000004008000000000000",
			ndr: "01010000a0e6100000000000000000f03f00000000000000400000000000000840",
		},
		{
			g:   geom.NewPoint(geom.XYM).SetSRID(4326).MustSetCoords(geom.Coord{1, 2, 3}),
			xdr: "0060000001000010e63ff000000000000040000000000000004008000000000000",
			ndr: "0101000060e6100000000000000000f03f00000000000000400000000000000840",
		},
		{
			g:   geom.NewPoint(geom.XYZM).SetSRID(4326).MustSetCoords(geom.Coord{1, 2, 3, 4}),
			xdr: "00e0000001000010e63ff0000000000000400000000000000040080000000000004010000000000000",
			ndr: "01010000e0e6100000000000000000f03f000000000000004000000000000008400000000000001040",
		},
		{
			g: geom.NewPolygon(geom.XY).SetSRID(4326).MustSetCoords([][]geom.Coord{
				{
					{-76.32498664201256, 40.047663287885534},
					{-76.32495043219086, 40.047748950935976},
					{-76.32479897120051, 40.04770947217201},
					{-76.32483518102224, 40.04762473113094},
					{-76.32498664201256, 40.047663287885534},
				},
			}),
			ndr: "0103000020e610000001000000050000002cc5c594cc1453c01758a3d4190644402cc5e5fccb1453c01b583ba31c06444029c59f81c91453c017580f581b0644402bc57f19ca1453c016583391180644402cc5c594cc1453c01758a3d419064440",
			xdr: "0020000003000010e60000000100000005c05314cc94c5c52c40440619d4a35817c05314cbfce5c52c4044061ca33b581bc05314c9819fc5294044061b580f5817c05314ca197fc52b4044061891335816c05314cc94c5c52c40440619d4a35817",
		},
	} {
		test(t, tc.g, tc.xdr, tc.ndr)
	}
}
