package igc

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/twpayne/go-geom"
)

func TestDecode(t *testing.T) {
	for _, tc := range []struct {
		s string
		t *T
	}{
		{
			s: "AXTR20C38FF2C110\r\n" +
				"HFDTE151115\r\n" +
				"B1316284654230N00839078EA0147801630\r\n",
			t: &T{
				LineString: geom.NewLineString(geom.Layout(5)).MustSetCoords([]geom.Coord{
					{8.6513, 46.90383333333333, 1630, 1447593388, 1478},
				}),
			},
		},
		{
			s: "ACPP274CPILOT - s/n:11002274\r\n" +
				"HFDTE020613\r\n" +
				"I033638FXA3940SIU4141TDS\r\n" +
				"B1053525151892N00203986WA0017900275000108\r\n",
			t: &T{
				LineString: geom.NewLineString(geom.Layout(5)).MustSetCoords([]geom.Coord{
					{-2.0664333333333333, 51.864866666666664, 275, 1370170432.8, 179},
				}),
			},
		},
		{
			s: "AXCC64BCompCheck-3.2\r\n" +
				"HFDTE100810\r\n" +
				"I033637LAD3839LOD4040TDS\r\n" +
				"B1146174031985N00726775WA010040114912340",
			t: &T{
				LineString: geom.NewLineString(geom.Layout(5)).MustSetCoords([]geom.Coord{
					{-7.446255666666667, 40.53308533333333, 1149, 1281440777, 1004},
				}),
			},
		},
	} {
		if got, err := Read(bytes.NewBufferString(tc.s)); err != nil || !reflect.DeepEqual(tc.t, got) {
			t.Errorf("Read(...(%#v)) == %#v, %v, want nil, %#v", tc.s, got, err, tc.t)
		}
	}
}

func TestDecodeHeaders(t *testing.T) {
	for _, tc := range []struct {
		s string
		t *T
	}{
		{
			s: "AFLY05094\r\n" +
				"HFDTE210407\r\n" +
				"HFFXA100\r\n" +
				"HFPLTPILOT:Tom Payne\r\n" +
				"HFGTYGLIDERTYPE:Gradient Aspen\r\n" +
				"HFGIDGLIDERID:G12242505057\r\n" +
				"HFDTM100GPSDATUM:WGS84\r\n" +
				"HFGPSGPS:FURUNO GH-80\r\n" +
				"HFRFWFIRMWAREVERSION:1.16\r\n" +
				"HFRHWHARDWAREVERSION:1.00\r\n" +
				"HFFTYFRTYPE:FLYTEC,5020\r\n",
			t: &T{
				Headers: []Header{
					{Source: "F", Key: "PLT", KeyExtra: "PILOT", Value: "Tom Payne"},
					{Source: "F", Key: "GTY", KeyExtra: "GLIDERTYPE", Value: "Gradient Aspen"},
					{Source: "F", Key: "GID", KeyExtra: "GLIDERID", Value: "G12242505057"},
					{Source: "F", Key: "DTM", KeyExtra: "100GPSDATUM", Value: "WGS84"},
					{Source: "F", Key: "GPS", KeyExtra: "GPS", Value: "FURUNO GH-80"},
					{Source: "F", Key: "RFW", KeyExtra: "FIRMWAREVERSION", Value: "1.16"},
					{Source: "F", Key: "RHW", KeyExtra: "HARDWAREVERSION", Value: "1.00"},
					{Source: "F", Key: "FTY", KeyExtra: "FRTYPE", Value: "FLYTEC,5020"},
				},
			},
		},
	} {
		if got, err := Read(bytes.NewBufferString(tc.s)); err != nil || !reflect.DeepEqual(tc.t.Headers, got.Headers) {
			t.Errorf("Read(...(%#v)) == %#v, %v, want nil, %#v", tc.s, got, err, tc.t)
		}
	}
}
