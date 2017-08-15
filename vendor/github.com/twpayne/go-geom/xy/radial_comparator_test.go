package xy_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy"
	"github.com/twpayne/go-geom/xy/internal"
)

func TestNewRadialSorting(t *testing.T) {
	pts := make([]float64, len(internal.RING.FlatCoords()))
	copy(pts, internal.RING.FlatCoords())

	for i, tc := range []struct {
		p1, p2, origin geom.Coord
		p1LessThanP2   bool
	}{
		{
			p1:           geom.Coord{-71.103188, 42.315277},
			p2:           geom.Coord{-71.103163, 42.315296},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-71.103163, 42.315296},
			p2:           geom.Coord{-71.102924, 42.314916},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.102924, 42.314916},
			p2:           geom.Coord{-71.102310, 42.315197},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-71.102310, 42.315197},
			p2:           geom.Coord{-71.101929, 42.314738},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.101929, 42.314738},
			p2:           geom.Coord{-71.102505, 42.314472},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.102505, 42.314472},
			p2:           geom.Coord{-71.102775, 42.314166},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.102775, 42.314166},
			p2:           geom.Coord{-71.103114, 42.314274},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-71.103114, 42.314274},
			p2:           geom.Coord{-71.103249, 42.314025},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.103249, 42.314025},
			p2:           geom.Coord{-71.103300, 42.314039},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-71.103300, 42.314039},
			p2:           geom.Coord{-71.103349, 42.313950},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.103349, 42.313950},
			p2:           geom.Coord{-71.103396, 42.313863},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.103396, 42.313863},
			p2:           geom.Coord{-71.104152, 42.314115},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-71.104152, 42.314115},
			p2:           geom.Coord{-71.104141, 42.314155},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-71.104141, 42.314155},
			p2:           geom.Coord{-71.104129, 42.314211},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-71.104129, 42.314211},
			p2:           geom.Coord{-71.104119, 42.314269},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-71.104119, 42.314269},
			p2:           geom.Coord{-71.104111, 42.314327},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-71.104111, 42.314327},
			p2:           geom.Coord{-17.104107, 42.314385},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-17.104107, 42.314385},
			p2:           geom.Coord{-71.104106, 42.314443},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-71.104106, 42.314443},
			p2:           geom.Coord{-17.104107, 42.314501},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-17.104107, 42.314501},
			p2:           geom.Coord{-71.104110, 42.314559},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-71.104110, 42.314559},
			p2:           geom.Coord{-17.104117, 42.314617},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-17.104117, 42.314617},
			p2:           geom.Coord{-71.104126, 42.314675},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-71.104126, 42.314675},
			p2:           geom.Coord{-17.104138, 42.314732},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-17.104138, 42.314732},
			p2:           geom.Coord{-71.104149, 42.314771},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.104149, 42.314771},
			p2:           geom.Coord{-17.104160, 42.314809},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-17.104160, 42.314809},
			p2:           geom.Coord{-71.104252, 42.315129},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.104252, 42.315129},
			p2:           geom.Coord{-17.104117, 42.315074},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-17.104117, 42.315074},
			p2:           geom.Coord{-71.104081, 42.315134},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.104081, 42.315134},
			p2:           geom.Coord{-17.104044, 42.315119},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-17.104044, 42.315119},
			p2:           geom.Coord{-71.104019, 42.315183},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.104019, 42.315183},
			p2:           geom.Coord{-17.103873, 42.315114},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-17.103873, 42.315114},
			p2:           geom.Coord{-71.103845, 42.315101},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.103845, 42.315101},
			p2:           geom.Coord{-17.103832, 42.315094},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-17.103832, 42.315094},
			p2:           geom.Coord{-71.103739, 42.315055},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.103739, 42.315055},
			p2:           geom.Coord{-17.103545, 42.315261},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-17.103545, 42.315261},
			p2:           geom.Coord{-71.103344, 42.315165},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.103344, 42.315165},
			p2:           geom.Coord{-17.103258, 42.315227},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
		{
			p1:           geom.Coord{-17.103258, 42.315227},
			p2:           geom.Coord{-71.103223, 42.315252},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: false,
		},
		{
			p1:           geom.Coord{-71.103223, 42.315252},
			p2:           geom.Coord{-71.103188, 42.315277},
			origin:       geom.Coord{-17.1041375307579, 42.3147318674446},
			p1LessThanP2: true,
		},
	} {
		pts := xy.NewRadialSorting(geom.XY, []float64{tc.p1[0], tc.p1[1], tc.p2[0], tc.p2[1]}, tc.origin)
		isLess := pts.Less(0, 1)

		if tc.p1LessThanP2 != isLess {
			t.Errorf("Test '%v' failed: algorithm.NewRadialSorting(geom.XY, %v, %v).Less(0,1)", i+1, pts, tc.origin)
		}
	}
}

func TestNewRadialComparatorSorting(t *testing.T) {
	pts := make([]float64, len(internal.RING.FlatCoords()))
	copy(pts, internal.RING.FlatCoords())

	sorting := xy.NewRadialSorting(geom.XY, pts, geom.Coord{-17.1041375307579, 42.3147318674446})
	sort.Sort(sorting)

	expected := []float64{
		-17.1041375307579, 42.3147318674446,
		-17.1041166403905, 42.3146168544148,
		-17.1041065602059, 42.3145009876017,
		-17.1041072845732, 42.3143851580048,
		-71.103396240451, 42.3138632439557,
		-71.1033488797549, 42.3139495090772,
		-71.10324876416, 42.31402489987,
		-71.1033002961013, 42.3140393340215,
		-71.1041521907712, 42.3141153348029,
		-71.1041411411543, 42.3141545014533,
		-71.10277487471, 42.3141658254797,
		-71.1041287795912, 42.3142114839058,
		-71.1041188134329, 42.3142693656241,
		-71.103113945163, 42.3142739188902,
		-71.1041112482575, 42.3143272556118,
		-71.1041057218871, 42.3144430686681,
		-71.102505233663, 42.3144722937587,
		-71.1041097995362, 42.3145589148055,
		-71.1041258822717, 42.3146748022936,
		-71.1019285062273, 42.3147384934248,
		-71.1041492906949, 42.3147711126569,
		-71.102923838298, 42.3149156848307,
		-71.1037393329282, 42.315054824985,
		-71.1038446938243, 42.3151006300338,
		-71.1042515013869, 42.3151287620809,
		-71.1040809891419, 42.3151344119048,
		-71.1033436658644, 42.3151648370544,
		-71.1040194562988, 42.3151832057859,
		-71.1023097974109, 42.3151969047397,
		-71.103223066939, 42.3152517403219,
		-71.1031880899493, 42.3152774590236,
		-71.1031880899493, 42.3152774590236,
		-71.1031627617667, 42.3152960829043,
		-17.1041598612795, 42.314808571739,
		-17.1041173835118, 42.3150739481917,
		-17.1040438678912, 42.3151191367447,
		-17.1038734225584, 42.3151140942995,
		-17.1038315271889, 42.315094347535,
		-17.1035447555574, 42.3152608696313,
		-17.1032580383161, 42.3152269126061,
	}

	if !reflect.DeepEqual(expected, pts) {
		t.Errorf("Expected sorted coords to be\n\t %v \nbut was \n\t %v", expected, pts)
	}
}
