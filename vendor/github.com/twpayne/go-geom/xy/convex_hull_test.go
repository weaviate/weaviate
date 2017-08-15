package xy

import (
	"reflect"
	"testing"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy/internal"
)

func TestPresort(t *testing.T) {

	calc := &convexHullCalculator{layout: geom.XY, stride: 2}
	coords := append([]float64{}, internal.RING.FlatCoords()...)
	calc.preSort(coords)

	expected := []float64{
		-71.103396240451, 42.3138632439557,
		-71.1041521907712, 42.3141153348029,
		-71.1041411411543, 42.3141545014533,
		-71.1041287795912, 42.3142114839058,
		-71.1041188134329, 42.3142693656241,
		-71.1041112482575, 42.3143272556118,
		-71.1041057218871, 42.3144430686681,
		-71.1041097995362, 42.3145589148055,
		-71.1041258822717, 42.3146748022936,
		-71.1041492906949, 42.3147711126569,
		-71.1042515013869, 42.3151287620809,
		-71.1040809891419, 42.3151344119048,
		-71.1040194562988, 42.3151832057859,
		-71.1038446938243, 42.3151006300338,
		-71.1037393329282, 42.315054824985,
		-71.1033436658644, 42.3151648370544,
		-71.103223066939, 42.3152517403219,
		-71.1031880899493, 42.3152774590236,
		-71.1031880899493, 42.3152774590236,
		-71.1031627617667, 42.3152960829043,
		-71.102923838298, 42.3149156848307,
		-71.1033002961013, 42.3140393340215,
		-71.1033488797549, 42.3139495090772,
		-71.103113945163, 42.3142739188902,
		-71.1023097974109, 42.3151969047397,
		-71.10324876416, 42.31402489987,
		-71.102505233663, 42.3144722937587,
		-71.1019285062273, 42.3147384934248,
		-71.10277487471, 42.3141658254797,
		-17.1035447555574, 42.3152608696313,
		-17.1032580383161, 42.3152269126061,
		-17.1040438678912, 42.3151191367447,
		-17.1038734225584, 42.3151140942995,
		-17.1038315271889, 42.315094347535,
		-17.1041173835118, 42.3150739481917,
		-17.1041598612795, 42.314808571739,
		-17.1041375307579, 42.3147318674446,
		-17.1041166403905, 42.3146168544148,
		-17.1041065602059, 42.3145009876017,
		-17.1041072845732, 42.3143851580048,
	}
	if !reflect.DeepEqual(coords, expected) {
		t.Errorf("Expected preSorted ring to be\n\t%v\nbut was\n\t%v", expected, coords)
	}
}

func TestOctRing(t *testing.T) {
	data := []float64{

		1, 1, 10,
		1, 0, 10,
		10, 1, 10,
		5, 4, 10,
		6, -1, 10,
		3, 3, 10,
		1, 2, 10,
		2, 1, 10,
		1.5, 3, 10,
	}

	calc := &convexHullCalculator{layout: geom.XYM, stride: 3}

	result := calc.computeOctPts(data)
	expected := []float64{
		1.0, 1.0, 10.0,
		1.5, 3.0, 10.0,
		5.0, 4.0, 10.0,
		10.0, 1.0, 10.0,
		10.0, 1.0, 10.0,
		10.0, 1.0, 10.0,
		6.0, -1.0, 10.0,
		1.0, 0.0, 10.0}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("Incorrect ordering and sorting of octPts. Expected \n\t%v \nwas \n\t%v", expected, result)
	}

	result = calc.computeOctRing(data)
	expected = []float64{
		1.0, 1.0, 10,
		1.5, 3.0, 10,
		5.0, 4.0, 10,
		10.0, 1.0, 10,
		6.0, -1.0, 10,
		1.0, 0.0, 10,
		1.0, 1.0, 10,
	}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("Incorrect ordering and sorting of OctRing. Expected \n\t%v \nwas \n\t%v", expected, result)
	}

}
func TestGrahamScan(t *testing.T) {
	calc := &convexHullCalculator{layout: geom.XY, stride: 2}
	coords := append([]float64{}, internal.RING.FlatCoords()...)
	scan := calc.grahamScan(coords)

	expected := []float64{
		-71.1031880899493, 42.3152774590236,
		-71.1031627617667, 42.3152960829043,
		-71.1023097974109, 42.3151969047397,
		-71.1019285062273, 42.3147384934248,
		-71.10277487471, 42.3141658254797,
		-71.103396240451, 42.3138632439557,
		-71.1041521907712, 42.3141153348029,
		-71.1042515013869, 42.3151287620809,
		-71.1040194562988, 42.3151832057859,
		-17.1038734225584, 42.3151140942995,
		-17.1038315271889, 42.315094347535,
		-71.1037393329282, 42.315054824985,
		-71.1031880899493, 42.3152774590236,
		-71.1031880899493, 42.3152774590236,
	}

	if !reflect.DeepEqual(scan, expected) {
		t.Fatalf("calc.grahamScan(...) failed.  Expected \n\t%v\nbut was\n\t%v", expected, scan)
	}
	if !reflect.DeepEqual(internal.RING.FlatCoords(), coords) {
		t.Fatalf("calc.grahamScan(...) mutated the input coords.  Expected \n\t%v\nbut was\n\t%v", internal.RING.FlatCoords(), coords)
	}
}
