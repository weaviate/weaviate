package xy

import (
	"sort"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/bigxy"
	"github.com/twpayne/go-geom/sorting"
	"github.com/twpayne/go-geom/xy/internal"
	"github.com/twpayne/go-geom/xy/orientation"
)

type convexHullCalculator struct {
	layout   geom.Layout
	stride   int
	inputPts []float64
}

func (calc *convexHullCalculator) lineOrPolygon(coordinates []float64) geom.T {

	cleanCoords := calc.cleanRing(coordinates)
	if len(cleanCoords) == 3*calc.stride {
		return geom.NewLineStringFlat(calc.layout, cleanCoords[0:len(cleanCoords)-calc.stride])
	}
	return geom.NewPolygonFlat(calc.layout, cleanCoords, []int{len(cleanCoords)})
}

func (calc *convexHullCalculator) cleanRing(original []float64) []float64 {

	cleanedRing := []float64{}
	var previousDistinctCoordinate []float64
	for i := 0; i < len(original)-calc.stride; i += calc.stride {
		if internal.Equal(original, i, original, i+calc.stride) {
			continue
		}
		currentCoordinate := original[i : i+calc.stride]
		nextCoordinate := original[i+calc.stride : i+calc.stride+calc.stride]
		if previousDistinctCoordinate != nil && calc.isBetween(previousDistinctCoordinate, currentCoordinate, nextCoordinate) {
			continue
		}
		cleanedRing = append(cleanedRing, currentCoordinate...)
		previousDistinctCoordinate = currentCoordinate
	}
	return append(cleanedRing, original[len(original)-calc.stride:]...)
}

func (calc *convexHullCalculator) isBetween(c1, c2, c3 []float64) bool {
	if bigxy.OrientationIndex(c1, c2, c3) != orientation.Collinear {
		return false
	}
	if c1[0] != c3[0] {
		if c1[0] <= c2[0] && c2[0] <= c3[0] {
			return true
		}
		if c3[0] <= c2[0] && c2[0] <= c1[0] {
			return true
		}
	}
	if c1[1] != c3[1] {
		if c1[1] <= c2[1] && c2[1] <= c3[1] {
			return true
		}
		if c3[1] <= c2[1] && c2[1] <= c1[1] {
			return true
		}
	}
	return false
}
func (calc *convexHullCalculator) grahamScan(coordData []float64) []float64 {
	coordStack := internal.NewCoordStack(calc.layout)
	coordStack.Push(coordData, 0)
	coordStack.Push(coordData, calc.stride)
	coordStack.Push(coordData, calc.stride*2)
	for i := 3 * calc.stride; i < len(coordData); i += calc.stride {
		p, remaining := coordStack.Pop()
		// check for empty stack to guard against robustness problems
		for remaining > 0 && bigxy.OrientationIndex(geom.Coord(coordStack.Peek()), geom.Coord(p), geom.Coord(coordData[i:i+calc.stride])) > 0 {
			p, _ = coordStack.Pop()
		}
		coordStack.Push(p, 0)
		coordStack.Push(coordData, i)
	}
	coordStack.Push(coordData, 0)
	return coordStack.Data

}
func (calc *convexHullCalculator) preSort(pts []float64) {

	// find the lowest point in the set. If two or more points have
	// the same minimum y coordinate choose the one with the minimu x.
	// This focal point is put in array location pts[0].
	for i := calc.stride; i < len(pts); i += calc.stride {
		if pts[i+1] < pts[1] || (pts[i+1] == pts[1] && pts[i] < pts[0]) {
			for k := 0; k < calc.stride; k++ {
				pts[k], pts[i+k] = pts[i+k], pts[k]
			}
		}
	}

	// sort the points radially around the focal point.
	sort.Sort(NewRadialSorting(calc.layout, pts, geom.Coord{pts[0], pts[1]}))

}

func (calc *convexHullCalculator) padArray3(pts []float64) []float64 {
	pad := make([]float64, 3*calc.stride)

	for i := 0; i < len(pad); i++ {
		if i < len(pts) {
			pad[i] = pts[i]
		} else {
			pad[i] = pts[0]
		}
	}
	return pad
}

func (calc *convexHullCalculator) computeOctRing(inputPts []float64) []float64 {
	stride := calc.stride
	octPts := calc.computeOctPts(inputPts)
	copyTo := 0
	for i := stride; i < len(octPts); i += stride {
		if !internal.Equal(octPts, i-stride, octPts, i) {
			copyTo += stride
		}
		for j := 0; j < stride; j++ {
			octPts[copyTo+j] = octPts[i+j]
		}
	}

	// points must all lie in a line
	if copyTo < 6 {
		return nil
	}

	copyTo += stride
	octPts = octPts[0 : copyTo+stride]

	// close ring
	for j := 0; j < stride; j++ {
		octPts[copyTo+j] = octPts[j]
	}

	return octPts
}

func (calc *convexHullCalculator) computeOctPts(inputPts []float64) []float64 {
	stride := calc.stride
	pts := make([]float64, 8*stride)
	for j := 0; j < len(pts); j += stride {
		for k := 0; k < stride; k++ {
			pts[j+k] = inputPts[k]
		}
	}

	for i := stride; i < len(inputPts); i += stride {

		if inputPts[i] < pts[0] {
			for k := 0; k < stride; k++ {
				pts[k] = inputPts[i+k]
			}
		}
		if inputPts[i]-inputPts[i+1] < pts[stride]-pts[stride+1] {
			for k := 0; k < stride; k++ {
				pts[stride+k] = inputPts[i+k]
			}
		}
		if inputPts[i+1] > pts[2*stride+1] {
			for k := 0; k < stride; k++ {
				pts[2*stride+k] = inputPts[i+k]
			}
		}
		if inputPts[i]+inputPts[i+1] > pts[3*stride]+pts[3*stride+1] {
			for k := 0; k < stride; k++ {
				pts[3*stride+k] = inputPts[i+k]
			}
		}
		if inputPts[i] > pts[4*stride] {
			for k := 0; k < stride; k++ {
				pts[4*stride+k] = inputPts[i+k]
			}
		}
		if inputPts[i]-inputPts[i+1] > pts[5*stride]-pts[5*stride+1] {
			for k := 0; k < stride; k++ {
				pts[5*stride+k] = inputPts[i+k]
			}
		}
		if inputPts[i+1] < pts[6*stride+1] {
			for k := 0; k < stride; k++ {
				pts[6*stride+k] = inputPts[i+k]
			}
		}
		if inputPts[i]+inputPts[i+1] < pts[7*stride]+pts[7*stride+1] {
			for k := 0; k < stride; k++ {
				pts[7*stride+k] = inputPts[i+k]
			}
		}
	}
	return pts

}

type comparator struct{}

func (c comparator) IsEquals(x, y geom.Coord) bool {
	return internal.Equal(x, 0, y, 0)
}
func (c comparator) IsLess(x, y geom.Coord) bool {
	return sorting.IsLess2D(x, y)
}
