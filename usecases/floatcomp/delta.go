package floatcomp

import "math"

func InDelta(f1, f2 float64, delta float64) bool {
	diff := math.Abs(f1 - f2)
	return diff <= delta
}
