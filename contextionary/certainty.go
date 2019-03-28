package contextionary

// DistanceToCertainty converts a vector distance to a certainty. For now it's
// a linear scale but will most likely change at some point.
func DistanceToCertainty(d float32) float32 {
	return 1 - d/12
}
