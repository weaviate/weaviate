package xyz

import (
	"math"

	"github.com/twpayne/go-geom"
)

// VectorDot calculates the dot product of two vectors
func VectorDot(v1Start, v1End, v2Start, v2End geom.Coord) float64 {
	v1Startv2Endx := v1End[0] - v1Start[0]
	v1Startv2Endy := v1End[1] - v1Start[1]
	v1Startv2Endz := v1End[2] - v1Start[2]
	v2Startv2Endx := v2End[0] - v2Start[0]
	v2Startv2Endy := v2End[1] - v2Start[1]
	v2Startv2Endz := v2End[2] - v2Start[2]
	return v1Startv2Endx*v2Startv2Endx + v1Startv2Endy*v2Startv2Endy + v1Startv2Endz*v2Startv2Endz
}

// VectorNormalize creates a coordinate that is the normalized vector from 0,0,0 to vector
func VectorNormalize(vector geom.Coord) geom.Coord {
	vLen := VectorLength(vector)
	return geom.Coord{vector[0] / vLen, vector[1] / vLen, vector[2] / vLen}
}

// VectorLength calculates the length of the vector from 0,0,0 to vector
func VectorLength(vector geom.Coord) float64 {
	return math.Sqrt(vector[0]*vector[0] + vector[1]*vector[1] + vector[2]*vector[2])
}
