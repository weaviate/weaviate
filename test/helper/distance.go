package testhelper

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/additional"
)

func CertaintyToDist(t *testing.T, in float32) float32 {
	asFloat64 := float64(in)
	dist := additional.CertaintyToDist(&asFloat64)
	if dist == nil {
		t.Fatalf(
			"somehow %+v of type %T failed to produce a non-null *float64", in, in)
	}
	return float32(*dist)
}

func CertaintyToScore(t *testing.T, in float32) float32 {
	dist := CertaintyToDist(t, in)
	return -dist
}
