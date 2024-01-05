//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package distancer

import (
	"fmt"
	"math"
)

func geoDist(a, b []float32) (float32, bool, error) {
	if len(a) != 2 || len(b) != 2 {
		return 0, false, fmt.Errorf("distance vectors must have len 2")
	}

	latA := a[0]
	latB := b[0]
	lonA := a[1]
	lonB := b[1]
	const R = float64(6371e3)

	latARadian := float64(latA * math.Pi / 180)
	latBRadian := float64(latB * math.Pi / 180)
	deltaLatRadian := float64(latB-latA) * math.Pi / 180
	deltaLonRadian := float64(lonB-lonA) * math.Pi / 180

	A := math.Sin(deltaLatRadian/2)*math.Sin(deltaLatRadian/2) +
		math.Cos(latARadian)*math.Cos(latBRadian)*math.Sin(deltaLonRadian/2)*math.Sin(deltaLonRadian/2)

	C := 2 * math.Atan2(math.Sqrt(A), math.Sqrt(1-A))

	return float32(R * C), true, nil
}

type GeoDistancer struct {
	a []float32
}

func (g GeoDistancer) Distance(b []float32) (float32, bool, error) {
	return geoDist(g.a, b)
}

type GeoProvider struct{}

func (gp GeoProvider) New(vec []float32) Distancer {
	return GeoDistancer{a: vec}
}

func (gp GeoProvider) SingleDist(vec1, vec2 []float32) (float32, bool, error) {
	return geoDist(vec1, vec2)
}

func (gp GeoProvider) Type() string {
	return "geo"
}

func (gp GeoProvider) Step(x, y []float32) float32 {
	panic("Not implemented")
}

func (gp GeoProvider) Wrap(x float32) float32 {
	panic("Not implemented")
}

func NewGeoProvider() Provider {
	return GeoProvider{}
}
