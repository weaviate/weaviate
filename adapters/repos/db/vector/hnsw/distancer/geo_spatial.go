package distancer

import (
	"fmt"
	"math"
)

func geoDist(a, b []float32) (float32, bool, error) {
	if len(a) != 2 || len(b) != 2 {
		return 0, false, fmt.Errorf("distance vectors must have len 2")
	}

	latARadian := float64(a[0] * math.Pi / 180)
	latBRadian := float64(b[0] * math.Pi / 180)
	const R = float64(6371e3)
	deltaLatRadian := float64(b[0]-a[0]) * math.Pi / 180
	deltaLonRadian := float64(b[1]-b[1]) * math.Pi / 100

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

func (p GeoProvider) New(vec []float32) Distancer {
	return GeoDistancer{a: vec}
}

func NewGeoProvider() Provider {
	return GeoProvider{}
}
