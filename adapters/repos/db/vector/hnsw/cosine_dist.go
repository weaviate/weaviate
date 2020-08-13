//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"fmt"
	"math"
)

func cosineSim(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("vectors have different dimensions")
	}

	var (
		sumProduct float64
		sumASquare float64
		sumBSquare float64
	)

	for i := range a {
		sumProduct += float64(a[i] * b[i])
		sumASquare += float64(a[i] * a[i])
		sumBSquare += float64(b[i] * b[i])
	}

	return float32(sumProduct / (math.Sqrt(sumASquare) * math.Sqrt(sumBSquare))), nil
}

func cosineDist(a, b []float32) (float32, error) {
	// before := time.Now()
	// defer m.addDistancing(before)
	sim, err := cosineSim(a, b)
	return 1 - sim, err
}

// when running multiple distance calculations against the same vector (i.e.
// vector A is always the same, but B changes each iteration), we can make
// cosine dist a lot more efficient by calculating the A-specific values just
// once
type reusableDistancer struct {
	fixedVector                   []float64
	fixedVetorSquredSumSquareRoot float64
}

func newReusableDistancer(fixedVec []float32) *reusableDistancer {
	var sum float64
	fixed := make([]float64, len(fixedVec))

	for i, elem := range fixedVec {
		asFloat64 := float64(elem)
		fixed[i] = asFloat64
		sum += asFloat64 * asFloat64
	}

	sqrt := math.Sqrt(sum)

	return &reusableDistancer{
		fixedVector:                   fixed,
		fixedVetorSquredSumSquareRoot: sqrt,
	}
}

func (d *reusableDistancer) distance(input []float32) (float32, error) {
	if len(input) != len(d.fixedVector) {
		return 0, fmt.Errorf("vectors have different dimensions")
	}

	var (
		sumProduct float64
		sumSquare  float64
	)

	for i := range input {
		asFloat64 := float64(input[i])
		sumProduct += asFloat64 * d.fixedVector[i]
		sumSquare += asFloat64 * asFloat64
	}

	return 1 - float32(sumProduct/(math.Sqrt(sumSquare)*d.fixedVetorSquredSumSquareRoot)), nil
}
