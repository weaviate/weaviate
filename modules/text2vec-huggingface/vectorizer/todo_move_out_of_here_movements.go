//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package vectorizer

import "fmt"

// CombineVectors combines all of the vector into sum of their parts
func (v *Vectorizer) CombineVectors(vectors [][]float32) []float32 {
	maxVectorLength := 0
	for i := range vectors {
		if len(vectors[i]) > maxVectorLength {
			maxVectorLength = len(vectors[i])
		}
	}
	sums := make([]float32, maxVectorLength)
	dividers := make([]float32, maxVectorLength)
	for _, vector := range vectors {
		for i := 0; i < len(vector); i++ {
			sums[i] += vector[i]
			dividers[i]++
		}
	}
	combinedVector := make([]float32, len(sums))
	for i := 0; i < len(sums); i++ {
		combinedVector[i] = sums[i] / dividers[i]
	}

	return combinedVector
}

// MoveTo moves one vector toward another
func (v *Vectorizer) MoveTo(source []float32, target []float32, weight float32,
) ([]float32, error) {
	multiplier := float32(0.5)

	if len(source) != len(target) {
		return nil, fmt.Errorf("movement: vector lengths don't match: got %d and %d",
			len(source), len(target))
	}

	if weight < 0 || weight > 1 {
		return nil, fmt.Errorf("movement: force must be between 0 and 1: got %f",
			weight)
	}

	out := make([]float32, len(source))
	for i, sourceItem := range source {
		out[i] = sourceItem*(1-weight*multiplier) + target[i]*(weight*multiplier)
	}

	return out, nil
}

// MoveAwayFrom moves one vector away from another
func (v *Vectorizer) MoveAwayFrom(source []float32, target []float32, weight float32,
) ([]float32, error) {
	multiplier := float32(0.5) // so the movement is fair in comparison with moveTo
	if len(source) != len(target) {
		return nil, fmt.Errorf("movement (moveAwayFrom): vector lengths don't match: "+
			"got %d and %d", len(source), len(target))
	}

	if weight < 0 {
		return nil, fmt.Errorf("movement (moveAwayFrom): force must be 0 or positive: "+
			"got %f", weight)
	}

	out := make([]float32, len(source))
	for i, sourceItem := range source {
		out[i] = sourceItem + weight*multiplier*(sourceItem-target[i])
	}

	return out, nil
}
