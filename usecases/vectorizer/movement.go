//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package vectorizer

import "fmt"

// MoveTo moves one vector toward another
func (v *Vectorizer) MoveTo(source []float32, target []float32, weight float32,
) ([]float32, error) {
	return move(source, target, weight, towards)
}

// MoveAwayFrom moves one vector away from another
func (v *Vectorizer) MoveAwayFrom(source []float32, target []float32, weight float32,
) ([]float32, error) {
	return move(source, target, weight, awayFrom)
}

type direction float32

const (
	towards  = direction(1)
	awayFrom = direction(-1)
)

func move(source []float32, target []float32, weight float32, direction direction) ([]float32, error) {
	multiplier := float32(0.5)

	if len(source) != len(target) {
		return nil, fmt.Errorf("movement: vector lengths don't match: got %d and %d", len(source), len(target))
	}

	if weight < 0 || weight > 1 {
		return nil, fmt.Errorf("movement: force must be between 0 and 1: got %f", weight)
	}

	res := make([]float32, len(source), len(source))
	for i, sourceItem := range source {
		res[i] = sourceItem*(1-weight*multiplier) + float32(direction)*target[i]*(weight*multiplier)
	}

	return res, nil

}
