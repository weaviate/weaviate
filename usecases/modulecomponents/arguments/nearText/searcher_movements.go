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

package nearText

import (
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/dto"
)

type movements[T dto.Embedding] struct{}

func newMovements[T dto.Embedding]() *movements[T] {
	return &movements[T]{}
}

var (
	ErrMultiVectorMoveTo   = errors.New("move to operations are not applicable for multivector embeddings")
	ErrMultiVectorMoveAway = errors.New("move away from operations are not applicable for multivector embeddings")
)

// MoveTo moves one vector toward another
func (v *movements[T]) MoveTo(source T, target T, weight float32,
) (T, error) {
	// the type of source/target should always be the same, but check both to be sure
	if _, sourceOk := any(source).([][]float32); sourceOk {
		return nil, ErrMultiVectorMoveTo
	}
	if _, targetOk := any(target).([][]float32); targetOk {
		return nil, ErrMultiVectorMoveTo
	}

	multiplier := float32(0.5)

	if len(source) != len(target) {
		return nil, fmt.Errorf("movement: vector lengths don't match: got %d and %d",
			len(source), len(target))
	}

	if weight < 0 || weight > 1 {
		return nil, fmt.Errorf("movement: force must be between 0 and 1: got %f",
			weight)
	}

	switch any(source).(type) {
	case []float32:
		out := make([]float32, len(source))
		for i, sourceItem := range any(source).([]float32) {
			out[i] = sourceItem*(1-weight*multiplier) + any(target).([]float32)[i]*(weight*multiplier)
		}
		return any(out).(T), nil
	default:
		return nil, errors.New("not implemented")
	}
}

// MoveAwayFrom moves one vector away from another
func (v *movements[T]) MoveAwayFrom(source T, target T, weight float32,
) (T, error) {
	// the type of source/target should always be the same, but check both to be sure
	if _, sourceOk := any(source).([][]float32); sourceOk {
		return nil, ErrMultiVectorMoveAway
	}
	if _, targetOk := any(target).([][]float32); targetOk {
		return nil, ErrMultiVectorMoveAway
	}

	multiplier := float32(0.5) // so the movement is fair in comparison with moveTo
	if len(source) != len(target) {
		return nil, fmt.Errorf("movement (moveAwayFrom): vector lengths don't match: "+
			"got %d and %d", len(source), len(target))
	}

	if weight < 0 {
		return nil, fmt.Errorf("movement (moveAwayFrom): force must be 0 or positive: "+
			"got %f", weight)
	}

	switch any(source).(type) {
	case []float32:
		out := make([]float32, len(source))
		for i, sourceItem := range any(source).([]float32) {
			out[i] = sourceItem + weight*multiplier*(sourceItem-any(target).([]float32)[i])
		}
		return any(out).(T), nil
	default:
		return nil, errors.New("not implemented")
	}
}
