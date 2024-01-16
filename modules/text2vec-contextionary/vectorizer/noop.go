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

package vectorizer

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
)

// NoOpVectorizer is a simple stand in that does nothing. Can be used when the
// feature should be turned off overall
type NoOpVectorizer struct{}

// Corpi is not implemented in the NoOpVectorizer
func (n *NoOpVectorizer) Corpi(ctx context.Context, corpi []string) ([]float32, error) {
	return []float32{}, nil
}

// MoveTo is not implemented in the NoOpVectorizer
func (n *NoOpVectorizer) MoveTo(source []float32, target []float32, weight float32) ([]float32, error) {
	return []float32{}, nil
}

// MoveAwayFrom is not implemented in the NoOpVectorizer
func (n *NoOpVectorizer) MoveAwayFrom(source []float32, target []float32, weight float32) ([]float32, error) {
	return []float32{}, nil
}

// NormalizedDistance is not implemented in the NoOpVectorizer
func (n *NoOpVectorizer) NormalizedDistance(a, b []float32) (float32, error) {
	return 0, nil
}

// Object is not implemented in the NoOpVectorizer
func (n *NoOpVectorizer) Object(ctx context.Context, concept *models.Object) ([]float32, error) {
	return []float32{}, nil
}

// NewNoOp creates a new NoOpVectorizer which can be used when no vectorization
// is desired, i.e. the feature is turned off completely
func NewNoOp() *NoOpVectorizer {
	return &NoOpVectorizer{}
}
