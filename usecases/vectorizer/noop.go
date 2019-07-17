package vectorizer

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
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

// Thing is not implemented in the NoOpVectorizer
func (n *NoOpVectorizer) Thing(ctx context.Context, concept *models.Thing) ([]float32, error) {
	return []float32{}, nil
}

// Action is not implemented in the NoOpVectorizer
func (n *NoOpVectorizer) Action(ctx context.Context, concept *models.Action) ([]float32, error) {
	return []float32{}, nil
}

// NewNoOp creates a new NoOpVectorizer which can be used when no vectorization
// is desired, i.e. the feature is turned off completely
func NewNoOp() *NoOpVectorizer {
	return &NoOpVectorizer{}
}
