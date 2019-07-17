package traverser

import "context"

// NoOpExplorer errors if an explore operation is attempted
type NoOpExplorer struct {
	err error
}

// GetClass errors
func (n *NoOpExplorer) GetClass(ctx context.Context,
	params *LocalGetParams) ([]interface{}, error) {
	return nil, n.err
}

// Concepts errors
func (n *NoOpExplorer) Concepts(ctx context.Context,
	params ExploreParams) ([]VectorSearchResult, error) {
	return nil, n.err
}

// NewNoOpExplorer with variable error
func NewNoOpExplorer(err error) *NoOpExplorer {
	return &NoOpExplorer{err: err}
}
