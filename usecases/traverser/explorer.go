package traverser

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// Explorer is a helper construct to perform vector-based searches. It does not
// contain monitoring or authorization checks. It should thus never be directly
// used by an API, but through a Traverser.
type Explorer struct {
	search vectorClassSearch
}

type vectorClassSearch interface {
	VectorClassSearch(ctx context.Context, kind kind.Kind,
		className string, vector []float32, limit int,
		filters *filters.LocalFilter) ([]VectorSearchResult, error)
}

// NewExplorer with search and connector repo
func NewExplorer(search vectorClassSearch) *Explorer {
	return &Explorer{search}
}

// GetClass from search and connector repo
func (e *Explorer) GetClass(ctx context.Context,
	params *LocalGetParams) ([]interface{}, error) {
	return nil, fmt.Errorf("explore not yet implemented")
}
