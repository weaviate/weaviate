package db

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (db *DB) Aggregate(ctx context.Context, params traverser.AggregateParams) (*aggregation.Result, error) {
	panic("not implemented") // TODO: Implement
}

func (db *DB) ClassSearch(ctx context.Context, params traverser.GetParams) ([]search.Result, error) {
	panic("not implemented")
}
func (db *DB) VectorClassSearch(ctx context.Context, params traverser.GetParams) ([]search.Result, error) {
	panic("not implemented")
}
func (db *DB) VectorSearch(ctx context.Context, vector []float32, limit int,
	filters *filters.LocalFilter) ([]search.Result, error) {
	panic("not implemented")
}
