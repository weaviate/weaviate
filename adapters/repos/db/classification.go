package db

import (
	"context"

	libfilters "github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/classification"
)

func (db *DB) GetUnclassified(ctx context.Context, kind kind.Kind, class string, properties []string, filter *libfilters.LocalFilter) ([]search.Result, error) {
	panic("not implemented") // TODO: Implement
}

func (db *DB) AggregateNeighbors(ctx context.Context, vector []float32, kind kind.Kind, class string, properties []string, k int, filter *libfilters.LocalFilter) ([]classification.NeighborRef, error) {
	panic("not implemented") // TODO: Implement
}
