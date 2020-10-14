package db

import (
	"context"

	"github.com/semi-technologies/weaviate/adapters/repos/db/aggregator"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (s *Shard) aggregate(ctx context.Context,
	params traverser.AggregateParams) (*aggregation.Result, error) {
	return aggregator.New(s.db, params, s.index.getSchema).Do(ctx)
}
