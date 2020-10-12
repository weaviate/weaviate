package db

import (
	"context"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (s *Shard) aggregate(ctx context.Context,
	params traverser.AggregateParams) (*aggregation.Result, error) {
	return NewAggregator(s, params).Do(ctx)
}

type Aggregator struct {
	shard  *Shard
	params traverser.AggregateParams
}

func NewAggregator(shard *Shard, params traverser.AggregateParams) *Aggregator {
	return &Aggregator{
		shard:  shard,
		params: params,
	}
}

func (a *Aggregator) Do(ctx context.Context) (*aggregation.Result, error) {
	out := aggregation.Result{}

	if a.params.GroupBy != nil {
		return nil, fmt.Errorf("grouping not supported yet")
	}

	// without grouping there is always exactly one group
	out.Groups = make([]aggregation.Group, 1)

	if a.params.IncludeMetaCount {
		if err := a.addMetaCount(ctx, &out); err != nil {
			return nil, errors.Wrap(err, "add meta count")
		}
	}

	return &out, nil
}

func (a *Aggregator) addMetaCount(ctx context.Context,
	out *aggregation.Result) error {

	if err := a.shard.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(helpers.ObjectsBucket)
		out.Groups[0].Count = b.Stats().KeyN

		return nil

	}); err != nil {
		return err
	}

	return nil
}
