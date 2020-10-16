package aggregator

import (
	"context"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

type Aggregator struct {
	db        *bolt.DB
	params    traverser.AggregateParams
	getSchema schemaUC.SchemaGetter
}

func New(db *bolt.DB, params traverser.AggregateParams,
	getSchema schemaUC.SchemaGetter) *Aggregator {
	return &Aggregator{
		db:        db,
		params:    params,
		getSchema: getSchema,
	}
}

func (a *Aggregator) Do(ctx context.Context) (*aggregation.Result, error) {
	if a.params.GroupBy != nil {
		return nil, fmt.Errorf("grouping not supported yet")
	}

	return newUnfilteredAggregator(a).Do(ctx)
}
