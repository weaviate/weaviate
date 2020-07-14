//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (db *DB) Aggregate(ctx context.Context, params traverser.AggregateParams) (*aggregation.Result, error) {
	return nil, fmt.Errorf("aggregations not supported (yet)")
}

func (db *DB) ClassSearch(ctx context.Context, params traverser.GetParams) ([]search.Result, error) {
	idx := db.GetIndex(params.Kind, schema.ClassName(params.ClassName))
	if idx == nil {
		return nil, fmt.Errorf("tried to browse non-existing index for %s/%s", params.Kind, params.ClassName)
	}

	res, err := idx.objectSearch(ctx, params.Pagination.Limit, params.Filters, false)
	if err != nil {
		return nil, errors.Wrapf(err, "object search at index %s", idx.ID())
	}

	// TODO: Inject uuid at expected field

	return storobj.SearchResults(res), nil
}
func (db *DB) VectorClassSearch(ctx context.Context, params traverser.GetParams) ([]search.Result, error) {
	return nil, fmt.Errorf("vector-based search not implemented (yet)")
}
func (db *DB) VectorSearch(ctx context.Context, vector []float32, limit int,
	filters *filters.LocalFilter) ([]search.Result, error) {
	return nil, fmt.Errorf("vector-based search not implemented (yet)")
}

func (d *DB) ThingSearch(ctx context.Context, limit int, filters *filters.LocalFilter,
	underscore traverser.UnderscoreProperties) (search.Results, error) {
	return d.objectSearch(ctx, kind.Thing, limit, filters, underscore)
}

func (d *DB) ActionSearch(ctx context.Context, limit int, filters *filters.LocalFilter,
	underscore traverser.UnderscoreProperties) (search.Results, error) {
	return d.objectSearch(ctx, kind.Action, limit, filters, underscore)
}

func (d *DB) objectSearch(ctx context.Context, kind kind.Kind, limit int, filters *filters.LocalFilter,
	underscore traverser.UnderscoreProperties) (search.Results, error) {

	var found search.Results

	// TODO: Search in parallel, rather than sequentially or this will be
	// painfully slow on large schemas
	for _, index := range d.indices {
		if index.Config.Kind != kind {
			continue
		}

		res, err := index.objectSearch(ctx, limit, filters, underscore.Classification) // TODO support all underscore props
		if err != nil {
			return nil, errors.Wrapf(err, "search index %s", index.ID())
		}

		found = append(found, storobj.SearchResults(res)...)
		if len(found) >= limit {
			// we are done
			break
		}
	}

	if len(found) > limit {
		found = found[:limit]
	}

	return found, nil
}
