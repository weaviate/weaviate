//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
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

func (d *DB) ThingSearch(ctx context.Context, limit int, filters *filters.LocalFilter, meta bool) (search.Results, error) {
	return d.objectSearch(ctx, kind.Thing, limit, filters, meta)
}

func (d *DB) ActionSearch(ctx context.Context, limit int, filters *filters.LocalFilter, meta bool) (search.Results, error) {
	return d.objectSearch(ctx, kind.Action, limit, filters, meta)
}

func (d *DB) objectSearch(ctx context.Context, kind kind.Kind, limit int, filters *filters.LocalFilter,
	meta bool) (search.Results, error) {

	var found search.Results

	// TODO: Search in parallel, rather than sequentially or this will be
	// painfully slow on large schemas
	for _, index := range d.indices {
		if index.Config.Kind != kind {
			continue
		}

		res, err := index.objectSearch(ctx, limit, filters, meta)
		if err != nil {
			return nil, errors.Wrapf(err, "search index %s", index.ID())
		}

		found = append(found, objectsToSearchResults(res)...)
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
