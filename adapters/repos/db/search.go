//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/refcache"
	"github.com/semi-technologies/weaviate/adapters/repos/db/sorter"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (db *DB) Aggregate(ctx context.Context,
	params aggregation.Params) (*aggregation.Result, error) {
	idx := db.GetIndex(schema.ClassName(params.ClassName))
	if idx == nil {
		return nil, fmt.Errorf("tried to browse non-existing index for %s", params.ClassName)
	}

	return idx.aggregate(ctx, params)
}

func (db *DB) GetQueryMaximumResults() int {
	return int(db.config.QueryMaximumResults)
}

func (db *DB) ClassSearch(ctx context.Context,
	params traverser.GetParams) ([]search.Result, error) {
	idx := db.GetIndex(schema.ClassName(params.ClassName))
	if idx == nil {
		return nil, fmt.Errorf("tried to browse non-existing index for %s", params.ClassName)
	}

	if params.Pagination == nil {
		return nil, fmt.Errorf("invalid params, pagination object is nil")
	}

	totalLimit, err := db.getTotalLimit(params.Pagination, params.AdditionalProperties)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pagination params")
	}

	res, err := idx.objectSearch(ctx, totalLimit, params.Sort,
		params.Filters, params.KeywordRanking, params.AdditionalProperties)
	if err != nil {
		return nil, errors.Wrapf(err, "object search at index %s", idx.ID())
	}

	// QUESTION: should we sort here?
	res, _, err = db.sort(res, nil, totalLimit, params.Sort, false, false)
	if err != nil {
		return nil, err
	}

	return db.enrichRefsForList(ctx,
		storobj.SearchResults(db.getStoreObjects(res, params.Pagination), params.AdditionalProperties),
		params.Properties, params.AdditionalProperties)
}

func (db *DB) VectorClassSearch(ctx context.Context,
	params traverser.GetParams) ([]search.Result, error) {
	if params.SearchVector == nil {
		return db.ClassSearch(ctx, params)
	}

	totalLimit, err := db.getTotalLimit(params.Pagination, params.AdditionalProperties)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pagination params")
	}

	idx := db.GetIndex(schema.ClassName(params.ClassName))
	if idx == nil {
		return nil, fmt.Errorf("tried to browse non-existing index for %s", params.ClassName)
	}

	targetDist := extractDistanceFromParams(params)
	res, dists, err := idx.objectVectorSearch(ctx, params.SearchVector, targetDist,
		totalLimit, params.Filters, params.Sort, params.AdditionalProperties)
	if err != nil {
		return nil, errors.Wrapf(err, "object vector search at index %s", idx.ID())
	}

	if totalLimit < 0 {
		params.Pagination.Limit = len(res)
	}

	// QUESTION: should we sort here?
	res, dists, err = db.sort(res, dists, totalLimit, params.Sort, false, false)
	if err != nil {
		return nil, err
	}

	return db.enrichRefsForList(ctx,
		storobj.SearchResultsWithDists(db.getStoreObjects(res, params.Pagination), params.AdditionalProperties,
			db.getDists(dists, params.Pagination)), params.Properties, params.AdditionalProperties)
}

func extractDistanceFromParams(params traverser.GetParams) float32 {
	// we need to check these conditions first before calling
	// traverser.ExtractCertaintyFromParams, because it will
	// panic if these conditions are not met
	if params.NearVector == nil && params.NearObject == nil &&
		len(params.ModuleParams) == 0 {
		return 0
	}

	certainty := traverser.ExtractCertaintyFromParams(params)
	return float32(1-certainty) * 2
}

func (db *DB) VectorSearch(ctx context.Context, vector []float32, offset, limit int,
	filters *filters.LocalFilter) ([]search.Result, error) {
	var found search.Results

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	var searchErrors []error
	totalLimit := offset + limit
	emptyAdditional := additional.Properties{
		// TODO: the fact that we need the vector for resorting shows that something
		// is not ideal here. We already get distances from the vector search, why not
		// pass them along and use them for sorting?
		Vector: true,
	}
	for _, index := range db.indices {
		wg.Add(1)
		go func(index *Index, wg *sync.WaitGroup) {
			defer wg.Done()

			res, _, err := index.objectVectorSearch(
				ctx, vector, 0, totalLimit, filters, nil, emptyAdditional)
			if err != nil {
				mutex.Lock()
				searchErrors = append(searchErrors, errors.Wrapf(err, "search index %s", index.ID()))
				mutex.Unlock()
			}

			mutex.Lock()
			found = append(found, storobj.SearchResults(res, emptyAdditional)...)
			mutex.Unlock()
		}(index, wg)
	}

	wg.Wait()

	if len(searchErrors) > 0 {
		var msg strings.Builder
		for i, err := range searchErrors {
			if i != 0 {
				msg.WriteString(", ")
			}
			msg.WriteString(err.Error())
		}
		return nil, errors.New(msg.String())
	}

	// TODO: use dists
	found, err := found.SortByDistanceToVector(vector)
	if err != nil {
		return nil, errors.Wrapf(err, "re-sort when merging indices")
	}

	// not enriching by refs, as a vector search result cannot provide
	// SelectProperties
	return db.getSearchResults(found, offset, limit), nil
}

func (d *DB) ObjectSearch(ctx context.Context, offset, limit int, sort []filters.Sort,
	filters *filters.LocalFilter, additional additional.Properties) (search.Results, error) {
	return d.objectSearch(ctx, offset, limit, sort, filters, additional)
}

func (d *DB) objectSearch(ctx context.Context, offset, limit int,
	sort []filters.Sort, filters *filters.LocalFilter,
	additional additional.Properties) (search.Results, error) {
	var found []*storobj.Object

	totalLimit := offset + limit
	// TODO: Search in parallel, rather than sequentially or this will be
	// painfully slow on large schemas
	for _, index := range d.indices {
		// TODO support all additional props
		res, err := index.objectSearch(ctx, totalLimit, sort, filters, nil, additional)
		if err != nil {
			return nil, errors.Wrapf(err, "search index %s", index.ID())
		}

		found = append(found, res...)
		if len(found) >= totalLimit {
			// we are done
			break
		}
	}

	// QUESTION: sort all results or just add already sorted class objects
	found, _, err := d.sort(found, nil, limit, sort, false, false)
	if err != nil {
		return nil, err
	}

	return d.getSearchResults(storobj.SearchResults(found, additional), offset, limit), nil
}

func (d *DB) enrichRefsForList(ctx context.Context, objs search.Results,
	props search.SelectProperties, additional additional.Properties) (search.Results, error) {
	res, err := refcache.NewResolver(refcache.NewCacher(d, d.logger)).
		Do(ctx, objs, props, additional)
	if err != nil {
		return nil, errors.Wrap(err, "resolve cross-refs")
	}

	return res, nil
}

func (db *DB) getTotalLimit(pagination *filters.Pagination, addl additional.Properties) (int, error) {
	if pagination.Limit == filters.LimitFlagSearchByDist {
		return filters.LimitFlagSearchByDist, nil
	}

	totalLimit := pagination.Offset + db.getLimit(pagination.Limit)
	if totalLimit > int(db.config.QueryMaximumResults) {
		return 0, errors.New("query maximum results exceeded")
	}
	return totalLimit, nil
}

func (d *DB) getSearchResults(found search.Results, paramOffset, paramLimit int) search.Results {
	offset, limit := d.getOffsetLimit(len(found), paramOffset, paramLimit)
	if offset == 0 && limit == 0 {
		return nil
	}
	return found[offset:limit]
}

func (d *DB) getStoreObjects(res []*storobj.Object, pagination *filters.Pagination) []*storobj.Object {
	offset, limit := d.getOffsetLimit(len(res), pagination.Offset, pagination.Limit)
	if offset == 0 && limit == 0 {
		return nil
	}
	return res[offset:limit]
}

func (d *DB) getDists(dists []float32, pagination *filters.Pagination) []float32 {
	offset, limit := d.getOffsetLimit(len(dists), pagination.Offset, pagination.Limit)
	if offset == 0 && limit == 0 {
		return nil
	}
	return dists[offset:limit]
}

func (d *DB) getOffsetLimit(arraySize int, offset, limit int) (int, int) {
	totalLimit := offset + d.getLimit(limit)
	if arraySize > totalLimit {
		return offset, totalLimit
	} else if arraySize > offset {
		return offset, arraySize
	}
	return 0, 0
}

func (db *DB) getLimit(limit int) int {
	if limit == filters.LimitFlagNotSet {
		return int(db.config.QueryLimit)
	}
	return limit
}

func (d *DB) sort(objects []*storobj.Object, distances []float32,
	limit int, sort []filters.Sort, keywordRanking, sortByDistance bool) ([]*storobj.Object, []float32, error) {
	return sorter.New(d.schemaGetter.GetSchemaSkipAuth()).
		Sort(objects, distances, limit, sort, keywordRanking, sortByDistance)
}
