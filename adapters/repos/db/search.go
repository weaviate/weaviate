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
	"sort"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/refcache"
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

	res, err := idx.objectSearch(ctx, totalLimit, params.Filters,
		params.KeywordRanking, params.Sort, params.AdditionalProperties)
	if err != nil {
		return nil, errors.Wrapf(err, "object search at index %s", idx.ID())
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

	return db.enrichRefsForList(ctx,
		storobj.SearchResultsWithDists(db.getStoreObjects(res, params.Pagination), params.AdditionalProperties,
			db.getDists(dists, params.Pagination)), params.Properties, params.AdditionalProperties)
}

func extractDistanceFromParams(params traverser.GetParams) float32 {
	certainty := traverser.ExtractCertaintyFromParams(params)
	if certainty != 0 {
		return float32(1-certainty) * 2
	}

	return float32(traverser.ExtractDistanceFromParams(params))
}

func (db *DB) VectorSearch(ctx context.Context, vector []float32, offset, limit int,
	filters *filters.LocalFilter) ([]search.Result, error) {
	var found search.Results

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	var searchErrors []error
	totalLimit := offset + limit

	for _, index := range db.indices {
		wg.Add(1)
		go func(index *Index, wg *sync.WaitGroup) {
			defer wg.Done()

			objs, dist, err := index.objectVectorSearch(
				ctx, vector, 0, totalLimit, filters, nil, additional.Properties{})
			if err != nil {
				mutex.Lock()
				searchErrors = append(searchErrors, errors.Wrapf(err, "search index %s", index.ID()))
				mutex.Unlock()
			}

			mutex.Lock()
			found = append(found, hydrateObjectsIntoSearchResults(objs, dist)...)

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

	sort.Slice(found, func(i, j int) bool {
		return found[i].Dist < found[j].Dist
	})

	// not enriching by refs, as a vector search result cannot provide
	// SelectProperties
	return db.getSearchResults(found, offset, limit), nil
}

func hydrateObjectsIntoSearchResults(objs []*storobj.Object, dists []float32) search.Results {
	res := storobj.SearchResults(objs, additional.Properties{})
	for i := range res {
		res[i].Dist = dists[i]
		res[i].Certainty = 1 - dists[i]
	}
	return res
}

func (d *DB) ObjectSearch(ctx context.Context, offset, limit int,
	filters *filters.LocalFilter, sort []filters.Sort, additional additional.Properties) (search.Results, error) {
	return d.objectSearch(ctx, offset, limit, filters, sort, additional)
}

func (d *DB) objectSearch(ctx context.Context, offset, limit int,
	filters *filters.LocalFilter, sort []filters.Sort,
	additional additional.Properties) (search.Results, error) {
	var found []*storobj.Object

	if err := d.validateSort(sort); err != nil {
		return nil, errors.Wrap(err, "search")
	}

	totalLimit := offset + limit
	// TODO: Search in parallel, rather than sequentially or this will be
	// painfully slow on large schemas
	for _, index := range d.indices {
		// TODO support all additional props
		res, err := index.objectSearch(ctx, totalLimit, filters, nil, sort, additional)
		if err != nil {
			return nil, errors.Wrapf(err, "search index %s", index.ID())
		}

		found = append(found, res...)
		if len(found) >= totalLimit {
			// we are done
			break
		}
	}

	return d.getSearchResults(storobj.SearchResults(found, additional), offset, limit), nil
}

func (d *DB) validateSort(sort []filters.Sort) error {
	if len(sort) > 0 {
		var errorMsgs []string
		for _, index := range d.indices {
			err := filters.ValidateSort(d.schemaGetter.GetSchemaSkipAuth(),
				index.Config.ClassName, sort)
			if err != nil {
				errorMsg := errors.Wrapf(err, "search index %s", index.ID()).Error()
				errorMsgs = append(errorMsgs, errorMsg)
			}
		}
		if len(errorMsgs) > 0 {
			return errors.Errorf("%s", strings.Join(errorMsgs, ", "))
		}
	}
	return nil
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
	if !addl.ReferenceQuery && totalLimit > int(db.config.QueryMaximumResults) {
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
