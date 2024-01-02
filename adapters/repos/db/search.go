//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/refcache"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/traverser"
)

func (db *DB) Aggregate(ctx context.Context,
	params aggregation.Params,
) (*aggregation.Result, error) {
	idx := db.GetIndex(params.ClassName)
	if idx == nil {
		return nil, fmt.Errorf("tried to browse non-existing index for %s", params.ClassName)
	}

	return idx.aggregate(ctx, params)
}

func (db *DB) GetQueryMaximumResults() int {
	return int(db.config.QueryMaximumResults)
}

// SparseObjectSearch is used to perform an inverted index search on the db
//
// Earlier use cases required only []search.Result as a return value from the db, and the
// Class ClassSearch method fit this need. Later on, other use cases presented the need
// for the raw storage objects, such as hybrid search.
func (db *DB) SparseObjectSearch(ctx context.Context, params dto.GetParams) ([]*storobj.Object, []float32, error) {
	idx := db.GetIndex(schema.ClassName(params.ClassName))
	if idx == nil {
		return nil, nil, fmt.Errorf("tried to browse non-existing index for %s", params.ClassName)
	}

	if params.Pagination == nil {
		return nil, nil, fmt.Errorf("invalid params, pagination object is nil")
	}

	totalLimit, err := db.getTotalLimit(params.Pagination, params.AdditionalProperties)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "invalid pagination params")
	}

	// if this is reference search and tenant is given (as origin class is MT)
	// but searched class is non-MT, then skip tenant to pass validation
	tenant := params.Tenant
	if !idx.partitioningEnabled && params.IsRefOrigin {
		tenant = ""
	}

	res, dist, err := idx.objectSearch(ctx, totalLimit,
		params.Filters, params.KeywordRanking, params.Sort, params.Cursor,
		params.AdditionalProperties, params.ReplicationProperties, tenant, params.Pagination.Autocut)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "object search at index %s", idx.ID())
	}

	return res, dist, nil
}

func (db *DB) Search(ctx context.Context, params dto.GetParams) ([]search.Result, error) {
	if params.Pagination == nil {
		return nil, fmt.Errorf("invalid params, pagination object is nil")
	}

	res, _, err := db.SparseObjectSearch(ctx, params)
	if err != nil {
		return nil, err
	}

	return db.ResolveReferences(ctx,
		storobj.SearchResults(db.getStoreObjects(res, params.Pagination), params.AdditionalProperties, params.Tenant),
		params.Properties, params.GroupBy, params.AdditionalProperties, params.Tenant)
}

func (db *DB) VectorSearch(ctx context.Context,
	params dto.GetParams,
) ([]search.Result, error) {
	if params.SearchVector == nil {
		return db.Search(ctx, params)
	}

	totalLimit, err := db.getTotalLimit(params.Pagination, params.AdditionalProperties)
	if err != nil {
		return nil, fmt.Errorf("invalid pagination params: %w", err)
	}

	idx := db.GetIndex(schema.ClassName(params.ClassName))
	if idx == nil {
		return nil, fmt.Errorf("tried to browse non-existing index for %s", params.ClassName)
	}

	targetDist := extractDistanceFromParams(params)
	res, dists, err := idx.objectVectorSearch(ctx, params.SearchVector,
		targetDist, totalLimit, params.Filters, params.Sort, params.GroupBy,
		params.AdditionalProperties, params.ReplicationProperties, params.Tenant)
	if err != nil {
		return nil, errors.Wrapf(err, "object vector search at index %s", idx.ID())
	}

	if totalLimit < 0 {
		params.Pagination.Limit = len(res)
	}

	return db.ResolveReferences(ctx,
		storobj.SearchResultsWithDists(db.getStoreObjects(res, params.Pagination),
			params.AdditionalProperties, db.getDists(dists, params.Pagination)),
		params.Properties, params.GroupBy, params.AdditionalProperties, params.Tenant)
}

func extractDistanceFromParams(params dto.GetParams) float32 {
	certainty := traverser.ExtractCertaintyFromParams(params)
	if certainty != 0 {
		return float32(additional.CertaintyToDist(certainty))
	}

	dist, _ := traverser.ExtractDistanceFromParams(params)
	return float32(dist)
}

// DenseObjectSearch is used to perform a vector search on the db
//
// Earlier use cases required only []search.Result as a return value from the db, and the
// Class VectorSearch method fit this need. Later on, other use cases presented the need
// for the raw storage objects, such as hybrid search.
func (db *DB) DenseObjectSearch(ctx context.Context, class string, vector []float32,
	offset int, limit int, filters *filters.LocalFilter, addl additional.Properties,
	tenant string,
) ([]*storobj.Object, []float32, error) {
	totalLimit := offset + limit

	index := db.GetIndex(schema.ClassName(class))
	if index == nil {
		return nil, nil, fmt.Errorf("tried to browse non-existing index for %s", class)
	}

	// TODO: groupBy think of this
	objs, dist, err := index.objectVectorSearch(ctx, vector, 0,
		totalLimit, filters, nil, nil, addl, nil, tenant)
	if err != nil {
		return nil, nil, fmt.Errorf("search index %s: %w", index.ID(), err)
	}

	return objs, dist, nil
}

func (db *DB) CrossClassVectorSearch(ctx context.Context, vector []float32, offset, limit int,
	filters *filters.LocalFilter,
) ([]search.Result, error) {
	var found search.Results

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	var searchErrors []error
	totalLimit := offset + limit

	db.indexLock.RLock()
	for _, index := range db.indices {
		wg.Add(1)
		go func(index *Index, wg *sync.WaitGroup) {
			defer wg.Done()

			objs, dist, err := index.objectVectorSearch(ctx, vector,
				0, totalLimit, filters, nil, nil,
				additional.Properties{}, nil, "")
			if err != nil {
				mutex.Lock()
				searchErrors = append(searchErrors, errors.Wrapf(err, "search index %s", index.ID()))
				mutex.Unlock()
			}

			mutex.Lock()
			found = append(found, storobj.SearchResultsWithDists(objs, additional.Properties{}, dist)...)
			mutex.Unlock()
		}(index, wg)
	}
	db.indexLock.RUnlock()

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

// Query a specific class
func (db *DB) Query(ctx context.Context, q *objects.QueryInput) (search.Results, *objects.Error) {
	totalLimit := q.Offset + q.Limit
	if totalLimit == 0 {
		return nil, nil
	}
	if len(q.Sort) > 0 {
		scheme := db.schemaGetter.GetSchemaSkipAuth()
		if err := filters.ValidateSort(scheme, schema.ClassName(q.Class), q.Sort); err != nil {
			return nil, &objects.Error{Msg: "sorting", Code: objects.StatusBadRequest, Err: err}
		}
	}
	idx := db.GetIndex(schema.ClassName(q.Class))
	if idx == nil {
		return nil, &objects.Error{Msg: "class not found " + q.Class, Code: objects.StatusNotFound}
	}
	if q.Cursor != nil {
		if err := filters.ValidateCursor(schema.ClassName(q.Class), q.Cursor, q.Offset, q.Filters, q.Sort); err != nil {
			return nil, &objects.Error{Msg: "cursor api: invalid 'after' parameter", Code: objects.StatusBadRequest, Err: err}
		}
	}
	res, _, err := idx.objectSearch(ctx, totalLimit, q.Filters,
		nil, q.Sort, q.Cursor, q.Additional, nil, q.Tenant, 0)
	if err != nil {
		switch err.(type) {
		case objects.ErrMultiTenancy:
			return nil, &objects.Error{Msg: "search index " + idx.ID(), Code: objects.StatusUnprocessableEntity, Err: err}
		default:
			return nil, &objects.Error{Msg: "search index " + idx.ID(), Code: objects.StatusInternalServerError, Err: err}
		}
	}
	return db.getSearchResults(storobj.SearchResults(res, q.Additional, ""), q.Offset, q.Limit), nil
}

// ObjectSearch search each index.
// Deprecated by Query which searches a specific index
func (db *DB) ObjectSearch(ctx context.Context, offset, limit int,
	filters *filters.LocalFilter, sort []filters.Sort,
	additional additional.Properties, tenant string,
) (search.Results, error) {
	return db.objectSearch(ctx, offset, limit, filters, sort, additional, tenant)
}

func (db *DB) objectSearch(ctx context.Context, offset, limit int,
	filters *filters.LocalFilter, sort []filters.Sort,
	additional additional.Properties, tenant string,
) (search.Results, error) {
	var found []*storobj.Object

	if err := db.validateSort(sort); err != nil {
		return nil, errors.Wrap(err, "search")
	}

	totalLimit := offset + limit
	// TODO: Search in parallel, rather than sequentially or this will be
	// painfully slow on large schemas
	// wrapped in func to unlock mutex within defer
	if err := func() error {
		db.indexLock.RLock()
		defer db.indexLock.RUnlock()

		for _, index := range db.indices {
			// TODO support all additional props
			res, _, err := index.objectSearch(ctx, totalLimit,
				filters, nil, sort, nil, additional, nil, tenant, 0)
			if err != nil {
				// Multi tenancy specific errors
				if errors.As(err, &objects.ErrMultiTenancy{}) {
					// validation failed (either MT class without tenant or non-MT class with tenant)
					if strings.Contains(err.Error(), "has multi-tenancy enabled, but request was without tenant") ||
						strings.Contains(err.Error(), "has multi-tenancy disabled, but request was with tenant") {
						continue
					}
					// tenant not added to class
					if strings.Contains(err.Error(), "no tenant found with key") {
						continue
					}
					// tenant does belong to this class
					if errors.As(err, &errTenantNotFound) {
						continue // tenant does belong to this class
					}
				}
				return errors.Wrapf(err, "search index %s", index.ID())
			}

			found = append(found, res...)
			if len(found) >= totalLimit {
				// we are done
				break
			}
		}
		return nil
	}(); err != nil {
		return nil, err
	}

	return db.getSearchResults(storobj.SearchResults(found, additional, tenant), offset, limit), nil
}

// ResolveReferences takes a list of search results and enriches them
// with any referenced objects
func (db *DB) ResolveReferences(ctx context.Context, objs search.Results,
	props search.SelectProperties, groupBy *searchparams.GroupBy,
	addl additional.Properties, tenant string,
) (search.Results, error) {
	if addl.NoProps {
		// If we have no props, there also can't be refs among them, so we can skip
		// the refcache resolver
		return objs, nil
	}

	if groupBy != nil {
		res, err := refcache.NewResolverWithGroup(refcache.NewCacherWithGroup(db, db.logger, tenant)).
			Do(ctx, objs, props, addl)
		if err != nil {
			return nil, fmt.Errorf("resolve cross-refs: %w", err)
		}
		return res, nil
	}

	res, err := refcache.NewResolver(refcache.NewCacher(db, db.logger, tenant)).
		Do(ctx, objs, props, addl)
	if err != nil {
		return nil, fmt.Errorf("resolve cross-refs: %w", err)
	}

	return res, nil
}

func (db *DB) validateSort(sort []filters.Sort) error {
	if len(sort) > 0 {
		var errorMsgs []string
		// needs to happen before the index lock as they might deadlock each other
		schema := db.schemaGetter.GetSchemaSkipAuth()
		db.indexLock.RLock()
		for _, index := range db.indices {
			err := filters.ValidateSort(schema,
				index.Config.ClassName, sort)
			if err != nil {
				errorMsg := errors.Wrapf(err, "search index %s", index.ID()).Error()
				errorMsgs = append(errorMsgs, errorMsg)
			}
		}
		db.indexLock.RUnlock()
		if len(errorMsgs) > 0 {
			return errors.Errorf("%s", strings.Join(errorMsgs, ", "))
		}
	}
	return nil
}

func (db *DB) getTotalLimit(pagination *filters.Pagination, addl additional.Properties) (int, error) {
	if pagination.Limit == filters.LimitFlagSearchByDist {
		return filters.LimitFlagSearchByDist, nil
	}

	totalLimit := pagination.Offset + db.getLimit(pagination.Limit)
	if totalLimit == 0 {
		return 0, fmt.Errorf("invalid default limit: %v", db.getLimit(pagination.Limit))
	}
	if !addl.ReferenceQuery && totalLimit > int(db.config.QueryMaximumResults) {
		return 0, errors.New("query maximum results exceeded")
	}
	return totalLimit, nil
}

func (db *DB) getSearchResults(found search.Results, paramOffset, paramLimit int) search.Results {
	offset, limit := db.getOffsetLimit(len(found), paramOffset, paramLimit)
	if offset == 0 && limit == 0 {
		return nil
	}
	return found[offset:limit]
}

func (db *DB) getStoreObjects(res []*storobj.Object, pagination *filters.Pagination) []*storobj.Object {
	offset, limit := db.getOffsetLimit(len(res), pagination.Offset, pagination.Limit)
	if offset == 0 && limit == 0 {
		return nil
	}
	return res[offset:limit]
}

func (db *DB) getDists(dists []float32, pagination *filters.Pagination) []float32 {
	offset, limit := db.getOffsetLimit(len(dists), pagination.Offset, pagination.Limit)
	if offset == 0 && limit == 0 {
		return nil
	}
	return dists[offset:limit]
}

func (db *DB) getOffsetLimit(arraySize int, offset, limit int) (int, int) {
	totalLimit := offset + db.getLimit(limit)
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
