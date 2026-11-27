//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	stderrors "errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/refcache"
	"github.com/weaviate/weaviate/adapters/repos/db/sorter"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/queryadmission"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/traverser"
)

func (db *DB) Aggregate(ctx context.Context,
	params aggregation.Params,
	modules *modules.Provider,
) (*aggregation.Result, error) {
	start := time.Now()
	defer func() {
		took := time.Since(start)
		db.logger.WithFields(logrus.Fields{
			"action": "aggregate_query_completed",
			"took":   took,
			"params": params,
		}).Debugf("aggregate query completed in %s", took)
	}()

	idx, err := db.indexForRead(ctx, params.ClassName)
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, fmt.Errorf("tried to browse non-existing index for %s", params.ClassName)
	}

	return idx.aggregate(ctx, nil, params, modules)
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
	start := time.Now()
	defer func() {
		took := time.Since(start)
		db.logger.WithFields(logrus.Fields{
			"action": "sparse_object_search_completed",
			"took":   took,
			"params": params,
		}).Debugf("sparse object search query completed in %s", took)
	}()

	if params.AdditionalProperties.QueryProfile {
		ctx = helpers.InitQueryProfileCollector(ctx)
	}

	idx, err := db.indexForRead(ctx, schema.ClassName(params.ClassName))
	if err != nil {
		return nil, nil, err
	}
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

	res, scores, err := idx.objectSearch(ctx, totalLimit,
		params.Filters, params.KeywordRanking, params.Sort, params.Cursor,
		params.AdditionalProperties, params.ReplicationProperties, tenant, params.Pagination.Autocut, params.Properties.GetPropertyNames())
	if err != nil {
		return nil, nil, errors.Wrapf(err, "object search at index %s", idx.ID())
	}

	return res, scores, nil
}

func (db *DB) Search(ctx context.Context, params dto.GetParams) ([]search.Result, error) {
	start := time.Now()
	defer func() {
		took := time.Since(start)
		db.logger.WithFields(logrus.Fields{
			"action": "search_completed",
			"took":   took,
			"params": params,
		}).Debugf("search query completed in %s", took)
	}()

	if params.Pagination == nil {
		return nil, fmt.Errorf("invalid params, pagination object is nil")
	}

	if params.AdditionalProperties.QueryProfile {
		ctx = helpers.InitQueryProfileCollector(ctx)
	}

	res, scores, err := db.SparseObjectSearch(ctx, params)
	if err != nil {
		return nil, err
	}

	res, scores = db.getStoreObjectsWithScores(res, scores, params.Pagination)
	results, err := db.ResolveReferences(ctx,
		storobj.SearchResultsWithScore(res, scores, params.AdditionalProperties, params.Tenant),
		params.Properties, params.GroupBy, params.AdditionalProperties, params.Tenant)
	if err != nil {
		return nil, err
	}
	if params.AdditionalProperties.QueryProfile {
		results = helpers.AttachQueryProfileToResults(ctx, results)
	}
	return results, nil
}

func (db *DB) VectorSearch(ctx context.Context,
	params dto.GetParams, targetVectors []string, searchVectors []models.Vector,
) ([]search.Result, error) {
	start := time.Now()
	defer func() {
		took := time.Since(start)
		db.logger.WithFields(logrus.Fields{
			"action":        "vector_search_completed",
			"took":          took,
			"params":        params,
			"targetVectors": targetVectors,
		}).Debugf("vector search query completed in %s", took)
	}()
	if len(searchVectors) == 0 || len(searchVectors) == 1 && isEmptyVector(searchVectors[0]) {
		results, err := db.Search(ctx, params)
		return results, err
	}

	if params.AdditionalProperties.QueryProfile {
		ctx = helpers.InitQueryProfileCollector(ctx)
	}

	totalLimit, err := db.getTotalLimit(params.Pagination, params.AdditionalProperties)
	if err != nil {
		return nil, fmt.Errorf("invalid pagination params: %w", err)
	}

	idx, err := db.indexForRead(ctx, schema.ClassName(params.ClassName))
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, fmt.Errorf("tried to browse non-existing index for %s", params.ClassName)
	}

	targetDist := extractDistanceFromParams(params)
	res, dists, err := idx.objectVectorSearch(ctx, searchVectors, targetVectors,
		targetDist, totalLimit, params.Filters, params.Sort, params.GroupBy,
		params.AdditionalProperties, params.ReplicationProperties, params.Tenant, params.TargetVectorCombination, params.Properties.GetPropertyNames())
	if err != nil {
		return nil, errors.Wrapf(err, "object vector search at index %s", idx.ID())
	}

	if totalLimit < 0 {
		params.Pagination.Limit = len(res)
	}

	results, err := db.ResolveReferences(ctx,
		storobj.SearchResultsWithDists(db.getStoreObjects(res, params.Pagination),
			params.AdditionalProperties, db.getDists(dists, params.Pagination)),
		params.Properties, params.GroupBy, params.AdditionalProperties, params.Tenant)
	if err != nil {
		return nil, err
	}
	if params.AdditionalProperties.QueryProfile {
		results = helpers.AttachQueryProfileToResults(ctx, results)
	}
	return results, nil
}

func isEmptyVector(searchVector models.Vector) bool {
	if isVectorEmpty, err := dto.IsVectorEmpty(searchVector); err == nil {
		return isVectorEmpty
	}
	return false
}

func extractDistanceFromParams(params dto.GetParams) float32 {
	certainty := traverser.ExtractCertaintyFromParams(params)
	if certainty != 0 {
		return float32(additional.CertaintyToDist(certainty))
	}

	dist, _ := traverser.ExtractDistanceFromParams(params)
	return float32(dist)
}

func (db *DB) CrossClassVectorSearch(ctx context.Context, vector models.Vector, targetVector string, offset, limit int,
	filters *filters.LocalFilter,
) ([]search.Result, error) {
	start := time.Now()
	defer func() {
		took := time.Since(start)
		db.logger.WithFields(logrus.Fields{
			"action":       "cross_class_vector_search_completed",
			"took":         took,
			"targetVector": targetVector,
		}).Debugf("cross class vector search query completed in %s", took)
	}()
	var found search.Results

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	var searchErrors []error
	totalLimit := offset + limit

	db.indexLock.RLock()
	for _, index := range db.indices {
		wg.Add(1)
		index := index
		f := func() {
			defer wg.Done()

			objs, dist, err := index.objectVectorSearch(ctx, []models.Vector{vector}, []string{targetVector},
				0, totalLimit, filters, nil, nil,
				additional.Properties{}, nil, "", nil, nil)
			if err != nil {
				mutex.Lock()
				searchErrors = append(searchErrors, errors.Wrapf(err, "search index %s", index.ID()))
				mutex.Unlock()
			}

			mutex.Lock()
			found = append(found, storobj.SearchResultsWithDists(objs, additional.Properties{}, dist)...)
			mutex.Unlock()
		}
		enterrors.GoWrapper(f, index.logger)
	}
	db.indexLock.RUnlock()

	wg.Wait()

	if len(searchErrors) > 0 {
		// Join (not stringify) so errors.Is/As still traverse the set, e.g.
		// an admission shed keeps its ErrOverloaded identity.
		return nil, stderrors.Join(searchErrors...)
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
	start := time.Now()
	defer func() {
		took := time.Since(start)
		db.logger.WithFields(logrus.Fields{
			"action": "query_completed",
			"took":   took,
			"params": q,
		}).Debugf("query completed in %s", took)
	}()
	totalLimit := q.Offset + q.Limit
	if q.Limit == 0 || totalLimit == 0 {
		return nil, nil
	}
	if len(q.Sort) > 0 {
		if err := filters.ValidateSort(db.schemaGetter.ReadOnlyClass, schema.ClassName(q.Class), q.Sort); err != nil {
			return nil, &objects.Error{Msg: "sorting", Code: objects.StatusBadRequest, Err: err}
		}
	}
	idx, err := db.indexForRead(ctx, schema.ClassName(q.Class))
	if err != nil {
		return nil, &objects.Error{Msg: "search index " + q.Class, Code: objects.StatusInternalServerError, Err: err}
	}
	if idx == nil {
		return nil, &objects.Error{Msg: "class not found " + q.Class, Code: objects.StatusNotFound}
	}
	if q.Cursor != nil {
		if err := filters.ValidateCursor(schema.ClassName(q.Class), q.Cursor, q.Offset, q.Filters, q.Sort); err != nil {
			return nil, &objects.Error{Msg: "cursor api: invalid 'after' parameter", Code: objects.StatusBadRequest, Err: err}
		}
	}
	res, _, err := idx.objectSearch(ctx, totalLimit, q.Filters,
		nil, q.Sort, q.Cursor, q.Additional, nil, q.Tenant, 0, nil)
	if err != nil {
		switch {
		case errors.As(err, &objects.ErrMultiTenancy{}):
			return nil, &objects.Error{Msg: "search index " + idx.ID(), Code: objects.StatusUnprocessableEntity, Err: err}
		case errors.Is(err, queryadmission.ErrOverloaded):
			// Admission shed: surface as 429, not a generic 500.
			return nil, &objects.Error{Msg: "search index " + idx.ID(), Code: objects.StatusTooManyRequests, Err: err}
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
	start := time.Now()
	defer func() {
		took := time.Since(start)
		db.logger.WithFields(logrus.Fields{
			"action":  "object_search_completed",
			"took":    took,
			"offset":  offset,
			"limit":   limit,
			"filters": filters.LogSummary(),
			"sort":    sort,
		}).Debugf("object search completed in %s", took)
	}()
	return db.objectSearch(ctx, offset, limit, filters, sort, additional, tenant)
}

func (db *DB) objectSearch(ctx context.Context, offset, limit int,
	filters *filters.LocalFilter, sort []filters.Sort,
	additional additional.Properties, tenant string,
) (search.Results, error) {
	if offset < 0 || limit < 0 {
		return nil, objects.NewErrInvalidUserInput("offset and limit must not be negative")
	}
	if limit == 0 {
		return nil, ctx.Err() // a limit of 0 means "all" to the sorters
	}
	totalLimit := offset + limit
	listed := listableCollections(db.copyIndices(), tenant)
	if err := validateSort(listed, sort); err != nil {
		return nil, errors.Wrap(err, "search")
	}

	var found []*storobj.Object
	if len(sort) > 0 {
		classes := make(map[string]*models.Class, len(listed))
		for _, c := range listed {
			classes[c.class.Class] = c.class
		}
		merger := sorter.NewObjectsSorter(func(name string) *models.Class { return classes[name] })
		// any collection can hold the global top, so search them all; merging per
		// batch holds one batch beyond the page, and the stable merge of results
		// fed in name order breaks ties by collection
		for start := 0; start < len(listed); start += listSortedConcurrency {
			end := min(start+listSortedConcurrency, len(listed))
			res, errs := db.searchCollections(ctx, listed[start:end], totalLimit, filters, sort, additional, tenant)
			for i := range res {
				if errs[i] != nil {
					return nil, errs[i]
				}
				found = append(found, res[i]...)
			}
			var err error
			if found, _, err = merger.Sort(found, nil, totalLimit, sort); err != nil {
				return nil, errors.Wrap(err, "sort")
			}
		}
	} else {
		// name order pages stably; growing batches keep a full first collection to one search
		for start, batch := 0, 1; start < len(listed) && len(found) < totalLimit; batch = min(2*batch, listBatchLimit) {
			end := min(start+batch, len(listed))
			res, errs := db.searchCollections(ctx, listed[start:end], totalLimit-len(found), filters, nil, additional, tenant)
			for i := range res {
				if len(found) >= totalLimit {
					break // a sequential walk would not have reached the rest
				}
				if errs[i] != nil {
					return nil, errs[i]
				}
				found = append(found, res[i]...)
			}
			start = end
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err // a search cut short by cancellation can leave even a full page wrong
	}

	return db.getSearchResults(storobj.SearchResults(found, additional, tenant), offset, limit), nil
}

type listedCollection struct {
	index *Index
	class *models.Class
}

const (
	// listSortedConcurrency bounds a sorted list to twice the memory of searching
	// one collection at a time: each search holds shards × (offset+limit) objects.
	listSortedConcurrency = 2
	// listBatchLimit caps an unsorted batch; each search in it fetches only what the page still needs.
	listBatchLimit = 4
)

// listableCollections returns the collections still in the schema that the
// request's tenant reaches, by name.
func listableCollections(indices map[string]*Index, tenant string) []listedCollection {
	listed := make([]listedCollection, 0, len(indices))
	for _, index := range indices {
		// gone from the schema: its drop is underway
		class := index.getSchema.ReadOnlyClass(index.Config.ClassName.String())
		if class == nil || !reachedBy(index, tenant) {
			continue
		}
		listed = append(listed, listedCollection{index: index, class: class})
	}
	slices.SortFunc(listed, func(a, b listedCollection) int { return strings.Compare(a.class.Class, b.class.Class) })
	return listed
}

// searchCollections searches listed in parallel; results and errors line up with it.
func (db *DB) searchCollections(ctx context.Context, listed []listedCollection, limit int,
	filters *filters.LocalFilter, sort []filters.Sort, additional additional.Properties, tenant string,
) ([][]*storobj.Object, []error) {
	res := make([][]*storobj.Object, len(listed))
	errs := make([]error, len(listed))
	eg := enterrors.NewErrorGroupWrapper(db.logger)
	eg.SetLimit(_NUMCPU)
	for i, c := range listed {
		eg.Go(func() error {
			res[i], errs[i] = db.searchCollection(ctx, c, limit, filters, sort, additional, tenant)
			return nil
		}, c.class.Class)
	}
	_ = eg.Wait() // every goroutine reports through errs
	return res, errs
}

func (db *DB) searchCollection(ctx context.Context, c listedCollection, limit int,
	filters *filters.LocalFilter, sort []filters.Sort, additional additional.Properties, tenant string,
) ([]*storobj.Object, error) {
	// TODO support all additional props
	propsNames := make([]string, len(c.class.Properties))
	for i, prop := range c.class.Properties {
		propsNames[i] = prop.Name
	}
	var res []*storobj.Object
	// recovered here, so a panic from a collection being dropped is left out too
	err := enterrors.RunRecovered(db.logger, func() error {
		var err error
		res, _, err = c.index.objectSearch(ctx, limit, filters, nil, sort, nil, additional, nil, tenant, 0, propsNames)
		return err
	})
	if err == nil {
		return res, nil
	}
	if outsideRequestTenancy(err) || (ctx.Err() == nil && c.index.beingDropped()) {
		return nil, nil
	}
	return nil, errors.Wrapf(err, "search index %s", c.index.ID())
}

// reachedBy decides from the local schema whether a request with tenant reads
// the collection: the tenant validator asks the RAFT leader, and can activate.
func reachedBy(index *Index, tenant string) bool {
	if !index.partitioningEnabled || tenant == "" {
		return !index.partitioningEnabled && tenant == ""
	}
	held := false
	_ = index.schemaReader.Read(index.Config.ClassName.String(), false, func(_ *models.Class, state *sharding.State) error {
		if state != nil {
			_, held = state.Physical[tenant]
		}
		return nil
	})
	return held
}

// outsideRequestTenancy reports a collection the request's tenant setting does not reach.
func outsideRequestTenancy(err error) bool {
	if !errors.As(err, &objects.ErrMultiTenancy{}) {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "has multi-tenancy enabled, but request was without tenant") ||
		strings.Contains(msg, "has multi-tenancy disabled, but request was with tenant") ||
		strings.Contains(msg, "no tenant found with key") ||
		errors.Is(err, enterrors.ErrTenantNotFound)
}

// ResolveReferences takes a list of search results and enriches them
// with any referenced objects
func (db *DB) ResolveReferences(ctx context.Context, objs search.Results,
	props search.SelectProperties, groupBy *searchparams.GroupBy,
	addl additional.Properties, tenant string,
) (search.Results, error) {
	start := time.Now()
	defer func() {
		took := time.Since(start)
		db.logger.WithFields(logrus.Fields{
			"action": "resolve_references_completed",
			"took":   took,
			"len":    len(objs),
			"props":  props,
		}).Debugf("resolve references completed in %s", took)
	}()
	if addl.NoProps {
		// If we have no props, there also can't be refs among them, so we can skip
		// the refcache resolver
		return objs, nil
	}

	if groupBy != nil {
		res, err := refcache.NewResolverWithGroup(refcache.NewCacher(db, db.logger, tenant), groupBy.Properties).
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

// sortedAs is the data type the merge compares by: int and number both as float64.
func sortedAs(dataType []string) string {
	t := strings.Join(dataType, ",")
	switch schema.DataType(t) {
	case schema.DataTypeInt:
		return string(schema.DataTypeNumber)
	case schema.DataTypeIntArray:
		return string(schema.DataTypeNumberArray)
	default:
		return t
	}
}

// validateSort also requires one data type per sorted property: merging compares values.
func validateSort(listed []listedCollection, sort []filters.Sort) error {
	if len(sort) == 0 {
		return nil
	}
	classes := make(map[string]*models.Class, len(listed))
	for _, c := range listed {
		classes[c.class.Class] = c.class
	}
	getClass := func(name string) *models.Class { return classes[name] }

	var errorMsgs []string
	for _, c := range listed {
		if err := filters.ValidateSort(getClass, c.index.Config.ClassName, sort); err != nil {
			errorMsgs = append(errorMsgs, errors.Wrapf(err, "search index %s", c.index.ID()).Error())
		}
	}
	if len(errorMsgs) > 0 {
		return objects.NewErrInvalidUserInput("%s", strings.Join(errorMsgs, ", "))
	}

	for _, clause := range sort {
		var dataType []string
		for _, c := range listed {
			prop, err := schema.GetPropertyByName(c.class, clause.Path[0])
			if err != nil {
				continue // an internal property, the same in every collection
			}
			if dataType == nil {
				dataType = prop.DataType
			} else if sortedAs(dataType) != sortedAs(prop.DataType) {
				return objects.NewErrInvalidUserInput("sort property %q has data type %v in one collection "+
					"and %v in another; set a class to sort by it", clause.Path[0], dataType, prop.DataType)
			}
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
		return 0, fmt.Errorf(
			"query maximum results exceeded: the total limit calculated from the provided offset '%d' and limit '%d' exceeds the configured value for QUERY_MAXIMUM_RESULTS '%d'. If you've supplied a negative offset or limit, this may be an underflow error",
			pagination.Offset, db.getLimit(pagination.Limit), db.config.QueryMaximumResults)
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

func (db *DB) getStoreObjectsWithScores(res []*storobj.Object, scores []float32, pagination *filters.Pagination) ([]*storobj.Object, []float32) {
	offset, limit := db.getOffsetLimit(len(res), pagination.Offset, pagination.Limit)
	if offset == 0 && limit == 0 {
		return nil, nil
	}
	res = res[offset:limit]
	// not all search results have scores
	if len(scores) == 0 {
		return res, scores
	}

	return res, scores[offset:limit]
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
