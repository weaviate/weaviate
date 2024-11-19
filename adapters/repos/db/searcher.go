package db

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/refcache"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

var ErrIndexNotFound = errors.New("searcher: index not found")

type Searcher struct {
	log logrus.FieldLogger
	// instead of creating on the fly?
	cacheref *refcache.Cacher
	config   SearchConfig

	// map["collection"] -> Index
	indices map[string]ReadIndex
}

func (s *Searcher) GetIndex(id string) (ReadIndex, error) {
	id = strings.ToLower(id)
	idx, ok := s.indices[id]
	if !ok {
		return nil, ErrIndexNotFound
	}
	return idx, nil
}

type ReadIndex interface {
	MultiObjectByID(ctx context.Context, query []multi.Identifier, tenant string) ([]*storobj.Object, error)
	ObjectVectorSearch(
		ctx context.Context,
		search [][]float32,
		target []string,
		distance float32,
		limit int,
		filters *filters.LocalFilter,
		sort []filters.Sort,
		groupBy *searchparams.GroupBy,
		additional additional.Properties,
		replProps *additional.ReplicationProperties,
		tenant string,
		targetCombination *dto.TargetCombination,
		properties []string,
	) ([]*storobj.Object, []float32, error)

	ObjectByID(
		ctx context.Context,
		id strfmt.UUID,
		props search.SelectProperties,
		addl additional.Properties,
		replProps *additional.ReplicationProperties,
		tenant string,
	) (*storobj.Object, error)

	ObjectSearch(
		ctx context.Context,
		limit int,
		filters *filters.LocalFilter,
		keywordRanking *searchparams.KeywordRanking,
		sort []filters.Sort,
		cusor *filters.Cursor,
		addl additional.Properties,
		repl *additional.ReplicationProperties,
		tenant string,
		autoCut int,
		properties []string,
	) ([]*storobj.Object, []float32, error)

	ID() string
}

type SearchConfig struct {
	QueryMaximumResults int
	QueryLimit          int
}

func (s *Searcher) MultiGet(ctx context.Context,
	query []multi.Identifier,
	additional additional.Properties,
	tenant string,
) ([]search.Result, error) {
	byIndex := map[string][]multi.Identifier{}
	for i, q := range query {
		// store original position to make assembly easier later
		q.OriginalPosition = i

		for className, index := range s.indices {
			if className != strings.ToLower(q.ClassName) {
				continue
			}

			queue := byIndex[index.ID()]
			queue = append(queue, q)
			byIndex[index.ID()] = queue
		}
	}

	out := make(search.Results, len(query))
	for indexID, queries := range byIndex {
		indexRes, err := s.indices[indexID].MultiObjectByID(ctx, queries, tenant)
		if err != nil {
			return nil, errors.Wrapf(err, "index %q", indexID)
		}

		for i, obj := range indexRes {
			if obj == nil {
				continue
			}
			res := obj.SearchResult(additional, tenant)
			out[queries[i].OriginalPosition] = *res
		}
	}

	return out, nil
}

func (s *Searcher) Search(ctx context.Context, params dto.GetParams) ([]search.Result, error) {
	if params.Pagination == nil {
		return nil, fmt.Errorf("invalid params, pagination object is nil")
	}

	res, scores, err := s.SparseObjectSearch(ctx, params)
	if err != nil {
		return nil, err
	}

	res, scores = s.getStoreObjectsWithScores(res, scores, params.Pagination)
	return s.ResolveReferences(ctx,
		storobj.SearchResultsWithScore(res, scores, params.AdditionalProperties, params.Tenant),
		params.Properties, params.GroupBy, params.AdditionalProperties, params.Tenant)
}

func (s *Searcher) VectorSearch(ctx context.Context, params dto.GetParams, targetVectors []string, searchVectors [][]float32) ([]search.Result, error) {
	if len(searchVectors) == 0 || len(searchVectors) == 1 && len(searchVectors[0]) == 0 {
		results, err := s.Search(ctx, params)
		return results, err
	}

	totalLimit, err := s.getTotalLimit(params.Pagination, params.AdditionalProperties)
	if err != nil {
		return nil, fmt.Errorf("invalid pagination params: %w", err)
	}

	idx, err := s.GetIndex(params.ClassName)
	if err != nil {
		return nil, fmt.Errorf("tried to browse non-existing index for %s", params.ClassName)
	}

	targetDist := extractDistanceFromParams(params)
	res, dists, err := idx.ObjectVectorSearch(ctx, searchVectors, targetVectors,
		targetDist, totalLimit, params.Filters, params.Sort, params.GroupBy,
		params.AdditionalProperties, params.ReplicationProperties, params.Tenant, params.TargetVectorCombination, params.Properties.GetPropertyNames())
	if err != nil {
		return nil, errors.Wrapf(err, "object vector search at index %s", idx.ID())
	}

	if totalLimit < 0 {
		params.Pagination.Limit = len(res)
	}

	return s.ResolveReferences(ctx,
		storobj.SearchResultsWithDists(s.getStoreObjects(res, params.Pagination),
			params.AdditionalProperties, s.getDists(dists, params.Pagination)),
		params.Properties, params.GroupBy, params.AdditionalProperties, params.Tenant)
}

func (s *Searcher) CrossClassVectorSearch(
	ctx context.Context,
	vector []float32,
	targetVector string,
	offset,
	limit int,
	filters *filters.LocalFilter,
) ([]search.Result, error) {
	var found search.Results

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	var searchErrors []error
	totalLimit := offset + limit

	for _, index := range s.indices {
		wg.Add(1)
		index := index
		f := func() {
			defer wg.Done()

			objs, dist, err := index.ObjectVectorSearch(ctx, [][]float32{vector}, []string{targetVector},
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
		enterrors.GoWrapper(f, s.log)
	}

	wg.Wait()

	if len(searchErrors) > 0 {
		var msg strings.Builder
		for i, err := range searchErrors {
			if i != 0 {
				msg.WriteString(", ")
			}
			errorMessage := fmt.Sprintf("%v", err)
			msg.WriteString(errorMessage)
		}
		return nil, errors.New(msg.String())
	}

	sort.Slice(found, func(i, j int) bool {
		return found[i].Dist < found[j].Dist
	})

	// not enriching by refs, as a vector search result cannot provide
	// SelectProperties
	return s.getSearchResults(found, offset, limit), nil
}

func (s *Searcher) Object(
	ctx context.Context,
	class string,
	id strfmt.UUID,
	props search.SelectProperties,
	addl additional.Properties,
	repl *additional.ReplicationProperties,
	tenant string,
) (*search.Result, error) {
	idx, err := s.GetIndex(class)
	if err != nil {
		return nil, ErrIndexNotFound
	}

	obj, err := idx.ObjectByID(ctx, id, props, addl, repl, tenant)
	if err != nil {
		switch err.(type) {
		case objects.ErrMultiTenancy:
			return nil, objects.NewErrMultiTenancy(fmt.Errorf("search index %s: %w", idx.ID(), err))
		default:
			return nil, errors.Wrapf(err, "search index %s", idx.ID())
		}
	}
	var r *search.Result
	if obj != nil {
		r = obj.SearchResult(addl, tenant)
	}
	if r == nil {
		return nil, nil
	}
	return s.enrichRefsForSingle(ctx, r, props, addl, tenant)
}

func (s *Searcher) ObjectsByID(
	ctx context.Context,
	id strfmt.UUID,
	props search.SelectProperties,
	additional additional.Properties,
	tenant string,
) (search.Results, error) {
	var result []*storobj.Object
	for _, index := range s.indices {
		res, err := index.ObjectByID(ctx, id, props, additional, nil, tenant)
		if err != nil {
			switch err.(type) {
			case objects.ErrMultiTenancy:
				return nil, objects.NewErrMultiTenancy(fmt.Errorf("search index %s: %w", index.ID(), err))
			default:
				return nil, errors.Wrapf(err, "search index %s", index.ID())
			}
		}

		if res != nil {
			result = append(result, res)
		}
	}

	if result == nil {
		return nil, nil
	}

	return s.ResolveReferences(ctx,
		storobj.SearchResults(result, additional, tenant), props, nil, additional, tenant)
}

func (s *Searcher) SparseObjectSearch(ctx context.Context, params dto.GetParams) ([]*storobj.Object, []float32, error) {
	idx, err := s.GetIndex(params.ClassName)
	if err != nil {
		return nil, nil, fmt.Errorf("tried to browse non-existing index for %s", params.ClassName)
	}

	if params.Pagination == nil {
		return nil, nil, fmt.Errorf("invalid params, pagination object is nil")
	}

	totalLimit, err := s.getTotalLimit(params.Pagination, params.AdditionalProperties)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "invalid pagination params")
	}

	tenant := params.Tenant

	res, scores, err := idx.ObjectSearch(ctx, totalLimit,
		params.Filters, params.KeywordRanking, params.Sort, params.Cursor,
		params.AdditionalProperties, params.ReplicationProperties, tenant, params.Pagination.Autocut, params.Properties.GetPropertyNames())
	if err != nil {
		return nil, nil, errors.Wrapf(err, "object search at index %s", idx.ID())
	}

	return res, scores, nil
}

func (s *Searcher) ResolveReferences(ctx context.Context,
	objs search.Results,
	props search.SelectProperties,
	groupBy *searchparams.GroupBy,
	addl additional.Properties,
	tenant string,
) (search.Results, error) {
	if addl.NoProps {
		// If we have no props, there also can't be refs among them, so we can skip
		// the refcache resolver
		return objs, nil
	}

	if groupBy != nil {
		res, err := refcache.NewResolverWithGroup(refcache.NewCacher(s, s.log, tenant), groupBy.Properties).
			Do(ctx, objs, props, addl)
		if err != nil {
			return nil, fmt.Errorf("resolve cross-refs: %w", err)
		}
		return res, nil
	}

	res, err := refcache.NewResolver(refcache.NewCacher(s, s.log, tenant)).
		Do(ctx, objs, props, addl)
	if err != nil {
		return nil, fmt.Errorf("resolve cross-refs: %w", err)
	}

	return res, nil
}

// helpers
func (s *Searcher) getTotalLimit(pagination *filters.Pagination, addl additional.Properties) (int, error) {
	if pagination.Limit == filters.LimitFlagSearchByDist {
		return filters.LimitFlagSearchByDist, nil
	}

	totalLimit := pagination.Offset + s.getLimit(pagination.Limit)
	if totalLimit == 0 {
		return 0, fmt.Errorf("invalid default limit: %v", s.getLimit(pagination.Limit))
	}
	if !addl.ReferenceQuery && totalLimit > int(s.config.QueryMaximumResults) {
		return 0, errors.New("query maximum results exceeded")
	}
	return totalLimit, nil
}

func (s *Searcher) getLimit(limit int) int {
	if limit == filters.LimitFlagNotSet {
		return int(s.config.QueryLimit)
	}
	return limit
}

func (s *Searcher) enrichRefsForSingle(ctx context.Context, obj *search.Result,
	props search.SelectProperties, additional additional.Properties, tenant string,
) (*search.Result, error) {
	res, err := refcache.NewResolver(refcache.NewCacher(s, s.log, tenant)).
		Do(ctx, []search.Result{*obj}, props, additional)
	if err != nil {
		return nil, errors.Wrap(err, "resolve cross-refs")
	}

	return &res[0], nil
}

func (s *Searcher) getSearchResults(found search.Results, paramOffset, paramLimit int) search.Results {
	offset, limit := s.getOffsetLimit(len(found), paramOffset, paramLimit)
	if offset == 0 && limit == 0 {
		return nil
	}
	return found[offset:limit]
}

func (s *Searcher) getOffsetLimit(arraySize int, offset, limit int) (int, int) {
	totalLimit := offset + s.getLimit(limit)
	if arraySize > totalLimit {
		return offset, totalLimit
	} else if arraySize > offset {
		return offset, arraySize
	}
	return 0, 0
}

func (s *Searcher) getStoreObjects(res []*storobj.Object, pagination *filters.Pagination) []*storobj.Object {
	offset, limit := s.getOffsetLimit(len(res), pagination.Offset, pagination.Limit)
	if offset == 0 && limit == 0 {
		return nil
	}
	return res[offset:limit]
}

func (s *Searcher) getDists(dists []float32, pagination *filters.Pagination) []float32 {
	offset, limit := s.getOffsetLimit(len(dists), pagination.Offset, pagination.Limit)
	if offset == 0 && limit == 0 {
		return nil
	}
	return dists[offset:limit]
}

func (s *Searcher) getStoreObjectsWithScores(res []*storobj.Object, scores []float32, pagination *filters.Pagination) ([]*storobj.Object, []float32) {
	offset, limit := s.getOffsetLimit(len(res), pagination.Offset, pagination.Limit)
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
