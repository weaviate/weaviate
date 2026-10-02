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

package traverser

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/ttl"
	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/configvalidation"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/autocut"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/floatcomp"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/traverser/grouper"
)

var _NUMCPU = runtime.GOMAXPROCS(0)

// Explorer is a helper construct to perform vector-based searches. It does not
// contain monitoring or authorization checks. It should thus never be directly
// used by an API, but through a Traverser.
type Explorer struct {
	searcher          objectsSearcher
	logger            logrus.FieldLogger
	modulesProvider   ModulesProvider
	schemaGetter      local.ClassReader
	nearParamsVector  *nearParamsVector
	targetParamHelper *TargetVectorParamHelper
	metrics           explorerMetrics
	config            config.Config
}

type explorerMetrics interface {
	AddUsageDimensions(className, queryType, operation string, dims int)
}

type ModulesProvider interface {
	ValidateSearchParam(name string, value interface{}, className string) error
	CrossClassValidateSearchParam(name string, value interface{}) error
	IsTargetVectorMultiVector(className, targetVector string) (bool, error)
	VectorFromSearchParam(ctx context.Context, className, targetVector, tenant, param string, params interface{},
		findVectorFn modulecapabilities.FindVectorFn[[]float32]) ([]float32, error)
	MultiVectorFromSearchParam(ctx context.Context, className, targetVector, tenant, param string, params interface{},
		findVectorFn modulecapabilities.FindVectorFn[[][]float32]) ([][]float32, error)
	TargetsFromSearchParam(className string, params interface{}) ([]string, error)
	CrossClassVectorFromSearchParam(ctx context.Context, param string,
		params interface{}, findVectorFn modulecapabilities.FindVectorFn[[]float32]) ([]float32, string, error)
	MultiCrossClassVectorFromSearchParam(ctx context.Context, param string,
		params interface{}, findVectorFn modulecapabilities.FindVectorFn[[][]float32]) ([][]float32, string, error)
	GetExploreAdditionalExtend(ctx context.Context, in []search.Result,
		moduleParams map[string]interface{}, searchVector models.Vector,
		argumentModuleParams map[string]interface{}) ([]search.Result, error)
	ListExploreAdditionalExtend(ctx context.Context, in []search.Result,
		moduleParams map[string]interface{},
		argumentModuleParams map[string]interface{}) ([]search.Result, error)
	RerankFetchDepth(ctx context.Context, className string, pageEnd int) (int, error)
	VectorFromInput(ctx context.Context, className, input, targetVector string) ([]float32, error)
	MultiVectorFromInput(ctx context.Context, className, input, targetVector string) ([][]float32, error)
}

type objectsSearcher interface {
	hybridSearcher

	// GraphQL Get{} queries
	Search(ctx context.Context, params dto.GetParams) ([]search.Result, error)
	VectorSearch(ctx context.Context, params dto.GetParams, targetVectors []string, searchVectors []models.Vector) ([]search.Result, error)

	// GraphQL Explore{} queries
	CrossClassVectorSearch(ctx context.Context, vector models.Vector, targetVector string, offset, limit int,
		filters *filters.LocalFilter) ([]search.Result, error)

	// Near-params searcher
	Object(ctx context.Context, className string, id strfmt.UUID,
		props search.SelectProperties, additional additional.Properties,
		properties *additional.ReplicationProperties, tenant string) (*search.Result, error)
	ObjectsByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties, tenant, namespace string) (search.Results, error)
}

type hybridSearcher interface {
	SparseObjectSearch(ctx context.Context, params dto.GetParams) ([]*storobj.Object, []float32, error)
	ResolveReferences(ctx context.Context, objs search.Results, props search.SelectProperties,
		groupBy *searchparams.GroupBy, additional additional.Properties, tenant string) (search.Results, error)
	DiversifyResults(ctx context.Context, selection *searchparams.Selection,
		className, targetVector string, results []search.Result, relevanceFromDist bool) ([]search.Result, error)
}

// NewExplorer with search and connector repo
func NewExplorer(searcher objectsSearcher, logger logrus.FieldLogger, modulesProvider ModulesProvider, metrics explorerMetrics, conf config.Config) *Explorer {
	return &Explorer{
		searcher:          searcher,
		logger:            logger,
		modulesProvider:   modulesProvider,
		metrics:           metrics,
		schemaGetter:      nil, // schemaGetter is set later
		nearParamsVector:  newNearParamsVector(modulesProvider, searcher),
		targetParamHelper: NewTargetParamHelper(),
		config:            conf,
	}
}

func (e *Explorer) SetSchemaGetter(sg local.ClassReader) {
	e.schemaGetter = sg
}

// GetClass from search and connector repo
func (e *Explorer) GetClass(ctx context.Context,
	params dto.GetParams,
) ([]interface{}, error) {
	searchStartTime := time.Now()
	if params.Pagination == nil {
		params.Pagination = &filters.Pagination{
			Offset: 0,
			Limit:  int(e.config.QueryDefaults.LimitGraphQL),
		}
	}

	if err := e.validateSort(params.ClassName, params.Sort); err != nil {
		return nil, errors.Wrap(err, "invalid 'sort' parameter")
	}

	if err := e.validateCursor(params); err != nil {
		return nil, errors.Wrap(err, "cursor api: invalid 'after' parameter")
	}

	// When boost is set, overfetch to give boost room to reorder results.
	// The candidate pool size is controlled by boost.Depth (per-query) with
	// a default of QueryBoostDefaultDepth. Capped at QueryMaximumResults.
	// We also zero the offset so boost sees all top candidates from position 0;
	// the original offset/limit are stored on the Boost struct and applied
	// after boost re-sorts. Under MMR the fetch/overfetch is owned by the MMR path.
	mmrActive := params.Selection != nil && params.Selection.MMR != nil
	if params.Boost != nil && params.Boost.Weight > 0 && !mmrActive {
		params.Boost.OriginalOffset = params.Pagination.Offset
		params.Boost.OriginalLimit = params.Pagination.Limit

		overfetch := params.Boost.Depth
		if overfetch == 0 {
			overfetch = e.config.QueryBoostDefaultDepth
		}
		if overfetch > int(e.config.QueryMaximumResults) {
			overfetch = int(e.config.QueryMaximumResults)
		}
		// Need at least offset+limit candidates so the original page is reachable.
		minCandidates := params.Boost.OriginalOffset + params.Boost.OriginalLimit
		if overfetch < minCandidates {
			overfetch = minCandidates
		}
		pagination := *params.Pagination
		pagination.Limit = overfetch
		pagination.Offset = 0
		params.Pagination = &pagination
	}

	// A reranker that drops results needs more candidates than the page.
	// rerankPage is the page the client asked for, or nil without over-fetch.
	rerankPage, err := e.overfetchForRerank(ctx, &params, searchStartTime)
	if err != nil {
		return nil, err
	}

	if params.KeywordRanking != nil {
		res, err := e.getClassKeywordBased(ctx, params, rerankPage)
		if err != nil {
			return nil, err
		}
		return e.searchResultsToGetResponse(ctx, res, nil, params, searchStartTime)
	}

	if params.NearVector != nil || params.NearObject != nil || len(params.ModuleParams) > 0 {
		res, searchVector, err := e.getClassVectorSearch(ctx, params, rerankPage)
		if err != nil {
			return nil, err
		}
		return e.searchResultsToGetResponse(ctx, res, searchVector, params, searchStartTime)
	}

	// Hybrid and plain list searches go through getClassList, which handles
	// Group workarounds, ListExploreAdditionalExtend, and usage tracking.
	res, err := e.getClassList(ctx, params, rerankPage)
	if err != nil {
		return nil, err
	}
	return e.searchResultsToGetResponse(ctx, res, nil, params, searchStartTime)
}

// applyBoostIfNeeded applies boost post-scoring when boost conditions are
// present and weight > 0. For vector searches, distances are converted to
// scores first since vector search populates Dist but not Score.
// The original pagination (offset/limit) is read from the Boost struct.
func (e *Explorer) applyBoostIfNeeded(res []search.Result, boost *filters.Boost, isVectorSearch bool) []search.Result {
	if boost == nil || boost.Weight <= 0 || len(res) == 0 {
		return res
	}
	if isVectorSearch {
		distToScore(res)
	}
	return applyBoostScoring(res, boost)
}

// rerankPage is the page the client asked for, kept while the search fetches
// more candidates for a reranker that drops results.
type rerankPage struct {
	offset int
	limit  int
	// searchStartTime is what the TTL filter of the response compares against.
	searchStartTime time.Time
}

// overfetchForRerank widens the pagination to the depth the class's reranker
// asks for and returns the page the client requested, which extendResults
// applies after the rerank. The candidates reach at least the end of the
// page, so a page past the depth fetches offset+limit of them.
//
// It returns nil when the query is not over-fetched:
//   - boost and MMR fetch their own depth and cut to the page before the
//     reranker runs
//   - grouping changes what a result is, so a page of groups cannot be cut
//     from reranked objects
//   - autocut picks the candidates by the jumps in their scores, and more
//     candidates would move the jump it cuts at
//   - a search by distance has no page size
func (e *Explorer) overfetchForRerank(ctx context.Context, params *dto.GetParams, searchStartTime time.Time,
) (*rerankPage, error) {
	if _, ok := params.AdditionalProperties.ModuleParams[modulecomponents.AdditionalPropertyRerank]; !ok || e.modulesProvider == nil {
		return nil, nil
	}
	boostActive := params.Boost != nil && params.Boost.Weight > 0
	mmrActive := params.Selection != nil && params.Selection.MMR != nil
	if boostActive || mmrActive || params.GroupBy != nil || params.Group != nil {
		return nil, nil
	}
	if params.Pagination == nil || params.Pagination.Autocut > 0 || params.Pagination.Offset < 0 {
		return nil, nil
	}
	page := rerankPage{
		offset: params.Pagination.Offset, limit: params.Pagination.Limit, searchStartTime: searchStartTime,
	}
	if page.limit == filters.LimitFlagNotSet {
		// The same page a query without a reranker gets: the hybrid page
		// is cut at QueryHybridMaximumResults, the others at the default.
		page.limit = int(e.config.QueryDefaults.Limit)
		if params.HybridSearch != nil {
			page.limit = int(e.config.QueryHybridMaximumResults)
		}
	}
	if page.limit <= 0 {
		return nil, nil
	}
	// The store checks offset+limit against QueryMaximumResults. The
	// candidates start at offset 0, so the page is checked here instead.
	pageEnd := page.offset + page.limit
	if pageEnd < page.limit || pageEnd > int(e.config.QueryMaximumResults) {
		return nil, fmt.Errorf("query maximum results exceeded: the total limit calculated from the provided "+
			"offset '%d' and limit '%d' exceeds the configured value for QUERY_MAXIMUM_RESULTS '%d'",
			page.offset, page.limit, e.config.QueryMaximumResults)
	}

	depth, err := e.modulesProvider.RerankFetchDepth(ctx, params.ClassName, pageEnd)
	if err != nil {
		return nil, fmt.Errorf("explorer: get class: rerank fetch depth: %w", err)
	}
	if depth <= 0 {
		return nil, nil
	}

	overfetch := *params.Pagination
	overfetch.Limit = min(max(depth, pageEnd), int(e.config.QueryMaximumResults))
	overfetch.Offset = 0
	params.Pagination = &overfetch
	return &page, nil
}

// queryProfileProperty is the additional property that carries the query
// profile on the first result.
const queryProfileProperty = "queryProfile"

// extendResults runs the additional properties of the modules on the results.
// extend is the provider call of the search path.
//
// With a rerankPage the reranker runs alone on all candidates. The results
// that the response would drop anyway are removed, the page is cut, and only
// then the other properties run. They must work on what the client gets: a
// generative module would otherwise be called for candidates that are thrown
// away, and a grouped answer would be built from them.
func (e *Explorer) extendResults(ctx context.Context, res []search.Result, params dto.GetParams,
	searchVector models.Vector, page *rerankPage,
	extend func(res []search.Result, moduleParams map[string]interface{}) ([]search.Result, error),
) ([]search.Result, error) {
	moduleParams := params.AdditionalProperties.ModuleParams
	if page == nil {
		return extend(res, moduleParams)
	}
	const rerank = modulecomponents.AdditionalPropertyRerank

	var queryProfile interface{}
	if len(res) > 0 {
		queryProfile = res[0].AdditionalProperties[queryProfileProperty]
	}

	res, err := extend(res, map[string]interface{}{rerank: moduleParams[rerank]})
	if err != nil {
		return nil, err
	}
	kept := make([]search.Result, 0, len(res))
	for _, result := range res {
		keep, err := e.keptInResponse(params, result, searchVector, page.searchStartTime)
		if err != nil {
			return nil, err
		}
		if keep {
			kept = append(kept, result)
		}
	}
	res = paginate(kept, page.offset, page.limit)
	if len(res) == 0 {
		return res, nil
	}

	// The reranker may have moved or dropped the result the profile was on.
	if queryProfile != nil {
		for i := range res {
			delete(res[i].AdditionalProperties, queryProfileProperty)
		}
		if res[0].AdditionalProperties == nil {
			res[0].AdditionalProperties = models.AdditionalProperties{}
		}
		res[0].AdditionalProperties[queryProfileProperty] = queryProfile
	}

	rest := make(map[string]interface{}, len(moduleParams))
	for name, value := range moduleParams {
		if name != rerank {
			rest[name] = value
		}
	}
	if len(rest) == 0 {
		return res, nil
	}
	return extend(res, rest)
}

// keptInResponse reports whether the response will contain the result: the
// TTL, certainty and distance filters of the response remove the others.
func (e *Explorer) keptInResponse(params dto.GetParams, res search.Result, searchVector models.Vector,
	searchStartTime time.Time,
) (bool, error) {
	keep, err := e.keepObjectsWithTTL(params, res, searchStartTime)
	if err != nil {
		return false, fmt.Errorf("object ttl filtering: %w", err)
	}
	return keep && (searchVector == nil || withinCertaintyAndDistance(params, res)), nil
}

// withinCertaintyAndDistance reports whether the result of a vector search
// satisfies the certainty or distance the query asked for.
func withinCertaintyAndDistance(params dto.GetParams, res search.Result) bool {
	// Dist is between 0..2, we need to reduce to the user space of 0..1
	normalizedResultDist := res.Dist / 2

	certainty := ExtractCertaintyFromParams(params)
	if 1-normalizedResultDist < float32(certainty) && 1-normalizedResultDist >= 0 {
		// TODO: Clean this up. The >= check is so that this logic does not run
		// non-cosine distance.
		return false
	}

	if certainty == 0 {
		distance, withDistance := ExtractDistanceFromParams(params)
		if withDistance && (!floatcomp.InDelta(float64(res.Dist), distance, 1e-6) &&
			float64(res.Dist) > distance) {
			return false
		}
	}
	return true
}

func (e *Explorer) getClassKeywordBased(ctx context.Context, params dto.GetParams, page *rerankPage) ([]search.Result, error) {
	if params.NearVector != nil || params.NearObject != nil || len(params.ModuleParams) > 0 {
		return nil, errors.Errorf("conflict: both near<Media> and keyword-based (bm25) arguments present, choose one")
	}

	if len(params.KeywordRanking.Query) == 0 {
		return nil, errors.Errorf("keyword search (bm25) must have query set")
	}

	if len(params.AdditionalProperties.ModuleParams) > 0 {
		// if a module-specific additional prop is set, assume it needs the vector
		// present for backward-compatibility. This could be improved by actually
		// asking the module based on specific conditions
		params.AdditionalProperties.Vector = true
	}

	res, err := e.searcher.Search(ctx, params)
	if err != nil {
		var e inverted.MissingIndexError
		if errors.As(err, &e) {
			return nil, e
		}
		// %w preserves the admission shed's identity (ErrOverloaded) through wrapping.
		return nil, fmt.Errorf("explorer: get class: vector search: %w", err)
	}

	res = e.applyBoostIfNeeded(res, params.Boost, false)

	if e.modulesProvider != nil {
		res, err = e.extendResults(ctx, res, params, nil, page,
			func(res []search.Result, moduleParams map[string]interface{}) ([]search.Result, error) {
				return e.modulesProvider.GetExploreAdditionalExtend(ctx, res, moduleParams, nil, params.ModuleParams)
			})
		if err != nil {
			return nil, fmt.Errorf("explorer: get class: extend: %w", err)
		}
	}

	if params.GroupBy != nil {
		groupedResults, err := e.groupSearchResults(ctx, res, params.GroupBy)
		if err != nil {
			return nil, err
		}
		return groupedResults, nil
	}
	return res, nil
}

func (e *Explorer) getClassVectorSearch(ctx context.Context,
	params dto.GetParams, page *rerankPage,
) ([]search.Result, models.Vector, error) {
	targetVectors, err := e.targetFromParams(ctx, params)
	if err != nil {
		return nil, nil, fmt.Errorf("explorer: get class: vectorize params: %w", enterrors.NewErrQueryVectorization(err))
	}

	targetVectors, err = e.targetParamHelper.GetTargetVectorOrDefault(e.schemaGetter.ReadOnlyClass,
		params.ClassName, targetVectors)
	if err != nil {
		return nil, nil, fmt.Errorf("explorer: get class: validate target vector: %w", err)
	}

	// MMR is terminal and per-window: fetch deep enough to reach offset+limit, boost
	// re-ranks it, MMR diversifies the [offset:offset+limit] relevance window, then
	// returns its top MMR.Limit. offset advances by limit, so windows are disjoint.
	mmr := params.Selection != nil && params.Selection.MMR != nil
	var (
		mmrTargetVector     string
		mmrOffset, mmrLimit int
		mmrPool             int
		stripVector         string
		stripDefaultVector  bool
	)
	if mmr {
		if len(targetVectors) > 0 {
			mmrTargetVector = targetVectors[0]
		}
		mmrOffset = params.Pagination.Offset
		mmrLimit = int(params.Selection.MMR.Limit)
		mmrPool = params.Pagination.Limit

		pool := *params.Pagination
		pool.Offset = 0
		pool.Limit = e.mmrFetchDepth(params.Boost, mmrOffset+mmrPool)
		params.Pagination = &pool

		if mmrTargetVector == "" {
			stripDefaultVector = !params.AdditionalProperties.Vector
			params.AdditionalProperties.Vector = true
		} else if !slices.Contains(params.AdditionalProperties.Vectors, mmrTargetVector) {
			stripVector = mmrTargetVector
			params.AdditionalProperties.Vectors = withVectorTarget(params.AdditionalProperties.Vectors, mmrTargetVector)
		}
	}

	res, searchVectors, err := e.searchForTargets(ctx, params, targetVectors, nil, page)
	if err != nil {
		return nil, nil, errors.Wrap(err, "explorer: get class: concurrentTargetVectorSearch")
	}

	if mmr {
		// Diversify only the [offset:offset+limit] relevance window, then keep its top
		// MMR.Limit. The window already consumes the offset, so the page is the prefix.
		res = e.paginateResults(res, mmrOffset, mmrPool)
		relevanceFromDist := params.Boost == nil || params.Boost.Weight <= 0
		res, err = e.searcher.DiversifyResults(ctx, params.Selection, params.ClassName, mmrTargetVector, res, relevanceFromDist)
		if err != nil {
			return nil, nil, errors.Wrap(err, "explorer: get class: diversify results")
		}
		res = e.paginateResults(res, 0, mmrLimit)
		if stripDefaultVector || stripVector != "" {
			for i := range res {
				if stripDefaultVector {
					res[i].Vector = nil
				}
				if stripVector != "" {
					delete(res[i].Vectors, stripVector)
				}
			}
		}

		// Module extension (rerankers) runs after MMR — skipped in searchForTargets
		// under MMR — so rerankers re-sort the diversified page, matching the
		// hybrid path where ListExploreAdditionalExtend follows selection.
		if e.modulesProvider != nil {
			var searchVector models.Vector
			if len(searchVectors) > 0 {
				searchVector = searchVectors[0]
			}
			res, err = e.modulesProvider.GetExploreAdditionalExtend(ctx, res,
				params.AdditionalProperties.ModuleParams, searchVector, params.ModuleParams)
			if err != nil {
				return nil, nil, errors.Errorf("explorer: get class: extend: %v", err)
			}
		}
	}

	if len(searchVectors) > 0 {
		return res, searchVectors[0], nil
	}
	return res, []float32{}, nil
}

// mmrFetchDepth returns how deep to fetch before MMR. It must reach at least windowEnd
// (offset+limit) so the [offset:offset+limit] window is populated; when boost is active it
// fetches Boost.Depth deep (so boost re-ranks that deep), floored at windowEnd.
func (e *Explorer) mmrFetchDepth(boost *filters.Boost, windowEnd int) int {
	if boost == nil || boost.Weight <= 0 {
		return windowEnd
	}
	depth := boost.Depth
	if depth == 0 {
		depth = e.config.QueryBoostDefaultDepth
	}
	if depth > int(e.config.QueryMaximumResults) {
		depth = int(e.config.QueryMaximumResults)
	}
	if depth < windowEnd {
		depth = windowEnd
	}
	return depth
}

// paginateResults returns the [offset:offset+limit] window of res; limit <= 0
// means no upper bound.
func (e *Explorer) paginateResults(res []search.Result, offset, limit int) []search.Result {
	return paginate(res, offset, limit)
}

// paginate returns the window [offset, offset+limit) of res. A limit of 0 or
// less means no upper bound.
func paginate[T any](res []T, offset, limit int) []T {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(res) {
		return []T{}
	}
	if limit <= 0 {
		return res[offset:]
	}
	end := offset + limit
	if end > len(res) {
		end = len(res)
	}
	return res[offset:end]
}

// searchForTargets runs the vector search. page is the rerank page of an
// over-fetched query and nil for every other caller.
func (e *Explorer) searchForTargets(ctx context.Context, params dto.GetParams, targetVectors []string, searchVectorParams *searchparams.NearVector, page *rerankPage) ([]search.Result, []models.Vector, error) {
	var err error
	searchVectors := make([]models.Vector, len(targetVectors))
	eg := enterrors.NewErrorGroupWrapper(e.logger)
	eg.SetLimit(2 * _NUMCPU)
	for i := range targetVectors {
		i := i
		eg.Go(func() error {
			var searchVectorParam *searchparams.NearVector
			if params.NearVector != nil {
				searchVectorParam = params.NearVector
			} else if searchVectorParams != nil {
				searchVectorParam = searchVectorParams
			}

			vec, err := e.vectorFromParamsForTarget(ctx, searchVectorParam, params.NearObject, params.ModuleParams, params.ClassName, params.Tenant, targetVectors[i], i)
			if err != nil {
				return fmt.Errorf("explorer: get class: vectorize search vector: %w", enterrors.NewErrQueryVectorization(err))
			}
			searchVectors[i] = vec
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	if len(params.AdditionalProperties.ModuleParams) > 0 || params.Group != nil {
		// if a module-specific additional prop is set, assume it needs the vector
		// present for backward-compatibility. This could be improved by actually
		// asking the module based on specific conditions
		// if a group is set, vectors are needed
		params.AdditionalProperties.Vector = true
	}

	res, err := e.searcher.VectorSearch(ctx, params, targetVectors, searchVectors)
	if err != nil {
		// %w preserves the admission shed's identity (ErrOverloaded) through wrapping.
		return nil, nil, fmt.Errorf("explorer: get class: vector search: %w", err)
	}

	if params.Pagination.Autocut > 0 {
		scores := make([]float32, len(res))
		for i := range res {
			scores[i] = res[i].Dist
		}
		cutOff := autocut.Autocut(scores, params.Pagination.Autocut)
		res = res[:cutOff]
	}

	if params.Group != nil {
		grouped, err := grouper.New(e.logger).Group(res, params.Group.Strategy, params.Group.Force)
		if err != nil {
			return nil, nil, fmt.Errorf("grouper: %w", err)
		}

		res = grouped
	}

	// Apply boost before module extensions (rerankers) so the order is:
	// vector search → boost → reranker. Only for non-hybrid paths.
	if params.HybridSearch == nil {
		res = e.applyBoostIfNeeded(res, params.Boost, true)
	}

	// This operation cannot be performed with hybrid search.
	// In case of hybrid it needs to be done later with combined results from vector and keyword search.
	// Under MMR it runs after diversification (in getClassVectorSearch), so rerankers
	// re-sort the final page on both the vector and the hybrid path.
	mmrActive := params.Selection != nil && params.Selection.MMR != nil
	if e.modulesProvider != nil && params.HybridSearch == nil && !mmrActive {
		res, err = e.extendResults(ctx, res, params, searchVectors[0], page,
			func(res []search.Result, moduleParams map[string]interface{}) ([]search.Result, error) {
				return e.modulesProvider.GetExploreAdditionalExtend(ctx, res, moduleParams, searchVectors[0], params.ModuleParams)
			})
		if err != nil {
			return nil, nil, fmt.Errorf("explorer: get class: extend: %w", err)
		}
	}
	e.trackUsageGet(res, params)

	return res, searchVectors, nil
}

func MinInt(ints ...int) int {
	min := ints[0]
	for _, i := range ints {
		if i < min {
			min = i
		}
	}
	return min
}

func MaxInt(ints ...int) int {
	max := ints[0]
	for _, i := range ints {
		if i > max {
			max = i
		}
	}
	return max
}

func (e *Explorer) CalculateTotalLimit(pagination *filters.Pagination) (int, error) {
	if pagination == nil {
		return 0, fmt.Errorf("invalid params, pagination object is nil")
	}

	if pagination.Limit == -1 {
		return int(e.config.QueryDefaults.Limit + int64(pagination.Offset)), nil
	}

	totalLimit := pagination.Offset + pagination.Limit

	return MinInt(totalLimit, int(e.config.QueryMaximumResults)), nil
}

func (e *Explorer) getClassList(ctx context.Context,
	params dto.GetParams, page *rerankPage,
) ([]search.Result, error) {
	// we will modify the params because of the workaround outlined below,
	// however, we only want to track what the user actually set for the usage
	// metrics, not our own workaround, so here's a copy of the original user
	// input
	userSetAdditionalVector := params.AdditionalProperties.Vector

	// if both grouping and whereFilter/sort are present, the below
	// class search will eventually call storobj.FromBinaryOptional
	// to unmarshal the record. in this case, we must manually set
	// the vector addl prop to unmarshal the result vector into each
	// result payload. if we skip this step, the grouper will attempt
	// to compute the distance with a `nil` vector, resulting in NaN.
	// this was the cause of [github issue 1958]
	// (https://github.com/weaviate/weaviate/issues/1958)
	if params.Group != nil && (params.Filters != nil || params.Sort != nil) {
		params.AdditionalProperties.Vector = true
	}
	var res []search.Result
	var err error
	if params.HybridSearch != nil {
		res, err = e.Hybrid(ctx, params)
		if err != nil {
			return nil, err
		}
	} else {
		res, err = e.searcher.Search(ctx, params)
		if err != nil {
			var e inverted.MissingIndexError
			if errors.As(err, &e) {
				return nil, e
			}
			return nil, fmt.Errorf("explorer: list class: search: %w", err)
		}
	}

	if params.Group != nil {
		grouped, err := grouper.New(e.logger).Group(res, params.Group.Strategy, params.Group.Force)
		if err != nil {
			return nil, fmt.Errorf("grouper: %w", err)
		}

		res = grouped
	}

	// Under hybrid + MMR, boost already ran inside the selection fn before MMR.
	hybridMMR := params.HybridSearch != nil && params.Selection != nil && params.Selection.MMR != nil
	if !hybridMMR {
		res = e.applyBoostIfNeeded(res, params.Boost, false)
	}

	if e.modulesProvider != nil {
		res, err = e.extendResults(ctx, res, params, nil, page,
			func(res []search.Result, moduleParams map[string]interface{}) ([]search.Result, error) {
				return e.modulesProvider.ListExploreAdditionalExtend(ctx, res, moduleParams, params.ModuleParams)
			})
		if err != nil {
			return nil, fmt.Errorf("explorer: list class: extend: %w", err)
		}
	}

	if userSetAdditionalVector {
		e.trackUsageGetExplicitVector(res, params)
	}

	return res, nil
}

func (e *Explorer) searchResultsToGetResponse(ctx context.Context, input []search.Result, searchVector models.Vector, params dto.GetParams, searchStartTime time.Time) ([]interface{}, error) {
	results, err := e.searchResultsToGetResponseWithType(ctx, input, searchVector, params, searchStartTime)
	if err != nil {
		return nil, err
	}

	output := make([]interface{}, 0, len(results))
	if params.GroupBy != nil {
		for _, result := range results {
			wrapper := map[string]interface{}{}
			wrapper["_additional"] = result.AdditionalProperties
			output = append(output, wrapper)
		}
	} else {
		for _, result := range results {
			output = append(output, result.Schema)
		}
	}
	return output, nil
}

func (e *Explorer) searchResultsToGetResponseWithType(ctx context.Context, input []search.Result, searchVector models.Vector, params dto.GetParams, searchStartTime time.Time) ([]search.Result, error) {
	var output []search.Result
	replEnabled, err := e.replicationEnabled(params)
	if err != nil {
		return nil, fmt.Errorf("search results to get response: %w", err)
	}
	for _, res := range input {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		keep, err := e.keepObjectsWithTTL(params, res, searchStartTime)
		if err != nil {
			return nil, fmt.Errorf("object ttl filtering: %w", err)
		}
		if !keep {
			continue
		}

		additionalProperties := make(map[string]interface{})

		if res.AdditionalProperties != nil {
			for additionalProperty, value := range res.AdditionalProperties {
				if value != nil {
					additionalProperties[additionalProperty] = value
				}
			}
		}

		if searchVector != nil {
			if !withinCertaintyAndDistance(params, res) {
				continue
			}

			if params.AdditionalProperties.Certainty {
				targetVectors := e.targetParamHelper.GetTargetVectorsFromParams(params)
				class := e.schemaGetter.ReadOnlyClass(params.ClassName)
				if err := configvalidation.CheckCertaintyCompatibility(class, targetVectors); err != nil {
					return nil, fmt.Errorf("additional: %w for class: %v", err, params.ClassName)
				}

				additionalProperties["certainty"] = additional.DistToCertainty(float64(res.Dist))
			}

			if params.AdditionalProperties.Distance {
				additionalProperties["distance"] = res.Dist
			}
		}

		if params.AdditionalProperties.ID {
			additionalProperties["id"] = res.ID
		}

		if params.AdditionalProperties.Score {
			additionalProperties["score"] = res.Score
		}

		if params.AdditionalProperties.ExplainScore {
			additionalProperties["explainScore"] = res.ExplainScore
		}

		if params.AdditionalProperties.Vector {
			additionalProperties["vector"] = res.Vector
		}

		if len(params.AdditionalProperties.Vectors) > 0 {
			vectors := make(map[string]models.Vector)
			for _, targetVector := range params.AdditionalProperties.Vectors {
				vectors[targetVector] = res.Vectors[targetVector]
			}
			additionalProperties["vectors"] = vectors
		}

		if params.AdditionalProperties.CreationTimeUnix {
			additionalProperties["creationTimeUnix"] = res.Created
		}

		if params.AdditionalProperties.LastUpdateTimeUnix {
			additionalProperties["lastUpdateTimeUnix"] = res.Updated
		}

		if replEnabled {
			additionalProperties["isConsistent"] = res.IsConsistent
		}

		if len(additionalProperties) > 0 {
			if additionalProperties["group"] != nil {
				e.extractAdditionalPropertiesFromGroupRefs(additionalProperties["group"], params.GroupBy.Properties)
			}
			res.Schema.(map[string]interface{})["_additional"] = additionalProperties
		}

		e.extractAdditionalPropertiesFromRefs(res.Schema, params.Properties)

		output = append(output, res)
	}

	return output, nil
}

func (e *Explorer) extractAdditionalPropertiesFromGroupRefs(
	additionalGroup interface{},
	props search.SelectProperties,
) {
	if group, ok := additionalGroup.(*additional.Group); ok {
		if len(group.Hits) > 0 {
			for _, hit := range group.Hits {
				e.extractAdditionalPropertiesFromRefs(hit, props)
			}
		}
	}
}

func (e *Explorer) extractAdditionalPropertiesFromRefs(propertySchema interface{}, params search.SelectProperties) {
	for _, selectProp := range params {
		for _, refClass := range selectProp.Refs {
			propertySchemaMap, ok := propertySchema.(map[string]interface{})
			if ok {
				refProperty := propertySchemaMap[selectProp.Name]
				if refProperty != nil {
					e.extractAdditionalPropertiesFromRef(refProperty, refClass)
				}
			}
			if refClass.RefProperties != nil {
				propertySchemaMap, ok := propertySchema.(map[string]interface{})
				if ok {
					innerPropertySchema := propertySchemaMap[selectProp.Name]
					if innerPropertySchema != nil {
						innerRef, ok := innerPropertySchema.([]interface{})
						if ok {
							for _, props := range innerRef {
								innerRefSchema, ok := props.(search.LocalRef)
								if ok {
									e.extractAdditionalPropertiesFromRefs(innerRefSchema.Fields, refClass.RefProperties)
								}
							}
						}
					}
				}
			}
		}
	}
}

func (e *Explorer) extractAdditionalPropertiesFromRef(ref interface{},
	refClass search.SelectClass,
) {
	innerRefClass, ok := ref.([]interface{})
	if ok {
		for _, innerRefProp := range innerRefClass {
			innerRef, ok := innerRefProp.(search.LocalRef)
			if !ok {
				continue
			}
			if innerRef.Class == refClass.ClassName {
				additionalProperties := make(map[string]interface{})
				if refClass.AdditionalProperties.ID {
					additionalProperties["id"] = innerRef.Fields["id"]
				}
				if refClass.AdditionalProperties.Vector {
					additionalProperties["vector"] = innerRef.Fields["vector"]
				}
				if len(refClass.AdditionalProperties.Vectors) > 0 {
					additionalProperties["vectors"] = innerRef.Fields["vectors"]
				}
				if refClass.AdditionalProperties.CreationTimeUnix {
					additionalProperties["creationTimeUnix"] = innerRef.Fields["creationTimeUnix"]
				}
				if refClass.AdditionalProperties.LastUpdateTimeUnix {
					additionalProperties["lastUpdateTimeUnix"] = innerRef.Fields["lastUpdateTimeUnix"]
				}
				if len(additionalProperties) > 0 {
					innerRef.Fields["_additional"] = additionalProperties
				}
			}
		}
	}
}

func (e *Explorer) CrossClassVectorSearch(ctx context.Context,
	params ExploreParams,
) ([]search.Result, error) {
	if err := e.validateExploreParams(params); err != nil {
		return nil, errors.Wrap(err, "invalid params")
	}

	vector, targetVector, err := e.vectorFromExploreParams(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("vectorize params: %w", enterrors.NewErrQueryVectorization(err))
	}

	res, err := e.searcher.CrossClassVectorSearch(ctx, vector, targetVector, params.Offset, params.Limit, nil)
	if err != nil {
		// %w preserves the admission shed's identity (ErrOverloaded) through wrapping.
		return nil, fmt.Errorf("vector search: %w", err)
	}

	e.trackUsageExplore(res, params)

	results := []search.Result{}
	for _, item := range res {
		item.Beacon = crossref.NewLocalhost(item.ClassName, item.ID).String()
		err = e.appendResultsIfSimilarityThresholdMet(item, &results, params)
		if err != nil {
			return nil, fmt.Errorf("append results based on similarity: %w", err)
		}
	}

	return results, nil
}

func (e *Explorer) appendResultsIfSimilarityThresholdMet(item search.Result,
	results *[]search.Result, params ExploreParams,
) error {
	distance, withDistance := extractDistanceFromExploreParams(params)
	certainty := extractCertaintyFromExploreParams(params)

	if withDistance && (floatcomp.InDelta(float64(item.Dist), distance, 1e-6) ||
		item.Dist <= float32(distance)) {
		*results = append(*results, item)
	} else if certainty != 0 && item.Certainty >= float32(certainty) {
		*results = append(*results, item)
	} else if !withDistance && certainty == 0 {
		*results = append(*results, item)
	}

	return nil
}

func (e *Explorer) validateExploreParams(params ExploreParams) error {
	if params.NearVector == nil && params.NearObject == nil && len(params.ModuleParams) == 0 {
		return errors.Errorf("received no search params, one of [nearVector, nearObject] " +
			"or module search params is required for an exploration")
	}

	return nil
}

func (e *Explorer) targetFromParams(ctx context.Context,
	params dto.GetParams,
) ([]string, error) {
	return e.nearParamsVector.targetFromParams(ctx, params.NearVector,
		params.NearObject, params.ModuleParams, params.ClassName, params.Tenant)
}

func (e *Explorer) vectorFromParamsForTarget(ctx context.Context,
	nv *searchparams.NearVector, no *searchparams.NearObject, moduleParams map[string]interface{}, className, tenant, target string, index int,
) (models.Vector, error) {
	return e.nearParamsVector.vectorFromParams(ctx, nv, no, moduleParams, className, tenant, target, index)
}

func (e *Explorer) vectorFromExploreParams(ctx context.Context,
	params ExploreParams,
) (models.Vector, string, error) {
	err := e.nearParamsVector.validateNearParams(params.NearVector, params.NearObject, params.ModuleParams)
	if err != nil {
		return nil, "", err
	}

	if len(params.ModuleParams) == 1 {
		for name, value := range params.ModuleParams {
			return e.crossClassVectorFromModules(ctx, name, value)
		}
	}

	if params.NearVector != nil {
		targetVector := ""
		if len(params.NearVector.TargetVectors) == 1 {
			targetVector = params.NearVector.TargetVectors[0]
		}
		return params.NearVector.Vectors[0], targetVector, nil
	}

	if params.NearObject != nil {
		// TODO: cross class
		vector, targetVector, err := e.nearParamsVector.crossClassVectorFromNearObjectParams(ctx, params.NearObject)
		if err != nil {
			return nil, "", fmt.Errorf("nearObject params: %w", err)
		}

		return vector, targetVector, nil
	}

	// either nearObject or nearVector or module search param has to be set,
	// so if we land here, something has gone very wrong
	panic("vectorFromExploreParams was called without any known params present")
}

// similar to vectorFromModules, but not specific to a single class
func (e *Explorer) crossClassVectorFromModules(ctx context.Context,
	paramName string, paramValue interface{},
) ([]float32, string, error) {
	if e.modulesProvider != nil {
		vector, targetVector, err := e.modulesProvider.CrossClassVectorFromSearchParam(ctx,
			paramName, paramValue, e.nearParamsVector.findVector,
		)
		if err != nil {
			return nil, "", fmt.Errorf("vectorize params: %w", enterrors.NewErrQueryVectorization(err))
		}
		return vector, targetVector, nil
	}
	return nil, "", errors.New("no modules defined")
}

func (e *Explorer) GetSchema() schema.Schema {
	s := e.schemaGetter.ReadOnlySchema()
	return schema.Schema{Objects: &s}
}

func (e *Explorer) replicationEnabled(params dto.GetParams) (bool, error) {
	if e.schemaGetter == nil {
		return false, fmt.Errorf("schemaGetter not set")
	}

	class := e.schemaGetter.ReadOnlyClass(params.ClassName)
	if class == nil {
		return false, fmt.Errorf("class not found in schema: %q", params.ClassName)
	}

	return class.ReplicationConfig != nil && class.ReplicationConfig.Factor > 1, nil
}

func (e *Explorer) keepObjectsWithTTL(params dto.GetParams, input search.Result, searchStartTime time.Time) (bool, error) {
	if e.schemaGetter == nil {
		return false, fmt.Errorf("schemaGetter not set")
	}

	class := e.schemaGetter.ReadOnlyClass(params.ClassName)
	if class == nil {
		return false, fmt.Errorf("class not found in schema: %q", params.ClassName)
	}

	// Skip TTL filtering if searchStartTime is zero (e.g., during hybrid search)
	if searchStartTime.IsZero() {
		return true, nil
	}
	if !ttl.IsTtlEnabled(class.ObjectTTLConfig) || !class.ObjectTTLConfig.FilterExpiredObjects {
		return true, nil
	}

	var expirationTime time.Time

	switch class.ObjectTTLConfig.DeleteOn {
	case filters.InternalPropCreationTimeUnix:
		expirationTime = time.UnixMilli(input.Created)

	case filters.InternalPropLastUpdateTimeUnix:
		expirationTime = time.UnixMilli(input.Updated)
	default:
		dateTime, exists := input.Schema.(map[string]interface{})[class.ObjectTTLConfig.DeleteOn]
		if !exists {
			// if object has no TTL date set, we keep it
			return true, nil
		}
		deleteOnTimeStr, ok := dateTime.(string)
		if !ok {
			return false, fmt.Errorf("date as string expected, got %T", dateTime)
		}
		var err error
		expirationTime, err = time.Parse(time.RFC3339, deleteOnTimeStr)
		if err != nil {
			return false, fmt.Errorf("parse date: %w", err)
		}
	}
	expirationThreshold := expirationTime.Add(time.Second * time.Duration(class.ObjectTTLConfig.DefaultTTL))
	return expirationThreshold.After(searchStartTime), nil
}

func ExtractDistanceFromParams(params dto.GetParams) (distance float64, withDistance bool) {
	if params.NearVector != nil {
		distance = params.NearVector.Distance
		withDistance = params.NearVector.WithDistance
		return distance, withDistance
	}

	if params.NearObject != nil {
		distance = params.NearObject.Distance
		withDistance = params.NearObject.WithDistance
		return distance, withDistance
	}

	if params.HybridSearch != nil {
		if params.HybridSearch.NearTextParams != nil {
			distance = params.HybridSearch.NearTextParams.Distance
			withDistance = params.HybridSearch.NearTextParams.WithDistance
			return distance, withDistance
		}
		if params.HybridSearch.NearVectorParams != nil {
			distance = params.HybridSearch.NearVectorParams.Distance
			withDistance = params.HybridSearch.NearVectorParams.WithDistance
			return distance, withDistance
		}
	}

	if len(params.ModuleParams) == 1 {
		distance, withDistance = extractDistanceFromModuleParams(params.ModuleParams)
	}

	return distance, withDistance
}

func ExtractCertaintyFromParams(params dto.GetParams) (certainty float64) {
	if params.NearVector != nil {
		certainty = params.NearVector.Certainty
		return certainty
	}

	if params.NearObject != nil {
		certainty = params.NearObject.Certainty
		return certainty
	}

	if len(params.ModuleParams) == 1 {
		certainty = extractCertaintyFromModuleParams(params.ModuleParams)
		return certainty
	}

	return certainty
}

func extractCertaintyFromExploreParams(params ExploreParams) (certainty float64) {
	if params.NearVector != nil {
		certainty = params.NearVector.Certainty
		return certainty
	}

	if params.NearObject != nil {
		certainty = params.NearObject.Certainty
		return certainty
	}

	if len(params.ModuleParams) == 1 {
		certainty = extractCertaintyFromModuleParams(params.ModuleParams)
	}

	return certainty
}

func extractDistanceFromExploreParams(params ExploreParams) (distance float64, withDistance bool) {
	if params.NearVector != nil {
		distance = params.NearVector.Distance
		withDistance = params.NearVector.WithDistance
		return distance, withDistance
	}

	if params.NearObject != nil {
		distance = params.NearObject.Distance
		withDistance = params.NearObject.WithDistance
		return distance, withDistance
	}

	if len(params.ModuleParams) == 1 {
		distance, withDistance = extractDistanceFromModuleParams(params.ModuleParams)
	}

	return distance, withDistance
}

func extractCertaintyFromModuleParams(moduleParams map[string]interface{}) float64 {
	for _, param := range moduleParams {
		if nearParam, ok := param.(modulecapabilities.NearParam); ok {
			if nearParam.SimilarityMetricProvided() {
				if certainty := nearParam.GetCertainty(); certainty != 0 {
					return certainty
				}
			}
		}
	}

	return 0
}

func extractDistanceFromModuleParams(moduleParams map[string]interface{}) (distance float64, withDistance bool) {
	for _, param := range moduleParams {
		if nearParam, ok := param.(modulecapabilities.NearParam); ok {
			if nearParam.SimilarityMetricProvided() {
				if certainty := nearParam.GetCertainty(); certainty != 0 {
					distance, withDistance = 0, false
					return distance, withDistance
				}
				distance, withDistance = nearParam.GetDistance(), true
				return distance, withDistance
			}
		}
	}

	return distance, withDistance
}

func (e *Explorer) trackUsageGet(res search.Results, params dto.GetParams) {
	if len(res) == 0 {
		return
	}

	op := e.usageOperationFromGetParams(params)
	if e.metrics != nil {
		e.metrics.AddUsageDimensions(params.ClassName, "get_graphql", op, res[0].Dims)
	}
}

func (e *Explorer) trackUsageGetExplicitVector(res search.Results, params dto.GetParams) {
	if len(res) == 0 {
		return
	}

	e.metrics.AddUsageDimensions(params.ClassName, "get_graphql", "_additional.vector",
		res[0].Dims)
}

func (e *Explorer) usageOperationFromGetParams(params dto.GetParams) string {
	if params.NearObject != nil {
		return "nearObject"
	}

	if params.NearVector != nil {
		return "nearVector"
	}

	// there is at most one module param, so we can return the first we find
	for param := range params.ModuleParams {
		return param
	}

	return "n/a"
}

func (e *Explorer) trackUsageExplore(res search.Results, params ExploreParams) {
	if len(res) == 0 {
		return
	}

	op := e.usageOperationFromExploreParams(params)
	e.metrics.AddUsageDimensions("n/a", "explore_graphql", op, res[0].Dims)
}

func (e *Explorer) usageOperationFromExploreParams(params ExploreParams) string {
	if params.NearObject != nil {
		return "nearObject"
	}

	if params.NearVector != nil {
		return "nearVector"
	}

	// there is at most one module param, so we can return the first we find
	for param := range params.ModuleParams {
		return param
	}

	return "n/a"
}
