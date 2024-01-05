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

package traverser

import (
	"context"
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/entities/autocut"
	"github.com/weaviate/weaviate/entities/vectorindex/common"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
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
	uc "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/traverser/grouper"
	"github.com/weaviate/weaviate/usecases/traverser/hybrid"
)

// Explorer is a helper construct to perform vector-based searches. It does not
// contain monitoring or authorization checks. It should thus never be directly
// used by an API, but through a Traverser.
type Explorer struct {
	searcher         objectsSearcher
	logger           logrus.FieldLogger
	modulesProvider  ModulesProvider
	schemaGetter     uc.SchemaGetter
	nearParamsVector *nearParamsVector
	metrics          explorerMetrics
	config           config.Config
}

type explorerMetrics interface {
	AddUsageDimensions(className, queryType, operation string, dims int)
}

type ModulesProvider interface {
	ValidateSearchParam(name string, value interface{}, className string) error
	CrossClassValidateSearchParam(name string, value interface{}) error
	VectorFromSearchParam(ctx context.Context, className string, param string,
		params interface{}, findVectorFn modulecapabilities.FindVectorFn, tenant string) ([]float32, error)
	CrossClassVectorFromSearchParam(ctx context.Context, param string,
		params interface{}, findVectorFn modulecapabilities.FindVectorFn) ([]float32, error)
	GetExploreAdditionalExtend(ctx context.Context, in []search.Result,
		moduleParams map[string]interface{}, searchVector []float32,
		argumentModuleParams map[string]interface{}) ([]search.Result, error)
	ListExploreAdditionalExtend(ctx context.Context, in []search.Result,
		moduleParams map[string]interface{},
		argumentModuleParams map[string]interface{}) ([]search.Result, error)
	VectorFromInput(ctx context.Context, className string, input string) ([]float32, error)
}

type objectsSearcher interface {
	hybridSearcher

	// GraphQL Get{} queries
	Search(ctx context.Context, params dto.GetParams) ([]search.Result, error)
	VectorSearch(ctx context.Context, params dto.GetParams) ([]search.Result, error)

	// GraphQL Explore{} queries
	CrossClassVectorSearch(ctx context.Context, vector []float32, offset, limit int,
		filters *filters.LocalFilter) ([]search.Result, error)

	// Near-params searcher
	Object(ctx context.Context, className string, id strfmt.UUID,
		props search.SelectProperties, additional additional.Properties,
		properties *additional.ReplicationProperties, tenant string) (*search.Result, error)
	ObjectsByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties, tenant string) (search.Results, error)
}

type hybridSearcher interface {
	SparseObjectSearch(ctx context.Context, params dto.GetParams) ([]*storobj.Object, []float32, error)
	DenseObjectSearch(context.Context, string, []float32, int, int,
		*filters.LocalFilter, additional.Properties, string) ([]*storobj.Object, []float32, error)
	ResolveReferences(ctx context.Context, objs search.Results, props search.SelectProperties,
		groupBy *searchparams.GroupBy, additional additional.Properties, tenant string) (search.Results, error)
}

// NewExplorer with search and connector repo
func NewExplorer(searcher objectsSearcher, logger logrus.FieldLogger, modulesProvider ModulesProvider, metrics explorerMetrics, conf config.Config) *Explorer {
	return &Explorer{
		searcher:         searcher,
		logger:           logger,
		modulesProvider:  modulesProvider,
		metrics:          metrics,
		schemaGetter:     nil, // schemaGetter is set later
		nearParamsVector: newNearParamsVector(modulesProvider, searcher),
		config:           conf,
	}
}

func (e *Explorer) SetSchemaGetter(sg uc.SchemaGetter) {
	e.schemaGetter = sg
}

// GetClass from search and connector repo
func (e *Explorer) GetClass(ctx context.Context,
	params dto.GetParams,
) ([]interface{}, error) {
	if params.Pagination == nil {
		params.Pagination = &filters.Pagination{
			Offset: 0,
			Limit:  100,
		}
	}

	if err := e.validateFilters(params.Filters); err != nil {
		return nil, errors.Wrap(err, "invalid 'where' filter")
	}

	if err := e.validateSort(params.ClassName, params.Sort); err != nil {
		return nil, errors.Wrap(err, "invalid 'sort' parameter")
	}

	if err := e.validateCursor(params); err != nil {
		return nil, errors.Wrap(err, "cursor api: invalid 'after' parameter")
	}

	if params.KeywordRanking != nil {
		return e.getClassKeywordBased(ctx, params)
	}

	if params.NearVector != nil || params.NearObject != nil || len(params.ModuleParams) > 0 {
		return e.getClassVectorSearch(ctx, params)
	}

	return e.getClassList(ctx, params)
}

func (e *Explorer) getClassKeywordBased(ctx context.Context, params dto.GetParams) ([]interface{}, error) {
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
		return nil, errors.Errorf("explorer: get class: vector search: %v", err)
	}

	if params.Group != nil {
		grouped, err := grouper.New(e.logger).Group(res, params.Group.Strategy, params.Group.Force)
		if err != nil {
			return nil, errors.Errorf("grouper: %v", err)
		}

		res = grouped
	}

	if e.modulesProvider != nil {
		res, err = e.modulesProvider.GetExploreAdditionalExtend(ctx, res,
			params.AdditionalProperties.ModuleParams, nil, params.ModuleParams)
		if err != nil {
			return nil, errors.Errorf("explorer: get class: extend: %v", err)
		}
	}

	return e.searchResultsToGetResponse(ctx, res, nil, params)
}

func (e *Explorer) getClassVectorSearch(ctx context.Context,
	params dto.GetParams,
) ([]interface{}, error) {
	searchVector, err := e.vectorFromParams(ctx, params)
	if err != nil {
		return nil, errors.Errorf("explorer: get class: vectorize params: %v", err)
	}

	params.SearchVector = searchVector

	if len(params.AdditionalProperties.ModuleParams) > 0 || params.Group != nil {
		// if a module-specific additional prop is set, assume it needs the vector
		// present for backward-compatibility. This could be improved by actually
		// asking the module based on specific conditions
		// if a group is set, vectors are needed
		params.AdditionalProperties.Vector = true
	}

	res, err := e.searcher.VectorSearch(ctx, params)
	if err != nil {
		return nil, errors.Errorf("explorer: get class: vector search: %v", err)
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
			return nil, errors.Errorf("grouper: %v", err)
		}

		res = grouped
	}

	if e.modulesProvider != nil {
		res, err = e.modulesProvider.GetExploreAdditionalExtend(ctx, res,
			params.AdditionalProperties.ModuleParams, searchVector, params.ModuleParams)
		if err != nil {
			return nil, errors.Errorf("explorer: get class: extend: %v", err)
		}
	}

	e.trackUsageGet(res, params)

	return e.searchResultsToGetResponse(ctx, res, searchVector, params)
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

func (e *Explorer) Hybrid(ctx context.Context, params dto.GetParams) ([]search.Result, error) {
	sparseSearch := func() ([]*storobj.Object, []float32, error) {
		params.KeywordRanking = &searchparams.KeywordRanking{
			Query:      params.HybridSearch.Query,
			Type:       "bm25",
			Properties: params.HybridSearch.Properties,
		}

		if params.Pagination == nil {
			return nil, nil, fmt.Errorf("invalid params, pagination object is nil")
		}

		totalLimit, err := e.CalculateTotalLimit(params.Pagination)
		if err != nil {
			return nil, nil, err
		}

		enforcedMin := MaxInt(params.Pagination.Offset+hybrid.DefaultLimit, totalLimit)

		oldLimit := params.Pagination.Limit
		params.Pagination.Limit = enforcedMin - params.Pagination.Offset

		res, dists, err := e.searcher.SparseObjectSearch(ctx, params)
		if err != nil {
			return nil, nil, err
		}
		params.Pagination.Limit = oldLimit

		return res, dists, nil
	}

	denseSearch := func(vec []float32) ([]*storobj.Object, []float32, error) {
		baseSearchLimit := params.Pagination.Limit + params.Pagination.Offset
		var hybridSearchLimit int
		if baseSearchLimit <= hybrid.DefaultLimit {
			hybridSearchLimit = hybrid.DefaultLimit
		} else {
			hybridSearchLimit = baseSearchLimit
		}
		res, dists, err := e.searcher.DenseObjectSearch(ctx,
			params.ClassName, vec, 0, hybridSearchLimit, params.Filters,
			params.AdditionalProperties, params.Tenant)
		if err != nil {
			return nil, nil, err
		}

		return res, dists, nil
	}

	postProcess := func(results hybrid.Results) ([]search.Result, error) {
		res1 := results.SearchResults()
		totalLimit, err := e.CalculateTotalLimit(params.Pagination)
		if err != nil {
			return nil, err
		}

		if len(res1) > totalLimit {
			res1 = res1[:totalLimit]
		}

		res, err := e.searcher.ResolveReferences(ctx, res1, params.Properties, nil, params.AdditionalProperties, params.Tenant)
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	res, err := hybrid.Search(ctx, &hybrid.Params{
		HybridSearch: params.HybridSearch,
		Keyword:      params.KeywordRanking,
		Class:        params.ClassName,
		Autocut:      params.Pagination.Autocut,
	}, e.logger, sparseSearch, denseSearch, postProcess, e.modulesProvider)
	if err != nil {
		return nil, err
	}

	var out hybrid.Results

	if params.Pagination.Limit <= 0 {
		params.Pagination.Limit = hybrid.DefaultLimit
	}

	if params.Pagination.Offset < 0 {
		params.Pagination.Offset = 0
	}

	if len(res) >= params.Pagination.Limit+params.Pagination.Offset {
		out = res[params.Pagination.Offset : params.Pagination.Limit+params.Pagination.Offset]
	}
	if len(res) < params.Pagination.Limit+params.Pagination.Offset && len(res) > params.Pagination.Offset {
		out = res[params.Pagination.Offset:]
	}
	if len(res) <= params.Pagination.Offset {
		out = hybrid.Results{}
	}

	return out.SearchResults(), nil
}

func (e *Explorer) getClassList(ctx context.Context,
	params dto.GetParams,
) ([]interface{}, error) {
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
			return nil, errors.Errorf("explorer: list class: search: %v", err)
		}
	}

	if params.Group != nil {
		grouped, err := grouper.New(e.logger).Group(res, params.Group.Strategy, params.Group.Force)
		if err != nil {
			return nil, errors.Errorf("grouper: %v", err)
		}

		res = grouped
	}

	if e.modulesProvider != nil {
		res, err = e.modulesProvider.ListExploreAdditionalExtend(ctx, res,
			params.AdditionalProperties.ModuleParams, params.ModuleParams)
		if err != nil {
			return nil, errors.Errorf("explorer: list class: extend: %v", err)
		}
	}

	if userSetAdditionalVector {
		e.trackUsageGetExplicitVector(res, params)
	}

	return e.searchResultsToGetResponse(ctx, res, nil, params)
}

func (e *Explorer) searchResultsToGetResponse(ctx context.Context,
	input []search.Result,
	searchVector []float32, params dto.GetParams,
) ([]interface{}, error) {
	output := make([]interface{}, 0, len(input))
	replEnabled, err := e.replicationEnabled(params)
	if err != nil {
		return nil, fmt.Errorf("search results to get response: %w", err)
	}
	for _, res := range input {
		additionalProperties := make(map[string]interface{})

		if res.AdditionalProperties != nil {
			for additionalProperty, value := range res.AdditionalProperties {
				if value != nil {
					additionalProperties[additionalProperty] = value
				}
			}
		}

		if searchVector != nil {
			// Dist is between 0..2, we need to reduce to the user space of 0..1
			normalizedResultDist := res.Dist / 2

			certainty := ExtractCertaintyFromParams(params)
			if 1-(normalizedResultDist) < float32(certainty) && 1-normalizedResultDist >= 0 {
				// TODO: Clean this up. The >= check is so that this logic does not run
				// non-cosine distance.
				continue
			}

			if certainty == 0 {
				distance, withDistance := ExtractDistanceFromParams(params)
				if withDistance && (!floatcomp.InDelta(float64(res.Dist), distance, 1e-6) &&
					float64(res.Dist) > distance) {
					continue
				}
			}

			if params.AdditionalProperties.Certainty {
				if err := e.checkCertaintyCompatibility(params.ClassName); err != nil {
					return nil, errors.Errorf("additional: %s", err)
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
				e.extractAdditionalPropertiesFromGroupRefs(additionalProperties["group"], params.Properties)
			}
			res.Schema.(map[string]interface{})["_additional"] = additionalProperties
		}

		e.extractAdditionalPropertiesFromRefs(res.Schema, params.Properties)

		output = append(output, res.Schema)
	}

	return output, nil
}

func (e *Explorer) extractAdditionalPropertiesFromGroupRefs(
	additionalGroup interface{},
	params search.SelectProperties,
) {
	if group, ok := additionalGroup.(*additional.Group); ok {
		if len(group.Hits) > 0 {
			var groupSelectProperties search.SelectProperties
			for _, selectProp := range params {
				if strings.HasPrefix(selectProp.Name, "_additional:group:hits:") {
					groupSelectProperties = append(groupSelectProperties, search.SelectProperty{
						Name:            strings.Replace(selectProp.Name, "_additional:group:hits:", "", 1),
						IsPrimitive:     selectProp.IsPrimitive,
						IncludeTypeName: selectProp.IncludeTypeName,
						Refs:            selectProp.Refs,
					})
				}
			}
			for _, hit := range group.Hits {
				e.extractAdditionalPropertiesFromRefs(hit, groupSelectProperties)
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

	vector, err := e.vectorFromExploreParams(ctx, params)
	if err != nil {
		return nil, errors.Errorf("vectorize params: %v", err)
	}

	res, err := e.searcher.CrossClassVectorSearch(ctx, vector, params.Offset, params.Limit, nil)
	if err != nil {
		return nil, errors.Errorf("vector search: %v", err)
	}

	e.trackUsageExplore(res, params)

	results := []search.Result{}
	for _, item := range res {
		item.Beacon = crossref.NewLocalhost(item.ClassName, item.ID).String()
		err = e.appendResultsIfSimilarityThresholdMet(item, &results, params)
		if err != nil {
			return nil, errors.Errorf("append results based on similarity: %s", err)
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

func (e *Explorer) vectorFromParams(ctx context.Context,
	params dto.GetParams,
) ([]float32, error) {
	return e.nearParamsVector.vectorFromParams(ctx, params.NearVector,
		params.NearObject, params.ModuleParams, params.ClassName, params.Tenant)
}

func (e *Explorer) vectorFromExploreParams(ctx context.Context,
	params ExploreParams,
) ([]float32, error) {
	err := e.nearParamsVector.validateNearParams(params.NearVector, params.NearObject, params.ModuleParams)
	if err != nil {
		return nil, err
	}

	if len(params.ModuleParams) == 1 {
		for name, value := range params.ModuleParams {
			return e.crossClassVectorFromModules(ctx, name, value)
		}
	}

	if params.NearVector != nil {
		return params.NearVector.Vector, nil
	}

	if params.NearObject != nil {
		// TODO: cross class
		vector, err := e.nearParamsVector.crossClassVectorFromNearObjectParams(ctx, params.NearObject)
		if err != nil {
			return nil, errors.Errorf("nearObject params: %v", err)
		}

		return vector, nil
	}

	// either nearObject or nearVector or module search param has to be set,
	// so if we land here, something has gone very wrong
	panic("vectorFromParams was called without any known params present")
}

// similar to vectorFromModules, but not specific to a single class
func (e *Explorer) crossClassVectorFromModules(ctx context.Context,
	paramName string, paramValue interface{},
) ([]float32, error) {
	if e.modulesProvider != nil {
		vector, err := e.modulesProvider.CrossClassVectorFromSearchParam(ctx,
			paramName, paramValue, e.nearParamsVector.findVector,
		)
		if err != nil {
			return nil, errors.Errorf("vectorize params: %v", err)
		}
		return vector, nil
	}
	return nil, errors.New("no modules defined")
}

func (e *Explorer) checkCertaintyCompatibility(className string) error {
	s := e.schemaGetter.GetSchemaSkipAuth()
	if s.Objects == nil {
		return errors.Errorf("failed to get schema")
	}
	class := s.GetClass(schema.ClassName(className))
	if class == nil {
		return errors.Errorf("failed to get class: %s", className)
	}
	vectorConfig, err := schema.TypeAssertVectorIndex(class)
	if err != nil {
		return err
	}
	if dn := vectorConfig.DistanceName(); dn != common.DistanceCosine {
		return certaintyUnsupportedError(dn)
	}

	return nil
}

func (e *Explorer) replicationEnabled(params dto.GetParams) (bool, error) {
	if e.schemaGetter == nil {
		return false, fmt.Errorf("schemaGetter not set")
	}
	sch := e.schemaGetter.GetSchemaSkipAuth()
	cls := sch.GetClass(schema.ClassName(params.ClassName))
	if cls == nil {
		return false, fmt.Errorf("class not found in schema: %q", params.ClassName)
	}

	return cls.ReplicationConfig != nil && cls.ReplicationConfig.Factor > 1, nil
}

func ExtractDistanceFromParams(params dto.GetParams) (distance float64, withDistance bool) {
	if params.NearVector != nil {
		distance = params.NearVector.Distance
		withDistance = params.NearVector.WithDistance
		return
	}

	if params.NearObject != nil {
		distance = params.NearObject.Distance
		withDistance = params.NearObject.WithDistance
		return
	}

	if len(params.ModuleParams) == 1 {
		distance, withDistance = extractDistanceFromModuleParams(params.ModuleParams)
	}

	return
}

func ExtractCertaintyFromParams(params dto.GetParams) (certainty float64) {
	if params.NearVector != nil {
		certainty = params.NearVector.Certainty
		return
	}

	if params.NearObject != nil {
		certainty = params.NearObject.Certainty
		return
	}

	if len(params.ModuleParams) == 1 {
		certainty = extractCertaintyFromModuleParams(params.ModuleParams)
		return
	}

	return
}

func extractCertaintyFromExploreParams(params ExploreParams) (certainty float64) {
	if params.NearVector != nil {
		certainty = params.NearVector.Certainty
		return
	}

	if params.NearObject != nil {
		certainty = params.NearObject.Certainty
		return
	}

	if len(params.ModuleParams) == 1 {
		certainty = extractCertaintyFromModuleParams(params.ModuleParams)
	}

	return
}

func extractDistanceFromExploreParams(params ExploreParams) (distance float64, withDistance bool) {
	if params.NearVector != nil {
		distance = params.NearVector.Distance
		withDistance = params.NearVector.WithDistance
		return
	}

	if params.NearObject != nil {
		distance = params.NearObject.Distance
		withDistance = params.NearObject.WithDistance
		return
	}

	if len(params.ModuleParams) == 1 {
		distance, withDistance = extractDistanceFromModuleParams(params.ModuleParams)
	}

	return
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
					return
				}
				distance, withDistance = nearParam.GetDistance(), true
				return
			}
		}
	}

	return
}

func (e *Explorer) trackUsageGet(res search.Results, params dto.GetParams) {
	if len(res) == 0 {
		return
	}

	op := e.usageOperationFromGetParams(params)
	e.metrics.AddUsageDimensions(params.ClassName, "get_graphql", op, res[0].Dims)
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
