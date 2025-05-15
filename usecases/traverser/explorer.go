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
	"runtime"

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
	"github.com/weaviate/weaviate/usecases/modulecomponents/generictypes"
	uc "github.com/weaviate/weaviate/usecases/schema"
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
	schemaGetter      uc.SchemaGetter
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
	ObjectsByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties, tenant string) (search.Results, error)
}

type hybridSearcher interface {
	SparseObjectSearch(ctx context.Context, params dto.GetParams) ([]*storobj.Object, []float32, error)
	ResolveReferences(ctx context.Context, objs search.Results, props search.SelectProperties,
		groupBy *searchparams.GroupBy, additional additional.Properties, tenant string) (search.Results, error)
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

	if err := e.validateSort(params.ClassName, params.Sort); err != nil {
		return nil, errors.Wrap(err, "invalid 'sort' parameter")
	}

	if err := e.validateCursor(params); err != nil {
		return nil, errors.Wrap(err, "cursor api: invalid 'after' parameter")
	}

	if params.KeywordRanking != nil {
		res, err := e.getClassKeywordBased(ctx, params)
		if err != nil {
			return nil, err
		}
		return e.searchResultsToGetResponse(ctx, res, nil, params)
	}

	if params.NearVector != nil || params.NearObject != nil || len(params.ModuleParams) > 0 {
		res, searchVector, err := e.getClassVectorSearch(ctx, params)
		if err != nil {
			return nil, err
		}
		return e.searchResultsToGetResponse(ctx, res, searchVector, params)
	}

	res, err := e.getClassList(ctx, params)
	if err != nil {
		return nil, err
	}
	return e.searchResultsToGetResponse(ctx, res, nil, params)
}

func (e *Explorer) getClassKeywordBased(ctx context.Context, params dto.GetParams) ([]search.Result, error) {
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

	if e.modulesProvider != nil {
		res, err = e.modulesProvider.GetExploreAdditionalExtend(ctx, res, params.AdditionalProperties.ModuleParams, nil, params.ModuleParams)
		if err != nil {
			return nil, errors.Errorf("explorer: get class: extend: %v", err)
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
	params dto.GetParams,
) ([]search.Result, models.Vector, error) {
	targetVectors, err := e.targetFromParams(ctx, params)
	if err != nil {
		return nil, nil, errors.Errorf("explorer: get class: vectorize params: %v", err)
	}

	targetVectors, err = e.targetParamHelper.GetTargetVectorOrDefault(e.schemaGetter.GetSchemaSkipAuth(),
		params.ClassName, targetVectors)
	if err != nil {
		return nil, nil, errors.Errorf("explorer: get class: validate target vector: %v", err)
	}

	res, searchVectors, err := e.searchForTargets(ctx, params, targetVectors, nil)
	if err != nil {
		return nil, nil, errors.Wrap(err, "explorer: get class: concurrentTargetVectorSearch)")
	}

	if len(searchVectors) > 0 {
		return res, searchVectors[0], nil
	}
	return res, []float32{}, nil
}

func (e *Explorer) searchForTargets(ctx context.Context, params dto.GetParams, targetVectors []string, searchVectorParams *searchparams.NearVector) ([]search.Result, []models.Vector, error) {
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
				return errors.Errorf("explorer: get class: vectorize search vector: %v", err)
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
		return nil, nil, errors.Errorf("explorer: get class: vector search: %v", err)
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
			return nil, nil, errors.Errorf("grouper: %v", err)
		}

		res = grouped
	}

	if e.modulesProvider != nil {
		res, err = e.modulesProvider.GetExploreAdditionalExtend(ctx, res,
			params.AdditionalProperties.ModuleParams, searchVectors[0], params.ModuleParams)
		if err != nil {
			return nil, nil, errors.Errorf("explorer: get class: extend: %v", err)
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
	params dto.GetParams,
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

	return res, nil
}

func (e *Explorer) searchResultsToGetResponse(ctx context.Context, input []search.Result, searchVector models.Vector, params dto.GetParams) ([]interface{}, error) {
	output := make([]interface{}, 0, len(input))
	results, err := e.searchResultsToGetResponseWithType(ctx, input, searchVector, params)
	if err != nil {
		return nil, err
	}

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

func (e *Explorer) searchResultsToGetResponseWithType(ctx context.Context, input []search.Result, searchVector models.Vector, params dto.GetParams) ([]search.Result, error) {
	var output []search.Result
	replEnabled, err := e.replicationEnabled(params)
	if err != nil {
		return nil, fmt.Errorf("search results to get response: %w", err)
	}
	for _, res := range input {
		if ctx.Err() != nil {
			return nil, ctx.Err()
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
				targetVectors := e.targetParamHelper.GetTargetVectorsFromParams(params)
				class := e.schemaGetter.ReadOnlyClass(params.ClassName)
				if err := configvalidation.CheckCertaintyCompatibility(class, targetVectors); err != nil {
					return nil, errors.Errorf("additional: %s for class: %v", err, params.ClassName)
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
		return nil, errors.Errorf("vectorize params: %v", err)
	}

	res, err := e.searcher.CrossClassVectorSearch(ctx, vector, targetVector, params.Offset, params.Limit, nil)
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
			return nil, "", errors.Errorf("nearObject params: %v", err)
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
			paramName, paramValue, generictypes.FindVectorFn(e.nearParamsVector.findVector),
		)
		if err != nil {
			return nil, "", errors.Errorf("vectorize params: %v", err)
		}
		return vector, targetVector, nil
	}
	return nil, "", errors.New("no modules defined")
}

func (e *Explorer) GetSchema() schema.Schema {
	return e.schemaGetter.GetSchemaSkipAuth()
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

	if params.HybridSearch != nil {
		if params.HybridSearch.NearTextParams != nil {
			distance = params.HybridSearch.NearTextParams.Distance
			withDistance = params.HybridSearch.NearTextParams.WithDistance
			return
		}
		if params.HybridSearch.NearVectorParams != nil {
			distance = params.HybridSearch.NearVectorParams.Distance
			withDistance = params.HybridSearch.NearVectorParams.WithDistance
			return
		}
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
