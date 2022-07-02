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

package traverser

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser/grouper"
	"github.com/sirupsen/logrus"
)

// Explorer is a helper construct to perform vector-based searches. It does not
// contain monitoring or authorization checks. It should thus never be directly
// used by an API, but through a Traverser.
type Explorer struct {
	search           vectorClassSearch
	distancer        distancer
	logger           logrus.FieldLogger
	modulesProvider  ModulesProvider
	schemaGetter     schema.SchemaGetter
	nearParamsVector *nearParamsVector
}

type ModulesProvider interface {
	ValidateSearchParam(name string, value interface{}, className string) error
	CrossClassValidateSearchParam(name string, value interface{}) error
	VectorFromSearchParam(ctx context.Context, className string, param string,
		params interface{}, findVectorFn modulecapabilities.FindVectorFn) ([]float32, error)
	CrossClassVectorFromSearchParam(ctx context.Context, param string,
		params interface{}, findVectorFn modulecapabilities.FindVectorFn) ([]float32, error)
	GetExploreAdditionalExtend(ctx context.Context, in []search.Result,
		moduleParams map[string]interface{}, searchVector []float32,
		argumentModuleParams map[string]interface{}) ([]search.Result, error)
	ListExploreAdditionalExtend(ctx context.Context, in []search.Result,
		moduleParams map[string]interface{},
		argumentModuleParams map[string]interface{}) ([]search.Result, error)
}

type distancer func(a, b []float32) (float32, error)

type vectorClassSearch interface {
	ClassSearch(ctx context.Context, params GetParams) ([]search.Result, error)
	VectorClassSearch(ctx context.Context, params GetParams) ([]search.Result, error)
	VectorSearch(ctx context.Context, vector []float32, offset, limit int,
		filters *filters.LocalFilter) ([]search.Result, error)
	ObjectByID(ctx context.Context, id strfmt.UUID,
		props search.SelectProperties, additional additional.Properties) (*search.Result, error)
}

// NewExplorer with search and connector repo
func NewExplorer(search vectorClassSearch,
	distancer distancer, logger logrus.FieldLogger,
	modulesProvider ModulesProvider) *Explorer {
	return &Explorer{
		search:           search,
		distancer:        distancer, // TODO: should be removed as we can't know distancer was used by the vector index
		logger:           logger,
		modulesProvider:  modulesProvider,
		schemaGetter:     nil, // schemaGetter is set later
		nearParamsVector: newNearParamsVector(modulesProvider, search),
	}
}

func (e *Explorer) SetSchemaGetter(sg schema.SchemaGetter) {
	e.schemaGetter = sg
}

// GetClass from search and connector repo
func (e *Explorer) GetClass(ctx context.Context,
	params GetParams) ([]interface{}, error) {
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
		return nil, errors.Wrap(err, "invalid 'sort' filter")
	}

	if params.KeywordRanking != nil {
		return e.getClassKeywordBased(ctx, params)
	}

	if params.NearVector != nil || params.NearObject != nil || len(params.ModuleParams) > 0 {
		return e.getClassExploration(ctx, params)
	}

	return e.getClassList(ctx, params)
}

func (e *Explorer) getClassKeywordBased(ctx context.Context,
	params GetParams) ([]interface{}, error) {
	if params.NearVector != nil || params.NearObject != nil || len(params.ModuleParams) > 0 {
		return nil, errors.Errorf("conflict: both near<Media> and keyword-based (bm25) arguments present, choose one")
	}

	if params.Filters != nil {
		return nil, errors.Errorf("filtered keyword search (bm25) not supported yet")
	}

	if len(params.KeywordRanking.Properties) == 0 {
		return nil, errors.Errorf("keyword search (bm25) requires exactly one property")
	}

	if len(params.KeywordRanking.Properties) > 1 {
		return nil, errors.Errorf("multi-property keyword search (BM25F) not supported yet")
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

	res, err := e.search.ClassSearch(ctx, params)
	if err != nil {
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

func (e *Explorer) getClassExploration(ctx context.Context,
	params GetParams) ([]interface{}, error) {
	searchVector, err := e.vectorFromParams(ctx, params)
	if err != nil {
		return nil, errors.Errorf("explorer: get class: vectorize params: %v", err)
	}

	params.SearchVector = searchVector

	if len(params.AdditionalProperties.ModuleParams) > 0 {
		// if a module-specific additional prop is set, assume it needs the vector
		// present for backward-compatibility. This could be improved by actually
		// asking the module based on specific conditions
		params.AdditionalProperties.Vector = true
	}

	res, err := e.search.VectorClassSearch(ctx, params)
	if err != nil {
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
			params.AdditionalProperties.ModuleParams, searchVector, params.ModuleParams)
		if err != nil {
			return nil, errors.Errorf("explorer: get class: extend: %v", err)
		}
	}

	return e.searchResultsToGetResponse(ctx, res, searchVector, params)
}

func (e *Explorer) getClassList(ctx context.Context,
	params GetParams) ([]interface{}, error) {
	// if both grouping and whereFilter/sort are present, the below
	// class search will eventually call storobj.FromBinaryOptional
	// to unmarshal the record. in this case, we must manually set
	// the vector addl prop to unmarshal the result vector into each
	// result payload. if we skip this step, the grouper will attempt
	// to compute the distance with a `nil` vector, resulting in NaN.
	// this was the cause of [github issue 1958]
	// (https://github.com/semi-technologies/weaviate/issues/1958)
	if params.Group != nil && (params.Filters != nil || params.Sort != nil) {
		params.AdditionalProperties.Vector = true
	}

	res, err := e.search.ClassSearch(ctx, params)
	if err != nil {
		return nil, errors.Errorf("explorer: list class: search: %v", err)
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

	return e.searchResultsToGetResponse(ctx, res, nil, params)
}

func (e *Explorer) searchResultsToGetResponse(ctx context.Context,
	input []search.Result,
	searchVector []float32, params GetParams) ([]interface{}, error) {
	output := make([]interface{}, 0, len(input))

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

			distance := ExtractDistanceFromParams(params)
			if distance != 0 && normalizedResultDist > float32(distance) && 1-normalizedResultDist >= 0 {
				// TODO: Clean this up. The >= check is so that this logic does not run
				// non-cosine distance.
				continue
			}

			if params.AdditionalProperties.Certainty {
				additionalProperties["certainty"] = 1 - normalizedResultDist
			}

			if params.AdditionalProperties.Distance {
				additionalProperties["distance"] = res.Dist
			}
		}

		if params.AdditionalProperties.ID {
			additionalProperties["id"] = res.ID
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

		if len(additionalProperties) > 0 {
			res.Schema.(map[string]interface{})["_additional"] = additionalProperties
		}

		e.extractAdditionalPropertiesFromRefs(res.Schema, params.Properties)

		output = append(output, res.Schema)
	}

	return output, nil
}

func (e *Explorer) extractAdditionalPropertiesFromRefs(propertySchema interface{}, params search.SelectProperties) {
	for _, selectProp := range params {
		for _, refClass := range selectProp.Refs {
			propertySchemaMap, ok := propertySchema.(map[string]interface{})
			if ok {
				refProperty := propertySchemaMap[selectProp.Name]
				if refProperty != nil {
					e.exctractAdditionalPropertiesFromRef(refProperty, refClass)
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

func (e *Explorer) exctractAdditionalPropertiesFromRef(ref interface{},
	refClass search.SelectClass) {
	innerRefClass, ok := ref.([]interface{})
	if ok {
		for _, innerRefProp := range innerRefClass {
			innerRef, ok := innerRefProp.(search.LocalRef)
			if !ok {
				continue
			}
			if innerRef.Class == refClass.ClassName {
				if refClass.AdditionalProperties.ID {
					additionalProperties := make(map[string]interface{})
					additionalProperties["id"] = innerRef.Fields["id"]
					innerRef.Fields["_additional"] = additionalProperties
				}
			}
		}
	}
}

func (e *Explorer) Concepts(ctx context.Context,
	params ExploreParams) ([]search.Result, error) {
	if err := e.validateExploreParams(params); err != nil {
		return nil, errors.Wrap(err, "invalid params")
	}

	vector, err := e.vectorFromExploreParams(ctx, params)
	if err != nil {
		return nil, errors.Errorf("vectorize params: %v", err)
	}

	res, err := e.search.VectorSearch(ctx, vector, params.Offset, params.Limit, nil)
	if err != nil {
		return nil, errors.Errorf("vector search: %v", err)
	}

	results := []search.Result{}
	for _, item := range res {
		item.Beacon = beacon(item)
		err = e.appendResultsIfSimilarityThresholdMet(item, &results, vector, params)
		if err != nil {
			return nil, errors.Errorf("append results based on similarity: %s", err)
		}
	}

	return results, nil
}

func (e *Explorer) appendResultsIfSimilarityThresholdMet(item search.Result,
	results *[]search.Result, vec []float32, params ExploreParams) error {

	// TODO: The distancer should no longer be needed because we should get the
	// raw distances as part of the search.Result
	dist, err := e.distancer(vec, item.Vector)
	if err != nil {
		return errors.Errorf("res %s: %v", item.Beacon, err)
	}

	item.Certainty = 1 - dist
	item.Dist = dist

	distance := extractDistanceFromExploreParams(params)
	if distance != 0 && item.Dist <= float32(distance/2) {
		*results = append(*results, item)
	}
	certainty := extractCertaintyFromExploreParams(params)
	if certainty != 0 && item.Certainty >= float32(certainty) {
		*results = append(*results, item)
	}
	if distance == 0 && certainty == 0 {
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
	params GetParams) ([]float32, error) {
	return e.nearParamsVector.vectorFromParams(ctx, params.NearVector,
		params.NearObject, params.ModuleParams, params.ClassName)
}

func (e *Explorer) vectorFromExploreParams(ctx context.Context,
	params ExploreParams) ([]float32, error) {
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
		vector, err := e.nearParamsVector.vectorFromNearObjectParams(ctx, params.NearObject)
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
	paramName string, paramValue interface{}) ([]float32, error) {
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

func ExtractDistanceFromParams(params GetParams) float64 {
	if params.NearVector != nil {
		return params.NearVector.Distance * 2
	}

	if params.NearObject != nil {
		return params.NearObject.Distance * 2
	}

	if len(params.ModuleParams) == 1 {
		return extractDistanceFromModuleParams(params.ModuleParams)
	}

	panic("extractDistance was called without any known params present")
}

func ExtractCertaintyFromParams(params GetParams) float64 {
	if params.NearVector != nil {
		return params.NearVector.Certainty
	}

	if params.NearObject != nil {
		return params.NearObject.Certainty
	}

	if len(params.ModuleParams) == 1 {
		return extractCertaintyFromModuleParams(params.ModuleParams)
	}

	panic("extractCertainty was called without any known params present")
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

func extractDistanceFromExploreParams(params ExploreParams) (distance float64) {
	if params.NearVector != nil {
		distance = params.NearVector.Distance * 2
		return
	}

	if params.NearObject != nil {
		distance = params.NearObject.Distance * 2
		return
	}

	if len(params.ModuleParams) == 1 {
		distance = extractDistanceFromModuleParams(params.ModuleParams)
	}

	return
}

func extractCertaintyFromModuleParams(moduleParams map[string]interface{}) float64 {
	for _, param := range moduleParams {
		if nearParam, ok := param.(modulecapabilities.NearParam); ok {
			if nearParam.SimilarityMetricProvided() {
				if certainty := nearParam.GetCertainty(); certainty != 0 {
					return certainty
				} else {
					return 1 - nearParam.GetDistance()
				}
			}
		}
	}

	return 0
}

func extractDistanceFromModuleParams(moduleParams map[string]interface{}) float64 {
	for _, param := range moduleParams {
		if nearParam, ok := param.(modulecapabilities.NearParam); ok {
			if nearParam.SimilarityMetricProvided() {
				if distance := nearParam.GetDistance(); distance != 0 {
					return distance * 2
				} else {
					return (1 - nearParam.GetCertainty()) * 2
				}
			}
		}
	}

	return 0
}

func beacon(res search.Result) string {
	return fmt.Sprintf("weaviate://localhost/%s", res.ID)
}
