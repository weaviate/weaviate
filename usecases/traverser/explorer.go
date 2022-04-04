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
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser/grouper"
	"github.com/sirupsen/logrus"
)

// Explorer is a helper construct to perform vector-based searches. It does not
// contain monitoring or authorization checks. It should thus never be directly
// used by an API, but through a Traverser.
type Explorer struct {
	search          vectorClassSearch
	distancer       distancer
	logger          logrus.FieldLogger
	modulesProvider ModulesProvider
	schemaGetter    schema.SchemaGetter
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
	return &Explorer{search, distancer, logger, modulesProvider, nil} // schemaGetter is set later
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
			normalizedDist := res.Dist / 2
			certainty := ExtractCertaintyFromParams(params)
			if 1-(normalizedDist) < float32(certainty) {
				continue
			}

			if params.AdditionalProperties.Certainty {
				additionalProperties["certainty"] = 1 - normalizedDist
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
		dist, err := e.distancer(vector, item.Vector)
		if err != nil {
			return nil, errors.Errorf("res %s: %v", item.Beacon, err)
		}
		item.Certainty = 1 - dist
		certainty := extractCertaintyFromExploreParams(params)
		if item.Certainty >= float32(certainty) {
			results = append(results, item)
		}
	}

	return results, nil
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
	err := e.validateNearParams(params.NearVector, params.NearObject, params.ModuleParams, params.ClassName)
	if err != nil {
		return nil, err
	}

	if len(params.ModuleParams) == 1 {
		for name, value := range params.ModuleParams {
			return e.vectorFromModules(ctx, params.ClassName, name, value)
		}
	}

	if params.NearVector != nil {
		return params.NearVector.Vector, nil
	}

	if params.NearObject != nil {
		vector, err := e.vectorFromNearObjectParams(ctx, params.NearObject)
		if err != nil {
			return nil, errors.Errorf("nearObject params: %v", err)
		}

		return vector, nil
	}

	// either nearObject or nearVector or module search param has to be set,
	// so if we land here, something has gone very wrong
	panic("vectorFromParams was called without any known params present")
}

func (e *Explorer) vectorFromExploreParams(ctx context.Context,
	params ExploreParams) ([]float32, error) {
	err := e.validateNearParams(params.NearVector, params.NearObject, params.ModuleParams)
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
		vector, err := e.vectorFromNearObjectParams(ctx, params.NearObject)
		if err != nil {
			return nil, errors.Errorf("nearObject params: %v", err)
		}

		return vector, nil
	}

	// either nearObject or nearVector or module search param has to be set,
	// so if we land here, something has gone very wrong
	panic("vectorFromParams was called without any known params present")
}

func (e *Explorer) vectorFromModules(ctx context.Context,
	className, paramName string, paramValue interface{}) ([]float32, error) {
	if e.modulesProvider != nil {
		vector, err := e.modulesProvider.VectorFromSearchParam(ctx,
			className, paramName, paramValue, e.findVector,
		)
		if err != nil {
			return nil, errors.Errorf("vectorize params: %v", err)
		}
		return vector, nil
	}
	return nil, errors.New("no modules defined")
}

// similar to vectorFromModules, but not specific to a single class
func (e *Explorer) crossClassVectorFromModules(ctx context.Context,
	paramName string, paramValue interface{}) ([]float32, error) {
	if e.modulesProvider != nil {
		vector, err := e.modulesProvider.CrossClassVectorFromSearchParam(ctx,
			paramName, paramValue, e.findVector,
		)
		if err != nil {
			return nil, errors.Errorf("vectorize params: %v", err)
		}
		return vector, nil
	}
	return nil, errors.New("no modules defined")
}

func (e *Explorer) validateNearParams(nearVector *searchparams.NearVector, nearObject *searchparams.NearObject,
	moduleParams map[string]interface{}, className ...string) error {
	if len(moduleParams) == 1 && nearVector != nil && nearObject != nil {
		return errors.Errorf("found 'nearText' and 'nearVector' and 'nearObject' parameters " +
			"which are conflicting, choose one instead")
	}

	if len(moduleParams) == 1 && nearVector != nil {
		return errors.Errorf("found both 'nearText' and 'nearVector' parameters " +
			"which are conflicting, choose one instead")
	}

	if len(moduleParams) == 1 && nearObject != nil {
		return errors.Errorf("found both 'nearText' and 'nearObject' parameters " +
			"which are conflicting, choose one instead")
	}

	if nearVector != nil && nearObject != nil {
		return errors.Errorf("found both 'nearVector' and 'nearObject' parameters " +
			"which are conflicting, choose one instead")
	}

	if e.modulesProvider != nil {
		if len(moduleParams) > 1 {
			params := []string{}
			for p := range moduleParams {
				params = append(params, fmt.Sprintf("'%s'", p))
			}
			return errors.Errorf("found more then one module param: %s which are conflicting "+
				"choose one instead", strings.Join(params, ", "))
		}

		for name, value := range moduleParams {
			if len(className) == 1 {
				err := e.modulesProvider.ValidateSearchParam(name, value, className[0])
				if err != nil {
					return err
				}
			} else {
				err := e.modulesProvider.CrossClassValidateSearchParam(name, value)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (e *Explorer) vectorFromNearObjectParams(ctx context.Context,
	params *searchparams.NearObject) ([]float32, error) {
	if len(params.ID) == 0 && len(params.Beacon) == 0 {
		return nil, errors.New("empty id and beacon")
	}

	var id strfmt.UUID
	if len(params.ID) > 0 {
		id = strfmt.UUID(params.ID)
	} else {
		ref, err := crossref.Parse(params.Beacon)
		if err != nil {
			return nil, err
		}
		id = ref.TargetID
	}

	return e.findVector(ctx, id)
}

func (e *Explorer) findVector(ctx context.Context, id strfmt.UUID) ([]float32, error) {
	res, err := e.search.ObjectByID(ctx, id, search.SelectProperties{}, additional.Properties{})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, errors.New("vector not found")
	}

	return res.Vector, nil
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

func extractCertaintyFromExploreParams(params ExploreParams) float64 {
	if params.NearVector != nil {
		return params.NearVector.Certainty
	}

	if params.NearObject != nil {
		return params.NearObject.Certainty
	}

	if len(params.ModuleParams) == 1 {
		return extractCertaintyFromModuleParams(params.ModuleParams)
	}

	panic("extractCertaintyFromExploreParams was called without any known params present")
}

func extractCertaintyFromModuleParams(moduleParams map[string]interface{}) float64 {
	for _, param := range moduleParams {
		if nearParam, ok := param.(modulecapabilities.NearParam); ok {
			return nearParam.GetCertainty()
		}
	}

	panic("extractCertaintyFromModuleParams was called without any known module near param present")
}

func beacon(res search.Result) string {
	return fmt.Sprintf("weaviate://localhost/%s", res.ID)
}
