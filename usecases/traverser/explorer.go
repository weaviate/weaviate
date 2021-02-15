//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	libprojector "github.com/semi-technologies/weaviate/usecases/projector"
	"github.com/semi-technologies/weaviate/usecases/sempath"
	"github.com/semi-technologies/weaviate/usecases/traverser/grouper"
	"github.com/sirupsen/logrus"
)

// Explorer is a helper construct to perform vector-based searches. It does not
// contain monitoring or authorization checks. It should thus never be directly
// used by an API, but through a Traverser.
type Explorer struct {
	search      vectorClassSearch
	vectorizer  CorpiVectorizer
	distancer   distancer
	logger      logrus.FieldLogger
	nnExtender  nnExtender
	projector   projector
	pathBuilder pathBuilder
	modules     []modulecapabilities.Module
}

type distancer func(a, b []float32) (float32, error)

type vectorClassSearch interface {
	ClassSearch(ctx context.Context, params GetParams) ([]search.Result, error)
	VectorClassSearch(ctx context.Context, params GetParams) ([]search.Result, error)
	VectorSearch(ctx context.Context, vector []float32, limit int,
		filters *filters.LocalFilter) ([]search.Result, error)
	ObjectByID(ctx context.Context, id strfmt.UUID,
		props SelectProperties, additional AdditionalProperties) (*search.Result, error)
}

type nnExtender interface {
	Multi(ctx context.Context, in []search.Result, limit *int) ([]search.Result, error)
}

type projector interface {
	Reduce(in []search.Result, params *libprojector.Params) ([]search.Result, error)
}

type pathBuilder interface {
	CalculatePath(in []search.Result, params *sempath.Params) ([]search.Result, error)
}

// NewExplorer with search and connector repo
func NewExplorer(search vectorClassSearch, vectorizer CorpiVectorizer,
	distancer distancer, logger logrus.FieldLogger, nnExtender nnExtender,
	projector projector, pathBuilder pathBuilder, modules []modulecapabilities.Module) *Explorer {
	return &Explorer{search, vectorizer, distancer, logger, nnExtender, projector, pathBuilder, modules}
}

// GetClass from search and connector repo
func (e *Explorer) GetClass(ctx context.Context,
	params GetParams) ([]interface{}, error) {
	if params.Pagination == nil {
		params.Pagination = &filters.Pagination{
			Limit: 100,
		}
	}

	if params.NearText != nil || params.NearVector != nil || params.NearObject != nil {
		return e.getClassExploration(ctx, params)
	}

	return e.getClassList(ctx, params)
}

func (e *Explorer) getClassExploration(ctx context.Context,
	params GetParams) ([]interface{}, error) {
	searchVector, err := e.vectorFromParams(ctx, params)
	if err != nil {
		return nil, errors.Errorf("explorer: get class: vectorize params: %v", err)
	}

	params.SearchVector = searchVector

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

	if params.AdditionalProperties.NearestNeighbors {
		withNN, err := e.nnExtender.Multi(ctx, res, nil)
		if err != nil {
			return nil, errors.Errorf("extend with nearest neighbors: %v", err)
		}

		res = withNN
	}

	if params.AdditionalProperties.FeatureProjection != nil {
		withFP, err := e.projector.Reduce(res, params.AdditionalProperties.FeatureProjection)
		if err != nil {
			return nil, errors.Errorf("extend with feature projections: %v", err)
		}

		res = withFP
	}

	if params.AdditionalProperties.SemanticPath != nil {
		p := params.AdditionalProperties.SemanticPath
		p.SearchVector = searchVector
		withPath, err := e.pathBuilder.CalculatePath(res, p)
		if err != nil {
			return nil, errors.Errorf("extend with semantic path: %v", err)
		}

		res = withPath
	}

	return e.searchResultsToGetResponse(ctx, res, searchVector, params)
}

func (e *Explorer) getClassList(ctx context.Context,
	params GetParams) ([]interface{}, error) {
	res, err := e.search.ClassSearch(ctx, params)
	if err != nil {
		return nil, errors.Errorf("explorer: get class: search: %v", err)
	}

	if params.Group != nil {
		grouped, err := grouper.New(e.logger).Group(res, params.Group.Strategy, params.Group.Force)
		if err != nil {
			return nil, errors.Errorf("grouper: %v", err)
		}

		res = grouped
	}

	if params.AdditionalProperties.NearestNeighbors {
		withNN, err := e.nnExtender.Multi(ctx, res, nil)
		if err != nil {
			return nil, errors.Errorf("extend with nearest neighbors: %v", err)
		}

		res = withNN
	}

	if params.AdditionalProperties.FeatureProjection != nil {
		withFP, err := e.projector.Reduce(res, params.AdditionalProperties.FeatureProjection)
		if err != nil {
			return nil, errors.Errorf("extend with feature projections: %v", err)
		}

		res = withFP
	}

	if params.AdditionalProperties.SemanticPath != nil {
		return nil, errors.Errorf("semantic path not possible on 'list' queries, only on 'explore' queries")
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
			if res.AdditionalProperties.Classification != nil {
				additionalProperties["classification"] = res.AdditionalProperties.Classification
			}

			if res.AdditionalProperties.Interpretation != nil {
				additionalProperties["interpretation"] = res.AdditionalProperties.Interpretation
			}

			if res.AdditionalProperties.NearestNeighbors != nil {
				additionalProperties["nearestNeighbors"] = res.AdditionalProperties.NearestNeighbors
			}

			if res.AdditionalProperties.FeatureProjection != nil {
				additionalProperties["featureProjection"] = res.AdditionalProperties.FeatureProjection
			}

			if res.AdditionalProperties.SemanticPath != nil {
				additionalProperties["semanticPath"] = res.AdditionalProperties.SemanticPath
			}
		}

		if searchVector != nil {
			dist, err := e.distancer(res.Vector, searchVector)
			if err != nil {
				return nil, errors.Errorf("explorer: calculate distance: %v", err)
			}

			certainty := e.extractCertaintyFromParams(params)
			if 1-(dist) < float32(certainty) {
				continue
			}

			if params.AdditionalProperties.Certainty {
				additionalProperties["certainty"] = 1 - dist
			}
		}

		if params.AdditionalProperties.ID {
			additionalProperties["id"] = res.ID
		}

		if len(additionalProperties) > 0 {
			res.Schema.(map[string]interface{})["_additional"] = additionalProperties
		}

		e.extractAdditionalPropertiesFromRefs(res.Schema, params.Properties)

		output = append(output, res.Schema)
	}

	return output, nil
}

func (e *Explorer) extractAdditionalPropertiesFromRefs(propertySchema interface{}, params SelectProperties) {
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

func (e *Explorer) exctractAdditionalPropertiesFromRef(ref interface{}, refClass SelectClass) {
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

// TODO: contains module-specific logic
func (e *Explorer) extractCertaintyFromParams(params GetParams) float64 {
	if params.NearText != nil {
		return params.NearText.Certainty
	}

	if params.NearVector != nil {
		return params.NearVector.Certainty
	}

	if params.NearObject != nil {
		return params.NearObject.Certainty
	}

	panic("extractCertainty was called without any known params present")
}

// TODO: contains module-specific logic
func (e *Explorer) extractCertaintyFromExploreParams(params ExploreParams) float64 {
	if params.NearText != nil {
		return params.NearText.Certainty
	}

	if params.NearVector != nil {
		return params.NearVector.Certainty
	}

	if params.NearObject != nil {
		return params.NearObject.Certainty
	}

	panic("extractCertainty was called without any known params present")
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

	res, err := e.search.VectorSearch(ctx, vector, params.Limit, nil)
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
		certainty := e.extractCertaintyFromExploreParams(params)
		if item.Certainty >= float32(certainty) {
			results = append(results, item)
		}
	}

	return results, nil
}

// TODO: This is temporary as the logic needs to be dynamic due to modules
// providing some of the near<> Features
func (e *Explorer) validateExploreParams(params ExploreParams) error {
	if params.NearText == nil && params.NearVector == nil && params.NearObject == nil {
		return errors.Errorf("received no search params, one of [nearText, nearVector, nearObject] " +
			"is required for an exploration")
	}

	return nil
}

func (e *Explorer) vectorFromParams(ctx context.Context,
	params GetParams) ([]float32, error) {
	err := e.validateNearParams(params.NearText, params.NearVector, params.NearObject)
	if err != nil {
		return nil, err
	}

	if params.NearText != nil {
		return e.vectorFromModules(ctx, params.NearText)
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

	// either nearText or nearVector has to be set, so if we land here, something
	// has gone very wrong
	panic("vectorFromParams was called without any known params present")
}

func (e *Explorer) vectorFromExploreParams(ctx context.Context,
	params ExploreParams) ([]float32, error) {
	err := e.validateNearParams(params.NearText, params.NearVector, params.NearObject)
	if err != nil {
		return nil, err
	}

	if params.NearText != nil {
		return e.vectorFromModules(ctx, params.NearText)
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

	// either nearText or nearVector has to be set, so if we land here, something
	// has gone very wrong
	panic("vectorFromParams was called without any known params present")
}

func (e *Explorer) vectorFromModules(ctx context.Context,
	nearTextParams *NearTextParams) ([]float32, error) {
	for _, mod := range e.modules {
		if searcher, ok := mod.(modulecapabilities.Searcher); ok {
			for paramName, searchVectorFn := range searcher.VectorSearches() {
				if paramName == "nearText" && nearTextParams != nil {
					// TODO: gh-1462 Introduce module params in traverser.GetParams instead of c11y specific params
					vector, err := searchVectorFn(ctx,
						ConvertFromTraverserNearTextParams(nearTextParams),
						e.findVector,
					)
					if err != nil {
						return nil, errors.Errorf("vectorize params: %v", err)
					}
					return vector, nil
				}
			}
		}
	}

	panic("vectorFromModules was called without any known params present")
}

func (e *Explorer) validateNearParams(nearText *NearTextParams, nearVector *NearVectorParams,
	nearObject *NearObjectParams) error {
	if nearText != nil && nearVector != nil && nearObject != nil {
		return errors.Errorf("found 'nearText' and 'nearVector' and 'nearObject' parameters " +
			"which are conflicting, choose one instead")
	}

	if nearText != nil && nearVector != nil {
		return errors.Errorf("found both 'nearText' and 'nearVector' parameters " +
			"which are conflicting, choose one instead")
	}

	if nearText != nil && nearObject != nil {
		return errors.Errorf("found both 'nearText' and 'nearObject' parameters " +
			"which are conflicting, choose one instead")
	}

	if nearVector != nil && nearObject != nil {
		return errors.Errorf("found both 'nearVector' and 'nearObject' parameters " +
			"which are conflicting, choose one instead")
	}

	if nearText != nil && nearText.MoveTo.Force > 0 &&
		nearText.MoveTo.Values == nil && nearText.MoveTo.Objects == nil {
		return errors.Errorf("'nearText.moveTo' parameter " +
			"needs to have defined either 'concepts' or 'objects' fields")
	}

	if nearText != nil && nearText.MoveAwayFrom.Force > 0 &&
		nearText.MoveAwayFrom.Values == nil && nearText.MoveAwayFrom.Objects == nil {
		return errors.Errorf("'nearText.moveAwayFrom' parameter " +
			"needs to have defined either 'concepts' or 'objects' fields")
	}

	return nil
}

func (e *Explorer) vectorFromNearObjectParams(ctx context.Context,
	params *NearObjectParams) ([]float32, error) {
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
	res, err := e.search.ObjectByID(ctx, id, SelectProperties{}, AdditionalProperties{})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, errors.New("vector not found")
	}

	return res.Vector, nil
}

func beacon(res search.Result) string {
	return fmt.Sprintf("weaviate://localhost/%s", res.ID)
}
