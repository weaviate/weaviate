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

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/filters"
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
}

type distancer func(a, b []float32) (float32, error)

type vectorClassSearch interface {
	ClassSearch(ctx context.Context, params GetParams) ([]search.Result, error)
	VectorClassSearch(ctx context.Context, params GetParams) ([]search.Result, error)
	VectorSearch(ctx context.Context, vector []float32, limit int,
		filters *filters.LocalFilter) ([]search.Result, error)
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
	projector projector, pathBuilder pathBuilder) *Explorer {
	return &Explorer{search, vectorizer, distancer, logger, nnExtender, projector, pathBuilder}
}

// GetClass from search and connector repo
func (e *Explorer) GetClass(ctx context.Context,
	params GetParams) ([]interface{}, error) {
	if params.Pagination == nil {
		params.Pagination = &filters.Pagination{
			Limit: 100,
		}
	}

	if params.NearText != nil || params.NearVector != nil {
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
		if res.AdditionalProperties != nil {
			if res.AdditionalProperties.Classification != nil {
				classification := map[string]interface{}{"classification": res.AdditionalProperties.Classification}
				res.Schema.(map[string]interface{})["_additional"] = classification
			}

			if res.AdditionalProperties.Interpretation != nil {
				interpretation := map[string]interface{}{"interpretation": res.AdditionalProperties.Interpretation}
				res.Schema.(map[string]interface{})["_additional"] = interpretation
			}

			if res.AdditionalProperties.NearestNeighbors != nil {
				nearestNeighbors := map[string]interface{}{"nearestNeighbors": res.AdditionalProperties.NearestNeighbors}
				res.Schema.(map[string]interface{})["_additional"] = nearestNeighbors
			}

			if res.AdditionalProperties.FeatureProjection != nil {
				featureProjection := map[string]interface{}{"featureProjection": res.AdditionalProperties.FeatureProjection}
				res.Schema.(map[string]interface{})["_additional"] = featureProjection
			}

			if res.AdditionalProperties.SemanticPath != nil {
				semanticPath := map[string]interface{}{"semanticPath": res.AdditionalProperties.SemanticPath}
				res.Schema.(map[string]interface{})["_additional"] = semanticPath
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
				certainty := map[string]interface{}{"certainty": 1 - dist}
				res.Schema.(map[string]interface{})["_additional"] = certainty
			}
		}

		if params.AdditionalProperties.ID {
			id := map[string]interface{}{"id": res.ID}
			res.Schema.(map[string]interface{})["_additional"] = id
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
				innerPropertySchema := propertySchema.(map[string]interface{})[selectProp.Name]
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

func (e *Explorer) exctractAdditionalPropertiesFromRef(ref interface{}, refClass SelectClass) {
	for _, innerRefProp := range ref.([]interface{}) {
		innerRef, ok := innerRefProp.(search.LocalRef)
		if !ok {
			continue
		}
		if innerRef.Class == refClass.ClassName {
			if refClass.AdditionalProperties.ID {
				innerRefID := map[string]interface{}{"id": innerRef.Fields["id"]}
				innerRef.Fields["_additional"] = innerRefID
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
	if params.NearText == nil && params.NearVector == nil {
		return errors.Errorf("received no search params, one of [nearText, nearVector] " +
			"is required for an exploration")
	}

	return nil
}

func (e *Explorer) vectorFromParams(ctx context.Context,
	params GetParams) ([]float32, error) {
	if params.NearText != nil && params.NearVector != nil {
		return nil, errors.Errorf("found both 'nearText' and 'nearVector' parameters " +
			"which are conflicting, choose one instead")
	}

	if params.NearText != nil {
		vector, err := e.vectorFromNearTextParams(ctx, params.NearText)
		if err != nil {
			return nil, errors.Errorf("vectorize params: %v", err)
		}

		return vector, nil
	}

	if params.NearVector != nil {
		return params.NearVector.Vector, nil
	}

	// either nearText or nearVector has to be set, so if we land here, something
	// has gone very wrong
	panic("vectorFromParams was called without any known params present")
}

func (e *Explorer) vectorFromExploreParams(ctx context.Context,
	params ExploreParams) ([]float32, error) {
	if params.NearText != nil && params.NearVector != nil {
		return nil, errors.Errorf("found both 'nearText' and 'nearVector' parameters " +
			"which are conflicting, choose one instead")
	}

	if params.NearText != nil {
		vector, err := e.vectorFromNearTextParams(ctx, params.NearText)
		if err != nil {
			return nil, errors.Errorf("vectorize params: %v", err)
		}

		return vector, nil
	}

	if params.NearVector != nil {
		return params.NearVector.Vector, nil
	}

	// either nearText or nearVector has to be set, so if we land here, something
	// has gone very wrong
	panic("vectorFromParams was called without any known params present")
}

func (e *Explorer) vectorFromNearTextParams(ctx context.Context,
	params *NearTextParams) ([]float32, error) {
	vector, err := e.vectorizer.Corpi(ctx, params.Values)
	if err != nil {
		return nil, errors.Errorf("vectorize keywords: %v", err)
	}

	if params.MoveTo.Force > 0 && len(params.MoveTo.Values) > 0 {
		moveToVector, err := e.vectorizer.Corpi(ctx, params.MoveTo.Values)
		if err != nil {
			return nil, errors.Errorf("vectorize move to: %v", err)
		}

		afterMoveTo, err := e.vectorizer.MoveTo(vector, moveToVector, params.MoveTo.Force)
		if err != nil {
			return nil, err
		}
		vector = afterMoveTo
	}

	if params.MoveAwayFrom.Force > 0 && len(params.MoveAwayFrom.Values) > 0 {
		moveAwayVector, err := e.vectorizer.Corpi(ctx, params.MoveAwayFrom.Values)
		if err != nil {
			return nil, errors.Errorf("vectorize move away from: %v", err)
		}

		afterMoveFrom, err := e.vectorizer.MoveAwayFrom(vector, moveAwayVector,
			params.MoveAwayFrom.Force)
		if err != nil {
			return nil, err
		}
		vector = afterMoveFrom
	}

	return vector, nil
}

func beacon(res search.Result) string {
	return fmt.Sprintf("weaviate://localhost/%s", res.ID)
}
