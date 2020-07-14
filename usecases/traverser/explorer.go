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
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
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

type explorerRepo interface {
	GetThing(context.Context, strfmt.UUID, *models.Thing) error
	GetAction(context.Context, strfmt.UUID, *models.Action) error
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

	if params.Explore != nil {
		return e.getClassExploration(ctx, params)
	}

	return e.getClassList(ctx, params)
}

func (e *Explorer) getClassExploration(ctx context.Context,
	params GetParams) ([]interface{}, error) {
	searchVector, err := e.vectorFromExploreParams(ctx, params.Explore)
	if err != nil {
		return nil, fmt.Errorf("explorer: get class: vectorize params: %v", err)
	}

	params.SearchVector = searchVector

	res, err := e.search.VectorClassSearch(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("explorer: get class: vector search: %v", err)
	}

	if params.Group != nil {
		grouped, err := grouper.New(e.logger).Group(res, params.Group.Strategy, params.Group.Force)
		if err != nil {
			return nil, fmt.Errorf("grouper: %v", err)
		}

		res = grouped
	}

	if params.UnderscoreProperties.NearestNeighbors {
		withNN, err := e.nnExtender.Multi(ctx, res, nil)
		if err != nil {
			return nil, fmt.Errorf("extend with nearest neighbors: %v", err)
		}

		res = withNN
	}

	if params.UnderscoreProperties.FeatureProjection != nil {
		withFP, err := e.projector.Reduce(res, params.UnderscoreProperties.FeatureProjection)
		if err != nil {
			return nil, fmt.Errorf("extend with feature projections: %v", err)
		}

		res = withFP
	}

	if params.UnderscoreProperties.SemanticPath != nil {
		p := params.UnderscoreProperties.SemanticPath
		p.SearchVector = searchVector
		withPath, err := e.pathBuilder.CalculatePath(res, p)
		if err != nil {
			return nil, fmt.Errorf("extend with semantic path: %v", err)
		}

		res = withPath
	}

	return e.searchResultsToGetResponse(ctx, res, params.Explore.Certainty, searchVector)
}

func (e *Explorer) getClassList(ctx context.Context,
	params GetParams) ([]interface{}, error) {

	res, err := e.search.ClassSearch(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("explorer: get class: search: %v", err)
	}

	if params.Group != nil {
		grouped, err := grouper.New(e.logger).Group(res, params.Group.Strategy, params.Group.Force)
		if err != nil {
			return nil, fmt.Errorf("grouper: %v", err)
		}

		res = grouped
	}

	if params.UnderscoreProperties.NearestNeighbors {
		withNN, err := e.nnExtender.Multi(ctx, res, nil)
		if err != nil {
			return nil, fmt.Errorf("extend with nearest neighbors: %v", err)
		}

		res = withNN
	}

	if params.UnderscoreProperties.FeatureProjection != nil {
		withFP, err := e.projector.Reduce(res, params.UnderscoreProperties.FeatureProjection)
		if err != nil {
			return nil, fmt.Errorf("extend with feature projections: %v", err)
		}

		res = withFP
	}

	if params.UnderscoreProperties.SemanticPath != nil {
		return nil, fmt.Errorf("semantic path not possible on 'list' queries, only on 'explore' queries")
	}

	return e.searchResultsToGetResponse(ctx, res, 0, nil)
}

func (e *Explorer) searchResultsToGetResponse(ctx context.Context,
	input []search.Result, requiredCertainty float64,
	searchVector []float32) ([]interface{}, error) {
	output := make([]interface{}, 0, len(input))

	for _, res := range input {
		if res.UnderscoreProperties != nil {
			if res.UnderscoreProperties.Classification != nil {
				res.Schema.(map[string]interface{})["_classification"] = res.UnderscoreProperties.Classification
			}

			if res.UnderscoreProperties.Interpretation != nil {
				res.Schema.(map[string]interface{})["_interpretation"] = res.UnderscoreProperties.Interpretation
			}

			if res.UnderscoreProperties.NearestNeighbors != nil {
				res.Schema.(map[string]interface{})["_nearestNeighbors"] = res.UnderscoreProperties.NearestNeighbors
			}

			if res.UnderscoreProperties.FeatureProjection != nil {
				res.Schema.(map[string]interface{})["_featureProjection"] = res.UnderscoreProperties.FeatureProjection
			}

			if res.UnderscoreProperties.SemanticPath != nil {
				res.Schema.(map[string]interface{})["_semanticPath"] = res.UnderscoreProperties.SemanticPath
			}
		}

		if searchVector != nil {
			dist, err := e.distancer(res.Vector, searchVector)
			if err != nil {
				return nil, fmt.Errorf("explorer: calculate distance: %v", err)
			}

			if 1-(dist) < float32(requiredCertainty) {
				continue
			}
		}

		output = append(output, res.Schema)
	}

	return output, nil
}

func (e *Explorer) Concepts(ctx context.Context,
	params ExploreParams) ([]search.Result, error) {
	if params.Network {
		return nil, fmt.Errorf("explorer: network exploration currently not supported")
	}

	vector, err := e.vectorFromExploreParams(ctx, &params)
	if err != nil {
		return nil, fmt.Errorf("vectorize params: %v", err)
	}

	res, err := e.search.VectorSearch(ctx, vector, params.Limit, nil)
	if err != nil {
		return nil, fmt.Errorf("vector search: %v", err)
	}

	results := []search.Result{}
	for _, item := range res {
		item.Beacon = beacon(item)
		dist, err := e.distancer(vector, item.Vector)
		if err != nil {
			return nil, fmt.Errorf("res %s: %v", item.Beacon, err)
		}
		item.Certainty = 1 - dist
		if item.Certainty >= float32(params.Certainty) {
			results = append(results, item)
		}
	}

	return results, nil
}

func (e *Explorer) vectorFromExploreParams(ctx context.Context,
	params *ExploreParams) ([]float32, error) {

	vector, err := e.vectorizer.Corpi(ctx, params.Values)
	if err != nil {
		return nil, fmt.Errorf("vectorize keywords: %v", err)
	}

	if params.MoveTo.Force > 0 && len(params.MoveTo.Values) > 0 {
		moveToVector, err := e.vectorizer.Corpi(ctx, params.MoveTo.Values)
		if err != nil {
			return nil, fmt.Errorf("vectorize move to: %v", err)
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
			return nil, fmt.Errorf("vectorize move away from: %v", err)
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
	return fmt.Sprintf("weaviate://localhost/%ss/%s", res.Kind.Name(), res.ID)

}
