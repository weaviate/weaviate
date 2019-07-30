//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// Explorer is a helper construct to perform vector-based searches. It does not
// contain monitoring or authorization checks. It should thus never be directly
// used by an API, but through a Traverser.
type Explorer struct {
	search     vectorClassSearch
	vectorizer CorpiVectorizer
	repo       explorerRepo
}

type vectorClassSearch interface {
	VectorClassSearch(ctx context.Context, kind kind.Kind,
		className string, vector []float32, limit int,
		filters *filters.LocalFilter) ([]VectorSearchResult, error)
	VectorSearch(ctx context.Context, index string,
		vector []float32, limit int) ([]VectorSearchResult, error)
}

type explorerRepo interface {
	GetThing(context.Context, strfmt.UUID, *models.Thing) error
	GetAction(context.Context, strfmt.UUID, *models.Action) error
}

// NewExplorer with search and connector repo
func NewExplorer(search vectorClassSearch, vectorizer CorpiVectorizer,
	repo explorerRepo) *Explorer {
	return &Explorer{search, vectorizer, repo}
}

// GetClass from search and connector repo
func (e *Explorer) GetClass(ctx context.Context,
	params *LocalGetParams) ([]interface{}, error) {
	if params.Filters != nil {
		msg := "combining 'explore' and 'where' parameters not possible yet - coming soon!"
		// TODO: enable in gh-911
		return nil, fmt.Errorf(msg)
	}

	searchVector, err := e.vectorFromExploreParams(ctx, params.Explore)
	if err != nil {
		return nil, fmt.Errorf("explorer: get class: vectorize params: %v", err)
	}

	// TODO: gh-881 default to config limit
	limit := 100
	if params.Pagination != nil {
		limit = params.Pagination.Limit
	}

	res, err := e.search.VectorClassSearch(ctx, params.Kind, params.ClassName,
		searchVector, limit, params.Filters)
	if err != nil {
		return nil, fmt.Errorf("explorer: get class: vector search: %v", err)
	}

	return e.searchResultsToGetResponse(ctx, res, params.Explore.Certainty, searchVector)
}

func (e *Explorer) searchResultsToGetResponse(ctx context.Context,
	input []VectorSearchResult, requiredCertainty float64,
	searchVector []float32) ([]interface{}, error) {
	output := make([]interface{}, 0, len(input))

	for _, res := range input {
		dist, err := e.vectorizer.NormalizedDistance(res.Vector, searchVector)
		if err != nil {
			return nil, fmt.Errorf("explorer: calculate distance: %v", err)
		}

		if 1-(dist) < float32(requiredCertainty) {
			continue
		}

		switch res.Kind {
		case kind.Thing:
			var thing models.Thing
			e.repo.GetThing(ctx, res.ID, &thing)
			output = append(output, thing.Schema)
		case kind.Action:
			var action models.Action
			e.repo.GetAction(ctx, res.ID, &action)
			output = append(output, action.Schema)
		default:
			return nil, fmt.Errorf("impossible kind %v", res.Kind)
		}
	}

	return output, nil
}

func (e *Explorer) Concepts(ctx context.Context,
	params ExploreParams) ([]VectorSearchResult, error) {
	if params.Network {
		return nil, fmt.Errorf("explorer: network exploration currently not supported")
	}

	vector, err := e.vectorFromExploreParams(ctx, &params)
	if err != nil {
		return nil, fmt.Errorf("vectorize params: %v", err)
	}

	res, err := e.search.VectorSearch(ctx, "*", vector, params.Limit)
	if err != nil {
		return nil, fmt.Errorf("vector search: %v", err)
	}

	results := []VectorSearchResult{}
	for _, item := range res {
		item.Beacon = beacon(item)
		dist, err := e.vectorizer.NormalizedDistance(vector, item.Vector)
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

func beacon(res VectorSearchResult) string {
	return fmt.Sprintf("weaviate://localhost/%ss/%s", res.Kind.Name(), res.ID)

}
